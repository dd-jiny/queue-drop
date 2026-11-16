# 고도화 백로그

각 단계가 끝날 때 성능·운영 관점에서 개선할 수 있는 항목을 쌓는 문서다. 기존 항목은 고치지 않고 아래에 추가한다. 중복되는 항목은 "관련: E-xx" 로 연결한다.

---

## phase-0 (공통 기반) 분석 — 2026-11-16

분석 범위: `git log dev..HEAD` (`chore/phase-0-foundation`, 커밋 11개). 비즈니스 로직이 없는 단계라서, 4·6단계에서 이 설정을 그대로 쓸 때 생길 수 있는 문제와 8단계 전에 준비할 것을 중심으로 본다.
기존 테스트 실행 결과(`*/build/test-results`)로 측정한 suite 시간: Postgres 컨텍스트 2.9~3.3초, Redis+Kafka 컨텍스트 5.7~5.9초, ArchUnit 0.4초 미만.

### E-01 DLT 파티션 1개와 `DeadLetterPublishingRecoverer` 기본 파티션 규칙이 맞지 않음
- **처리**: ✅ 0단계에서 반영 (2026-11-16) — 개선안 2 채택: `entry.created.DLT`·`coupon.issued.DLT` 파티션 6. 4·6단계 DLT 통합 테스트는 원본 토픽을 파티션 6으로 만들어 검증한다.
- **단계/위치**: phase-0, `infra/kafka/create-topics.sh:25-28`. 사용처는 phase-4 `KafkaConsumerConfig`(`docs/plan/phases/phase-4-entry-apply.md:296-302`)와 phase-6(`phase-6-coupon.md:478`)
- **현상**: `entry.created`·`coupon.issued` 는 파티션 6개, 각 DLT 는 파티션 1개다. 4·6단계 계획은 `new DeadLetterPublishingRecoverer(template)` 를 기본 생성자로 쓴다.
- **문제/기회**: Spring Kafka 의 기본 destination resolver 는 `TopicPartition(원본토픽 + ".DLT", 원본 레코드의 partition)` 으로 보낸다. 원본 파티션이 1~5번이면 DLT 에 그 파티션이 없어서 producer 가 메타데이터를 `max.block.ms`(기본 60초) 동안 기다리다 `TimeoutException` 으로 실패한다(관련: E-02). recoverer 가 실패하면 레코드는 DLT 로 가지 못하고 다시 시도되어 **파티션 6개 중 5개에서 DLT 처리가 안 된다**. DB 장애처럼 DLT 가 꼭 필요한 상황에서 그 파티션의 컨슈머가 멈춘다.
- **근거**: Spring Kafka 레퍼런스 "Publishing Dead-letter Records": 기본 resolver 를 쓰면 DLT 파티션 수가 원본 토픽 이상이어야 한다. 테스트 환경은 Testcontainers Kafka 의 자동 토픽 생성(파티션 1)을 쓰므로(`phase-0-foundation.md:563`) 원본도 파티션 1이라 **테스트로는 드러나지 않는다**. 브로커에서 직접 재현하지는 않았다(문서 기반).
- **개선안**:
  1. resolver 를 지정해 파티션을 버린다: `new DeadLetterPublishingRecoverer(template, (r, e) -> new TopicPartition(r.topic() + ".DLT", -1))`. 파티션 수를 그대로 두고 재처리 순서는 신경 쓰지 않는다. DLT 는 저빈도라 이 방식이 맞다. 4·6단계 코드 한 줄을 고치면 된다.
  2. DLT 파티션을 원본과 같은 6개로 맞춘다. resolver 는 기본값 그대로 쓰고 파티션별 순서도 유지된다. 대신 DLT 재처리 도구가 파티션 6개를 읽어야 한다.
  - 어느 쪽이든 **DLT 경로 통합 테스트에서는 원본 토픽을 실제 파티션 수(6)로 `AdminClient` 로 만들어야** 회귀를 잡을 수 있다.
- **우선순위**: 높음 — 장애 복구 경로(NFR-03)가 설계대로 동작하지 않는다. 고치는 비용은 한 줄 또는 스크립트 숫자 하나다. 4단계 구현 전에 정해야 한다.
- **영향 범위**: `infra/kafka/create-topics.sh` 또는 `phase-4-entry-apply.md`·`phase-6-coupon.md` 의 `KafkaConsumerConfig`, `infrastructure.md` 5장 토픽 표, DLT 통합 테스트

### E-02 `max.block.ms` 를 정하지 않아 `send()` 호출 자체가 최대 60초 블로킹될 수 있음
- **처리**: ✅ 0단계에서 반영 (2026-11-16) — 공통 producer 설정에 `max.block.ms: 1000`. `send()` 가 바로 던지는 예외를 보상 경로에 넣는 것은 4·6단계 발행 어댑터 구현 시 적용한다.
- **단계/위치**: phase-0, `entry-service/src/main/resources/application.yml:26-33` (6개 서비스 공통 producer 블록)
- **현상**: `request.timeout.ms=1000`, `delivery.timeout.ms=3000` 으로 "send 결과는 3초 안에 확정된다"고 본다. 호출 측은 `send().get(3.5s)` 를 쓴다.
- **문제/기회**: `delivery.timeout.ms` 는 `send()` 가 반환된 **뒤**의 시간만 제한한다. `KafkaProducer.send()` 는 메타데이터를 기다리거나 `buffer.memory` 가 가득 찰 때 반환 전에 **호출 스레드를 `max.block.ms`(기본 60,000ms) 동안 막는다**. future 를 받기 전이므로 `get(3.5s)` 도 소용없다. 브로커 재시작, 리더 변경, 토픽이 아직 없을 때(kafka-init 이 끝나기 전) 응모 요청 스레드가 60초 동안 묶이고, D2 의 "3.5초 안에 보상" 전제가 깨진다.
- **근거**: Kafka producer 설정 문서(`max.block.ms`: send()/partitionsFor() 블로킹 상한). 계산(추정): Tomcat 기본 스레드 200 / 60초 = 초당 약 3.3건만 처리할 수 있다. 피크 때 사실상 서비스가 멈춘다.
- **개선안**:
  1. 공통 producer 설정에 `max.block.ms: 1000` 정도를 넣는다. 최악 응답 시간은 send 블로킹 1초 + delivery 3초 + 여유로 계산하고, `get` 대기 시간(3.5초)과 응답 시간 목표를 함께 다시 정한다. send 가 동기로 `KafkaException`/`TimeoutException` 을 던지는 경로도 보상 대상에 넣어야 한다(정합성: 기록되지 않은 것이 확실하므로 보상해도 안전하다).
  2. 앱 시작 시 producer 메타데이터를 미리 받아 둔다(`partitionsFor` 호출 또는 health indicator). 처음 보내는 요청의 블로킹은 사라지지만 운영 중 리더 변경에는 효과가 없으므로 1과 함께 쓴다.
- **우선순위**: 높음 — 장애 상황에서 응답 경로가 무너지는 지점이다. 설정 한 줄로 고칠 수 있다.
- **영향 범위**: 6개 서비스 `application.yml`, `phase-0-foundation.md` 3.9, `phase-4-entry-apply.md`·`phase-6-coupon.md` producer 설정 표와 보상 경로, `concurrency-consistency.md` D2

### E-03 queue·ranking·coupon 의 Redis 명령 타임아웃이 기본값(60초)
- **단계/위치**: phase-0, `queue-service/src/main/resources/application.yml:8-10`, `ranking-service/.../application.yml:8-10`, `coupon-service/.../application.yml:20-22`, `entry-service/.../application.yml:20-22`
- **현상**: `spring.data.redis.timeout` 을 설정하지 않았다. 4단계 계획만 entry 에 `timeout: 1s` 를 넣는다(`phase-4-entry-apply.md:239`).
- **문제/기회**: Lettuce 는 명령 타임아웃 기본값이 60초다. Redis 가 AOF fsync 지연(E-06)이나 재시작으로 멈추면 대기열 진입·쿠폰 발급 요청이 60초 동안 스레드를 잡는다. 하나의 Redis 를 모든 서비스가 같이 쓰므로(`infrastructure.md` 6장) 한 곳이 막히면 전 서비스로 번진다.
- **근거**: Spring Boot `RedisProperties.timeout` 미설정 시 Lettuce `RedisURI` 기본 60초(문서 기반, 측정 안 함).
- **개선안**:
  1. 0단계 공통 설정에 `spring.data.redis.timeout: 1s`(조회성 ranking 은 500ms 도 가능)를 넣는다. 타임아웃이 나면 판정 결과를 알 수 없으므로 503 으로 응답한다. 이때 Lua 가 이미 실행됐을 수 있다는 점(애매한 실패)을 각 단계 보상 정책에 적어야 한다.
  2. 서비스별 단계에서 각자 넣는다(지금 계획). 빠뜨릴 위험이 있다.
- **우선순위**: 중간 — 정상 상황에서는 영향이 없다. 장애 전파를 막는 기본값이므로 비용 대비 효과가 크다.
- **영향 범위**: 4개 서비스 `application.yml`, `phase-0-foundation.md` 3.9, phase-3·6·7 문서

### E-04 동기 발행 구간이 Tomcat 스레드를 잡아 Kafka 지연이 처리량 상한이 됨
- **단계/위치**: phase-0 공통 producer 설정, phase-4/6 `send().get(3.5s)`
- **현상**: 응모·쿠폰 요청 스레드가 브로커 ack(acks=all)까지 기다린다. `linger.ms=0`, `max.in.flight=5`.
- **문제/기회**: 처리량 상한은 대략 스레드 수 / 발행 대기 시간이다(Little's law). 정상(ack 5~10ms 추정)일 때는 200 / 0.01초 = 초당 2만 건으로 여유가 있다. 브로커가 느려져 1초가 걸리면 초당 200건, 타임아웃(3.5초)이 나면 초당 57건으로 떨어진다. 입장 배치(초당 100명 기본)와 비슷한 수준이라 대기가 쌓이기 시작한다.
- **근거**: 계산(추정). 실제 ack 지연은 8단계에서 측정한다(E-13).
- **개선안**:
  1. 현재 구조를 유지하고 8단계에서 `tomcat.threads.busy`·발행 지연 p99 를 측정해 상한을 문서화한다. **현재 규모(S1: 사용자 3,000 / 정원 1,000)에서는 정상 경로 변경이 필요 없다**.
  2. Java 21 가상 스레드(`spring.threads.virtual.enabled=true`)로 스레드 수 제한을 없앤다. 대신 Hikari 풀·Redis 연결이 새 병목이 되고, 장애 시 대기 요청이 무한히 쌓이므로 동시 요청 제한(세마포어/게이트웨이 rate limit)이 필요하다.
  3. `linger.ms` 1~5ms 로 배치를 키운다. 피크 처리량은 늘지만 정상 지연이 그만큼 늘어난다.
- **우선순위**: 중간 — 측정 항목으로 두고, 구조 변경은 8단계 결과를 보고 정한다.
- **영향 범위**: `phase-8-load-test.md` 3.6 시나리오, `entry-apply.md` 리스크 절

### E-05 리스너 concurrency 값이 문서마다 다름 (1 / 3 / 파티션 6)
- **단계/위치**: `infrastructure.md` 4장 튜닝 표(`SPRING_KAFKA_LISTENER_CONCURRENCY` 기본 1), `phase-4-entry-apply.md:264`(`concurrency: 3`, 주석 "0단계 토픽 파티션 수와 맞춘다"), `create-topics.sh:22-23`(파티션 6)
- **현상**: 0단계 공통 설정에는 concurrency 가 없다. 4단계 주석은 파티션 수에 맞춘다고 하지만 값이 다르다.
- **문제/기회**: 기능 문제는 없다(concurrency ≤ 파티션). 다만 레코드당 저장이 3ms(추정)면 컨슈머 1개는 초당 약 330건을 처리하므로, 정원 1,000 은 약 3초면 lag 이 해소된다. **현재 규모에서는 concurrency 1 로도 S1 목표(lag 30초 내 해소)를 채운다(추정)**. 혼란은 8단계 튜닝 때 어떤 값이 실제로 적용됐는지 헷갈리는 데서 온다.
- **근거**: 계산(추정).
- **개선안**:
  1. 기본값 하나를 정하고(예: 3) 4단계 주석을 "파티션 6 이하"로 고친다.
  2. 서비스별 환경변수로만 받고 문서에는 범위만 적는다.
  - 배치 리스너·벌크 insert 는 현재 규모에서 불필요하다.
- **우선순위**: 낮음 — 문서 정리 수준이다.
- **영향 범위**: `infrastructure.md` 4장, `phase-4-entry-apply.md`, `phase-6-coupon.md`

### E-06 Redis AOF `everysec` 이 보장하는 범위와 재시작 후 복구 절차가 문서에 없음
- **단계/위치**: phase-0, `docker-compose.yml:22`
- **현상**: `appendonly yes`, `appendfsync everysec`. 설계(D1)는 "재시작 시 판정 유실 → 오버 응모 방지"를 근거로 든다.
- **문제/기회**:
  - Redis 는 응답을 보내기 전에 AOF 버퍼를 `write()` 로 OS 에 넘기므로, **Redis 프로세스나 컨테이너만 죽을 때는 응답한 쓰기가 유실되지 않는다**. 유실은 OS/Docker VM 이 죽을 때만 생기고, 최대 약 1~2초 분량이다.
  - 그 경우 `DECR remaining`·`SADD applicants` 는 사라지고 Kafka `entry.created` 와 DB 에는 남는다. 잔여 정원이 실제보다 커져 **오버 응모가 생길 수 있고**, 같은 사용자가 다시 응모할 수 있다(DB 유니크 제약이 있으면 DB 단에서는 막힌다). 규모(추정): 응모 초당 500건 × 1~2초 = 최대 500~1,000건 판정 유실.
  - 지연 쪽: fsync 는 백그라운드 스레드에서 하지만, 이전 fsync 가 2초 넘게 걸리면 메인 스레드가 쓰기를 미룬다(`aof_delayed_fsync`). macOS Docker Desktop 볼륨은 fsync 가 느릴 수 있어 8단계에서 확인해야 한다.
- **근거**: Redis persistence 문서(everysec 의 유실 범위, `aof_delayed_fsync`). 측정은 안 했다.
- **개선안**:
  1. 지금 설정을 유지하고 "Redis 재시작 후 복구 절차"를 문서로 남긴다. 진행 중인 드로우를 응모 중단 상태로 두고, DB(또는 `entry.created`)로 `remaining = 정원 - 응모 수`, `applicants` 를 다시 만든 뒤 재개한다. 정합성을 해치지 않는다. 비용은 운영 절차와 관리 API 하나다.
  2. `appendfsync always` 로 바꾼다. 유실은 0에 가까워지지만 쓰기마다 fsync 를 하므로 Lua 판정 지연이 디스크 지연만큼 늘어난다(Docker Desktop 에서 ms 단위 추정). 8단계에서 p99 를 비교한 뒤에만 고려한다.
- **우선순위**: 중간 — 로컬 과제 범위에서 VM 이 죽을 확률은 낮다. 그래도 설계가 "AOF 로 오버 응모 방지"라고 단정하므로 보장 범위를 바로잡고 복구 절차를 두는 효과가 크다.
- **영향 범위**: `concurrency-consistency.md` D1·D6, `infrastructure.md` 5·6장, phase-4(재구성 API 여부)

### E-07 단일 Redis 공유·단일 브로커(RF=1)에서 `acks=all` 의 실제 의미
- **단계/위치**: phase-0, `docker-compose.yml:19-50`, `create-topics.sh:11` (`--replication-factor 1`)
- **현상**: 브로커 1대, RF=1, `min.insync.replicas` 기본값 1. Redis 1대를 queue·entry·coupon·ranking 이 같이 쓴다.
- **문제/기회**:
  - RF=1 에서 `acks=all` 은 사실상 `acks=1` 이다. 브로커는 메시지마다 fsync 하지 않으므로 호스트가 죽으면 ack 받은 레코드도 잃을 수 있다. "성공 응답 = 이벤트 기록" 은 이 구성에서 "브로커 프로세스 장애까지"만 보장한다.
  - Redis 공유: 랭킹 조회(ZREVRANGE)와 선착순 Lua 가 같은 싱글 스레드를 쓴다. Lua 는 짧지만(키 5개, O(1)) 랭킹 조회가 몰리면 꼬리 지연이 생길 수 있다.
- **근거**: 구성 분석. 측정은 8단계에서 한다.
- **개선안**:
  1. **현재 규모에서는 변경이 불필요하다**. 문서에 보장 범위만 정확히 적는다. 8단계 S1 실행 중 Redis `latency` 와 `SLOWLOG` 를 수집한다.
  2. 랭킹 조회가 판정 p99 에 영향을 주는 것이 측정되면 ranking 전용 Redis 인스턴스로 분리한다(compose 서비스 하나 추가, ranking 의 `REDIS_HOST` 만 바꿈).
  3. 가용성(3 브로커 RF=3 + `min.insync.replicas=2`, Redis Sentinel)은 과제 범위 밖이다.
- **우선순위**: 낮음 — 문서 정확성 수준이다. 분리는 측정 결과가 있을 때만 한다.
- **영향 범위**: `infrastructure.md` 6장, `concurrency-consistency.md` D2

### E-08 Testcontainers 컨테이너가 Spring 컨텍스트마다 새로 뜸 (빌드 시간·메모리 확장성)
- **단계/위치**: phase-0, `common/src/testFixtures/java/com/queuedrop/common/test/container/*ContainerConfig.java`
- **현상**: 컨테이너를 `@TestConfiguration` 안에 `@Bean @ServiceConnection` 으로 정의했다. 컨테이너 수명이 **ApplicationContext** 에 묶인다. reuse 는 쓰지 않고, Gradle `--parallel`·`maxParallelForks` 도 설정하지 않았다.
- **문제/기회**:
  - 지금은 서비스당 컨텍스트가 1개라 수용할 만하다(측정: Postgres 3초, Redis+Kafka 6초, 6개 서비스를 순차로 돌리면 컨테이너 기동만 약 25초).
  - 4단계 이후 `@MockitoBean`, `@TestPropertySource`, 다른 `@Import` 조합이 생기면 **컨텍스트 캐시 키가 달라질 때마다 Postgres·Redis·Kafka 를 새로 띄운다**. 컨텍스트 캐시는 기본 32개까지 살아 있으므로 컨테이너도 함께 쌓여 Docker 메모리가 부족해질 수 있다. 추정: entry 에 컨텍스트 변형 5개 × 3개 컨테이너 × 약 4초 ≈ 60초, 서비스 4개면 수 분.
- **근거**: suite 시간은 측정값(`*/build/test-results`), 확장 예측은 추정.
- **개선안**:
  1. 컨테이너를 `static` 싱글턴으로 바꾼다(추상 `IntegrationTestSupport` 의 `static final` 필드 + `@ServiceConnection`, 또는 `@ImportTestcontainers`). JVM 하나 안에서 모든 컨텍스트가 같은 컨테이너를 쓴다. 테스트끼리 데이터가 섞이지 않도록 고유 ID·키 접두사나 정리 로직을 써야 한다.
  2. 로컬에서만 `testcontainers.reuse.enable=true` + `.withReuse(true)` 를 켠다. JVM 과 빌드가 바뀌어도 컨테이너를 재사용하므로 반복 실행이 가장 빠르다. 대신 이전 실행의 데이터가 남고, CI 에서는 효과가 없다.
  3. Gradle `org.gradle.parallel=true` 로 서비스 모듈을 병렬 실행한다. 벽시계 시간은 줄지만 동시에 뜨는 컨테이너 수가 늘어 메모리 사용이 커진다.
  - 1을 기본으로 하고 2는 개인 선택으로 두는 것이 무난하다. 4단계 `IntegrationTestSupport` 를 만들 때 함께 정하는 것이 가장 싸다.
- **우선순위**: 중간 — 지금은 문제가 없지만, 4단계에서 구조를 정하지 않으면 나중에 테스트 전체를 고쳐야 한다.
- **영향 범위**: `common` testFixtures, `phase-0-foundation.md` 3.7·리스크, phase-4·6 `IntegrationTestSupport`

### E-09 이미지 pull·docker credential helper 지연이 테스트 시간과 실패에 영향
- **단계/위치**: phase-0, 테스트 컨테이너 이미지(`postgres:17`, `redis:7.4`, `apache/kafka:4.0.0`)
- **현상**: 이미지가 로컬에 없거나 credential helper(`docker-credential-desktop`)가 응답하지 않으면 Testcontainers 가 인증 조회에서 오래 기다린다. 30초 타임아웃 사례가 있었다.
- **문제/기회**: 공개 이미지만 쓰는데도 credential 조회 때문에 첫 테스트가 30초 늦어지거나 실패한다. 원인이 코드와 무관해서 진단에 시간이 많이 든다.
- **근거**: 보고된 사례(30초). 재현하지 않았다.
- **개선안**:
  1. 문서에 "테스트 전 `docker compose pull` 로 같은 이미지를 미리 받아 둔다"를 적는다. compose 와 테스트가 같은 태그를 쓰므로 바로 효과가 있다.
  2. 로컬 `~/.docker/config.json` 의 `credsStore` 문제를 트러블슈팅 절에 적는다. 개인 환경 설정이므로 강제하지 않는다.
- **우선순위**: 낮음 — 개발 환경 이슈다. 문서 한 줄이면 된다.
- **영향 범위**: `README` 또는 `phase-0-foundation.md` 리스크 절

### E-10 발행 장애 시 5xx 마다 error 로그와 스택 트레이스가 남아 로그가 폭증함
- **단계/위치**: phase-0, `entry-service/src/main/java/com/queuedrop/entry/presentation/api/GlobalExceptionHandler.java:31-32, 101` (6개 서비스 동일)
- **현상**: status ≥ 500 인 `ApplicationException` 은 `log.error(..., e)` 로 cause 체인 전체(Kafka `TimeoutException` 포함)를 남긴다. 4단계의 발행 실패 503 도 여기에 해당한다.
- **문제/기회**: 브로커 장애가 피크와 겹치면 실패하는 요청마다 스택 트레이스(수십~백여 줄, 약 5~10KB 추정)가 찍힌다. 초당 500건이면 초당 2.5~5MB 이고, Kafka 클라이언트 자체 WARN 로그도 더해진다. 기본 콘솔 appender 는 동기라서 **로그 출력이 요청 스레드를 잡아 응답 경로를 더 느리게 만든다**. 원인 하나에 같은 스택이 수만 번 찍혀 오히려 진단이 어려워진다.
- **근거**: 계산(추정). 실제 크기는 4단계 발행 실패 테스트에서 확인할 수 있다.
- **개선안**:
  1. "예상된 가용성 실패"(503, 의존 시스템 장애)와 "버그"(500)를 나눈다. 503 은 `warn` + 코드·cause 클래스·메시지 한 줄만 남기고, 장애 사실은 카운터 메트릭(`entry_publish_failed`)으로 본다. 500 은 지금처럼 스택을 남긴다. `ErrorCode` 에 로그 레벨·스택 여부 속성을 두는 방법도 있다.
  2. 스택 트레이스 샘플링: 같은 코드는 N초에 한 번만 스택을 남기고 나머지는 한 줄만 남긴다. 정보는 유지되지만 구현이 조금 복잡하다.
  3. Logback `AsyncAppender`(`neverBlock=true`)로 요청 스레드와 분리한다. 폭증 시 로그가 버려질 수 있다.
  - 1+3 조합을 권장한다. 정합성에는 영향이 없다.
- **우선순위**: 중간 — 4단계에서 503 경로가 생기면 바로 해당된다. 8단계 장애 주입 시나리오의 결과를 왜곡할 수 있다.
- **영향 범위**: 6개 `GlobalExceptionHandler`, `common.error.ErrorCode`(속성 추가 시), `layered-architecture.md` 4.5 로그 정책, `phase-0-foundation.md` 3.8 로그 표, logback 설정

### E-11 `GlobalExceptionHandler` 6개 복제본과 entry 에만 있는 테스트
- **단계/위치**: phase-0, `{user,product,queue,entry,coupon,ranking}-service/.../presentation/api/GlobalExceptionHandler.java` (패키지 선언을 빼면 6개 파일 내용이 같다. md5 로 확인), 테스트는 `entry-service/.../GlobalExceptionHandlerTest.java` 하나뿐
- **현상**: 119줄짜리 같은 클래스가 6개 있다. 410줄짜리 계약 테스트는 entry 에만 있다.
- **문제/기회**: E-10 같은 정책을 바꾸면 6군데를 고쳐야 하고, 하나를 빠뜨려도 테스트가 잡지 못한다(5개 서비스는 테스트가 없다). 서비스마다 에러 응답 계약이 조금씩 달라지는 위험이 있다. 성능 문제는 아니다.
- **근거**: 코드 확인.
- **개선안**:
  1. `common` 에 web 의존을 두는 별도 모듈(예: `common-web`)을 만들고 `AbstractGlobalExceptionHandler` 를 둔다. 각 서비스 presentation 은 `@RestControllerAdvice class GlobalExceptionHandler extends ...` 만 남긴다. 중복은 없어지지만 common 에 Spring MVC 의존이 들어오고, 계층 규칙(R3: application 의 `org.springframework.web` 금지)과 충돌하지 않게 패키지를 정리해야 한다.
  2. 복제는 유지하고 계약 테스트를 testFixtures 의 추상 테스트(`AbstractGlobalExceptionHandlerContractTest`)로 올려 6개 서비스가 상속하게 한다. 구조는 그대로 두고 어긋남만 막는다.
  3. ArchUnit/텍스트 비교 테스트로 6개 파일이 같은지 검사한다. 가장 싸지만 서비스별 확장을 막는다.
- **우선순위**: 낮음 — 과제 범위에서는 2가 비용 대비 효과가 가장 좋다. E-10 을 반영할 때 함께 처리한다.
- **영향 범위**: 6개 서비스 presentation, `common` testFixtures, `layered-architecture.md` 4.5

### E-12 DLT 보존 기간이 기본값(7일)이고 DLT 적재를 관측하는 수단이 없음
- **단계/위치**: phase-0, `infra/kafka/create-topics.sh:26-28` (관련: E-01)
- **현상**: DLT 3종은 `retention.ms` 를 지정하지 않아 브로커 기본값 7일을 쓴다. DLT 에 레코드가 쌓여도 알림이나 메트릭이 없고, 5단계 건수 비교(D6)에서야 드러난다.
- **문제/기회**: DLT 는 사람이 재처리하는 큐다. 추첨이 끝나고 한참 뒤에 불일치를 발견하면 DLT 레코드가 이미 사라졌을 수 있다. 원본 `entry.created` 는 30일 보존이므로 원본 오프셋으로 다시 읽을 수는 있지만, 어느 레코드가 실패했는지 알 수 없다.
- **근거**: 구성 분석.
- **개선안**:
  1. DLT `retention.ms` 를 원본과 같은 30일 이상으로 늘린다. 용량은 무시해도 될 수준이다(추정: 실패 레코드 수천 건 × 1KB).
  2. DLT 토픽의 end offset 을 메트릭/로그로 노출한다(Kafka UI 로 수동 확인하거나 8단계 Prometheus 에서 kafka-exporter 사용).
- **우선순위**: 낮음 — 스크립트 옵션 하나로 끝난다. E-01 을 고칠 때 함께 처리한다.
- **영향 범위**: `create-topics.sh`, `infrastructure.md` 5장, `phase-8-load-test.md` 관찰 메트릭

### E-13 8단계 부하 테스트 전에 확보해 둘 측정 지표
- **단계/위치**: phase-0 공통 설정 → phase-8 `docs/plan/phases/phase-8-load-test.md` 3.2 "관찰할 메트릭"
- **현상**: 8단계 계획에 Actuator/Prometheus 노출과 커스텀 카운터가 있다. 0단계에서 정한 타임아웃과 내구성 설정을 검증할 지표는 따로 정리되지 않았다.
- **문제/기회**: E-02·E-04·E-06·E-10 의 판단 근거는 모두 측정값이다. 8단계에서 시나리오를 돌린 뒤에 메트릭을 추가하면 다시 돌려야 한다.
- **근거**: 위 항목들의 추정 계산을 검증할 대상.
- **개선안** (수집 목록. Boot 는 Kafka producer/consumer 메트릭을 Micrometer 에 자동으로 연결하므로 대부분 노출 설정만 하면 된다):
  - **Kafka producer**: `kafka.producer.request.latency.avg/max`, `record.retry.rate`, `record.error.rate`, `bufferpool.wait.time`, `buffer.available.bytes`, 그리고 앱에서 직접 재는 `send().get()` 대기 Timer(p50/p95/p99). 보상 횟수 카운터.
  - **Kafka consumer**: `records.lag.max`(파티션별), 레코드당 처리 시간 Timer, DLT 전송 수.
  - **응답 경로**: `tomcat.threads.busy`/`max`(`server.tomcat.mbeanregistry.enabled=true` 필요), `http.server.requests` p99(상태 코드별), `hikaricp.connections.pending`·`acquire`.
  - **Redis**: Lettuce 명령 지연(`lettuce.command.completion`, 활성화 필요), 서버 `INFO persistence` 의 `aof_delayed_fsync`·`aof_last_write_status`, `INFO stats` 의 `instantaneous_ops_per_sec`, `SLOWLOG GET`, `LATENCY LATEST`.
  - **로그**: 시나리오별 로그 줄 수/크기(E-10 검증).
  - **장애 주입 1회**: S1 도중 `docker compose pause kafka` 를 5초 하고 응답 p99·보상 수·로그 양·복구 시간을 기록해 E-02·E-10 을 검증한다.
  - 선택지: (a) 위 지표를 Prometheus+Grafana 대시보드로 모은다(8단계 계획의 선택 항목을 필수로 올린다), (b) Actuator `/metrics` 를 k6 종료 시점에 스냅샷으로만 남긴다. b 는 싸지만 시계열(lag 해소 시간)을 볼 수 없다.
- **우선순위**: 중간 — 8단계 결과의 설득력을 정한다. 4단계 구현 중에 Timer·카운터 위치를 함께 정하면 비용이 거의 들지 않는다.
- **영향 범위**: `phase-8-load-test.md` 3.2·3.6, phase-4·6 어댑터(Timer/카운터 위치)

---

## phase-1 (인증: user-service, gateway) 분석 — 2026-11-16

분석 범위: `git diff dev...HEAD` (`feat/phase-1-auth`, 커밋 11개). 기준 문서 `phase-1-auth.md`, `infrastructure.md`.
측정 환경: 개발 머신 호스트 JVM(Java 21, 논리 코어 15). Docker 컨테이너 안에서 잰 값이 아니므로 8단계 환경에서는 더 느릴 수 있다.
- BCrypt strength 10(`spring-security-crypto` 6.5.11): `matches` 평균 51.3ms, `encode` 평균 51.2ms, 15스레드 병렬 처리량 235건/초
- JWT 검증(jjwt 0.13.0 + gson, HS256, 클레임 5개): 유효 토큰 평균 3.3µs, 서명 위조 토큰 4.7µs
- 테스트 suite 시간(`build/test-results`): `SignupIntegrationTest` 11.7초, `AdminBootstrapIntegrationTest` 3.8초, `UserRepositoryImplTest` 0.8초, `GatewayContextTest` 1.3초, 나머지 단위 테스트 0.5초 미만
- 재현 테스트: 저장소에 추가하지 않고 scratch 디렉터리에서 Gradle init script 로 테스트 소스만 붙여 1회 실행했다(E-14).

### E-14 `createAdminIfAbsent` 동시 기동 경로가 `UnexpectedRollbackException` 으로 기동을 실패시킴
- **처리**: 분석 중 작업 트리(커밋 전)에 개선안 1(`@Transactional` 제거)이 반영된 것을 확인했다. 같은 재현 테스트를 다시 돌려 2회차가 `false` 를 반환하는 것을 확인했다(2026-11-16). 저장소에 회귀 테스트는 아직 없다.
- **단계/위치**: phase-1, `user-service/.../application/service/UserService.java:81-100`, `infrastructure/persistence/UserRepositoryImpl.java:22-30`
- **현상**: `existsByEmail` 이 false 인 뒤 다른 인스턴스가 먼저 저장하면 `saveAndFlush` 에서 유니크 위반이 난다. `UserRepositoryImpl` 이 이를 `DuplicateEmailException` 으로 바꾸고 서비스가 잡아 `false` 를 반환한다. 주석은 "인스턴스 여러 대가 동시에 기동한 경우"를 처리한다고 적었다.
- **문제/기회**: `SimpleJpaRepository.saveAndFlush` 는 자체 `@Transactional` 을 가진다. 예외가 이 프록시를 지나갈 때 바깥 트랜잭션(`createAdminIfAbsent`)에 참여 중이므로 **rollback-only 로 표시된다**. 서비스는 예외를 삼키고 정상 반환하지만 커밋 시점에 `UnexpectedRollbackException` 이 나고, `AdminAccountInitializer`(ApplicationRunner)가 실패해 **두 번째 인스턴스가 기동하지 못한다**. 의도한 "false 반환"과 반대로 동작한다. 가입 경로(`signup`)는 같은 예외를 `ApplicationException` 으로 다시 던져 롤백하므로 409 가 그대로 나가고 영향이 없다.
- **근거**: 재현함. `@SpringBootTest` + Postgres 에서 `UserRepository` 를 spy 로 바꿔 두 번째 호출의 `existsByEmail` 만 false 로 만들었다. 결과: 1회차 `true`, 2회차 `UnexpectedRollbackException: Transaction silently rolled back because it has been marked as rollback-only`. 기존 T1-28/T1-29 는 `InMemoryUserRepository` 를 써서 이 경로를 잡지 못하고, T1-31 은 순차 재기동이라 `existsByEmail` 이 true 인 경로만 지난다.
- **개선안**:
  1. `createAdminIfAbsent` 에서 `@Transactional` 을 뺀다. `existsByEmail`·`saveAndFlush` 가 각자 트랜잭션으로 돌므로 위반이 나면 그 트랜잭션만 롤백되고, 예외 번역 뒤 `false` 를 반환한다. 변경이 가장 작다. 관리자 생성은 한 건만 쓰므로 트랜잭션을 나눠도 정합성에는 영향이 없다.
  2. 저장을 `INSERT ... ON CONFLICT (email) DO NOTHING` 네이티브 쿼리(영향 행 수로 생성 여부 판단)로 바꾼다. 예외가 생기지 않고 쿼리도 하나로 줄지만, 저장소 포트에 관리자 전용 메서드가 하나 늘어난다.
  3. `UserRepositoryImpl.save` 를 `@Transactional(propagation = REQUIRES_NEW)` 로 감싼다. 가입 경로까지 별도 트랜잭션이 되어 커넥션을 2개 쓰므로 권하지 않는다.
  - 어느 쪽이든 실제 DB 와 spy 로 "exists=false 인데 이미 저장됨" 경로를 검증하는 통합 테스트(T1-31 보강)를 둔다.
- **우선순위**: 중간 — 지금 compose 는 user-service 1대라 발생 확률이 낮다. 하지만 코드와 주석이 처리한다고 주장하는 장애 경로가 정반대로 동작하고, 1단계 안에서 고치는 비용이 수 줄이다. **1단계 PR 전 수정을 권장한다**.
- **영향 범위**: `UserService`, `phase-1-auth.md` 3장 `createAdminIfAbsent` 코드·T1-31, (2를 고르면) `UserRepository` 포트

### E-15 로그인에서 없는 이메일은 BCrypt 를 건너뛰어 응답 시간으로 계정 존재 여부가 드러남
- **처리**: ✅ 1단계에서 반영 (2026-11-16) — 이메일이 없으면 같은 인코더로 만든 더미 해시와 `matches` 1회 수행.
- **단계/위치**: phase-1, `UserService.java:66-75`
- **현상**: `findByEmail(...).filter(matches)` 구조라 이메일이 없으면 `matches` 를 호출하지 않는다. 응답 코드는 같다(`USER_INVALID_CREDENTIALS`).
- **문제/기회**: 있는 이메일은 약 51ms, 없는 이메일은 DB 조회 1회(수 ms)만에 응답한다. 응답 코드를 같게 만든 목적(`phase-1-auth.md:409` "계정 존재 여부 노출 방지")이 응답 시간 차이로 무너진다. 성능 문제가 아니라 설계 의도와 어긋나는 문제다.
- **근거**: BCrypt `matches` 51.3ms(측정). DB 조회 시간은 추정.
- **개선안**:
  1. 계정이 없을 때 기동 시 한 번 만든 더미 해시로 `matches` 를 호출해 시간을 맞춘다. 코드 3~4줄. 없는 이메일 요청도 BCrypt CPU 를 쓰므로 무작위 이메일 공격의 비용이 약 51ms 로 같아진다(지금은 공격자에게 더 싼 경로가 있다).
  2. 그대로 두고 문서에 "응답 코드만 같게 하고 시간 차이는 범위 밖"이라고 적는다. 과제 범위에서 허용할 수 있다.
- **우선순위**: 중간 — 비용이 작고 문서와 구현이 어긋난다. 1단계 PR 전에 1을 넣거나 2로 문서를 맞추는 것을 권한다.
- **영향 범위**: `UserService.login`(또는 `PasswordEncoder` 포트에 더미 비교 메서드), `phase-1-auth.md` 409행 부근, `UserServiceTest`

### E-16 BCrypt 가 트랜잭션 안에서 실행되어 DB 커넥션을 약 50ms 동안 잡음
- **단계/위치**: phase-1, `UserService.java:39-63`(signup: `encode`), `:66-75`(login: `matches`, `readOnly` 트랜잭션)
- **현상**: `@Transactional` 이 시작될 때 JPA 트랜잭션 매니저가 Hikari 커넥션을 가져오고, BCrypt 계산(약 51ms)이 끝나고 커밋할 때까지 쥐고 있다. 실제 SQL 은 가입 2개(exists + insert), 로그인 1개(select)이고 각각 1ms 안팎(추정)이다.
- **문제/기회**: 커넥션 점유 시간의 90% 이상이 CPU 계산이다. Hikari 기본 풀 10개 기준 처리량 상한은 10 / 0.052s ≈ 190건/초로, 측정한 CPU 상한(호스트 15코어 235건/초)보다 먼저 걸린다. Docker 에 CPU 를 4~8개 줬다면 CPU 상한(추정 60~125건/초)이 먼저 걸리므로 풀은 병목이 아니다. 즉 **현재 구조에서는 풀보다 CPU 가 상한이고, 풀 고갈은 CPU 가 많을 때만 나타난다**. 피해 범위도 user-service 안이다(다른 서비스는 DB 가 따로다).
- **근거**: BCrypt 측정값과 계산. Docker CPU 할당은 추정.
- **개선안**:
  1. 해시 계산을 트랜잭션 밖으로 뺀다. 가입은 `encode` 를 먼저 하고 짧은 트랜잭션에서 exists+insert, 로그인은 트랜잭션 없이(또는 조회만 트랜잭션으로) `findByEmail` 뒤 `matches`. 커넥션 점유가 약 1ms 로 줄어든다. 대신 중복 이메일 가입도 BCrypt 비용을 먼저 내므로 409 응답이 51ms 느려진다(exists 를 트랜잭션 전에 한 번 더 보면 피할 수 있으나 쿼리가 늘어난다).
  2. 그대로 둔다. **현재 규모(사용자 3,000~5,200, 로그인은 오픈 전에 끝냄)에서는 CPU 가 먼저 막히므로 효과가 작다**. 8단계에서 `hikaricp.connections.pending` 이 0보다 크게 나올 때만 1을 적용한다.
  - strength 를 낮추는 것(예: 8 → 약 13ms)은 보안 수준을 낮추므로 부하 테스트 프로필에서만 고려하고 기본값은 유지한다.
- **우선순위**: 낮음 — 측정 전에는 2. 고치는 비용은 작지만 지금 규모에서 체감 효과가 없다.
- **영향 범위**: `UserService`, `phase-1-auth.md` 서비스 코드, `phase-8-load-test.md` 리스크 절(관련: E-20)

### E-17 `user.registered` 의 `afterCommit` 발행이 커넥션과 요청 스레드를 잡은 채로 최대 `max.block.ms` 블로킹
- **단계/위치**: phase-1, `user-service/.../messaging/KafkaUserEventPublisher.java:28-48` (관련: E-02, E-10)
- **현상**: 커밋 후 콜백 안에서 `kafkaTemplate.send()` 를 호출한다. 응답은 기다리지 않지만, 메타데이터가 없거나 버퍼가 차면 `send()` 자체가 최대 `max.block.ms`(1초) 동안 막힌다. 실패하면 warn 로그와 스택을 남긴다.
- **문제/기회**:
  - Spring 은 `triggerAfterCommit` 을 커넥션 반환(`cleanupAfterCompletion`) **전에** 호출한다. 그래서 Kafka 장애 중에는 가입 1건이 Tomcat 스레드와 **Hikari 커넥션을 약 1초** 더 쥔다. 풀 10개면 장애 중 가입·로그인 합산 처리량이 초당 10건 안팎으로 떨어진다(추정). 로그인은 Kafka 를 쓰지 않는데도 같이 느려진다.
  - 실패마다 스택이 찍히므로 8단계 `prepare-users`(50 VU) 도중 브로커가 멈추면 E-10 과 같은 로그 폭증이 생긴다.
  - 정상 상황(메타데이터 캐시됨)에서는 `send()` 가 수십 µs 안에 반환되므로 영향이 없다.
- **근거**: Spring `AbstractPlatformTransactionManager.processCommit` 순서(문서/소스 기반). 장애 주입은 하지 않았다.
- **개선안**:
  1. `afterCommit` 대신 `afterCompletion(STATUS_COMMITTED)` 에서 보낸다. 이 콜백도 커넥션 반환 전에 실행되므로 **효과가 없다**. 고르지 않는다.
  2. 콜백에서 전용 단일 스레드 executor(유한 큐, 꽉 차면 버리고 카운트)로 넘긴다. 요청 스레드와 커넥션이 즉시 풀린다. 유실 범위는 지금과 같다(어차피 비동기 발행이다). 대신 종료 시 큐에 남은 이벤트가 사라질 수 있다.
  3. 그대로 두고 E-13 의 장애 주입에서 가입 p99 와 `hikaricp.connections.pending` 만 확인한다. 가입은 부하 측정 구간 밖이라 지금 규모에서는 이것으로 충분하다.
  - 로그는 E-10 정책을 따라 `warn` 한 줄 + 카운터(`user_registered_publish_failed`)로 줄이는 것을 같이 권한다.
- **우선순위**: 낮음 — 가입은 오픈 전에 끝나는 경로다. 3으로 두고 E-18 의 유실 대책과 함께 정한다.
- **영향 범위**: `KafkaUserEventPublisher`, `layered-architecture.md` "커밋 후 발행" 절, `concurrency-consistency.md`(커밋 후 발행 설명)

### E-18 `user.registered` 유실 시 랭킹 닉네임을 복구할 수단이 없음 (7단계 대비)
- **단계/위치**: phase-1 `KafkaUserEventPublisher.java`, phase-7 `phase-7-ranking.md:720, 789` (`rank:user:info` HSET)
- **현상**: 발행은 커밋 후 비동기이고, 실패하면 로그만 남긴다. 7단계는 닉네임이 없으면 `null` 로 표시하고, 리스크 절에 "닉네임은 복구 수단 없음"이라고 적었다.
- **문제/기회**:
  - 유실 조건: 브로커가 `delivery.timeout.ms`(3초) 넘게 응답하지 않을 때, `send()` 가 1초 안에 메타데이터를 받지 못할 때, 커밋 직후 프로세스가 죽을 때(버퍼에 있던 레코드). 그 사이 가입한 사용자는 **영구적으로 닉네임이 없다**. 예시(추정): 8단계 `prepare-users` 가 초당 수십 건씩 가입하는 중 브로커가 10초 멈추면 수백 명이 랭킹에서 `null` 로 나온다.
  - 7단계 Redis(`rank:user:info`)가 유실되고 7일(토픽 보존 기본값)이 지나도 같은 문제가 생긴다.
  - 원본 데이터(users 테이블)는 남아 있으므로 정합성 문제는 아니고 표시 문제다. HSET 은 덮어쓰기라 재발행해도 멱등이다.
- **근거**: 코드와 7단계 계획 분석. 장애 주입은 하지 않았다.
- **개선안**:
  1. user-service 에 관리자 전용 재발행 API(예: `POST /api/users/admin/republish-registered?fromId=`)를 둔다. users 를 id 순 keyset 페이징(`where id > ? order by id limit 500`, PK 인덱스 사용)으로 읽어 다시 발행한다. 비용은 API 하나이고, product 의 `drop.created` 재발행 API 와 같은 패턴이라 운영 절차가 통일된다. 8단계 `prepare-users` 뒤에 한 번 돌리면 유실 여부와 상관없이 닉네임이 맞춰진다.
  2. `user.registered` 를 compacted 토픽(`cleanup.policy=compact`, key=userId)으로 바꾼다. Redis 유실 시 처음부터 다시 읽으면 복구된다. 발행 유실 자체는 막지 못하므로 1과 함께 써야 한다.
  3. Transactional Outbox. 유실이 없어지지만 테이블·폴러가 생겨 표시용 이벤트에는 과하다. 권하지 않는다.
  4. ranking 이 닉네임이 없을 때 user-service 를 동기 조회한다. 서비스 간 동기 호출이 새로 생기고 랭킹 조회 지연이 user-service 상태에 묶이므로 권하지 않는다.
- **우선순위**: 중간 — 7단계 랭킹 화면의 완성도와 8단계 리포트에 영향을 준다. 1은 7단계 시작 전(또는 2단계 재발행 API 와 같이) 넣으면 싸다.
- **영향 범위**: user-service presentation/application(재발행 유스케이스), gateway 공개/관리자 경로, `phase-7-ranking.md` 리스크·복구 절, `infra/kafka/create-topics.sh`(2를 고르면), `phase-8-load-test.md` 준비 절차

### E-19 가입 경로의 쿼리 수와 users 인덱스
- **단계/위치**: phase-1, `UserService.java:42, 56`, `UserRepositoryImpl.java:25`, `V1__create_users.sql:8`
- **현상**: 가입은 `existsByEmail`(select ... limit 1) 1회 + `saveAndFlush` insert 1회(IDENTITY 라 insert 직후 id 반환)로 왕복 2회다(begin/commit 별도). 로그인은 `findByEmail` 1회. `uk_users_email` 유니크 제약이 btree 인덱스를 만들어 두 조회 모두 인덱스를 쓴다. 다른 인덱스는 없다.
- **문제/기회**:
  - exists 를 빼고 유니크 위반만으로 중복을 판단하면 왕복이 1회 준다. 그러나 중복 가입마다 Postgres 에러 로그와 Hibernate `SqlExceptionHelper` ERROR 로그가 남고 IDENTITY 값이 하나씩 버려진다. 가입 1건 비용은 BCrypt 51ms 가 대부분이라 쿼리 1회(약 1ms 추정)를 줄여도 2% 미만이다.
  - 인덱스: 조회 조건이 email 뿐이므로 지금 인덱스로 충분하다. 커버링 인덱스나 추가 인덱스는 필요 없다. users 5,200행이면 EXPLAIN 결과도 seq scan 과 차이가 없을 규모라 측정하지 않았다.
  - `findByEmail` 이 모든 컬럼(6개, 약 200바이트)을 읽지만 로그인에 비밀번호 해시·role·id 가 모두 필요하므로 불필요한 조회가 아니다.
- **근거**: 코드와 스키마 확인. 쿼리 시간은 추정.
- **개선안**:
  1. **현재 규모에서는 불필요**. 그대로 둔다.
  2. (참고) 이메일 대소문자 정규화를 `Email` 값 객체가 하므로 `lower(email)` 함수 인덱스는 필요 없다. 다른 경로(SQL 직접 입력 등)로 대문자가 들어올 수 있다면 `CHECK (email = lower(email))` 를 추가할 수 있다.
- **우선순위**: 낮음 — 기록용.
- **영향 범위**: 없음

### E-20 8단계에서 3,000 VU 가 동시에 로그인하면 user-service CPU 가 오픈 직전까지 포화될 수 있음
- **단계/위치**: phase-8 `phase-8-load-test.md:384`(VU 시작 시 로그인), `:545`, `:949` 리스크 / phase-1 BCrypt (관련: E-16)
- **현상**: 계획은 각 VU 가 시작하자마자 로그인하고 `OPEN_DELAY_SEC`(90초) 동안 기다린다. 로그인 요청은 `phase:prepare` 태그로 임계값에서 뺀다.
- **문제/기회**:
  - 3,000건 × 51ms = CPU 153초 분량이다. Docker 에 4코어를 주면 약 38초, 8코어면 약 19초 동안 user-service 가 CPU 를 다 쓴다(추정, 호스트 측정값 기준으로 컨테이너에서는 더 느릴 수 있다). k6 도 같은 머신에서 3,000 VU 를 띄우므로 CPU 를 나눠 쓴다.
  - 0초에 3,000건이 몰리면 Tomcat 스레드 200개를 넘는 요청은 accept 큐에서 기다린다. 마지막 요청 대기가 k6 기본 timeout(60초)에 가까워지면 로그인 실패가 `unexpected` 로 잡혀 시나리오 전체가 실패할 수 있다.
  - 로그인 부하는 측정 구간 밖이지만, 오픈 직전까지 CPU 가 포화되면 다른 서비스의 JIT 예열이 늦어지고 첫 몇 초의 `queue_enter` p99 가 왜곡될 수 있다.
  - gateway 쪽 인증 비용은 문제가 아니다: JWT 검증 3.3µs × 폴링 3,000건/초 ≈ 코어 1개의 1% (E-21).
- **근거**: BCrypt 측정값 기반 계산. Docker CPU 할당과 k6 영향은 추정.
- **개선안**:
  1. 토큰을 미리 발급해 파일로 넘긴다. `prepare-users.js` 끝에 (또는 별도 `issue-tokens.js`) 낮은 동시성으로 로그인해 `tokens.json` 에 저장하고, 시나리오는 `SharedArray` 로 읽는다. 로그인 부하가 시나리오에서 완전히 빠진다. TTL 을 2h 로 늘리는 계획(`phase-8-load-test.md:171`)이 이미 있으므로 재사용에 문제가 없다. 계획의 "토큰을 파일로 주고받을 필요가 없다"는 문장은 바꿔야 한다.
  2. 지금 구조를 두고 VU 시작을 나눈다(`startTime` 을 다르게 한 시나리오 여러 개, 또는 VU 별로 `sleep(i / LOGIN_RATE)` 뒤 로그인). "오픈 전부터 대기하던 사용자" 흐름은 그대로 두면서 로그인을 초당 N건으로 평탄화한다. `OPEN_DELAY_SEC` 를 3,000 / N 보다 크게 잡아야 한다.
  3. k6 가 `JWT_SECRET` 으로 HS256 토큰을 직접 서명한다(`k6/crypto` 의 hmac). user-service 를 전혀 거치지 않지만 `sub`(userId)를 알아야 하므로 prepare 단계에서 id 목록을 남겨야 하고, 운영 비밀키를 부하 도구에 넣게 된다. 권하지 않는다.
  - 1을 권장한다. 로그인 자체의 처리량(초당 N건)을 리포트에 넣고 싶으면 별도 시나리오 S-login 으로 분리해 잰다.
- **우선순위**: 중간 — 8단계 결과의 안정성에 직접 영향을 준다. 스크립트만 바꾸면 되고 프로덕션 코드 변경은 없다.
- **영향 범위**: `phase-8-load-test.md` 3.4 설계 원칙·`drop-open-spike.js`·`prepare-users.js`·리스크 절

### E-21 gateway 의 요청당 인증 비용과 이벤트 루프 블로킹 여부
- **단계/위치**: phase-1, `gateway/.../filter/JwtAuthenticationFilter.java:34-59`, `auth/JwtVerifier.java:26-39`, `auth/PublicPathMatcher.java:27-29`, `auth/ErrorResponseWriter.java:21-27`
- **현상**: 요청마다 (1) 헤더 제거 mutate, (2) 공개 경로 규칙 10개 선형 탐색, (3) HS256 서명 검증과 클레임 JSON 파싱, (4) 사용자 헤더 주입 mutate 를 한다. 실패 응답은 `JsonMapper` 로 작은 JSON 을 직렬화한다. 모두 CPU 작업이고 I/O 나 블로킹 호출은 없다.
- **문제/기회**:
  - 측정한 검증 비용은 3.3µs(위조 토큰 4.7µs)다. `PathPattern.matches` 10회는 1µs 미만(추정), 에러 JSON 직렬화도 µs 단위다. 이벤트 루프(코어 수만큼)를 막을 만한 작업은 없다. 계산(추정): S1 폴링 3,000 req/s × 약 5µs = 초당 15ms CPU. **현재 규모에서는 최적화가 불필요하다**. 토큰 검증 캐시, 규칙 trie 화 등은 권하지 않는다.
  - 다만 `PublicPathMatcher` 는 `stream().anyMatch` 로 요청마다 스트림 객체를 만든다. 할당이 조금 있지만 측정할 만한 수준이 아니다.
- **근거**: JWT 검증은 측정(호스트 JVM 20만 회 평균), 나머지는 추정.
- **개선안**:
  1. 그대로 둔다(권장).
  2. 8단계 S4(최대 부하)에서 gateway CPU 가 먼저 포화되면 그때 async-profiler 로 확인한다. 예상되는 병목은 인증 필터가 아니라 Netty 프록시 I/O·커넥션 풀(E-22)이다.
- **우선순위**: 낮음 — 기록용.
- **영향 범위**: 없음

### E-22 gateway 다운스트림 HTTP 클라이언트 타임아웃을 정하지 않음
- **단계/위치**: phase-1, `gateway/src/main/resources/application.yml:7-35` (관련: E-02, E-03)
- **현상**: 라우트 6개를 정의했지만 `spring.cloud.gateway.server.webflux.httpclient.connect-timeout`·`response-timeout`, 커넥션 풀 설정이 없다.
- **문제/기회**: 기본값으로는 connect 는 Netty 기본(30초), 응답은 **타임아웃이 없다**(문서 기반, 측정 안 함). entry/coupon 이 E-02·E-03 장애로 응답을 늦게 주면 gateway 는 끝까지 기다리고, k6 나 클라이언트가 먼저 끊는다. gateway 는 논블로킹이라 스레드가 묶이지는 않지만 다운스트림 커넥션과 요청이 쌓이고, 장애 시 사용자는 "몇십 초 뒤 끊김"을 본다. 서비스별 응답 시간 목표(응모 p99 500ms)를 gateway 가 강제할 수단이 없다.
- **근거**: Spring Cloud Gateway 레퍼런스 "Http timeouts configuration"(전역 `connect-timeout` ms, `response-timeout` Duration, 라우트별 `metadata` 로 덮어쓰기). 측정 안 함.
- **개선안**:
  1. 전역 `connect-timeout: 1000`, `response-timeout: 5s` 를 둔다. 응모·쿠폰처럼 서비스 안에서 최대 3.5초 + α 가 걸리는 경로보다 길게 잡아야 한다. 짧으면 **서비스는 응모를 처리했는데 gateway 가 504 를 주는 애매한 실패**가 생긴다(정합성: 클라이언트가 재시도하면 DUPLICATE 409 가 나므로 오버 응모는 없지만, 사용자 화면에는 실패로 보인다). 그래서 서비스 타임아웃보다 확실히 길게 잡는다.
  2. 라우트별로 `metadata.response-timeout` 을 다르게 준다(조회성 ranking·product 는 2s, entry·coupon 은 5s). 더 정밀하지만 설정이 늘어난다.
  - 커넥션 풀(`pool.max-connections`, `pending-acquire-timeout`)은 8단계 S4 결과를 보고 정한다. 현재 규모에서는 기본값으로 충분할 것으로 본다(추정).
- **우선순위**: 중간 — 설정 두 줄이고 장애 시 응답 경로의 상한을 정한다. 4단계(응모 동기 경로)가 생기기 전이나 그때 함께 넣는다.
- **영향 범위**: `gateway/application.yml`, `infrastructure.md` 4장 튜닝 표, `phase-8-load-test.md` 설정 표

### E-23 인증 실패(401)와 사용자 이벤트 발행 실패를 관측할 지표가 없음
- **단계/위치**: phase-1, `JwtAuthenticationFilter.java:39-50`(실패 시 로그·카운터 없음), `KafkaUserEventPublisher.java:32, 35`(warn 로그만) (관련: E-13)
- **현상**: gateway 는 401 을 조용히 반환한다(로그 폭증이 없어서 좋다). 사유(MISSING/INVALID/EXPIRED)는 응답 본문에만 있다. user-service 발행 실패는 로그로만 남는다.
- **문제/기회**: 8단계에서 토큰 TTL 이 측정 도중 끝나거나(`EXPIRED`), 두 서비스의 `JWT_SECRET` 이 어긋나면(`INVALID`) k6 에는 "401 급증"만 보이고 원인 구분이 어렵다. `http.server.requests` 는 상태 코드만 태그로 가진다.
- **근거**: 코드 확인.
- **개선안**:
  1. gateway 에 `auth.rejected{reason}` 카운터를 둔다(Micrometer, 필터에서 `increment` 한 줄). 비용이 거의 없고 로그를 늘리지 않는다. user-service 에는 `user.registered.publish{result}` 카운터.
  2. 401 마다 debug 로그를 남기고 필요할 때 레벨을 올린다. 운영 중 원인 파악은 되지만 지표로 추세를 볼 수 없다.
  - 기동 시 두 서비스가 같은 시크릿을 쓰는지 확인할 방법은 없으므로(서로 모름), compose 에서 같은 `JWT_SECRET` 변수를 넘기는 지금 구조를 유지하고 리포트 체크리스트에 적는다.
- **우선순위**: 낮음 — E-13 수집 목록에 추가하는 수준이다. 8단계 Actuator/Prometheus 를 붙일 때 같이 넣는다.
- **영향 범위**: `JwtAuthenticationFilter`, `KafkaUserEventPublisher`, `phase-8-load-test.md` 3.2 관찰 메트릭

### E-24 `SignupIntegrationTest` 의 고정 대기 6초와 `AdminBootstrapIntegrationTest` 의 별도 컨텍스트
- **단계/위치**: phase-1, `user-service/src/test/java/com/queuedrop/user/SignupIntegrationTest.java:46, 136, 163`, `AdminBootstrapIntegrationTest.java:27-31, 71-83` (관련: E-08)
- **현상**:
  - `SignupIntegrationTest` 11.7초: "발행되지 않음"을 확인하려고 `QUIET_PERIOD` 3초를 두 테스트에서 기다린다(고정 6초). 나머지는 Kafka 컨테이너 기동과 첫 메시지 대기다.
  - `AdminBootstrapIntegrationTest` 3.8초: `@SpringBootTest(properties=...)` 로 컨텍스트 캐시 키가 달라 **Postgres 컨테이너를 하나 더 띄우고**(E-08), 그 안에서 `SpringApplicationBuilder` 로 애플리케이션을 한 번 더 기동한다(웹 없음, Kafka 는 지연 연결이라 브로커 없이 기동됨).
- **문제/기회**: user-service 테스트 총 시간은 약 18초로 지금은 문제가 없다. 다만 "부재 확인 = 고정 대기" 패턴이 4·6단계(보상 후 미발행 확인 등)로 복사되면 테스트마다 3초씩 늘어난다. 두 번째 기동 방식은 기동 시간이 늘어날수록(서비스가 커질수록) 비용이 커진다.
- **근거**: 측정값(`build/test-results`). 확장 예측은 추정.
- **개선안**:
  1. 부재 확인은 "마커 메시지" 방식으로 바꾼다. 롤백 쪽을 발행 요청한 뒤 커밋 쪽(대조군)을 보내고, 같은 파티션(`user.registered` 는 파티션 1)이라 대조군이 도착했을 때 그보다 먼저 요청된 롤백 메시지가 없으면 부재로 판단한다. T1-17 은 이미 대조군이 있어 대기 3초를 없앨 수 있다. 다만 `send()` 순서와 파티션 순서가 같다는 전제(같은 producer, 같은 파티션)를 테스트 주석에 적어야 한다. 중복 가입 테스트도 같은 방식으로 뒤에 정상 가입 1건을 보내 마커로 쓴다.
  2. `QUIET_PERIOD` 를 1초로 줄인다. 단순하지만 느린 CI 에서 거짓 통과(늦게 온 메시지를 놓침) 가능성이 커진다.
  3. 관리자 재기동 검증은 두 번째 애플리케이션 대신 `AdminAccountInitializer.run()` 을 같은 컨텍스트에서 한 번 더 호출해 확인한다. 기동 1회가 빠지지만 "실제 재기동" 재현성은 떨어진다. 컨테이너 중복은 E-08 의 싱글턴 컨테이너로 해결한다.
- **우선순위**: 낮음 — 지금 18초는 수용 가능하다. 4단계 `IntegrationTestSupport` 를 만들 때 1의 패턴을 정해 두면 비용이 가장 적다.
- **영향 범위**: `SignupIntegrationTest`, `AdminBootstrapIntegrationTest`, phase-4·6 통합 테스트 작성 규칙

---

## phase-2 (드로우: product-service) 분석 — 2026-11-16

분석 범위: `git diff dev...HEAD` (`feat/phase-2-drop`, 커밋 5개), 단계 문서 `phase-2-drop.md` v1.2, 5단계 `markDrawn` 경로(`phase-5-draw.md` 3.5).
- **측정 환경**: 로컬 Docker `postgres:17` 임시 컨테이너(분석 후 삭제)에 `V1__create_products_and_drops.sql` 을 적용하고, 합성 데이터를 넣어 `EXPLAIN (ANALYZE, BUFFERS)` 를 1회씩 실행했다(워밍 캐시). 데이터: 1시간 간격 오픈·응모 2시간인 드로우 1만 건/10만 건, 기준 시각 기준 SCHEDULED 100, OPEN 2, CLOSED 6, 나머지 DRAWN(99% 이상). 쿼리는 Hibernate 가 만드는 형태(`order by open_at, id offset ? rows fetch first ? rows`, `select count(d.id) ...`)로 직접 썼다.
- **재현 테스트**: E-25 는 저장소에 추가하지 않고 scratch 디렉터리의 테스트 소스를 Gradle init script 로 붙여 1회 실행했다(`DropRepositoryImpl` + Testcontainers Postgres).
- 각 항목의 **구분**: "명백한 버그"(이번 단계에서 수정) / "고도화"(나중 단계).

### E-25 큰 `page` 값이면 offset 이 `Integer.MAX_VALUE` 를 넘어 500 과 ERROR 스택 트레이스가 남음
- **구분**: 명백한 버그 (장애 경로, 공개 API) — 이번 단계에서 수정
- **처리**: ✅ 2단계에서 반영 (2026-11-16) — 개선안 채택: `page` 에 `@Max(10000)`(400 `COMMON_INVALID_INPUT`) + `DropRepositoryImpl` 에서 `(long) page*size > Integer.MAX_VALUE` 이면 빈 페이지(건수는 실제 값). 회귀 테스트: `DropControllerTest`·`DropRepositoryImplTest`.
- **단계/위치**: phase-2, `DropController.java:47` (`@Min(0) int page`, 상한 없음), `DropRepositoryImpl.java:39`, `GlobalExceptionHandler.java:95-103` (관련: E-10)
- **현상**: `page` 는 하한만 검증한다. `PageRequest.of(page, size)` 의 offset 은 `long`(page × size)이지만 Spring Data JPA 가 `Query.setFirstResult(int)` 로 넘기기 전에 `PageableUtils.getOffsetAsInteger` 에서 `InvalidDataAccessApiUsageException("Page offset exceeds Integer.MAX_VALUE")` 를 던진다. 이 예외는 `ErrorResponse` 구현체가 아니라서 catch-all 이 **500 + `log.error("unexpected error", e)`** 로 처리한다.
- **문제/기회**: 인증 없는 공개 API 에 `?page=21474837&size=100` 같은 요청 하나로 500 을 만들 수 있다. 클라이언트 입력 오류인데 서버 장애로 집계되고, 반복 호출하면 ERROR 로그와 스택 트레이스가 쌓인다(E-10 과 같은 로그 폭증 경로). 데이터 정합성 문제는 없다.
- **근거**: **측정** — scratch 테스트로 `dropRepository.search(null, now, 21_474_837, 100)` 호출 시 `org.springframework.dao.InvalidDataAccessApiUsageException: Page offset exceeds Integer.MAX_VALUE (2147483647)` 확인. 컨트롤러까지 거친 500 응답은 `GlobalExceptionHandler` 코드로 확인(실행 안 함). 경계: size=100 이면 page ≥ 21,474,837, size=1 이면 page = `Integer.MAX_VALUE` 에서 발생.
- **개선안**:
  1. `page` 에 `@Max` 를 둔다(예: `@Max(10_000)` → size 100 이어도 offset 100만). 400 `COMMON_INVALID_INPUT` 으로 떨어지고 기존 `HandlerMethodValidationException` 처리 경로를 그대로 쓴다. 한 줄이고 테스트 추가가 쉽다. 상한 값 자체는 임의라서 단계 문서 API 표에 적어야 한다.
  2. application(`DropQueryService.search`)에서 `(long) page * size > Integer.MAX_VALUE` 이면 `INVALID_INPUT` 을 던진다. 상한이 정확하지만 영속 기술(JPA int offset)의 제약이 application 에 새어 나온다.
  3. 범위를 넘으면 빈 페이지를 돌려준다. 사용자에게 오류가 보이지 않지만 의미 없는 요청을 정상으로 처리하게 된다.
  - 1을 권장한다. 매우 깊은 offset 은 비용 면에서도 의미가 없다(E-27).
- **우선순위**: 높음 — 공개 API 의 500 경로이고 수정 비용이 한 줄이다.
- **영향 범위**: `DropController`, `DropControllerTest`(장원영), `phase-2-drop.md` API 표·검증 규칙

### E-26 드로우 목록 쿼리 계획과 인덱스 — CLOSED 조회와 DRAWN count 는 전 테이블을 훑음
- **구분**: 고도화 (현재 규모에서는 불필요)
- **단계/위치**: phase-2, `DropJpaRepository.java:17-26`, `V1__create_products_and_drops.sql:25-27`
- **현상**: 인덱스는 `product_id`, `open_at`, `close_at` 단일 컬럼 3개다. 실제 계획(측정)은 다음과 같다.

  | 쿼리 | 계획 | 1만 건 | 10만 건 |
  |------|------|--------|---------|
  | 전체 page 0 | `idx_drops_open_at` Index Scan + Incremental Sort(open_at 선정렬, id 동순위 정렬) | 0.04ms | 0.05ms |
  | 전체 count | Seq Scan | 0.71ms | 6.7ms |
  | SCHEDULED (+count) | `idx_drops_open_at` 범위 스캔, `NOT drawn` 필터 | 0.02ms | 0.02ms |
  | OPEN (+count) | `idx_drops_close_at` 범위 스캔(`close_at > now`), 101건 읽고 99건 필터 | 0.02ms | 0.01ms |
  | CLOSED | `idx_drops_open_at` **전체** 스캔, 9,994 / 99,994건 필터로 버림 | 0.47ms | 4.8ms |
  | DRAWN page 0 | `idx_drops_open_at` 앞에서 21건 | 0.01ms | 0.02ms |
  | DRAWN count | Seq Scan | 0.74ms | 6.8ms |
  | 상세(id) | PK Index Scan | 0.01ms | 0.01ms |

  - 정렬 `openAt → id` 는 `open_at` 인덱스 순서 + Incremental Sort 로 처리되어 별도 정렬 비용이 거의 없다. `(open_at, id)` 복합 인덱스가 없어도 문제없다.
  - CLOSED(`drawn = false and close_at <= now`)는 과거 드로우 대부분이 `close_at <= now` 를 만족해서 범위 조건이 걸러 주지 못하고, 선택도가 높은 조건은 `drawn = false` 인데 이를 위한 인덱스가 없다. 시간이 지나 DRAWN 이 쌓일수록 선형으로 느려진다.
  - `idx_drops_product_id` 를 쓰는 쿼리는 지금 없다(상품별 드로우 조회 API 없음, 상품 삭제 없음).
- **문제/기회**: 이 서비스의 드로우는 관리자가 등록하므로 수백~수천 건 규모로 본다(추정). 1만 건에서 가장 느린 쿼리가 0.7ms 라서 **현재 규모에서는 인덱스 추가가 불필요하다**. 10만 건이 되어도 7ms 수준이다.
- **근거**: 측정(위 표). 데이터 분포는 합성이며 실제 분포(CLOSED 체류 시간, drawn 비율)와 다를 수 있다.
- **개선안**:
  1. 그대로 둔다(권장). 8단계 부하 테스트 데이터(드로우 수십 건)에서는 차이가 측정되지 않을 것이다(추정).
  2. 드로우가 10만 건 이상 쌓이면 부분 인덱스 `CREATE INDEX idx_drops_undrawn_open_at_id ON drops (open_at, id) WHERE drawn = false` 를 추가한다. **측정(10만 건)**: CLOSED 4.8ms → 0.009ms, SCHEDULED count 는 Index Only Scan 으로 바뀜. 인덱스 크기 16kB(미추첨 108건만 담음) — 단일 컬럼 인덱스 2.2MB 대비 무시할 수준이고, `markDrawn` UPDATE 때 이 인덱스에서 행이 빠지는 쓰기 비용이 조금 생긴다. 이 인덱스가 생기면 `idx_drops_open_at` 은 전체·DRAWN 목록 정렬용으로만 남는다.
  3. `idx_drops_product_id` 를 지운다. 쓰기마다 인덱스 1개가 줄지만 드로우 등록은 드물어 효과가 없고, 나중에 상품별 조회가 생기면 다시 필요하다. 권하지 않는다.
  - DRAWN count 의 Seq Scan 은 E-27 의 count 정책으로 다룬다.
- **우선순위**: 낮음 — 측정상 현재 규모에서 비용이 없다.
- **영향 범위**: (2 선택 시) `V2__*.sql`, `erd.md` drops 인덱스 목록

### E-27 offset 페이징과 count 쿼리, 오름차순 정렬로 최신 드로우가 마지막 페이지에 놓임
- **구분**: 고도화 (정렬 방향은 설계 결정 사항)
- **단계/위치**: phase-2, `DropRepositoryImpl.java:22, 39-48`, `DropJpaRepository.java` (Spring Data `Page`)
- **현상**:
  - 모든 상태 조회가 `Page` 를 반환해 요청당 내용 쿼리 + count 쿼리 2개를 실행한다. 단 Spring Data 는 첫 페이지이고 결과가 size 보다 적으면 count 를 생략한다(OPEN·CLOSED 처럼 결과가 적은 조회는 쿼리 1개).
  - 정렬은 모든 상태에서 `openAt ASC, id ASC` 다. 전체·DRAWN 목록의 page 0 에는 **가장 오래된** 드로우가 나온다.
- **문제/기회**:
  - 비용: offset 99,000 조회가 **측정** 14ms(10만 건), count 6.7ms. 현재 규모(수천 건)에서는 1ms 미만이라 keyset 페이징으로 바꿀 이유가 없다.
  - 사용성: 드로우가 쌓일수록 전체·DRAWN 목록에서 최근 것을 보려면 마지막 페이지로 가야 한다. 클라이언트가 이를 위해 `totalElements` 로 마지막 페이지를 계산해 깊은 offset 을 요청하게 되면 가장 비싼 경로(깊은 offset + count)가 기본 경로가 된다. SCHEDULED·OPEN 은 "곧 열리는 순/먼저 열린 순"이라 오름차순이 자연스럽다.
- **근거**: 비용은 측정(E-26 환경). 클라이언트 사용 패턴은 추정.
- **개선안**:
  1. 상태별로 정렬 방향을 나눈다: SCHEDULED·OPEN 은 `openAt ASC, id ASC`, CLOSED·DRAWN·전체는 `openAt DESC, id DESC`. B-tree 는 역방향 스캔이 가능해 인덱스 추가가 없다(DESC 계획은 측정 안 함 — 동일 비용 추정). 응답 순서가 바뀌므로 **API 계약 변경**이고 `DropRepositoryImplTest`·`DropControllerTest` 의 정렬 기대값도 바뀐다. 사용자 결정이 필요하다.
  2. `sort` 파라미터를 받는다. 유연하지만 허용 컬럼 화이트리스트와 검증이 늘어난다.
  3. 그대로 두고 문서에 "목록은 오픈 시각 오름차순"이라고만 명시한다.
  - count 는 그대로 둔다. 관리 화면에 총 건수가 필요하고 현재 비용이 작다. 수십만 건 이상에서 문제가 되면 그때 `Slice`(hasNext 만) 또는 keyset(`openAt, id` 커서)으로 바꾼다.
- **우선순위**: 낮음 — 성능 문제는 아니다. 정렬 방향은 7단계 이후 화면 요구가 생기면 정한다.
- **영향 범위**: `DropRepositoryImpl.ORDER`, `phase-2-drop.md` 조회 API 설명, `drop-register.md` 조회 절, 관련 테스트

### E-28 공개 목록·상세 조회 캐시 — 현재 규모에서는 불필요, 상태를 캐시하면 경계 시각에 틀린 상태를 보여 줌
- **구분**: 고도화 (현재 규모에서는 불필요)
- **단계/위치**: phase-2, `DropQueryService.java:28-41`, `DropResult.of(drop, now)`
- **현상**: 매 요청 DB 를 조회하고, 응답 `status` 는 요청 시각 `now` 로 계산한다. 캐시는 없다.
- **문제/기회**:
  - 부하 추정: 오픈 직전 사용자가 상세를 여러 번 새로 고친다고 해도 피크 트래픽의 대부분은 queue 폴링(S1 3,000 req/s)으로 간다. product 상세 쿼리는 실행 0.01ms(측정) + 왕복을 합쳐도 커넥션 점유가 요청당 1ms 미만(추정)이라 풀 10개로 초당 수천 건 이상을 받는다(추정). 병목은 DB 보다 Tomcat·JSON 직렬화 쪽이 먼저 온다(추정). 8단계 시나리오도 product API 를 피크 경로에 넣지 않는다.
  - 정합성 위험: 응답 DTO(상태 포함)를 그대로 캐시하면 TTL 동안 `openAt` 이 지나도 SCHEDULED 로 보이고, 목록 캐시는 상태 필터 결과 자체가 시각에 따라 바뀐다. `markDrawn` 이후에도 TTL 만큼 CLOSED 로 보인다.
- **근거**: 부하 수치는 추정, 쿼리 시간은 측정(E-26).
- **개선안**:
  1. 지금은 캐시를 두지 않는다(권장). 8단계에서 product-service 가 상위 병목으로 보일 때만 다시 본다.
  2. 필요해지면 **상세만** 로컬 캐시(Caffeine)로 `Drop` 원본(상태 없는 값)을 캐시하고 `status(now)` 는 읽을 때 계산한다. 드로우의 시각·정원은 등록 후 불변이고 바뀌는 값은 `drawn` 하나라서, 5단계 `markDrawn` 에서 해당 키를 무효화하면 된다. 인스턴스가 여러 개면 다른 인스턴스는 TTL(예: 5초) 동안 drawn=false 를 보일 수 있다 — 화면 표시용이라 허용 가능한 지연이지만 문서에 적어야 한다.
  3. 목록은 `Cache-Control: max-age=1~2` 정도의 짧은 HTTP 캐시만 고려한다. 상태 경계에서 최대 2초 늦게 바뀌는 것을 받아들이는 선택이다. 서버 측 목록 캐시는 시각 의존 필터 때문에 무효화가 복잡해 권하지 않는다.
- **우선순위**: 낮음 — 측정상 필요성이 없고, 잘못 넣으면 상태 표시가 틀린다.
- **영향 범위**: (2 선택 시) `DropQueryService` 또는 infrastructure 캐시 어댑터, 5단계 `DropService.markDrawn`

### E-29 `drop.created` 유실을 감지하거나 자동 복구할 수단이 없음 (재발행은 사람이 로그를 보고 해야 함)
- **구분**: 고도화 (D4 확정안의 남은 위험 — 3·4단계 수신 측이 생긴 뒤 다룸)
- **단계/위치**: phase-2, `KafkaDropEventPublisher.java:27-49`, `DropService.java:54-59` (관련: E-02, E-12, E-17, E-18, E-23)
- **현상**:
  - 발행 비용: 드로우 등록 1건당 메시지 1건, 파티션 1개 토픽. 관리자 등록 경로라 처리량 문제는 없다. afterCommit 안에서 `send()` 가 메타데이터 대기로 최대 `max.block.ms` 1초까지 요청 스레드를 잡는다(E-17 과 같은 구조). 관리자 API 라 영향이 작다.
  - 유실 경로: (a) 커밋 후 `send()` 전에 프로세스 종료, (b) producer 버퍼에 있는 동안 `kill -9`(정상 종료는 producer close 가 flush 함), (c) `delivery.timeout.ms` 10초 안에 브로커 ack 실패. (c) 만 ERROR 로그가 남고, (a)(b) 는 **로그조차 없다**.
  - 복구는 관리자 재발행 API 하나다. 관리자는 어느 드로우가 전파되지 않았는지 알 방법이 없다.
- **문제/기회**: 오픈 최소 여유가 1분이라, 유실을 사람이 알아채기 전에 오픈 시각이 지날 수 있다. 그러면 queue 는 meta 가 없어 진입을 거절하고(DROP_NOT_FOUND), entry 는 스냅샷이 없어 응모가 실패한다 — 사용자 입장에서는 드로우 전체 장애다. 빈도는 낮지만(브로커 장애·배포 중 종료와 등록이 겹칠 때) 영향이 크다.
  - 부수 관찰: 재발행은 마감·추첨이 끝난 드로우에도 허용된다. 수신 측이 NX/`EXISTS` 로 멱등이라 정상 상태에서는 안전하다. 다만 5단계 이후 Redis 키를 정리(삭제·EXPIRE)한 드로우에 재발행하면 `entry-initialize.lua` 가 `state=OPEN`, `remaining=M` 으로 키를 **다시 만든다**. 응모 Lua 가 시각(`now >= closeAt`)으로 막으므로 오버 응모는 없지만(4단계 문서 기준 추정), TTL 없는 키가 남아 메모리 누수가 된다. 5단계 키 정리 정책을 정할 때 함께 확인해야 한다.
- **근거**: 유실 경로는 코드·Kafka producer 동작 기반 분석(측정 안 함). 영향은 3·4단계 문서 기준.
- **개선안**:
  1. **오픈 전 자동 재발행(권장)**: product-service 스케줄러가 주기적으로(예: 30초) `drawn = false and open_at between now and now + 5분` 인 드로우를 재발행한다. 수신 측이 이미 멱등이라 정합성 위험이 없고, 대상은 몇 건이며 SCHEDULED 쿼리는 인덱스 범위 스캔(측정 0.02ms)이다. 유실이 있어도 오픈 전에 자동으로 메워진다. 대가: 드로우마다 오픈 전 최대 10회 정도 중복 메시지(수신 측 Redis/DB 왕복 몇 번), 다중 인스턴스면 중복이 배가된다(멱등이라 무해). 오픈 여유 1분보다 주기를 짧게 잡아야 한다.
  2. Transactional Outbox(설계에서 포기한 대안). 유실을 원천 차단하지만 테이블·릴레이·정리 작업이 늘어난다. 과제 범위 대비 과하다.
  3. 감지만 추가: 발행 실패 카운터(`drop.created.publish{result}`, E-23 과 같은 방식) + 수신 측이 스냅샷이 없는 dropId 로 요청을 받으면 경고 로그. 원인을 빨리 알 수 있지만 복구는 여전히 수동이다. 1과 함께 두면 좋다.
  - 재발행 범위는 1을 넣을 때 `drawn = false and now < closeAt` 로 제한하는 것을 검토한다(위 부수 관찰). 관리자 API 의 허용 범위를 바꾸는 것이므로 설계 결정이다.
- **우선순위**: 중간 — 빈도는 낮지만 발생하면 해당 드로우 전체가 실패한다. 개선안 1은 프로덕션 코드 수십 줄이며, 수신 측 멱등성이 검증되는 4단계 이후(또는 8단계 전)에 넣는 것이 자연스럽다.
- **영향 범위**: (1) product-service presentation 스케줄러·`DropRepository` 조회 메서드, `concurrency-consistency.md` D4, `drop-register.md` 6장. (3) `KafkaDropEventPublisher`, 8단계 관찰 메트릭

### E-30 5단계 `markDrawn` 경로 — 단계 문서 코드가 2단계 구현과 맞지 않고, 갱신은 SELECT + 전체 컬럼 UPDATE 2회
- **구분**: 고도화 (5단계 문서 수정 필요, 성능은 현재 규모에서 불필요)
- **단계/위치**: phase-2 `Drop.java:89-91`, `DropMapper.java:14-17`, `DropRepositoryImpl.java:28-30` / 계획 `phase-5-draw.md:601-613`
- **현상**:
  - 5단계 문서는 `dropRepository.save(drop.markDrawn())` 와 `DropException(DropErrorCode.DROP_NOT_FOUND)` 를 쓰는데, 2단계 구현은 `markDrawn()` 이 `void` 이고 예외는 `ApplicationException(ProductErrorCode.DROP_NOT_FOUND)` 다(v1.2 에러 계약). 문서대로 쓰면 컴파일되지 않는다.
  - 문서 흐름대로면 쿼리는 `findById`(SELECT) 1회 + `save` 1회다. `save` 는 id 가 있어 `merge` 가 되는데, 같은 트랜잭션에서 이미 로드된 엔티티가 영속성 컨텍스트에 있으므로 추가 SELECT 없이 상태를 복사하고 flush 시 **전체 컬럼 UPDATE** 1회를 낸다(코드 분석, 측정 안 함).
  - 동시성: `draw.completed` 는 파티션 1개·컨슈머 1개라 같은 dropId 메시지가 동시에 처리되지 않고, 다른 쓰기 경로(재발행은 읽기만)도 없어 lost update 위험은 없다.
- **문제/기회**: 추첨 완료는 드로우당 1건이라 쿼리 2회는 비용 문제가 아니다 — **현재 규모에서는 최적화 불필요**. 문서 불일치만 5단계 착수 전에 고치면 된다.
- **근거**: 코드·문서 대조. 쿼리 수는 Spring Data `save` 의 `isNew` 판정과 JPA merge 동작 기반 추정.
- **개선안**:
  1. 5단계 문서를 2단계 구현에 맞춘다(권장): `drop.markDrawn(); dropRepository.save(drop);`, 예외는 `ApplicationException(ProductErrorCode.DROP_NOT_FOUND)`. 리스너의 not-retryable 등록 대상도 이 예외(코드 기준 판별)로 바꿔 적는다 — `ApplicationException` 전체를 not-retryable 로 등록하면 다른 코드까지 재시도 대상에서 빠지므로 주의.
  2. 조건부 UPDATE 포트(`markDrawnIfNotDrawn(dropId)` → `update drops set drawn = true where id = :id and drawn = false`)로 쿼리 1회로 줄인다. 멱등이 SQL 로 보장되지만 도메인 `markDrawn()` 을 거치지 않아 규칙이 영속 계층으로 새고, 없는 dropId 와 이미 추첨된 dropId 를 구분하려면 추가 조회가 필요하다. 권하지 않는다.
  - E-28 개선안 2(상세 캐시)를 도입하면 이 메서드에서 무효화해야 한다.
- **우선순위**: 낮음(성능) / 문서 수정은 5단계 착수 전 필수.
- **영향 범위**: `phase-5-draw.md` 3.5, (2 선택 시) `DropRepository`, `DropJpaRepository`
