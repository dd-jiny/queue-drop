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
