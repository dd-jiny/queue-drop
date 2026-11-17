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
- **처리 (coupon)**: 6단계 착수 전 `coupon-service` 에 `spring.data.redis.timeout: 1s` 반영 결정 (phase-6 v1.4). queue·ranking 은 남음.
- **처리 (ranking)**: 7단계 착수 전 `ranking-service` 에 `spring.data.redis.timeout: 1s` 반영 결정 — Redis 예외는 `RankingStoreUnavailableException`, 조회는 503 `RANKING_TEMPORARILY_FAILED` (phase-7 v1.2). queue 는 남음.

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
- **8단계 측정 결과 (2026-11-17)**: 상한이 실제로 드러난 곳은 쿠폰이다. 쿠폰 11,000~15,000 동시(0.5초 jitter)에서 발급 p99 693~1,196ms(목표 500ms 미달), 서버 처리 평균 11~14ms·GC 최대 정지 13ms — 지연은 앞단 대기다. 응모는 대기열이 유입을 막아 초당 1,000건 입장에서도 서버 측 p99 7.6ms 라 상한에 닿지 않았다. 후속 분석·선택지는 E-56 에 모은다 (리포트 B6).

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
- **상태 (2026-11-17)**: **로그인만 적용** (phase-8 #8 P3). 8단계 측정에서 3,000 동시 로그인 시 `hikaricp.connections.pending` 최대 189 → 적용 후 0 (active 최대 10 → 2). 로그인 p99 는 3.3s → 2.9s 로 CPU(BCrypt)가 상한. **가입은 미적용** (가입은 사전 준비 단계라 측정 대상 아님).

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
- **참고 (2026-11-17)**: 7단계 v1.2 에서 표시 fallback 을 설계(ranking.md)와 통일했다 — 닉네임이 없으면 `null` 이 아니라 `user-{id}`, 제목이 없으면 `drop-{id}`. 위 "현상"의 `null` 은 작성 당시 계획 기준이다. 유실 문제 자체는 그대로다.

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
- **8단계 측정 결과 (2026-11-17)**: 개선안 2 와 같은 방식(`LOGIN_SPREAD_SEC` 로 VU 별 로그인 분산)을 썼다. 분산 없이 10,000명이 동시에 로그인하면(X1-1차) BCrypt 상한(약 235건/초, user-service CPU 98%) 때문에 약 42초가 걸리고, gateway 풀 대기 30초를 넘긴 2,670건이 500 이 됐다. 분산 후(X1) 로그인 p99 85ms, 실패 0. 서비스 쪽 상한 자체는 E-75, 500 응답 의미는 E-72.

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
- **8단계 반영 (2026-11-17)**: 업스트림 풀은 P1 에서 `FIXED`·서비스당 200·`acquire-timeout` 30s 로 정했다(B1 연결 리셋 해소). **`connect-timeout`·`response-timeout` 은 여전히 없다** — 이 항목의 개선안 1 은 그대로 남는다. 풀 대기 초과의 응답 코드는 E-72, gateway 메모리는 E-73.

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
- **처리**: 5단계 착수 전 문서 반영 (2026-11-17) — 개선안 1. `phase-5-draw.md` v1.2 3.5 를 `drop.markDrawn(); save(drop)` + `ApplicationException(ProductErrorCode.DROP_NOT_FOUND)` 로, 비재시도는 에러 코드 `DROP_NOT_FOUND` 만.

---

## phase-3 (대기열: queue-service) 분석 — 2026-11-16

분석 범위: `git diff dev..HEAD` (`feat/phase-3-queue`, 커밋 5개). 기준 문서 `phase-3-queue.md` v1.2, `queue-enter.md` v1.2.
- **측정 환경**: 로컬 `queuedrop-redis`(redis:7.4)에 임시 키(dropId 999999991)를 만들어 측정하고 바로 지웠다. 게이트는 scratch Python 스크립트가 RESP 로 `SET key NX PX {interval}` 을 고정 주기(절대 시각 기준, `fixedRate` 와 같은 방식)로 보내 통과 횟수를 셌다. `admit.lua` 실행 시간은 `SLOWLOG`(측정 동안만 임계값 0, 이후 10000 으로 복구)로 확인했다. 메모리는 `MEMORY USAGE`.
- 각 항목의 **구분**: "버그 성격"(이번 단계에서 반영 권장) / "고도화"(나중 단계).

### E-31 게이트 TTL 을 주기와 같게 잡아 단일 인스턴스에서도 약 40% 주기를 건너뜀
- **구분**: 버그 성격 (설계 목표 처리량 "1초마다 N명" 미달, 정합성 영향 없음) — 이번 단계 반영 권장
- **처리**: ✅ 3단계에서 반영 (2026-11-16) — 개선안 1 채택: `AdmissionProperties.gateWindowMs()` = 주기의 90%(최소 1ms). 설계 문서의 처리량 표현을 "상한 초당 약 1.11N명"으로 갱신. 회귀 테스트: `AdmissionServiceTest`.
- **단계/위치**: phase-3, `RedisSchedulerGate.java:32-34`, `AdmissionService.java:31`, `AdmissionScheduler.java:17` (`fixedRate = interval`, 게이트 `PX = interval`)
- **현상**: 게이트 키는 `SET NX PX 1000` 시점부터 정확히 1000ms 뒤에 만료된다. 같은 인스턴스의 다음 실행도 1000ms 뒤에 오는데, 이번 SET 의 도착 지연(스케줄러 깨어남 + 네트워크)이 지난번보다 조금이라도 짧으면 키가 아직 살아 있어 `tryPass` 가 false 다. 그 주기에는 아무도 입장하지 않고, 다음 주기(2초 뒤)에야 통과한다.
- **문제/기회**: 설계 문서(6장 리스크, `queue-enter.md` 리스크)는 흔들림을 "다중 인스턴스에서 1~2초"로만 적었지만 **인스턴스 1개에서도 같은 일이 생긴다**. 실제 입장 속도가 설계값의 60% 안팎이고 실행마다 달라서, 8단계에서 "오픈 후 정원 소진 시간"과 응모 TPS 측정값이 N 설정과 맞지 않게 된다. 테스트(T3-20·21)는 `admit()` 을 직접 부르므로 잡지 못한다.
- **근거**: **측정** — 인스턴스 1개 모사, 주기 1000ms × 60회 → **36회 통과(60%)**, 주기 200ms × 150회 → 94회 통과(63%). Java `scheduleAtFixedRate` 도 예정 시각 기준이라 같은 경향일 것으로 본다(추정, JVM 으로는 측정 안 함).
- **개선안**:
  1. 게이트 window 를 주기보다 조금 짧게 잡는다: `window = interval - margin`(예: `max(interval × 0.9, interval - 100ms)`). 한 줄 수정이고 단일 인스턴스는 매 주기 통과한다. 대가: 다중 인스턴스에서 최악의 경우 900ms 마다 한 번 통과할 수 있어 상한이 "초당 약 1.11N 명"이 된다. 문서의 "초당 N명 이하" 표현을 "주기당 N명, 최소 간격 0.9×주기"로 바꿔야 한다.
  2. 주기 버킷 키: `SET queue:admission:gate:{floor(now/interval)} NX PX {2×interval}`. 다중 인스턴스에서도 버킷당 정확히 1회지만, 인스턴스 시계 차이에 의존하고, 실행 위상이 버킷 경계 근처에 있으면 단일 인스턴스에서도 같은 버킷에 두 번 들어가 건너뛸 수 있다. `now` 를 Redis `TIME` 으로 받으면 시계 문제는 사라지지만 왕복이 1회 늘어난다.
  3. 그대로 두고 8단계 리포트에 실효 처리량(약 0.6N/s)을 기록한다. 권하지 않는다 — 설정값과 실제가 어긋나 튜닝 근거가 흐려진다.
  - 1을 권장. 회귀 테스트는 `RedisSchedulerGateTest` 에 "window 만큼 기다린 뒤 같은 인스턴스가 다시 통과" 같은 경계 케이스로 고정한다(장원영).
- **우선순위**: 높음 — 수정 비용이 작고, 대기열의 핵심 수치(초당 입장 인원)가 설정과 맞지 않는다.
- **영향 범위**: `AdmissionService`(window 계산) 또는 `RedisSchedulerGate`, `queue-enter.md` 4장·리스크, `phase-3-queue.md` 3.3 게이트 설명·6장 리스크, `RedisSchedulerGateTest`

### E-32 폴링 GET 1회에 Redis 왕복 4번, 진입 POST 는 3번 (현재 규모에서는 불필요)
- **구분**: 고도화 (현재 규모에서는 불필요)
- **단계/위치**: phase-3, `QueueService.java:28-46`, `RedisWaitingQueueRepository.java:44-72`, `RedisRemainingCapacityReader.java:17-20`, `enqueue.lua:22-28`
- **현상**:
  - `status`: `HGETALL meta`(존재 확인용) → `GET entry:{id}:remaining` → `EXISTS token` → `ZRANK waiting` 을 차례로 보낸다(4 RTT).
  - `enter`: `HGETALL meta` → `GET remaining` → `EVALSHA enqueue.lua`(3 RTT). 신규 진입이면 Lua 안에서 `EXISTS, ZRANK, INCR, ZADD, HGET, PEXPIREAT×2, ZRANK` 8개 명령을 실행한다. `PEXPIREAT` 2개와 `HGET` 은 신규 진입마다 같은 값으로 다시 건다.
- **문제/기회**: 8단계 S1(사용자 3,000)에서 대기 3,000명, N=100 이면 대기열이 30초 안에 빠진다. 폴링 간격 1/3/5초를 따르면 상태 조회는 약 100 + 300 + 400 ≈ 800 req/s, 최악(모두 1초)으로 3,000 req/s → Redis 명령 3,200~12,000/s 다(추정). 단일 Redis 처리량(수만~10만 ops/s)에 비해 작다. 요청 지연은 순차 왕복 4회라 로컬 RTT 0.1~0.3ms 기준 0.5~1ms 수준(추정)으로 응답 시간 목표 안에 든다. **지금은 바꿀 필요가 없다.**
- **근거**: 코드 기반 명령 수 계산, 요청량은 추정(사용자가 `nextPollAfterMs` 를 따른다는 가정).
- **개선안**:
  1. 그대로 둔다(권장). 8단계에서 queue p99 나 Redis CPU 가 상위 병목으로 보일 때만 다시 본다.
  2. `QueueMeta` 로컬 캐시(Caffeine, 만료 = `closeAt`): meta 는 등록 후 바뀌지 않아 정합성 문제가 없다. GET 3 RTT, POST 2 RTT 로 줄고 E-33 의 스케줄러 조회도 함께 줄어든다. 주의: "없음"(DROP_NOT_FOUND)은 캐시하면 안 된다 — `drop.created` 가 늦게 도착한 드로우가 계속 404 가 된다.
  3. `status.lua` 하나로 묶는다(1 RTT, 우선순위 판정을 Lua 로 옮김). 왕복은 가장 적지만 상태 우선순위 규칙이 application 에서 Lua 로 새고, entry 소유 키를 읽는 Lua 가 하나 더 늘어 E-39(Cluster) 부담이 커진다.
  - `enqueue.lua` 의 `PEXPIREAT` 재설정은 `seq == 1` 이거나 `PTTL == -1` 일 때만 걸도록 줄일 수 있지만, ZPOPMIN 으로 키가 비었다가 다시 생기는 경우를 놓치기 쉬워 권하지 않는다(효과도 명령 2개).
- **우선순위**: 낮음 — 현재 규모에서 측정상 문제가 될 근거가 없다.
- **영향 범위**: (2) `RedisWaitingQueueRepository.findMeta` 또는 infrastructure 캐시 데코레이터. (3) `scripts/status.lua`, `QueueService.status`

### E-33 스케줄러가 매 주기 등록된 모든 미마감 드로우의 meta 를 HGETALL 로 다시 읽음
- **구분**: 고도화 (현재 규모에서는 불필요)
- **단계/위치**: phase-3, `AdmissionService.java:33-42`, `RedisWaitingQueueRepository.java:85-94`
- **현상**: 주기당 명령은 `SET gate` + `ZREMRANGEBYSCORE` + `ZRANGEBYSCORE active-drops` + 드로우마다 `HGETALL meta`, 그리고 열린 드로우마다 `EVALSHA admit.lua` 다. `queue:active-drops` 의 score 는 `closeAt` 이라 **아직 열리지 않은(SCHEDULED) 드로우도 매초 조회**한 뒤 `isOpen` 에서 건너뛴다. 모든 호출은 순차 왕복이다.
- **문제/기회**: 드로우를 며칠 앞서 수백 건 등록하면 매초 수백 번의 HGETALL 과 그만큼의 왕복 시간(수백 × 0.1~0.3ms ≈ 수십 ms, 추정)이 스케줄러 한 주기에 들어간다. 주기 1초보다 충분히 짧아 처리량 문제는 없다. 현재 규모(동시에 등록된 드로우 수십 건 이하, 추정)에서는 **불필요**.
- **근거**: 코드 기반 명령 수 계산. 등록 드로우 수는 E-26 의 추정(SCHEDULED 100건)을 따름.
- **개선안**:
  1. 그대로 둔다(권장).
  2. E-32 개선안 2(meta 로컬 캐시)를 넣으면 HGETALL 이 사라진다.
  3. `active-drops` 에 넣을 때 openAt 기준 ZSet(`queue:scheduled-drops`)을 하나 더 두고 오픈 시각이 지난 것만 옮긴다. 조회는 정확해지지만 키가 늘고, 옮기는 단계가 실패하면 열린 드로우가 입장 대상에서 빠지는 새 장애 경로가 생긴다.
  - 열린 드로우가 여러 개면 드로우마다 `admit.lua` 를 따로 부른다. 동시 오픈 드로우가 많아지면 파이프라인으로 묶을 수 있지만 현재 시나리오는 1~2개다.
- **우선순위**: 낮음
- **영향 범위**: (3) `RedisWaitingQueueRepository.saveMeta`·`findActiveDropIds`, `architecture.md` 5장 키 표

### E-34 잔여 정원과 무관하게 매 주기 N명에게 토큰 발급 — 소진 직전 대량 입장, 응모 완료자 재진입이 입장 슬롯을 씀
- **처리**: 개선안 1 확정 (2026-11-16 사용자 결정) — 주기당 N명 유지. 8단계 부하 테스트에서 소진 직전 SOLD_OUT 응답 수만 관찰한다.
- **구분**: 고도화 (설계 결정 사항, 4단계 연동 때 판단)
- **단계/위치**: phase-3, `admit.lua:10-21` (관련: E-31)
- **현상**:
  - `remaining > 0` 이면 남은 수와 관계없이 `ZPOPMIN N` 을 한다. 잔여 1개여도 100명이 대기열에서 빠져 토큰을 받는다. 이들은 대기열 자리를 잃고, 응모하면 4단계 Lua 에서 SOLD_OUT 으로 거절된다.
  - 토큰은 응모 성공 후에도 TTL(5분)까지 남는다. 만료되면 상태가 NOT_IN_QUEUE 가 되어, 이미 응모한 사용자가 다시 진입해 줄 끝에서 입장 슬롯 1개를 쓰고 응모 시 DUPLICATE 를 받는다.
- **문제/기회**: 정합성은 4단계 Lua 가 지키므로 오버 응모는 없다. 다만 소진 직전에는 "순서대로 1명씩"이 아니라 "N명이 동시에 경쟁"이 되어 대기열의 공정성 의미가 약해지고, 불필요한 토큰·응모 요청이 생긴다. 정원 1,000 / N=100 에서는 마지막 한 배치 정도라 영향이 작다(추정).
- **근거**: 코드 분석. 응모 후 재진입 빈도는 클라이언트 동작에 달려 있어 추정 불가.
- **개선안**:
  1. 그대로 둔다(현재 설계). 8단계에서 소진 직전 SOLD_OUT 응답 수만 관찰한다.
  2. `admit.lua` 에서 `n = min(N, remaining)` 으로 줄인다. 한 줄이고 소진 직전 대량 입장이 줄어든다. 다만 아직 응모하지 않은 토큰 보유자 수를 빼지 않으므로 여전히 초과 발급은 있다. 토큰 보유자가 응모하지 않으면 정원 채우는 속도가 느려진다. 설계 문서의 "주기당 N명"이 바뀌므로 사용자 결정.
  3. 응모 완료자 재진입 차단: queue 가 entry 의 응모 기록(`entry:{dropId}:...`)을 읽어 진입을 막거나, entry 가 응모 성공 시 토큰을 지운다. 둘 다 서비스 간 키 소유권(D3)을 넓히므로 권하지 않는다. 클라이언트가 응모 성공 후 폴링을 멈추는 것으로 충분하다.
- **우선순위**: 낮음 — 정합성 문제가 아니고 S1 규모 영향이 작다. 4단계에서 응모 Lua 와 함께 검토.
- **영향 범위**: (2) `admit.lua`, `queue-enter.md` 4장, `phase-3-queue.md` 3.3, `RedisWaitingQueueRepositoryTest`
- **8단계 관찰 결과 (2026-11-17)**: 결정대로 주기당 N명을 유지하고 소진 직전 `ENTRY_SOLD_OUT` 만 셌다.

  | 실행 | 사용자 / 정원 M / 배치 | `ENTRY_SOLD_OUT` | 정합성 |
  |------|------------------------|------------------|--------|
  | S1 | 3,000 / 1,000 / 100 | 0 | PASS |
  | X1 | 10,000 / 3,000 / 100 | 0 | PASS |
  | X2 | 10,000 / 5,000 / **1,000** | **427** | PASS |
  | X4 | 20,000 / 5,000 / **1,000** | **518** | PASS |

  - 배치 100 에서는 0건이다. 정원이 배치의 배수이고 입장자가 거의 모두 응모하면 마지막 배치가 남은 자리와 딱 맞기 때문이다(측정 조건에 기댄 결과, 정원이 배수가 아니거나 응모하지 않는 입장자가 있으면 생긴다 — 추정). 배치 1,000 에서는 마지막 배치 입장자의 약 43~52%가 "입장했는데 매진"을 본다. 초과 응모는 0 — 응모 Lua 가 정합성을 지켰다. S2(5,000 / M 100 / 배치 100)는 소진 후 `queue.admitted` 가 정확히 100 증가 후 멈췄다.
  - **정책 재검토 선택지 (결정은 사용자 몫)**:
    1. 지금 정책 유지. 배치를 키우는 운영(배치 1,000)에서만 생기는 현상으로 보고 클라이언트 문구("입장 후에도 매진될 수 있음")로 다룬다. 비용 0.
    2. 개선안 2(`n = min(N, remaining)`). 427·518 이 대부분 사라질 것으로 본다(추정 — 응모 전 토큰 보유자를 빼지 않으므로 0 은 아님). 대가: 입장자가 응모하지 않으면 정원이 다음 주기로 밀려 채우는 속도가 느려진다. `admit.lua` 한 줄 + 설계 문서 "주기당 N명" 변경.
    3. `n = min(N, remaining × α)` (α 1.1~1.2 정도 여유). 응모하지 않는 입장자를 감안해 채우는 속도와 매진 체감 사이에서 고른다. α 의 근거가 될 "입장 후 미응모 비율"을 아직 재지 않았다는 점이 약하다.

### E-35 마감 후에도 WAITING 사용자에게 `nextPollAfterMs` 1~5초를 계속 돌려줌
- **구분**: 고도화 (폴링 부하, API 상태값은 그대로)
- **단계/위치**: phase-3, `QueueService.java:40-46` (`findMeta` 결과를 쓰지 않음)
- **현상**: 마감 후에는 스케줄러가 입장을 멈추므로 남은 대기자의 순번은 더 줄지 않는다. 그런데 `status` 는 `closeAt` 을 보지 않고 WAITING + 1~5초를 돌려준다. 정원이 다 팔리면 SOLD_OUT(폴링 중단)으로 끝나지만, **정원이 남은 채 마감된 드로우**는 meta 가 만료되는 `closeAt + 1일` 까지 또는 클라이언트가 스스로 멈출 때까지 폴링이 이어진다.
- **문제/기회**: 대기자 수천 명이 남은 채 마감되면 마감 후에도 초당 수백~수천 건의 의미 없는 요청이 남는다(추정). v1.2 기본안("별도 CLOSED 상태 없음, 클라이언트는 product 로 판단")은 상태 종류를 늘리지 않으려는 결정인데, `nextPollAfterMs = 0`(폴링 중단)은 기존 응답 형식 안에서 표현할 수 있다.
- **근거**: 코드 분석. 요청 수는 추정.
- **개선안**:
  1. 마감 후(`meta.isClosed(now)`)에는 상태는 그대로 두고 `nextPollAfterMs = 0` 을 준다. 이미 읽은 meta 를 쓰므로 Redis 호출이 늘지 않는다. "0 = 폴링 중단" 계약은 이미 있다. 단, 클라이언트가 WAITING 인데 0 을 받는 경우를 화면에서 처리해야 하므로 `queue-enter.md` 3장에 한 줄 추가가 필요하다.
  2. v1.2 결정을 바꿔 `CLOSED` 상태를 추가한다. 의미가 분명하지만 상태 우선순위·응답 계약이 바뀐다(사용자 결정).
  3. 그대로 두고 8단계 시나리오에서 마감 후 폴링을 멈추게 한다. 서버 쪽 보호는 없다.
- **우선순위**: 낮음 — 비용이 작으니 다른 수정과 함께 넣을 만하다. 8단계 전에 정하면 된다.
- **영향 범위**: `QueueService.status`, `QueueServiceTest`, `queue-enter.md` 3장, `phase-3-queue.md` 3.2

### E-36 `drop.created` 처리 실패 시 기본 에러 핸들러가 즉시 10회 재시도 후 버림 — Redis 일시 장애가 드로우 전체 장애로 이어짐
- **구분**: 고도화 (장애 경로, E-29 의 수신 측 버전) — 4단계 전에 반영 권장
- **처리**: ✅ 3단계에서 반영 (2026-11-16) — 설계(architecture.md 4장·D4)대로 `KafkaConsumerConfig` 에 `DefaultErrorHandler(FixedBackOff(1000ms, 3회))`, `saveMeta` 를 `save_meta.lua` 하나로 원자화. Redis 명령 타임아웃(E-03)은 기존 백로그대로 보류.
- **단계/위치**: phase-3, `DropCreatedListener.java:18-21`, `QueueMetaRegisterService.java:19-28`, `RedisWaitingQueueRepository.java:34-42`, `application.yml` (관련: E-03, E-29)
- **현상**:
  - queue 에는 `CommonErrorHandler` 빈이 없어 Spring Kafka 기본 `DefaultErrorHandler`(`FixedBackOff(0, 9)`, 간격 없이 총 10회)가 쓰인다. Redis 연결이 몇 초만 끊겨도 10회가 바로 실패하고 ERROR 로그 후 오프셋이 커밋된다. queue 는 그 드로우의 meta 를 영영 갖지 못한다.
  - `saveMeta` 는 `HSET` → `EXPIREAT` → `ZADD active-drops` 3번의 독립 왕복이다. HSET 뒤에 실패하면 TTL 없는 meta 가 남거나, meta 는 있는데 `active-drops` 에 없어 진입은 되는데 **입장이 영영 안 되는** 상태가 된다(재전달되면 복구되지만 위 기본 핸들러는 재시도를 금방 소진한다).
  - Redis 타임아웃이 기본 60초(E-03)라, Redis 가 응답 없이 멈추면 한 번 시도에 60초를 쓴다.
- **문제/기회**: 오픈 최소 여유 1분 안에 이 경로가 실패하면 오픈 시각에 진입이 404 `QUEUE_DROP_NOT_FOUND` 가 된다. 수신 측은 멱등(덮어쓰기)이라 길게 재시도해도 안전하다.
- **근거**: Spring Kafka 문서 기준 기본값(측정 안 함). 부분 실패 상태는 코드 분석.
- **개선안**:
  1. queue 에 `DefaultErrorHandler(new ExponentialBackOff(...))` 를 둔다(예: 0.5초 시작, 최대 간격 10초, 최대 경과 5분). 파티션 1개라 재시도 동안 뒤의 `drop.created` 도 밀리지만 등록 빈도가 낮아 문제없다. 역직렬화 실패는 재시도해도 소용없으므로 not-retryable 로 둔다. 설정 몇 줄.
  2. `saveMeta` 를 `MULTI/EXEC` 또는 Lua 하나로 묶어 부분 실패 상태를 없앤다. 순서를 `ZADD active-drops` 를 마지막이 아니라 처음으로 두는 것만으로는 해결되지 않는다(스케줄러가 meta 없음으로 건너뛰므로 위험은 줄지만 TTL 없는 키 문제는 남음).
  3. E-29 개선안 1(오픈 전 자동 재발행)과 같이 두면 수신 측이 놓친 경우까지 메워진다.
  - 1+2 를 권장. E-03(Redis 타임아웃 1초)이 같이 들어가야 재시도 간격이 의미가 있다.
- **우선순위**: 중간 — 발생 빈도는 낮지만 드로우 전체 실패로 이어지고 수정 비용이 작다. 4단계 entry 도 같은 `drop.created` 를 받으므로 그때 공통 방식으로 정하면 좋다.
- **영향 범위**: queue `infrastructure/config`(Kafka 에러 핸들러), `RedisWaitingQueueRepository.saveMeta`, `DropCreatedListenerIntegrationTest`, `phase-3-queue.md` 6장, `concurrency-consistency.md` D4

### E-37 입장 스케줄러 — 한 드로우의 예외가 그 주기 전체를 멈추고, 실패가 매초 ERROR 스택으로 남음
- **구분**: 고도화 (장애 격리·로그)
- **처리**: ✅ 3단계에서 반영 (2026-11-16) — 드로우별 try/catch, 실패는 스택 없이 `log.error` 1줄. 회귀 테스트: `AdmissionServiceTest`.
- **단계/위치**: phase-3, `AdmissionService.java:36-42`, `AdmissionScheduler.java:17-20` (관련: E-03, E-10)
- **현상**: 드로우 루프에 예외 처리가 없다. 어느 드로우에서든 Redis 예외가 나면 뒤 드로우는 그 주기에 입장하지 못하고, `@Scheduled` 기본 핸들러가 ERROR + 스택 트레이스를 남긴다. Redis 장애 동안 1초마다 반복된다. 게이트는 이미 잡힌 상태라 그 주기는 다른 인스턴스도 실행하지 않는다.
- **문제/기회**: 동시 오픈 드로우가 1~2개인 지금은 영향이 작다. Redis 가 멈추면(E-03, 60초 타임아웃) 단일 스케줄러 스레드가 60초 동안 묶이고, 풀리면 `fixedRate` 가 밀린 실행을 연달아 돌린다 — 게이트가 연속 실행을 막으므로 입장이 몰리지는 않는다.
- **근거**: 코드 분석, 스케줄러 동작은 Spring 문서 기준(측정 안 함).
- **개선안**:
  1. 드로우 단위로 `try/catch` 해서 warn 한 줄(dropId, 예외 메시지) + 실패 카운터를 남기고 다음 드로우로 넘어간다. 한 드로우의 문제가 다른 드로우를 막지 않는다.
  2. 그대로 두고 E-03(Redis 타임아웃 1초)만 넣는다. 예외 전파는 그대로지만 묶이는 시간이 1초로 줄어든다.
  - 1+2 를 권장. 로그 정책은 E-10 과 맞춘다.
- **우선순위**: 낮음 — 현재 동시 오픈 드로우 수가 적다. 8단계 장애 주입 전에 넣으면 충분하다.
- **영향 범위**: `AdmissionService`, `AdmissionServiceTest`

### E-38 대기열 관측 지표가 없음 — 게이트 통과율·입장 수·대기 인원·주기 소요 시간
- **구분**: 고도화 (관측성, 8단계 준비) (관련: E-13, E-31)
- **단계/위치**: phase-3 전체. 현재는 `AdmissionService.java:41` 의 debug 로그(입장 인원) 하나뿐이다. Actuator 의존성도 아직 없다(8단계에서 추가 예정, `phase-8-load-test.md` 3.2 에 `queue.admitted` 카운터만 계획).
- **현상**: 입장 처리량이 설정과 맞는지(E-31), 대기열이 얼마나 쌓였는지, 진입이 신규인지 재진입인지 알 수 없다.
- **문제/기회**: E-31 같은 문제는 지표가 있었다면 바로 보였을 것이다. 8단계에서 "오픈 후 정원 소진 시간"을 해석하려면 입장 속도와 대기 인원 추이가 필요하다.
- **근거**: 코드 확인.
- **개선안** (8단계 계획의 "커스텀 메트릭은 infrastructure 어댑터에서" 규칙을 따름):
  - `queue.gate{result=pass|skip}` 카운터 — `RedisSchedulerGate`. 비용 0, E-31 수정 검증용.
  - `queue.admitted{dropId}` 카운터 — 8단계 계획에 있음. dropId 태그는 드로우 수만큼 시계열이 생기므로 드로우가 수십 개를 넘으면 태그를 빼야 한다.
  - `queue.admission.cycle` Timer — 주기 소요 시간(E-33 판단 근거).
  - `queue.waiting.size` 게이지 — 열린 드로우마다 `ZCARD` 가 1회씩 더 든다. 스케줄러가 이미 `admit` 을 부르므로 `admit.lua` 가 `ZCARD` 를 함께 돌려주게 하면 추가 왕복이 없다(반환 형식 변경).
  - `queue.enter{result=new|rejoin|admitted}` 카운터 — 재진입 비율(E-34 판단 근거). Lua 반환값을 구분해야 하므로 비용이 조금 있다.
  - 대안: 지표 없이 `log.info` 로 주기별 요약(드로우별 입장 수·대기 수)을 남긴다. 8단계만 보면 충분하지만 추세 그래프를 그릴 수 없다.
- **우선순위**: 중간 — 게이트 카운터와 입장 카운터는 8단계 전에 필요하다. 나머지는 낮음.
- **영향 범위**: `RedisSchedulerGate`, `RedisWaitingQueueRepository.admit`, (게이지) `admit.lua` 반환 형식, `phase-8-load-test.md` 3.2 메트릭 표

### E-39 키 설계·메모리와 Redis Cluster 비호환 — 현재 규모에서는 문제없음, Cluster 전환 시 재설계 범위
- **처리**: 지금은 하지 않음 확정 (2026-11-16 사용자 결정) — 단일 Redis 전제 유지, 해시 태그 미적용. Cluster 전환 시 아래 개선안으로 재설계.
- **구분**: 고도화 (현재 규모에서는 불필요, 문서화된 리스크의 구체화)
- **단계/위치**: phase-3, `QueueKeys.java`, `admit.lua:19`(토큰 키를 스크립트 안에서 조합), `admit.lua:10`(entry 소유 키), 4단계 응모 Lua(`queue:{dropId}:token:{userId}` 를 entry Lua 가 읽음)
- **현상·메모리**:
  - 토큰 키 1개 **72B**(측정, `MEMORY USAGE`). 드로우당 동시에 살아 있는 토큰은 최대 `N × TTL / 주기` = 100 × 300 = 3만 개 ≈ 2.2MB(계산).
  - `waiting` ZSet 10만 명 **9.7MB**(측정). S1(3,000명)은 1MB 미만(계산).
  - 만료: meta·waiting·seq 는 `closeAt + 1일`, 토큰은 자체 TTL, `active-drops` 는 스케줄러가 정리 — 마감 드로우 키가 무한히 남는 경로는 E-36 의 부분 실패(TTL 없는 meta) 외에는 없다.
  - Cluster: `admit.lua` 는 KEYS 에 선언하지 않은 토큰 키에 쓰고, `queue:1:waiting` 과 `entry:1:remaining` 은 서로 다른 슬롯이다. 4단계 응모 Lua 도 queue 토큰 키를 읽는다.
- **문제/기회**: 메모리는 정원 수만·대기 수십만 규모에서도 수십 MB 수준이라 **현재 규모에서는 손댈 이유가 없다**. Cluster 는 단일 Redis 전제가 설계 결정이고, 전환하지 않는 한 문제가 아니다.
- **근거**: 메모리는 측정 + 계산, Cluster 규칙은 Redis 문서 기준.
- **개선안** (Cluster 전환이 필요해질 때):
  1. 해시 태그로 드로우 단위 슬롯을 맞춘다: `queue:{1}:waiting`, `entry:{1}:remaining`, `queue:{1}:token:{userId}`. 같은 드로우의 키가 한 슬롯에 모이므로 미선언 키 접근은 Cluster 에서 실제로 동작한다(Redis 는 선언을 권장할 뿐, 같은 슬롯이면 실행된다). 대가: 인기 드로우 하나가 한 노드에 몰려 핫 슬롯이 되며, Cluster 의 이점이 "드로우 간 분산"으로 한정된다. 키 이름이 바뀌므로 queue·entry 를 동시에 배포해야 한다.
  2. 토큰을 드로우당 Hash 하나(`queue:{1}:tokens`, field = userId)에 담고 Redis 7.4 의 필드 TTL(`HEXPIRE`)을 쓴다. 키를 KEYS 로 선언할 수 있고 키 수가 드로우당 1개로 준다. 대가: 3만 필드 빅키(수 MB), entry 응모 Lua 의 토큰 검사가 `HEXISTS` 로 바뀌는 계약 변경, 필드 TTL 은 7.4 이상 전용.
  - 지금은 아무것도 하지 않는다(권장). 키 이름을 바꾸는 작업은 4단계가 queue 키를 읽기 시작하기 **전**이 가장 싸므로, Cluster 가능성이 조금이라도 있다면 4단계 착수 전에 1번(해시 태그)만 정하는 것을 검토한다.
- **우선순위**: 낮음 — 단일 Redis 전제가 유지되는 한 효과가 없다.
- **영향 범위**: (1) `QueueKeys`, entry 키 상수, `architecture.md` 5장 키 표, 3·4단계 문서 Lua 주석

### E-40 [명백한 버그] 유령 이벤트가 보상보다 먼저 소비되면 `isApplicant` 검사를 통과해 DB 에만 응모가 남음 — 오버 응모, 5단계 건수 비교가 영영 맞지 않음
- **구분**: 명백한 버그 (정합성, D2 의 남은 리스크 처리가 경쟁 조건을 가짐)
- **단계/위치**: phase-4, `EntryApplyService.java:41-46`(발행 실패 → 보상), `EntrySaveService.java:25-34`(검사 후 저장), `entry-compensate.lua`, `GhostEventScenarioTest`
- **현상**: 저장 컨슈머는 "Redis 응모자에 있으면 저장"(check-then-act)이고, 보상은 발행 실패를 확인한 **뒤에** 실행된다. 유령 이벤트(브로커는 기록, producer 는 실패로 판단)가 생기는 실제 순서는 대개 이렇다:
  1. t=0 판정 OK(SADD) → send
  2. 브로커가 기록·복제 완료 → 레코드가 컨슈머에 보임(수~수십 ms 안)
  3. 컨슈머: `SISMEMBER` = true(아직 보상 전) → **DB INSERT**
  4. t≈3s producer 가 `delivery.timeout.ms` 만료로 실패 판단 → 보상 Lua(`SREM` + `INCR remaining`) → 사용자에게 503
  - 결과: DB 에는 A 가 있고 Redis 응모자에는 없으며, 풀린 한 자리를 다른 사용자가 가져가 **DB 응모 수 = 정원 + 1** 이 될 수 있다. A 는 실패 응답을 받았는데 추첨 대상이 된다.
- **문제/기회**: 설계(D2)와 T4-31 은 "보상 → 그 뒤 유령 이벤트 소비" 순서만 가정한다. ack 유실형 유령 이벤트에서는 소비가 보상보다 먼저 오는 쪽이 오히려 일반적이다(보상은 최대 3초 뒤, 소비는 수십 ms). 5단계 D6 는 `SCARD == COUNT` 를 기다리는데 DB 가 더 크면 **영영 일치하지 않아 추첨이 멈춘다**. 쿠폰(6단계)은 같은 패턴을 그대로 쓰므로 사후 안전장치 없이 **초과 발급**이 된다.
- **근거**: 코드 분석 + Kafka 동작(acks=all 레코드는 HW 전진 즉시 컨슈머에 보이고, producer 응답 유실과 무관). 발생 빈도는 브로커/네트워크 장애 순간에 한정(추정: 정상 운영에서는 0, 장애 주입 시 재현 가능). T4-31 의 `ghostEventWithoutReapply` 도 테스트 publisher 가 보낸 직후 즉시 실패를 던지므로 이 창(수 ms)이 작을 뿐 존재해 드물게 실패할 수 있다(추정).
- **개선안**:
  1. **저장 확정 표시 + 보상 거절** (권장): 컨슈머가 DB INSERT 전에 Lua 로 `SISMEMBER applicants` 이면 `SADD entry:{dropId}:persisted userId` 하고 1 반환(아니면 0 → 무시). 보상 Lua 는 `persisted` 에 있으면 되돌리지 않고 "이미 기록됨"을 반환하고, `EntryApplyService` 는 이 경우 503 대신 **202(성공)** 로 응답한다(실제로 기록됐으므로 불변식 "성공 응답 = Kafka 기록"과 오히려 맞음). 대가: 키 1개(정원 크기 Set) 추가, 컨슈머 왕복 수 동일(SISMEMBER → Lua), 보상 Lua·응답 분기 변경. INSERT 가 이후 실패하면 기존 DLT 경로(Redis 에 있고 DB 에 없음)와 같다.
  2. **5단계에서 집합 대조**: D6 를 "건수 비교"에서 "Redis 응모자 ∖ DB, DB ∖ Redis 대조"로 바꾸고, DB 에만 있는 응모는 추첨 전 제외(삭제 또는 상태 표시)한다. 4단계 코드는 그대로지만 응모자 수만큼 `SMEMBERS`(정원 수만이면 수백 KB) + DB 전체 조회가 필요하고, 쿠폰에는 적용할 수 없다(사후 단계가 없음).
  3. 보상을 하지 않고 "불확실(타임아웃)"이면 판정을 유지한 채 503 을 주는 방식 — 메시지가 정말 유실됐을 때 Redis 에만 남아 D6 가 멈추므로 권장하지 않음.
  - 1 을 권장(응모·쿠폰 공통). T4-31 에 "리스너를 멈추지 않고, 소비 완료를 기다린 뒤 실패를 던지는" 변형을 추가해 재현한다.
- **우선순위**: 높음 — 정합성(오버 응모·초과 발급) 불변식 위반이고, 6단계 쿠폰이 같은 코드를 복제하기 전에 고쳐야 비용이 작다.
- **영향 범위**: `concurrency-consistency.md` D2(남은 리스크·확정 내용) + 변경 이력, `architecture.md` 5장 키 표(`entry:{dropId}:persisted`), `phase-4-entry-apply.md` 4.4·5.2, `entry-compensate.lua`, `RedisEntryGate`, `EntryGate` 포트(보상 결과 반환), `EntryApplyService`, `EntrySaveService`, `GhostEventScenarioTest`, 6단계 쿠폰 문서
- **처리**: 4단계에서 반영 (2026-11-17 사용자 결정) — 개선안 1 을 요청 ID 와 함께 채택: 판정 전에 만든 eventId 를 `entry:{dropId}:requests`(Hash) 에 기록, 저장 컨슈머는 `entry-claim.lua`(applicants·requests 확인 후 `SADD persisted`)로 `isApplicant` 확인을 대체, 보상 Lua 는 COMPENSATED / ALREADY_PERSISTED / NOT_OURS 3결과, ALREADY_PERSISTED 면 202. 설계 D2 v1.3, `phase-4-entry-apply.md` v1.5 (T4-36~38·40·42, 커밋 #10a~c). 쿠폰은 6단계에서 같은 방식 적용 여부를 확정한다.

### E-41 [명백한 버그(장애 경로)] Redis 응답 타임아웃 시 판정만 되고 발행·보상이 모두 빠짐 — 응답은 500, 5단계 건수 비교가 영영 맞지 않음
- **구분**: 명백한 버그 (장애 경로) + 응답 코드 정의 누락 (카리나 후보 4)
- **단계/위치**: phase-4, `EntryApplyService.java:32`, `RedisEntryGate.tryApply`, `GlobalExceptionHandler.handleUnexpected`, `application.yml`(`spring.data.redis.timeout: 1s`)
- **현상**: `tryApply` 에서 `RedisConnectionFailureException`/`QueryTimeoutException` 이 나면 그대로 올라가 catch-all 이 **500 `INTERNAL_ERROR` + ERROR 스택**을 남긴다. 명령 타임아웃(1초)은 "Lua 가 실행됐는지 모름"을 뜻한다 — Redis 가 느려서 Lua 는 실행됐고 응답만 늦은 경우, `applicants` 에 들어가고 정원이 차감됐지만 Kafka 발행도 보상도 없다.
- **문제/기회**: 그 사용자는 Redis 에만 있고 DB 에는 영영 없다 → D6 `SCARD > COUNT` 가 유지돼 **추첨이 멈춘다**(DLT 처럼 재처리할 메시지도 없음). 사용자가 다시 누르면 409 `DUPLICATE`, `GET /drops/{id}/me` 는 계속 "저장 대기"(pendingSave)를 보여 준다. 또 설계 에러 표에 없는 500 이라 클라이언트가 재시도 가능 여부를 알 수 없다. 오픈 순간 Redis 지연(피크 부하)에서 가장 일어나기 쉬운 경로다.
- **근거**: 코드 분석. Lettuce 는 한 연결에서 명령 순서를 보장하므로, 타임아웃 뒤 같은 연결로 보낸 보상 Lua 는 늦게 도착한 판정 Lua **뒤에** 실행된다(Lettuce 동작 기준, 측정 안 함).
- **개선안**:
  1. `EntryApplyService` 가 `tryApply` 의 인프라 예외를 잡아 **보상 Lua 를 한 번 실행**(멱등: 실행 안 됐으면 `SREM` 0 → 아무것도 안 함)하고 503 `ENTRY_TEMPORARILY_FAILED` 를 준다. 포트 계약상 어댑터가 `DataAccessException` 을 포트 예외(예: `GateUnavailableException`)로 바꿔 던지게 한다(application 이 Spring Data 예외에 의존하지 않도록). 보상도 실패하면 기존처럼 log.error — 이때는 E-40/D6 의 운영 대응 대상.
  2. 1 + D6 에 "Redis 에만 있는 응모" 대조 단계를 둔다(E-40 개선안 2 와 같은 작업) — 보상까지 실패한 극단 경우까지 메운다. 비용이 크므로 5단계에서 판단.
  - 공통: `GlobalExceptionHandler` 또는 포트 예외로 Redis 장애를 503 에 매핑하고, 설계 에러 표(`entry-apply.md`)에 행 추가. 503 은 스택 없이 warn 1줄(E-47).
- **우선순위**: 높음 — 피크 시 Redis 지연이라는 현실적인 조건에서 추첨 정지로 이어지고, 수정은 서비스 한 곳 + 예외 매핑이다.
- **영향 범위**: `EntryApplyService`, `RedisEntryGate`, `EntryGate` 포트(예외 계약), `GlobalExceptionHandler`, `EntryApplyServiceTest`(Gate 예외 케이스), `docs/design/.../entry-apply.md` 에러 표, `concurrency-consistency.md` D2 남은 리스크
- **처리**: 4단계에서 반영 (2026-11-17 사용자 결정) — 개선안 1 채택: `RedisEntryGate` 가 `DataAccessException` 을 `GateUnavailableException`(application/port)으로 감싸고, `EntryApplyService` 가 같은 eventId 로 보상을 시도한 뒤 503 `ENTRY_TEMPORARILY_FAILED`(E-40 의 `requests` 덕분에 Lua 가 실행되지 않았으면 NOT_OURS 로 무변화). 보상까지 실패한 경우는 리스크로 남기고 개선안 2(대조)는 5단계에서 판단. 503 로그 정책(E-47)은 별도. 설계 D2 v1.3, `phase-4-entry-apply.md` v1.5 (T4-39·41, 커밋 #10a·b).
- **처리(개선안 2)**: 만들지 않음 (2026-11-17 사용자 결정, D5-1) — 건수 불일치 장기화는 WARN 후 관리자 수동 추첨(DB 기준)으로 해소하며, Redis 에만 남은 응모는 제외된다.

### E-42 응모 Redis 키(`entry:{dropId}:meta/remaining/applicants/seq`)에 TTL 이 없음
- **구분**: 고도화 (메모리·운영) (관련: E-39, E-36)
- **단계/위치**: phase-4, `entry-initialize.lua`, `entry-apply.lua`(seq 는 첫 INCR 시 생성), `EntryRedisKeys`
- **현상**: 4개 키 모두 만료가 없다. queue 키는 `closeAt + 1일` 로 만료된다(E-39).
- **문제/기회**: 드로우가 끝나도 키가 영구히 남는다. 메모리: `applicants` 는 512개를 넘으면 intset → hashtable 로 바뀌어 원소당 약 50~70B(추정) — 정원 1만이면 약 0.6MB, 10만이면 약 6MB. 드로우 수백 개가 쌓이면 수백 MB~GB 로 커지고, AOF 재작성 시간도 늘어난다. 단, **5단계 추첨이 `applicants`(SCARD)와 meta(state 변경)를 쓰므로 `closeAt` 직후 만료시키면 안 된다**. 추첨이 지연되면(E-40/E-41 로 D6 대기) TTL 이 먼저 끝나 건수 비교 근거가 사라질 위험이 있다.
- **근거**: 키 구조는 코드 확인, 메모리는 Redis 인코딩 규칙 기준 계산(추정, `MEMORY USAGE` 측정 안 함).
- **개선안**:
  1. `entry-initialize.lua` 에서 4개 키에 `EXPIREAT closeAt + 7일`(seq 는 생성 시점이 apply 라 initialize 에서 `SET seq 0` 으로 미리 만들고 같이 만료). 단순하지만 추첨이 7일 넘게 멈추면 근거가 사라진다(그 전에 알림이 떠야 함).
  2. 5단계 추첨 완료(`DRAWN`) 시 추첨 서비스가 `EXPIRE` 1일을 건다 — 추첨이 끝난 드로우만 정리돼 안전하지만, 추첨이 실패한 드로우 키는 남는다. 1 을 안전망(예: closeAt + 30일)으로 같이 두면 둘 다 커버.
  - 2 + 긴 안전망 TTL 을 권장. 현재 규모(드로우 수십 개, 정원 수천~수만)에서는 수십 MB 이하라 급하지 않다.
- **우선순위**: 낮음 — 지금 규모에서는 메모리 문제가 없다. 5단계에서 추첨 완료 처리와 함께 정하면 된다.
- **영향 범위**: `entry-initialize.lua`, 5단계 추첨 서비스·문서, `architecture.md` 5장 키 표(TTL 열)
- **처리**: 5단계 반영 예정 (2026-11-17, 기본안) — 개선안 2: 추첨 커밋 후 6개 키(requests·persisted 포함) EXPIRE 1일 (`phase-5-draw.md` D5-4, T5-30·31). 안전망 TTL(개선안 1)은 apply/claim Lua 마다 EXPIREAT 가 필요해 보류, 이 항목에 남긴다.

### E-43 응모 응답 경로에 Kafka 동기 대기(최대 3.5초)가 들어가 Kafka 지연 시 Tomcat 스레드가 고갈됨
- **구분**: 고도화 (응답 경로·피크 병목) (카리나 후보 3)
- **단계/위치**: phase-4, `KafkaEntryEventPublisher.java`(`get(3500ms)`), `application.yml`(`delivery.timeout.ms 3000`, `linger.ms 0`)
- **현상**: 정상 시 응답 = Redis Lua 1회 + acks=all 왕복(로컬 단일 브로커 수 ms, 추정). Kafka 가 느리거나 리더 선출 중이면 요청마다 최대 3.5초 플랫폼 스레드를 잡는다.
- **문제/기회**: Tomcat 기본 200 스레드 / 3.5초 ≈ **초당 57건**으로 처리량이 떨어지고(계산), 같은 인스턴스의 조회 API·헬스체크까지 밀린다. 입장 N=100명/초 기준이면 Kafka 장애 몇 초 동안 응모 요청이 큐에 쌓이고 클라이언트 타임아웃 → 재시도로 부하가 증폭된다. 단, 동기 대기 자체는 D2 불변식("성공 응답 = Kafka 기록")의 대가라 없앨 수 없다.
- **근거**: 계산(추정), 정상 지연은 측정 안 함(8단계에서 측정).
- **개선안**:
  1. **가상 스레드**(`spring.threads.virtual.enabled: true`, Java 21): 블로킹 대기가 플랫폼 스레드를 잡지 않아 동시 대기 수 제한이 사실상 사라진다. 설정 1줄. 대가: Hikari·Lettuce 풀이 실제 상한이 되므로 그쪽 풀 크기 확인 필요, `synchronized` 핀닝(Java 21) 점검.
  2. `delivery.timeout.ms` 를 줄인다(예: 1500 / await 2000). 실패가 빨라지는 대신 일시 지연에도 503 이 늘어난다.
  3. 응모 엔드포인트 동시 실행 수 제한(세마포어 bulkhead)으로 다른 API 를 보호. 초과 시 즉시 503.
  - `linger.ms 0` 은 지금 규모에서 맞다. 초당 수천 건 이상이면 `linger.ms 1~2` 가 배치 효율로 오히려 p99 를 낮출 수 있으나 8단계 측정 후 판단.
  - 1 을 권장(비용 최소). 8단계 부하 테스트에서 Kafka 지연 주입으로 확인.
- **우선순위**: 중간 — 정상 시에는 문제없고 장애 시 증폭 위험. 8단계 전에 1 정도면 충분.
- **영향 범위**: `application.yml`(entry, 6단계 coupon 동일), `phase-8-load-test.md` 시나리오

### E-44 내 응모 목록 — 정렬 동률 순서 미정, 페이지네이션 없음 (현재 규모에서는 불필요)
- **구분**: 고도화 (조회) (카리나 후보 1)
- **단계/위치**: phase-4, `EntryJpaRepository.findAllByUserIdOrderByAppliedAtDesc`, `V1__create_entry_tables.sql`(`idx_entries_user_applied (user_id, applied_at DESC)`)
- **현상**: `user_id` 로 전부 읽고 `applied_at DESC` 정렬. 인덱스가 정렬까지 커버하므로 정렬 비용은 없다(EXPLAIN 측정 안 함, 인덱스 구조상 추정). 엔티티 전체 컬럼 조회, N+1 없음.
- **문제/기회**: 사용자당 응모는 드로우당 1건이라 수십~수백 건이 상한 — **현재 규모에서는 페이지네이션·커버링 인덱스 불필요**. `applied_at` 은 서버 시각(ms 이하 정밀도)이라 동률은 드물지만, 동률이면 페이지 간 순서가 흔들릴 수 있어 페이지네이션을 넣는 순간 문제가 된다.
- **근거**: 응모 구조상 계산.
- **개선안**:
  1. 지금은 정렬에 `id DESC` 타이브레이커만 추가(인덱스 변경 불필요, 결과 건수가 작아 추가 정렬 비용 무시 가능).
  2. 목록이 커지면 keyset 페이징 `(applied_at, id) < (?, ?) LIMIT n` + 인덱스 `(user_id, applied_at DESC, id DESC)` 로 교체. offset 페이징은 권장하지 않음(E-27 와 같은 이유).
- **우선순위**: 낮음 — 현재 규모에서는 불필요. 1 은 비용이 거의 없어 5단계 결과 조회 확장 시 함께 넣으면 된다.
- **영향 범위**: `EntryJpaRepository`, (2 선택 시) Flyway V2, API 문서

### E-45 `drop.created` 처리 실패 시 1초×3회 후 DLT — Redis 수 초 장애로 드로우 전체가 응모 불가(CLOSED)
- **구분**: 고도화 (장애 복구) (관련: E-36, E-29)
- **단계/위치**: phase-4, `KafkaConsumerConfig`(`FixedBackOff(1000, 3)` 를 `drop.created`·`entry.created` 공통 적용), `DropSnapshotRegisterService`
- **현상**: entry 는 queue(E-36)와 달리 초기화가 Lua 하나로 원자적이라 부분 실패 상태는 없다(좋음). 그러나 Redis 가 3~4초 이상 응답하지 않으면 재시도를 소진하고 `drop.created.DLT` 로 간다. 그 드로우는 meta 가 없어 응모 Lua 가 모든 요청에 `CLOSED` 를 돌려준다. 복구는 사람이 DLT 를 재처리하거나 재발행 API 를 호출해야 한다.
- **문제/기회**: 등록과 오픈 사이(최소 여유 1분) 에 이 경로가 실패하면 오픈 시각에 응모가 전부 거절된다. 수신 측이 멱등(`ON CONFLICT DO NOTHING`, `EXISTS` NX)이라 길게 재시도해도 안전하다. `entry.created` 는 반대로 lag 을 키우지 않도록 짧은 재시도가 맞으므로 정책을 토픽별로 나눠야 한다.
- **근거**: 코드 분석(측정 안 함).
- **개선안**:
  1. `drop.created` 리스너 전용 컨테이너 팩토리에 `ExponentialBackOff`(예: 0.5초 시작, 최대 10초, 최대 경과 5분) 후 DLT. `entry.created` 는 현행 유지. 대가: 팩토리 빈 1개 추가.
  2. 현행 유지 + E-29 개선안 1(오픈 전 자동 재발행)로 메운다.
  - 1 을 권장하고 queue(E-36)와 같은 방식으로 맞춘다.
- **우선순위**: 중간 — 빈도는 낮지만 드로우 전체 실패, 수정 비용 작음.
- **영향 범위**: `KafkaConsumerConfig`, `DropCreatedRedeliveryTest`, `architecture.md` 4장 실패 정책 표

### E-46 저장 컨슈머가 레코드마다 Redis 1왕복 + DB 1트랜잭션 — 현재 규모에서는 불필요, 정원 수십만이면 배치 리스너
- **구분**: 고도화 (Kafka·DB 처리량, 현재 규모에서는 불필요)
- **단계/위치**: phase-4, `EntryCreatedListener`, `EntrySaveService.save`, `EntryRepositoryImpl.saveIfAbsent`(native INSERT, 건당 커밋)
- **현상**: 레코드 1건 = `SISMEMBER` 1회 + INSERT 1회 + 커밋 1회. 파티션 6, 인스턴스 2 × concurrency 3.
- **문제/기회**: 건당 약 2~5ms(추정)면 컨슈머 6개 합계 초당 1,000~3,000건. 정원 수만이 소진되는 수십 초 동안 lag 이 잠시 생겨도 마감 전 따라잡으므로 **현재 규모에서는 문제없다**. lag 은 D6 추첨 대기 시간과 `pendingSave` 노출 시간으로만 나타난다.
- **근거**: 계산(추정). 8단계에서 consumer lag 측정 필요.
- **개선안**:
  1. 배치 리스너(`max.poll.records` 수백) + `SMISMEMBER` 1회 + 다중 행 `INSERT ... ON CONFLICT DO NOTHING` 1회. 왕복이 수백 분의 1로 줄어든다. 대가: 배치 중 한 건 실패 시 재처리 단위가 배치 전체(멱등이라 안전), DLT 로 보낼 레코드 식별이 복잡해짐(`BatchListenerFailedException`), E-40 개선안 1 을 넣으면 Lua 도 배치 형태로 바꿔야 함.
  2. 파티션·concurrency 증설 — 코드 변경 없음, 파티션 수 변경은 키 순서 보장에 영향(같은 사용자 레코드 순서) 있으나 `ON CONFLICT` 로 무해.
- **우선순위**: 낮음 — 현재 규모에서는 불필요. 8단계에서 lag 이 마감 시각을 넘길 때만 검토.
- **영향 범위**: `EntryCreatedListener`, `EntrySaveService`, `EntryRepositoryImpl`, `KafkaConsumerConfig`

### E-47 응모 관측 지표 없음, 발행 실패 503 이 요청마다 ERROR 스택으로 남음
- **구분**: 고도화 (관측성·로그, 8단계 준비) (관련: E-38, E-10)
- **단계/위치**: phase-4, `GlobalExceptionHandler.handleApplication`(status ≥ 500 이면 스택 포함 error), `EntryApplyService.compensateQuietly`, `EntrySaveService`(유령 무시 warn)
- **현상**: 판정 결과별 건수, 발행 지연, 보상 횟수, 유령 이벤트 무시 횟수를 볼 수단이 로그뿐이다. Kafka 장애 시 503 `ENTRY_TEMPORARILY_FAILED` 가 요청마다 원인 예외 스택과 함께 ERROR 로 찍힌다(초당 수백 건이면 로그 폭주, 디스크·수집 비용).
- **문제/기회**: 8단계 "정원 소진 시간", E-34 결정의 `SOLD_OUT` 응답 수 관찰, E-40/E-41 발생 여부 확인에 지표가 필요하다.
- **근거**: 코드 확인.
- **개선안**:
  1. Micrometer: `entry.apply{outcome=OK|CLOSED|NO_TOKEN|DUPLICATE|SOLD_OUT|FAILED}` 카운터(infrastructure 어댑터 또는 presentation), `entry.publish` Timer, `entry.compensate{result}` 카운터, `entry.save.ghost_skipped` 카운터. 태그에 dropId 는 넣지 않는다(시계열 수).
  2. 503 은 warn 1줄(코드 + 원인 메시지), 스택은 샘플링하거나 debug 로. 보상 실패(`log.error`)만 스택 유지.
- **우선순위**: 중간 — 8단계 전에 필요. 로그 정책(2)은 비용이 작아 E-41 과 함께 넣을 만하다.
- **영향 범위**: `GlobalExceptionHandler`, `EntryApplyService`(또는 어댑터), `phase-8-load-test.md` 3.2 메트릭 표

---

## phase-5 (추첨: entry-service · product-service) 분석 — 2026-11-17

분석 범위: `git diff dev...HEAD` (`feat/phase-5-draw`, 커밋 9개). 기준 문서 `phase-5-draw.md` v1.13 6장. **Gradle·EXPLAIN 실행 안 함**(동시 빌드 중) — 쿼리 계획은 스키마·인덱스 구조 기준 추정이다.
- E-30(markDrawn) 은 문서대로 반영됨(`DropService.markDrawn`: SELECT 1 + UPDATE 1, 드로우당 1회라 문제없음). E-41 개선안 2 는 만들지 않음, E-42 는 개선안 2 로 반영됨(`RedisEntryGate.expireAfterDraw`, 6개 키 파이프라인 EXPIRE 1왕복) — 확인 완료.

### E-48 [명백한 버그(정합성, 운영 경로)] 수동 추첨 뒤 DLT 재처리·지연 저장이 오면 DRAWN 드로우에 `APPLIED` 응모가 영구히 남음
- **구분**: 버그 성격 (정합성 — 운영 절차 경로) (관련: E-41, D5-1)
- **단계/위치**: phase-5, `EntrySaveService.java:25-34`, `EntryJpaRepository.insertIfAbsent`, `DrawService.drawManually`
- **현상**: 저장 컨슈머는 `claimForSave`(Redis `applicants`·`requests` 확인)만 통과하면 드로우 상태와 무관하게 `APPLIED` 로 INSERT 한다. 수동 추첨은 건수 비교 없이 DB 기준으로 추첨하므로, 그 시점에 DLT 에 머물던(또는 lag 중인) `entry.created` 가 **추첨 후** 처리되면 — 키 TTL(1일) 안이라 claim 은 성공 — `entries` 에 `APPLIED` 행이 생긴다. 추첨은 다시 돌지 않으므로 그 행은 영영 `WON/LOST` 가 되지 않는다.
- **문제/기회**: 사용자는 `GET /drops/{id}/me` 에서 "응모 완료, 결과 없음"을 계속 보고, `draw_results.applicant_count` 와 `COUNT(entries)` 가 어긋난다. 6장 리스크는 "수동 추첨에서 빠진다"까지만 적었고 "빠진 응모가 APPLIED 로 남는다"는 다루지 않는다. 운영 절차(DLT 먼저 재처리)를 어기거나, 수동 추첨 직후 컨슈머 lag 이 해소되면 바로 일어난다. 자동 추첨 경로는 `SCARD == COUNT` 일 때만 돌아 이 문제가 없다.
- **근거**: 코드 분석. 키는 추첨 후 1일 유지되므로 그 안의 재처리는 claim 이 true(추정 아님, `entry-claim.lua` 가 state 를 보지 않음). 1일 뒤 재처리는 claim false → 유령 무시로 조용히 사라진다(이건 의도와 일치).
- **개선안**:
  1. INSERT 를 스냅샷 행과 직렬화: `INSERT ... SELECT ... WHERE EXISTS (SELECT 1 FROM drop_snapshots WHERE drop_id = :dropId AND status <> 'DRAWN' FOR SHARE)`. 추첨 트랜잭션이 첫 문장에서 같은 행을 배타 잠금하므로 READ COMMITTED 에서도 추첨 중 INSERT 는 대기 후 DRAWN 을 보고 0건이 된다. 0건이면 warn + 건너뜀(유령과 같은 처리). 대가: 저장 쿼리마다 스냅샷 행 공유 잠금(추첨 중이 아니면 경합 없음).
  2. DRAWN 이면 `LOST` 로 넣는다 — 사용자에게 결과를 보여 주지만 "추첨에 참여하지 않았는데 낙첨"이라 의미가 틀리고 `applicant_count` 와도 맞지 않는다. 권하지 않는다.
  3. 문서만: 6장 리스크·3.4 운영 절차에 "수동 추첨 후에는 해당 dropId 의 DLT 를 재처리하지 않는다"를 적는다. 코드 0줄이지만 lag 경로는 못 막는다.
  - 1 + 3 권장.
- **우선순위**: 중간 — 수동 추첨이라는 드문 경로지만 발생하면 사용자 노출 상태가 영구히 틀린다. 수정은 native 쿼리 1개 + 테스트.
- **영향 범위**: `EntryJpaRepository.insertIfAbsent`, `EntrySaveService`, `phase-5-draw.md` 3.4·6장, `draw.md`(수동 추첨 절), `concurrency-consistency.md` D6
- **처리 (2026-11-17, 사용자 결정)**: 개선안 2 변형 채택 — 추첨이 끝난 드로우면 `LOST` 로 저장 (APPLIED 영구 잔류 방지, 202 받은 응모의 기록 유지). 스냅샷 행 FOR SHARE 로 추첨과 직렬화. 5단계에서 반영 (phase-5-draw.md v1.14).
  - 남은 측정 항목 (신민아 MINOR): 피크 시 저장마다 같은 스냅샷 행 FOR SHARE → MultiXact·WAL 비용, 마감 시각 `markClosingIfOpen` UPDATE 가 공유 잠금 연속으로 밀리는지. 동시 잠금 수는 컨슈머 동시성(3 × 인스턴스)으로 제한 — 8단계 부하 테스트에서 확인.

### E-49 Kafka 장애 시 스케줄러 1주기가 `미발행 건수 × 3.5초` 로 늘어나 다른 드로우의 마감·추첨이 밀림
- **구분**: 고도화 (응답 경로 아님 — 스케줄러 지연·장애 격리)
- **단계/위치**: phase-5, `DrawService.java:140-145`(`republishUnpublished`), `publishSafely`, `DrawScheduler.run`, `KafkaDrawEventPublisher`(`get(3500ms)`)
- **현상**: Spring 기본 TaskScheduler 는 스레드 1개이고 `fixedDelay` 라 한 주기 안에서 마감 → 추첨 → 재발행이 순차 실행된다. Kafka 가 내려가면 재발행 루프가 미발행 건마다 최대 3.5초(메타데이터 미캐시면 `max.block.ms` 1초) 대기하고 실패해도 다음 건으로 계속 간다. 추첨 직후 `afterCommit` 발행도 같은 대기를 한다.
- **문제/기회**: 미발행 10건이면 주기가 5초 → 약 40초(계산). 그 동안 새로 마감된 드로우의 `CLOSING` 전이·추첨이 늦어진다. 응모 차단은 Lua 의 closeAt 검사라 **정합성 영향은 없고** 결과 공개만 지연된다. 현재 규모(동시 마감 드로우 수 개)에서는 수십 초 지연에 그친다.
- **근거**: 계산(추정), 측정 안 함.
- **개선안**:
  1. 재발행 루프에서 첫 `EventPublishException` 이 나면 그 주기의 재발행을 중단(break) — 1건만 기다리고 다음 주기에 다시 시도. 코드 수 줄. 대가: 특정 메시지만 실패하는 경우(직렬화 등) 뒤 건이 막힌다 → 그 건을 마지막으로 돌리거나 실패 건을 건너뛰는 카운트를 둔다.
  2. 재발행을 별도 `@Scheduled` 메서드로 분리하고 `spring.task.scheduling.pool.size: 2` — 마감·추첨이 재발행에 막히지 않는다. 대가: 스케줄러 스레드 2개, 같은 메서드 중복 실행은 여전히 없음(메서드별 fixedDelay).
  - 1 권장(비용 최소). 2 는 다른 스케줄 작업이 늘면 함께.
- **우선순위**: 낮음 — 정합성 무관, 장애 중 결과 공개 지연뿐.
- **영향 범위**: `DrawService.republishUnpublished`, (2) `DrawScheduler`, `application.yml`

### E-50 DLT 재발행 producer 가 `max.block.ms` 1초를 그대로 물려받아 첫 DLT 전송이 메타데이터 대기로 실패할 수 있음
- **구분**: 고도화 (장애 경로 신뢰성) (카리나 메모 1, 관련: E-45)
- **단계/위치**: phase-5, `entry-service .../KafkaConsumerConfig.java:40,55`, `product-service .../KafkaConsumerConfig.java:55,98`, 두 서비스 `application.yml`(`max.block.ms: 1000`)
- **현상**: bytes 템플릿은 `producerFactory.getConfigurationProperties()` 를 복사하므로 `max.block.ms 1000`·`delivery.timeout.ms`(entry 3000, product 10000)를 그대로 쓴다. JSON 레코드의 DLT 는 서비스 공용 `KafkaTemplate` 을 쓴다. `{topic}.DLT` 는 평소 한 번도 쓰지 않는 토픽이라 producer 메타데이터 캐시에 없다 → 첫 DLT 전송이 메타데이터를 1초 안에 못 받으면 `send()` 가 동기로 실패한다. 5단계 중 테스트에서 같은 현상을 확인했다(커밋 `f6d15eb`, 테스트만 `KafkaTopicReadiness` 로 우회, 운영 설정은 그대로).
- **문제/기회**: recoverer 가 던지면 `DefaultErrorHandler` 는 레코드를 다시 seek 하므로 **유실은 없다**(추정 — 재전달 후 재시도 소진 상태라 바로 recoverer 를 다시 부르고, 그때는 메타데이터가 캐시돼 성공). 대가는 ERROR 로그 + 1회 추가 재처리이고, 브로커가 실제로 느린 상황(DLT 가 필요한 바로 그 상황)에서는 반복될 수 있다. DLT 전송은 컨슈머 스레드에서만 일어나 응답 경로와 무관하므로 1초 상한을 둘 이유가 약하다.
- **근거**: 코드 + 5단계 테스트 실패 이력. 재처리 동작은 Spring Kafka `DefaultErrorHandler`/`FailedRecordTracker` 동작 기준 추정.
- **개선안**:
  1. DLT 전용 producer factory 를 bytes·JSON 둘 다 만들고 `max.block.ms` 를 5~10초로 덮어쓴다(설정 복사 후 `put`). 응답 경로 템플릿은 그대로. 대가: 비빈 producer 1개 추가(지금 bytes 와 같은 수명 관리).
  2. 기동 시 `ApplicationRunner` 로 DLT 토픽들에 `partitionsFor` 를 호출해 메타데이터를 미리 받는다(테스트 우회와 같은 방법). 코드 작음, 다만 메타데이터는 `metadata.max.idle.ms`(기본 5분) 동안 안 쓰면 캐시에서 빠져 장시간 운영 후에는 효과가 사라진다.
  - 1 권장. 6단계 coupon 이 같은 설정을 복제하기 전에 넣으면 비용이 작다.
- **우선순위**: 낮음~중간 — 유실은 없고 로그·지연뿐. 복제 전에 처리하면 한 번에 끝난다.
- **영향 범위**: entry·product `KafkaConsumerConfig`, (6단계) coupon 동일, `architecture.md` 4장 실패 정책 표 비고

- **처리 (coupon)**: 6단계에서 coupon `KafkaConsumerConfig` 는 DLT 재발행 전용 producer(`max.block.ms` 5s)로 시작 (phase-6 v1.4). entry·product 는 남음.

### E-51 추첨 쿼리 계획과 인덱스 — 현재 규모에서는 불필요, 응모자 수십만이면 커버링·부분 인덱스
- **구분**: 고도화 (조회 최적화, 현재 규모에서는 불필요) (관련: E-26)
- **단계/위치**: phase-5, `DropSnapshotJpaRepository.findDueOpenIds`·`findByStatus`, `EntryJpaRepository.countByDropId`·`findUserIdsOrderBySeq`·`markWon`·`markRemainingLost`, `V1__create_entry_tables.sql`, `V2__create_draw_results.sql`
- **현상** (EXPLAIN 미측정, 인덱스 구조 기준 추정):
  - `findDueOpenIds`/`findByStatus(CLOSING)`: `drop_snapshots.status` 인덱스 없음 → 5초마다 Seq Scan 2회. 행 수 = 누적 드로우 수(수십~수백)라 수 µs~ms.
  - `countByDropId`: `uk_entries_drop_user` 또는 `uk_entries_drop_seq` 로 Index Only Scan 가능하지만, 마감 직후에는 INSERT 직후라 visibility map 이 안 채워져 heap fetch 가 섞인다. CLOSING 동안 5초마다 1회.
  - `findUserIdsOrderBySeq`: `uk_entries_drop_seq (drop_id, entry_seq)` 순서로 읽어 정렬은 없고, `user_id` 때문에 heap 을 행마다 읽는다. 드로우당 1회.
  - `markWon`: 1,000개 IN 청크 × ⌈K/1000⌉ 문장, `uk_entries_drop_user` 사용. `markRemainingLost`: M−K 행 UPDATE. `status`·`updated_at` 은 인덱스에 없지만 INSERT 로 꽉 찬 페이지(fillfactor 100)라 HOT 갱신이 대부분 안 돼 3개 인덱스 모두에 새 항목이 생긴다(추정).
  - `draw_results`: `findByPublishedFalse` 는 부분 인덱스, `findByDropId` 는 UK — 문제없음.
- **문제/기회**: M=1만이면 전부 수십 ms 이하로 **현재 규모에서는 최적화 불필요**. M=10만이면 LOST 갱신 약 10만 행 + 인덱스 30만 항목 + WAL 수십 MB, 1~수 초(추정)로 추첨 트랜잭션이 길어지고 테이블·인덱스 블로트가 응모 행 수만큼 생긴다.
- **근거**: 스키마·쿼리 대조(추정). 8단계 또는 응모자 10만 시나리오에서 `EXPLAIN (ANALYZE, BUFFERS)` 로 확인 필요.
- **개선안**:
  1. `uk_entries_drop_seq` 를 `(drop_id, entry_seq) INCLUDE (user_id)` 로 — `findUserIdsOrderBySeq` 가 Index Only Scan. 대가: 인덱스 크기 증가, 응모 INSERT 비용 소폭 증가.
  2. `drop_snapshots` 부분 인덱스 `(status, close_at) WHERE status <> 'DRAWN'` — 누적 드로우가 수만 개가 되기 전에는 의미 없음.
  3. LOST 를 저장하지 않고 "DRAWN 드로우의 APPLIED = 낙첨"으로 해석 — UPDATE M−K 행이 사라지지만 상태 의미가 바뀌어 조회·설계(`erd.md`, 응모 상태 enum)를 고쳐야 한다. 권하지 않는다.
  4. `markWon` 을 `user_id = ANY(:ids)` 배열 파라미터 1문장으로 — 문장 수만 줄고 효과 미미.
  - 지금은 아무것도 하지 않고 8단계 측정 후 1 정도만 검토.
- **우선순위**: 낮음 — 현재 규모에서는 불필요.
- **영향 범위**: Flyway V3(선택 시), `EntryJpaRepository`, `erd.md`

### E-52 응모자 전원을 메모리에 올려 셔플 — 현재 규모에서는 불필요, 알고리즘 교체는 시드 재현성과 충돌
- **구분**: 고도화 (메모리, 현재 규모에서는 불필요) (6장 리스크 1번 구체화)
- **단계/위치**: phase-5, `Drawer.draw`, `DrawTransaction.execute` ②③
- **현상**: `List<Long>`(박싱) M개 + 복사본 M개 + PG 드라이버 결과 버퍼(fetchSize 미지정이라 전체 적재) + `Set.copyOf` K개.
- **문제/기회**: 원소당 약 16B(Long)+참조 4~8B, 버퍼 포함 M=10만이면 약 5~10MB, M=100만이면 50~100MB(계산, 추정) — 수백만부터 힙 압박. 현재 규모(수천~수만)에서는 1MB 내외로 **불필요**.
- **근거**: 계산(추정).
- **개선안**:
  1. 부분 Fisher–Yates(K회 스왑만) 또는 `long[]` 원시 배열로 메모리·시간을 K 비례로. **주의**: `Collections.shuffle(list, new Random(seed))` 와 결과가 달라져 기존 `draw_results.seed` 로 재현이 안 된다 → `draw_results` 에 알고리즘 버전 컬럼을 두고 버전별로 재현해야 정합성(재현 가능성)을 지킨다.
  2. 복사본 제거(`findUserIdsOrderBySeq` 결과가 이미 새 리스트라 Drawer 에서 복사하지 않음) — 메모리 절반이지만 도메인 함수의 "입력 불변" 계약을 깬다. 권하지 않는다.
  - 응모자 수십만 요구가 생길 때만 1 을 버전 컬럼과 함께.
- **우선순위**: 낮음 — 현재 규모에서는 불필요.
- **영향 범위**: `Drawer`, `DrawTransaction`, (1) Flyway·`draw.md` 재현 절차

### E-53 추첨 관측 지표 없음, 장기 CLOSING 경고가 5초마다 반복
- **구분**: 고도화 (관측성, 8단계 준비) (관련: E-38, E-47)
- **단계/위치**: phase-5, `DrawService.java:89`(`draw waiting too long`), `publishSafely`, `closeGateQuietly`, `afterCommit`
- **현상**: 마감→추첨 소요, 추첨 트랜잭션 시간, 미발행 건수, 건수 불일치 지속 시간을 로그로만 본다. 임계(5분)를 넘긴 CLOSING 드로우는 이후 **매 주기 WARN** — 드로우 1개당 시간당 720줄. Kafka 장애 중에는 재발행 실패 WARN(스택 포함)도 주기마다 건수만큼 찍힌다.
- **문제/기회**: 수동 추첨(D5-1) 판단 근거가 이 WARN 이라 사람이 보기 쉬워야 하는데, 반복 로그는 묻힌다. 8단계 "마감 후 결과 공개까지 시간" 측정에도 지표가 필요하다.
- **근거**: 코드 확인.
- **개선안**:
  1. Micrometer: `draw.closing.drops`(gauge, 현재 CLOSING 수), `draw.unpublished`(gauge), `draw.execute`(Timer, outcome 태그), `draw.lag`(마감→DRAWN 소요 DistributionSummary). dropId 태그 금지.
  2. 장기 CLOSING WARN 을 임계 최초 통과 1회 + 이후 N분마다(예: `closingTooLong` 경과 시간을 임계 배수로 나눠 경계에서만) 로그. 재발행 실패는 스택 없이 1줄.
  - 2 는 비용이 작아 E-47 로그 정책과 함께.
- **우선순위**: 중간 — 8단계 전에 필요, 수동 추첨 운영 판단에 직접 쓰인다.
- **영향 범위**: `DrawService`, `phase-8-load-test.md` 메트릭 표

### E-54 스케줄러 주기당 Redis·DB 호출 수 — 현재 규모에서는 불필요
- **구분**: 고도화 (현재 규모에서는 불필요)
- **단계/위치**: phase-5, `DrawScheduler.run`, `DrawService.closeDueDrops`·`drawReadyDrops`
- **현상**: 주기(5초)당 고정 DB 3회(due OPEN·CLOSING·미발행 조회) + 마감 대상당 UPDATE 1 + HSET 1 + CLOSING 드로우당 SCARD 1 + COUNT 1. 인스턴스 2개면 두 배, 같은 드로우의 추첨 시도는 두 번째 인스턴스가 행 잠금에서 대기 후 0건(커넥션 1개를 추첨 트랜잭션 동안 점유).
- **문제/기회**: CLOSING 이 수십 개 쌓여도 주기당 Redis·DB 각 수십 회라 부하 무시 가능. 대기 인스턴스의 커넥션 점유도 추첨 1건 길이(현재 규모 수십 ms)뿐. 정원 10만 이상에서 E-51 의 추첨 시간(수 초)과 겹치면 Hikari 기본 10개 중 1개를 그만큼 잡는다.
- **근거**: 코드 분석(계산).
- **개선안**:
  1. 현행 유지(권장).
  2. 필요해지면 `markDrawnIfClosing` 을 `SELECT ... FOR UPDATE SKIP LOCKED` 계열(또는 `pg_try_advisory_xact_lock(dropId)`)로 바꿔 대기 없이 즉시 건너뛰기. 정합성은 같다(조건부 UPDATE 유지 시). 대가: 쿼리 1개 추가.
  3. CLOSING 드로우의 SCARD 를 파이프라인 1왕복으로 — 수십 개 이하에서는 효과 없음.
- **우선순위**: 낮음 — 현재 규모에서는 불필요.
- **영향 범위**: `DropSnapshotJpaRepository`, `DrawTransaction`

### E-55 gateway 기동 시 macOS netty 네이티브 DNS 리졸버 미로딩 ERROR 로그
- **구분**: 고도화 (로컬 개발 로그 노이즈) (카리나 메모 2)
- **단계/위치**: gateway, `gateway/build.gradle`
- **현상**: macOS(특히 Apple Silicon)에서 Reactor Netty 가 `MacOSDnsServerAddressStreamProvider` 를 못 찾아 "Unable to load ... fallback to system defaults. This may result in incorrect DNS resolutions on MacOS" ERROR 를 남긴다. 기능은 시스템 기본 DNS 로 동작한다.
- **문제/기회**: 실제 장애가 아닌 ERROR 가 기동 로그 첫머리에 남아 진짜 ERROR 를 가린다. VPN·사설 DNS 를 쓰는 macOS 에서는 실제로 이름 해석이 틀릴 수 있다(netty 경고 내용 그대로). Linux 컨테이너·CI 에서는 생기지 않는다.
- **근거**: 로그 관찰(카리나 메모), netty 동작.
- **개선안**:
  1. `runtimeOnly "io.netty:netty-resolver-dns-native-macos::osx-aarch_64"`(버전은 Boot BOM 관리) — Intel Mac 이면 `osx-x86_64` 도 추가. 대가: Linux 이미지에 쓰지 않는 네이티브 jar 가 들어감(수십 KB), 클래시파이어 관리.
  2. OS 조건부 의존성(`if (System.getProperty('os.name').toLowerCase().contains('mac'))`) — 배포물에서 제외되지만 빌드가 OS 에 따라 달라진다.
  3. 로거 `io.netty.resolver.dns.DnsServerAddressStreamProviders` 레벨을 로컬 프로필에서 낮춘다 — 증상만 숨김.
  - 1 권장(비용 최소).
- **우선순위**: 낮음 — 로컬 개발 환경 노이즈.
- **영향 범위**: `gateway/build.gradle`

---

## phase-6 (쿠폰: coupon-service) 분석 — 2026-11-17

분석 범위: `git diff dev...HEAD` (`feat/phase-6-coupon`, 커밋 7개). 기준 문서 `phase-6-coupon.md` v1.7 6장. **Gradle·EXPLAIN 실행 안 함**(동시 빌드 중). 쿼리 계획과 Redis 메모리는 스키마·인코딩 구조를 보고 추정했다.
- 확인 완료: E-03(coupon `timeout: 1s`), E-50(DLT 전용 producer `max.block.ms` 5s, JSON·bytes 둘 다), E-40/E-41 방식(요청 ID·claim·보상 3결과, Redis 예외 → 보상 → 503)이 코드에 들어가 있다. DLT 적재 ERROR 로그(`loggingRecoverer`)도 있다. 발급 경로는 DB 를 쓰지 않는다(좋음).
- 명백한 버그(정합성·장애 경로)는 찾지 못했다. 아래 E-58 은 정합성 위험이지만, 6장 리스크("Redis 유실 → 재초기화 시 초과 발급, AOF 전제")에 이미 적힌 수용 리스크라서 버그로 분류하지 않았다.

### E-56 쿠폰 피크 발급 경로 — 매진 전과 후의 부하 구조가 다르고, Kafka·Redis 장애 때 부하가 커짐
- **구분**: 고도화 (응답 경로·피크 병목) (관련: E-04, E-43, D6-2)
- **단계/위치**: phase-6, `CouponIssueService.issue`, `KafkaCouponEventPublisher.publishIssued`(`get(3500ms)`), `coupon-issue.lua`, `application.yml`(가상 스레드 미사용, Redis `timeout: 1s`)
- **현상**: 대기열이 없어서 오픈 순간 요청이 모두 Tomcat 으로 들어온다. 요청 1건은 Lua 1회(O(1): HGET·SISMEMBER·GET)를 실행하고, 판정이 `OK` 일 때만 acks=all 동기 대기를 한다. 매진·중복·시작 전 요청은 Redis 1왕복으로 끝난다.
- **문제/기회**:
  - **정상 시**: Kafka 대기는 성공 요청(수량 M 이하)에만 걸린다. 매진 뒤에는 Redis 만 거치므로 E-43 의 Tomcat 고갈 위험은 응모보다 작다. **현재 규모에서는 정상 경로 최적화가 필요 없다.**
  - **Kafka 장애 시**: 성공 판정 → 3.5초 대기 → 보상(INCR) 순서라 자리가 계속 되돌아온다. 그래서 장애 중에는 **매진되지 않고**, 동시에 `min(잔여, 200 스레드)` 개 요청이 3.5초씩 붙잡힌다. 처리량은 인스턴스당 약 57건/초로 떨어진다(계산). 잔여가 스레드 수보다 적으면 나머지 요청은 일시적으로 `SOLD_OUT` 을 받는다. 그 뒤 보상으로 자리가 돌아오면 늦게 온 사람이 받게 되어, 사용자 눈에는 선착순이 뒤집힌 것처럼 보인다. 정합성(초과 발급)에는 영향이 없다.
  - **Redis 포화 시**: 판정 타임아웃(1초)이 나면 `compensateQuietly` 가 Lua 를 한 번 더 실행해서 Redis 호출이 2배가 된다. 503 을 받은 클라이언트가 재시도하면 부하가 더 커진다. 같은 Redis 를 응모·대기열·랭킹이 함께 쓰므로 영향이 서비스 전체로 번진다(6장 리스크 1).
- **근거**: 코드 분석과 계산(추정). Redis 단일 스레드의 EVALSHA 처리량은 수만~10만/초 수준(추정)이다. 정상 시 지연은 측정하지 않았다(T6-13 기록과 8단계에서 확인).
- **개선안**:
  1. **가상 스레드**(E-43 개선안 1): Kafka 장애 때 스레드 고갈을 없앤다. 장애 중 매진되지 않는 현상은 그대로 남는다.
  2. **D6-2 (b) 로컬 매진 플래그**를 넣을 때는 **짧은 TTL(예: 200ms~1s)** 을 둔다. 보상으로 자리가 되돌아오므로 영구 플래그를 쓰면 언더 발급이 고정된다. 초과 발급 방향은 아니다. 대가: 인스턴스마다 매진 인지가 최대 TTL 만큼 늦다.
  3. gateway `RequestRateLimiter`(사용자당 초당 N회)로 연타·재시도 증폭을 막는다. Redis 를 쓰는 rate limiter 는 오히려 Redis 부하를 늘리므로 로컬 토큰 버킷이 맞다.
  4. 보상 호출에 짧은 백오프·서킷을 둔다(Redis 포화 시 2배 호출 방지). 대가: 보상이 늦어지는 동안 수량 한 자리가 일시적으로 묶인다.
  - 1 을 권장한다(설정 1줄, E-43 과 함께). 2·3 은 8단계 쿠폰 스파이크 측정에서 Redis CPU 가 포화될 때만 넣는다.
- **우선순위**: 중간 — 정상 경로는 문제없다. 장애와 피크가 겹칠 때 증폭이 일어나는데, 쿠폰 오픈이 그런 상황이다.
- **영향 범위**: `application.yml`, `phase-6-coupon.md` D6-2, `phase-8-load-test.md` 쿠폰 스파이크 시나리오, (3) `gateway/application.yml`
- **8단계 측정 결과 (2026-11-17, 리포트 B6·5장)**:
  - 쿠폰 동시 요청 3,000~4,000 은 워밍업 후 p99 153~204ms 로 목표(500ms) 안이었다. 11,000 동시(X3)에서 p99 693~878ms, 15,000 동시(X5)에서 857~1,196ms 로 **10,000 동시 이상에서 미달**. 서버 처리 평균 11.4~13.9ms, 최대 62~293ms, GC 최대 정지 13ms. 정합성은 모두 PASS.
  - 리포트 계산: gateway 업스트림 연결 200(= Tomcat 스레드 200) / 요청당 약 12ms ≈ 인스턴스당 약 16,000 req/s → 0.5초에 몰린 11,000~15,000건을 0.7~0.9초에 소화한다. 수치는 측정과 맞는다.
  - **보충 관찰**: 요청의 73~79%(X3 8,000 + DUPLICATE 800 / 11,000)는 Kafka 를 거치지 않는 409 응답인데도 서버 평균이 11~14ms 다. Kafka ack 대기만으로는 평균이 설명되지 않는다. 후보는 같은 호스트의 CPU 경합(k6 10,000 VU 이상), 409 마다 생기는 예외와 WARN 로그(E-60 개선안 1, 콜드 상태 실험에서는 차이 없었음), Redis 대기다. `http.server.requests` 를 status(202 / 409) 별로 나눠 보면 구분할 수 있다(이미 수집 중인 메트릭, 추가 비용 없음). 409 경로가 짧아지면 같은 연결 200 개로 소화하는 양이 그만큼 는다.
  - **선택지 (B6)**:
    1. **coupon-service 수평 확장**. Lua 판정이 Redis 에서 원자적이라 인스턴스를 늘려도 정합성은 그대로다. gateway 풀은 호스트(인스턴스)별이라 처리량이 대략 인스턴스 수에 비례한다(추정). 대가: 단일 Redis·단일 브로커가 다음 병목이 되고(E-07), 로드밸런서·서비스 디스커버리가 필요하다. 설계 변경 없음.
    2. **가상 스레드 + gateway 풀 확대**. 콜드 상태 실험만 했고(p99 851ms, 개선 없음) 워밍업 상태 실험은 OOM 으로 중단돼 **미검증**. 연결 수를 늘리면 gateway·서비스 메모리(E-73, B5)와 Hikari·Redis 커넥션이 새 상한이 된다. 장애 시 대기 요청이 무한히 쌓이지 않게 동시 요청 제한을 함께 둬야 한다(E-04 개선안 2).
    3. **발행 비동기화**(응답 의미를 "판정 성공"으로 바꿈). 처리량은 Redis 1왕복 수준으로 오르지만 D2 "성공 = 발행 확정"을 깨서 유실 시 사용자는 성공을 보고 DB 에는 없는 상태가 생긴다. 아웃박스 등 보완이 필요하고 설계 결정 변경이라 사용자 결정 대상.
    4. **매진 이후 경로 단축**(이 항목 개선안 2 로컬 매진 플래그 TTL 200ms~1s, E-60 개선안 1 로그 레벨). 10,000 동시 중 대부분이 매진 응답이라 효과가 가장 클 수 있다(추정). 정합성 영향 없음(언더 발급 방향, TTL 로 한정).
    - 순서 권장: 먼저 status 별 서버 시간으로 409 경로 비용을 확인 → 크면 4, 아니면 1. 3 은 과제 범위를 넘는다.
  - **status 별 확인 결과 (카리나, 2026-11-17, 리포트 B6 표)**: 409 평균 11.5~18.4ms / p99 35.6~53.4ms, 202 평균 11.3~18.3ms / p99 33.1~77.2ms — **409 경로 비용이 202 와 같다**. Kafka 대기는 주원인이 아니고, 두 경로 공통 구간(Redis Lua 호출·요청 스레드·CPU 경합)이 버스트 중 평시(1~3ms)보다 느려진다. 따라서 4(매진 이후 경로 단축 — Redis 왕복 자체를 건너뜀)가 1 과 함께 유력하다. 다음 확인: `lettuce.command.*` 지연, Redis `INFO commandstats`.
  - **우선순위 재평가**: 중간 유지 — 목표 규모(기본 3,000명)는 충족했고, 미달은 확대 규모(10,000 동시 이상)에서만 나왔다.

### E-57 쿠폰 Redis 키 메모리 — TTL 이 없어 이벤트가 끝나도 계속 쌓임 (현재 규모에서는 불필요)
- **구분**: 고도화 (Redis 메모리, 현재 규모에서는 불필요) (관련: E-42, E-39)
- **단계/위치**: phase-6, `CouponKeys`, `coupon-issue.lua`·`coupon-claim.lua`
- **현상**: 이벤트당 `meta`·`remaining`·`issued`(Set)·`requests`(Hash userId→UUID)·`persisted`(Set) 5개 키를 쓴다. 발급 종료 시각이 없어서(6장 리스크) TTL 이 없고, 지우는 경로도 없다.
- **문제/기회**: 원소 수는 수량 M 을 넘지 않는다. M 이 Set 128~512개, Hash 128개를 넘으면 hashtable 인코딩이 된다. 원소당 메모리는 `issued` 약 60B, `requests` 약 100B(UUID 36자 값), `persisted` 약 60B로, 합쳐 **자리당 약 220B** 다(추정). M=1만이면 이벤트당 약 2.2MB, 이런 이벤트 100개가 쌓이면 약 220MB 가 영구히 남는다. 현재 규모(이벤트 수 개~수십 개)에서는 수십 MB 이하라 **불필요**하다.
- **근거**: Redis 인코딩 규칙을 보고 계산(추정). `MEMORY USAGE` 는 측정하지 않았다.
- **개선안**:
  1. 현행 유지. 이벤트가 늘면 관리 API "이벤트 종료"를 만들고 `requests`·`persisted` 만 EXPIRE 한다. **주의**: `requests` 를 일찍 지우면 DLT 재처리 때 claim 이 0 을 반환해 발급이 조용히 사라진다. 그래서 DLT·lag 이 0 인 것을 확인한 뒤에만 지워야 한다. `meta`·`remaining` 을 지우거나 만료시키면 재초기화(SET NX)가 수량을 처음부터 다시 채워 **초과 발급**이 된다(E-58). 따라서 이 두 키는 남기거나, E-58 의 DB 기반 가드를 먼저 넣어야 한다.
  2. `requests` 값을 UUID 문자열 대신 16바이트 바이너리로 저장한다. 메모리는 약 30% 줄지만 디버깅(로그 대조)이 어려워진다. 권하지 않는다.
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다.
- **영향 범위**: `architecture.md` 5장 키 표(TTL 열), `CouponKeys`, (1) 관리 API·`coupon-issue.md`

### E-58 재초기화 API 가 Redis 키가 사라진 상태에서는 수량을 처음부터 다시 채움 — DB 발급 건수를 보지 않음
- **구분**: 고도화 (정합성 위험 — 운영 경로, 6장 수용 리스크의 보강) (관련: E-06)
- **단계/위치**: phase-6, `CouponEventService.reinitialize`, `RedisCouponGate.initialize`(SET NX `remaining` + HSETNX `meta`, 별도 명령 2개)
- **현상**: 재초기화는 키가 있으면 아무것도 바꾸지 않는다(SET NX). 키가 없으면 `remaining = totalQuantity` 로 채운다. `issued_coupons` 건수와 `issued` Set 은 확인하지 않는다.
- **문제/기회**: 다음 경우에 재초기화를 호출하면 이미 발급된 N장 위에 M장을 다시 발급해 **최대 M+N 장(초과 발급)** 이 된다. DB UK 는 같은 사용자만 막는다.
  - Redis 볼륨을 잃은 경우(`docker compose down -v`, 볼륨 재생성)
  - E-06 의 OS/VM 장애로 AOF 마지막 1~2초가 사라진 경우. 쿠폰 오픈 직후 1~2초는 수량 대부분이 발급되는 구간일 수 있다.
  - 운영자가 키를 지운 경우(E-57 개선안 1)

  "Redis 장애 후 재초기화"는 6장 리스크가 안내하는 복구 수단이라 사람이 실제로 누를 가능성이 높다. AOF 1초 유실은 재초기화 없이도 생긴다. 이 경우 `remaining` 이 유실분만큼 커지는데, 이건 E-06 과 같은 성격이다.
- **근거**: 코드 분석. 키가 없는 상태에서 SET NX 는 반드시 성공한다(추정 아님).
- **개선안**:
  1. **가드**: 재초기화 전에 `COUNT(*) FROM issued_coupons WHERE coupon_event_id = ?` 를 조회하고, 0 보다 크면 409(`COUPON_REINIT_REQUIRES_REBUILD`)로 거부한다. 키가 남아 있는 정상 재호출은 건수와 관계없이 지금처럼 동작하게 하려면 `EXISTS remaining` 을 먼저 본다. 코드는 몇 줄이다. 대가: 키를 잃은 이벤트는 복구할 수 없어 서비스 불가(언더 발급 방향)가 된다.
  2. **DB 기반 재구성**: 컨슈머 lag·DLT 가 0 인 것을 확인한 뒤 `remaining = total − COUNT`, `issued`·`persisted` 에 DB 의 userId 를 넣는다. `requests` 는 비워 둔다(진행 중인 요청이 없다는 전제). E-06 개선안 1 의 쿠폰판이다. 대가: 관리 API 1개와 lag 확인 절차가 필요하다. 진행 중 메시지가 남아 있으면 그 발급은 claim 0 으로 사라진다(언더 발급).
  - 1 을 지금 권장한다(초과 발급 경로를 비용 최소로 막음). 2 는 E-06 복구 절차를 정할 때 함께 만든다.
- **우선순위**: 중간 — 빈도는 낮지만 일어나면 불변식("수량 이하 발급")이 깨진다. 가드 비용은 쿼리 1개다.
- **영향 범위**: `CouponEventService.reinitialize`, `IssuedCouponRepository`(count 추가), `CouponErrorCode`, `coupon-issue.md` 재초기화 절, `phase-6-coupon.md` 6장 리스크

### E-59 보상 실패(영구 DUPLICATE)·DLT 적재분을 복구할 도구와 절차가 없음
- **구분**: 고도화 (운영·복구) (관련: E-12, 6장 리스크 "DLT 적재분 방치"·"보상까지 실패한 사용자")
- **단계/위치**: phase-6, `CouponIssueService.compensateQuietly`(ERROR 로그에 couponEventId·userId·requestId), `KafkaConsumerConfig.loggingRecoverer`(ERROR 로그에 topic·partition·offset·key)
- **현상**: 두 경우 모두 감지 수단은 ERROR 로그 1줄뿐이고, 복구 방법은 문서에 없다.
  - (a) 보상 실패: `issued`·`requests` 에는 남고 `persisted` 에는 없으며, 메시지도 없다. 사용자는 영구히 `DUPLICATE`(409)를 받고 `/me` 에는 쿠폰이 보이지 않는다. 수량 한 자리가 샌다.
  - (b) DLT: claim 은 됐을 수도, 안 됐을 수도 있다. 사용자는 202 를 받았는데 `/me` 에는 영원히 비어 있다.
- **문제/기회**: 두 경우 모두 **안전한 복구 수단이 이미 코드에 있다**. 도구로 꺼내기만 하면 된다.
  - (a) 로그의 requestId 로 `coupon-compensate.lua` 를 다시 실행하면 된다. Lua 가 `persisted`·requestId 를 대조하므로, 늦게 도착한 유령 메시지가 먼저 저장됐다면 `ALREADY_PERSISTED` 로 거절된다. 따라서 잘못 되돌릴 위험이 없다.
  - (b) DLT 레코드를 key 와 함께 `coupon.issued` 에 다시 넣으면 된다. claim(재전달에도 1)과 `ON CONFLICT DO NOTHING` 덕분에 멱등이다. 다만 역직렬화 실패로 bytes 그대로 적재된 레코드는 다시 넣어도 또 실패한다. 이건 원인(스키마)부터 고쳐야 한다.
- **근거**: Lua·서비스 코드 분석.
- **개선안**:
  1. **운영 절차 문서 + 스크립트**: (a) `redis-cli EVALSHA <compensate> 4 coupon:{id}:remaining coupon:{id}:issued coupon:{id}:requests coupon:{id}:persisted {userId} {requestId}` 를 실행하고 반환값을 해석한다. (b) `kafka-console-consumer --property print.key=true` → `kafka-console-producer --property parse.key=true` 로 DLT 를 원본 토픽에 다시 넣는 `infra/kafka/replay-dlt.sh` 를 둔다. 코드 변경은 없다. 대가: 사람이 로그를 보고 한 건씩 처리해야 한다.
  2. **관리 API**: `POST /api/coupons/events/{id}/compensate {userId, requestId}`(ADMIN)로 기존 `gate.compensate` 를 그대로 호출한다. 정합성 검사는 Lua 가 하므로 새 로직이 없다. 대가: 엔드포인트와 테스트 1세트가 늘어난다.
  3. **자동 대조**: `issued ∖ persisted` 중 오래된 항목을 찾아 보상한다. "오래됨"을 판단하려면 `requests` 값에 시각을 넣어야 하고, Lua 3개의 형식을 바꿔야 한다. 현재 규모에서는 과하다.
  - 1 을 권장한다(8단계 장애 주입 시나리오의 복구 단계로 바로 쓸 수 있다). 2 는 운영 화면이 생길 때 검토한다.
- **우선순위**: 중간 — 발생 빈도는 낮다. 다만 일어나면 사용자에게 보이는 상태가 영구히 틀리고, 지금은 복구 방법을 아는 사람이 없다. 비용은 문서와 스크립트 수준이다.
- **영향 범위**: `phase-6-coupon.md` 6장(운영 절차), `coupon-issue.md` 장애 대응 절, `infra/kafka/`(스크립트), (2) `CouponEventController`·`CouponEventService`

### E-60 쿠폰 관측 지표 없음, 매진 이후 요청마다 WARN 로그 1줄이 찍힘
- **구분**: 고도화 (관측성·로그, 8단계 준비) (관련: E-10, E-47, E-12)
- **단계/위치**: phase-6, `GlobalExceptionHandler.handleApplication`(4xx 는 `log.warn` 1줄), `CouponIssueService`, `CouponSaveService`(유령 무시 warn), `KafkaConsumerConfig.loggingRecoverer`
- **현상**:
  - `SOLD_OUT`·`DUPLICATE`·`NOT_STARTED`(409)도 요청마다 WARN 1줄을 남긴다. 503 은 스택이 포함된 ERROR 다(E-10).
  - 판정 결과별 건수, 발행 지연, 보상 결과, 유령 무시, DLT 적재를 볼 지표가 없다.
- **문제/기회**:
  - 쿠폰은 대기열이 없어서 **매진 뒤 요청이 가장 많다**. 매진 후 초당 1만 건이면 WARN 1만 줄/초(약 1.5MB/s, 추정)가 동기 콘솔 appender 로 요청 스레드에서 찍힌다. 그러면 Redis 1왕복으로 끝나야 할 매진 경로가 로그 I/O 에 묶인다. 응모는 대기열이 유입을 조절해서 덜하지만, 쿠폰에서는 가장 큰 로그원이 된다.
  - 정합성 상태도 볼 수 없다. 다음 대조식을 쓰면 별도 저장 없이 이상을 감지할 수 있다(정상 시 둘 다 진행 중 건수만큼만 차이 남).
    - `SCARD issued − SCARD persisted` = 진행 중 + DLT 미처리 + 보상 실패 누수
    - `SCARD persisted − COUNT(issued_coupons)` = claim 후 DB 저장 실패(DLT)
- **근거**: 코드 확인. 로그량은 계산(추정)이다.
- **개선안**:
  1. **로그**: 비즈니스 거절 코드(`SOLD_OUT`·`DUPLICATE`·`NOT_STARTED`)는 debug 로 내리거나 로그에서 빼고, 카운터로 센다. 방법은 `ErrorCode` 에 로그 레벨 속성을 두는 것이다(E-10 개선안 1 과 같은 장치, E-11 의 6개 복제본 문제와 함께 처리). 503 은 warn 1줄.
  2. **Micrometer**: `coupon.issue{outcome}` 카운터, `coupon.publish` Timer, `coupon.compensate{result=COMPENSATED|NOT_OURS|ALREADY_PERSISTED|FAILED}`, `coupon.save.ghost_skipped`, `coupon.dlt` 카운터. couponEventId 태그는 넣지 않는다.
  3. **대조 점검**: 위 두 차이를 이벤트별로 관리 조회 API 또는 주기적 로그로 노출한다(SCARD 2회 + COUNT 1회). 8단계 결과 검증("발급 수 = 수량")에도 그대로 쓴다.
  - 1 을 가장 먼저 권장한다(쿠폰 스파이크 측정을 왜곡하는 직접 원인). 2·3 은 8단계 전에 넣는다.
- **우선순위**: 중간 — 1 은 8단계 쿠폰 스파이크 결과에 직접 영향을 준다.
- **영향 범위**: `GlobalExceptionHandler`(6개 복제본, E-11), `common.error.ErrorCode`, `CouponIssueService`·`CouponSaveService`(또는 어댑터), `phase-8-load-test.md` 메트릭 표

### E-61 쿠폰 이벤트 조회 — 매 요청 DB PK 조회 + Redis GET, 불변 데이터라 캐시해도 안전 (현재 규모에서는 불필요)
- **구분**: 고도화 (조회 최적화, 현재 규모에서는 불필요) (관련: E-28)
- **단계/위치**: phase-6, `CouponQueryService.event`, `CouponEventRepositoryImpl.findById`, `RedisCouponGate.remaining`
- **현상**:
  - 공개 API `GET /api/coupons/events/{id}` 는 요청마다 `coupon_events` PK 조회 1회(readOnly 트랜잭션, 커넥션 1개 점유)와 Redis GET 1회를 실행한다. 트랜잭션 밖에서 Redis 를 호출하는 구조는 좋다.
  - `remaining` 키가 없으면(초기화 실패) 0 을 반환한다. 시작 전 이벤트가 "잔여 0"으로 보이고, 운영자 화면에서는 초기화 실패와 매진을 구분할 수 없다.
- **문제/기회**:
  - 오픈 직전·직후에는 사용자가 잔여 수량을 보려고 새로고침을 반복한다. PK 조회는 0.x ms 라 Hikari 기본 10개로도 초당 수천 건은 버틴다(추정). 따라서 **현재 규모에서는 불필요**하다.
  - E-28(드로우 상태 캐시는 위험)과 달리 `CouponEvent` 는 수정·삭제 API 가 없는 **불변 데이터**다. 그래서 name·totalQuantity·issueStartAt 은 무효화 없이 캐시해도 정합성 문제가 없다. 잔여 수량은 계속 Redis 에서 읽는다.
- **근거**: 코드 분석과 계산(추정).
- **개선안**:
  1. 현행 유지(권장).
  2. 필요해지면 infrastructure 에 Caffeine 로컬 캐시를 둔다(크기 상한 수백, TTL 불필요, 없는 id 는 캐시하지 않음). DB 호출이 0 이 된다. 대가: 의존성 1개. 나중에 수정 API 가 생기면 무효화가 필요하다.
  3. `remaining` 이 null 일 때 0 대신 "미초기화"를 구분해 돌려준다(응답 필드 추가 또는 로그). 운영 감지용이다.
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다. 3 은 E-59 운영 절차와 함께 검토한다.
- **영향 범위**: `CouponEventRepositoryImpl`(캐시 시), `CouponQueryService`, (3) `CouponEventResponse`·`coupon-issue.md` API 표

### E-62 내 쿠폰 조회 — 이벤트명을 이벤트별 `findById` 로 조회(N+1 형태), 메모리 재정렬은 중복, 페이지네이션 없음 (현재 규모에서는 불필요)
- **구분**: 고도화 (JPA·조회, 현재 규모에서는 불필요) (관련: E-44)
- **단계/위치**: phase-6, `CouponQueryService.myCoupons`, `IssuedCouponJpaRepository.findByUserIdOrderByIssuedAtDescIdDesc`, `V2__create_issued_coupons.sql`(`idx_issued_coupons_user (user_id, issued_at DESC)`)
- **현상**:
  - 쿼리는 1(목록) + D(서로 다른 이벤트 수)회다. HashMap 으로 같은 이벤트는 한 번만 조회한다.
  - DB 가 이미 `issued_at DESC, id DESC` 로 정렬해서 주는데, 서비스가 `issuedAt` 역순으로 한 번 더 정렬한다. 안정 정렬이라 결과는 같다.
  - 인덱스에 `id` 가 없어 동률 타이브레이크는 Incremental Sort 또는 Sort 로 처리한다(추정). 전체 컬럼을 읽는다.
  - 저장 INSERT 는 FK 확인을 위해 `coupon_events` 행에 `FOR KEY SHARE` 를 건다. 같은 이벤트에 컨슈머 6개가 동시에 INSERT 하면 MultiXact 가 생긴다.
- **문제/기회**: 사용자당 쿠폰은 이벤트당 1장이라 D 는 수~수십이다. 쿼리 수십 회, 각 PK 조회라 **현재 규모에서는 불필요**하다. MultiXact 도 정원 수만에서는 무시할 수준이다(추정, E-48 남은 측정 항목과 같은 성격).
- **근거**: 코드 분석. EXPLAIN 은 측정하지 않았다.
- **개선안**:
  1. 현행 유지 + 메모리 재정렬 제거(정렬 책임을 저장소 한 곳으로). 비용 0. 단, 포트 계약 "최신순 반환"을 주석이나 테스트로 고정해야 한다.
  2. 목록이 커지면 이렇게 바꾼다.
     - 이벤트명 조회: `eventRepository.findAllById(ids)` 1회(IN). 또는 JPQL 프로젝션 `SELECT ic.couponEventId, ce.name, ic.issuedAt FROM issued_coupons ic JOIN coupon_events ce ...` 1쿼리.
     - 인덱스: `(user_id, issued_at DESC, id DESC)`.
     - 페이징: keyset.

     대가: 도메인 저장소 포트에 메서드가 추가되거나, 조회 전용 포트가 생긴다.
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다. 1 은 비용이 거의 없다.
- **영향 범위**: `CouponQueryService`, (2) `CouponEventRepository`, Flyway V3

### E-63 쿠폰 저장 컨슈머 처리량 — 레코드당 claim Lua 1회 + INSERT 1트랜잭션 (현재 규모에서는 불필요)
- **구분**: 고도화 (Kafka·DB 처리량, 현재 규모에서는 불필요) (관련: E-46, D6-3)
- **단계/위치**: phase-6, `CouponIssuedListener`, `CouponSaveService.save`, `application.yml`(`concurrency: 3`, 파티션 6)
- **현상**: 레코드 1건 = `coupon-claim.lua` 1회 + native INSERT 1회(건당 커밋)다. 파티션 키 `{couponEventId}:{userId}` 로 6개 파티션에 고르게 퍼져서 핫 파티션이 없다(좋음). 인스턴스 1개면 스레드 3개가 파티션 6개를 나눠 맡는다.
- **문제/기회**: 건당 2~5ms 로 보면(추정, E-46 과 같은 가정) 스레드 6개 합계 초당 1,200~3,000건이다. M=1만이 매진되면 lag 은 약 3~8초다. 쿠폰은 응모와 달리 마감·추첨이 lag 을 기다리지 않는다. lag 은 "`/me` 가 잠깐 비어 보임"(D6-3)으로만 나타나므로 **현재 규모에서는 불필요**하다.
- **근거**: 계산(추정). 8단계에서 consumer lag 을 측정해야 한다.
- **개선안**:
  1. 현행 유지(권장).
  2. 정원 수십만이면 배치 리스너 + 배치 claim Lua(사용자·requestId 배열) + 다중 행 `INSERT ... ON CONFLICT DO NOTHING`. E-46 개선안 1 과 같고, claim Lua 도 배치 형태로 바꿔야 한다. 대가: DLT 대상 레코드를 식별하기 복잡하다(`BatchListenerFailedException`).
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다.
- **영향 범위**: `CouponIssuedListener`, `CouponSaveService`, `coupon-claim.lua`, `KafkaConsumerConfig`

---

## phase-7 (랭킹: ranking-service) 분석 — 2026-11-17

분석 범위: `git log dev..HEAD` (`feat/phase-7-ranking`, 커밋 6개), ranking-service 전체, `phase-7-ranking.md` v1.5 (3.6 운영 절차, 6장 리스크). Gradle 은 실행하지 않았다(코드 읽기만). 수치는 모두 추정이다.

### E-64 인기 드로우 조회 — 정상 3~4회, 최악 22회 Redis 왕복. 공개 경로라 캐시 여지가 있음 (현재 규모에서는 불필요)
- **구분**: 고도화 (Redis 왕복·응답 경로, 현재 규모에서는 불필요) (관련: E-32, E-21)
- **단계/위치**: phase-7, `RankingQueryService.java:48-78`(`topDrops`), `RedisRankingRepository.removeDrops`, `RedisDropWindowRepository.findAll`
- **현상**:
  - 배치 하나 = `ZREVRANGE WITHSCORES` 1회 + 마감 시각 `HMGET` 1회. 그 뒤 종료 드로우가 있으면 `ZREM` 1회, 제목 `HMGET` 1회.
  - 정상(종료 드로우가 상위에 없음, size ≤ 50): 3회. lazy 제거가 있으면 4회.
  - 최악(상위 500 이 대부분 종료 드로우거나, 진행 중 드로우가 size 보다 적은데 보드가 500개 이상): 10배치 × 2 + 2 = 22회.
  - lazy `ZREM` 은 멱등이다. 인스턴스 여러 개가 같은 종료 드로우를 동시에 지워도 결과가 같고, 비용은 member 당 O(log N) 이다. 한 번 지우면 다음 조회부터는 보이지 않으므로 쓰기는 마감 직후 몇 번만 일어난다. **경합은 문제가 되지 않는다.**
- **문제/기회**:
  - 왕복 1회를 0.2~0.5ms 로 잡으면 정상은 1~2ms 다. 문제가 되는 것은 Redis 가 느릴 때다. 명령마다 타임아웃이 1초라서, 실패하지 않고 느리기만 하면(예: 명령당 800ms) 최악 경로는 요청 하나가 십수 초를 잡는다.
  - `/api/rankings/drops` 는 인증이 없는 공개 경로다. 최악 경로가 반복되면 요청 1건이 Redis 명령 22건으로 늘어난다(증폭).
  - 현재 규모(동시 진행 드로우 수 개~수십 개, 종료 드로우는 lazy 제거로 상위에 쌓이지 않음)에서는 거의 항상 정상 경로를 탄다. **현재 규모에서는 불필요하다.**
- **근거**: 코드 분석, 계산(추정). 측정하지 않았다.
- **개선안**:
  1. 현행 유지(권장). 8단계에서 `/api/rankings/drops` p99 만 본다.
  2. 탐색을 Lua 하나로 묶는다(`ZREVRANGE` → `HMGET closeAt` → 필터 → `HMGET title` 을 서버에서 한 번에, 왕복 1회). 대가: 진행 중 판단(`DropWindow.isClosed`)이 도메인에서 Lua 로 옮겨가 도메인 규칙이 둘로 나뉘고, 스크립트 실행 시간이 탐색 한도에 비례해 늘어난다(500개면 수백 µs, 추정).
  3. 공개 조회 3종(`drops`, `users/weekly`, `users/all`)에 인스턴스 로컬 캐시(Caffeine, TTL 1~2초, 키 = API + size + week)를 둔다. 피크 때 Redis 부하가 인스턴스 수 × 초당 1회로 고정된다. 대가: 최대 2초 늦은 순위(랭킹은 원래 최종 일관성이라 허용 범위), 캐시 의존성 추가. `/users/me` 는 사용자별이라 캐시 효과가 없다.
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다. 8단계에서 랭킹 폴링 부하를 넣는다면 3 이 가장 싸다.
- **영향 범위**: (2) `rank.lua` 와 별도 스크립트, `RankingRepository` 포트. (3) `RankingQueryService` 또는 presentation, `build.gradle`

### E-65 `drop.created` 를 받지 못한 드로우는 마감 후에도 "진행 중 인기 드로우"에 영구히 남음
- **구분**: 장애 경로의 표시 오류 (정합성 영향 없음) (관련: E-29)
- **단계/위치**: phase-7, `DropWindow.isInProgress`(closeAt null → 진행 중), `RankingQueryService.topDrops`, `rank.lua`(closeAt 미상 → dedupe TTL 30일)
- **현상**: closeAt 을 모르면 진행 중으로 본다(기본안). `drop.created` 가 유실되면(E-29 경로) 그 드로우는 응모 수 점수로 인기 보드에 올라가고, 마감이 지나도 lazy 제거 대상이 아니므로 **계속 상위에 `drop-{id}` 로 표시**된다. 응모 수가 많은 드로우일수록 오래 1위에 머문다.
- **문제/기회**: 사용자 화면에 마감된 드로우가 진행 중으로 보인다. 데이터 정합성(응모·추첨)과는 무관하다. product 재발행 API 를 누르면 closeAt 이 HSET 되어 다음 조회에서 지워지지만, 관리자가 이 상태를 알 방법이 없다(E-29 와 같은 구조).
- **근거**: 코드 분석. 발생 빈도는 E-29 와 같다(브로커 장애·배포와 드로우 등록이 겹칠 때, 낮음).
- **개선안**:
  1. 감지만 추가: `topDrops` 에서 closeAt 미상 드로우를 보면 WARN 1줄(dropId). 비용 몇 줄. 복구는 재발행 API(수동).
  2. E-29 개선안 1(오픈 전 자동 재발행)이 들어가면 이 경우도 함께 사라진다. 따로 만들 필요가 없다.
  3. closeAt 미상 드로우를 결과에서 빼되 지우지는 않는다. 정상 경로(drop.created 가 첫 응모보다 늦게 도착)에서 진행 중 드로우가 잠깐 안 보이게 되므로 권하지 않는다.
- **우선순위**: 낮음~중간 — 빈도는 낮지만 눈에 띄는 오표시다. E-29 를 처리하면 같이 해결되므로 E-29 쪽에서 묶어 처리한다.
- **영향 범위**: (1) `RankingQueryService`, `phase-7-ranking.md` 6장 리스크

### E-66 리스너가 실패로 건너뛴 응모를 메우는 절차가 "전체 재집계"뿐이고, 이 절차는 30일보다 오래된 누적 점수를 지움
- **구분**: 운영·복구 절차 (관련: E-12, E-06)
- **단계/위치**: phase-7, `KafkaConsumerConfig`(1초 × 3회 후 로그·스킵), `phase-7-ranking.md` 3.6, `create-topics.sh:22`(`entry.created` 보존 30일)
- **현상**:
  - Redis 장애가 길어지면 레코드마다 약 4~7초(시도 4회 × 최대 1초 타임아웃 + 대기 3초) 동안 재시도한 뒤 ERROR 로그를 남기고 건너뛴다. 로그에 topic·partition·offset 이 남는다.
  - 3.6 절차는 집계 키를 모두 지우고 `--to-earliest` 로 다시 읽는 방식 하나다.
- **문제/기회**:
  - 몇 건이 빠진 경우에도 전체 재집계를 해야 한다. 이때 `rank:user:all` 을 지우므로, `entry.created` 보존 기간(30일) 밖의 누적 점수는 **원래 맞던 값까지 사라진다**. 30일이 지난 서비스에서는 "누락 몇 건 복구"가 "누적 랭킹 초기화"가 된다. 오래된 주간 키(30~35일)도 일부만 복구된다.
  - 사실 rank.lua 는 dedupe Set 으로 멱등이다. 건너뛴 오프셋부터 다시 읽어도, 그 뒤 이미 집계된 레코드는 `SISMEMBER` 에서 0 으로 끝난다. 건너뛴 레코드의 드로우는 그 시각에 진행 중이었으므로, dedupe Set 은 장애 시점부터 최소 7일(closeAt + 7일) 살아 있다. **장애 후 7일 안이면 키를 지우지 않고 부분 재처리할 수 있다.**
- **근거**: 코드·Lua 분석(측정 안 함). 레코드당 소요 시간은 `FixedBackOff(1000, 3)` + `timeout: 1s` 로 계산한 값이다(추정).
- **개선안**:
  1. **3.6 에 "부분 재처리" 절차를 추가한다(권장, 문서만)**: 서비스 중지 → ERROR 로그의 파티션별 최소 offset 으로 `--reset-offsets --to-offset`(또는 장애 시작 시각으로 `--to-datetime`), `--topic entry.created:{partition}` → 재시작. **집계 키는 지우지 않는다.** 조건: 장애 후 7일 이내(dedupe 생존). 이 조건을 넘기면 이중 집계되므로 3.6 전체 재집계로 간다. 부수 효과: 이미 lazy 제거된 종료 드로우가 인기 보드에 다시 들어오지만, 다음 조회에서 지워진다.
  2. 랭킹 전용 DLT(`entry.created.ranking.DLT`, 6장 비고)를 둔다. 누락 레코드를 그대로 다시 넣을 수 있지만, 토픽·recoverer·재처리 도구가 늘어난다. 1 로 충분하므로 현재 규모에서는 과하다.
  3. 30일보다 오래된 누적 점수까지 지키려면 entry_db 기반 재집계 배치가 필요하다(6장 리스크에 범위 밖으로 기록됨). 과제 범위 밖이다.
- **우선순위**: 중간 — 코드 변경 없이 문서 한 절로 끝나고, 운영 중 가장 흔한 복구 상황(짧은 Redis 장애)에 맞는 절차다.
- **영향 범위**: `phase-7-ranking.md` 3.6·6장 리스크, `docs/design/05-features/ranking.md` 운영 절(있으면)

- **처리 (2026-11-17)**: 7단계에서 phase-7-ranking.md 3.6 에 부분 재처리 절차를 문서로 반영 (코드 변경 없음). 자동화 스크립트는 남음.

### E-67 리스너 처리량 — `concurrency` 미설정(1)과 `ack-mode: record` 의 레코드마다 동기 커밋 (현재 규모에서는 불필요)
- **구분**: 고도화 (Kafka 처리량, 현재 규모에서는 불필요) (관련: E-05, E-46, E-63)
- **단계/위치**: phase-7, `ranking-service/src/main/resources/application.yml:27, 34`, `EntryCreatedListener`
- **현상**:
  - entry·coupon 과 달리 `listener.concurrency` 가 없어, 인스턴스 하나에서 스레드 1개가 `entry.created` 파티션 6개를 모두 처리한다.
  - `ack-mode: record` 라 레코드마다 `commitSync` 를 한다. 레코드 1건 = rank.lua 1회(0.3~1ms) + 오프셋 커밋 1회(브로커 왕복 1~3ms) 다.
  - 파티션 키는 `{dropId}:{userId}` 라 한 드로우에 응모가 몰려도 파티션이 고르게 퍼진다(핫 파티션 없음, 좋음). rank.lua 의 `rank:drop:popular`·`rank:user:all` 은 모든 응모가 쓰는 단일 키지만, Redis 가 단일 스레드라 어차피 직렬이고 명령은 µs 단위다.
- **문제/기회**: 스레드 1개 기준 초당 약 250~750건이다. 정원 1만이 오픈 몇 초 안에 차면 랭킹 반영이 15~40초 늦어진다. 랭킹은 최종 일관성이라 기능 문제는 아니다. **현재 규모(S1 정원 1,000 → 수 초)에서는 불필요하다.**
- **근거**: 계산(추정). 8단계에서 `ranking-service` 컨슈머 그룹 lag 을 측정하면 확인된다.
- **개선안**:
  1. 현행 유지(권장). 8단계에서 lag 이 문제되면 2 또는 3.
  2. `concurrency: 3`(entry·coupon 과 같은 값, 인스턴스 2개 기준). 설정 한 줄이다. Redis 쪽 변화는 없다.
  3. `ack-mode: batch`(Spring 기본). 커밋이 poll 단위로 줄어 처리량이 크게 오른다. 프로세스가 죽으면 마지막 poll 분량이 재전달되지만 rank.lua 가 dedupe 로 멱등이라 결과가 같다(응모·쿠폰 저장 컨슈머와 달리 안전). 대가: 테스트 `KafkaConsumption` 이 커밋 오프셋으로 완료를 판단하므로, 커밋 시점이 바뀌어도 테스트가 통과하는지 확인해야 한다.
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다.
- **영향 범위**: `ranking-service/application.yml`, (3) `phase-7-ranking.md` 3.3 설정 주석, `KafkaConsumption`

### E-68 `/users/me` 가 Redis 왕복 4회 — `ZREVRANK` 와 `ZSCORE` 를 따로 호출 (현재 규모에서는 불필요)
- **구분**: 고도화 (Redis 왕복, 현재 규모에서는 불필요)
- **단계/위치**: phase-7, `RedisRankingRepository.java:65-78`(`rankOf`), `RankingQueryService.myRank`
- **현상**: 보드 하나당 `ZREVRANK` + `ZSCORE` 2회, 주간·누적 두 보드라 4회다. 두 호출 사이에 member 가 지워지는 경우를 코드가 방어한다.
- **문제/기회**: 정상이면 1~2ms 로 작다. 다만 로그인 사용자가 랭킹 화면을 열 때마다 부르는 경로라 호출 빈도는 공개 조회와 같다. Redis 7.2+ 의 `ZREVRANK key member WITHSCORE` 를 쓰면 보드당 1회(합계 2회)이고 두 값이 한 명령에서 나와 방어 코드도 필요 없다. 파이프라인이면 1회다.
- **근거**: 코드 분석. 이 프로젝트는 Redis 7.4 를 쓴다. Spring Data Redis 의 `ZSetOperations` 에 `WITHSCORE` 지원 메서드가 있는지는 확인하지 않았다(없으면 `execute` 로 원시 명령 호출).
- **개선안**:
  1. 현행 유지(권장).
  2. `ZREVRANK ... WITHSCORE` 로 바꾼다. 4회 → 2회. 원시 명령을 쓰면 응답 파싱 코드가 늘어난다.
  3. `executePipelined` 로 4개 명령을 한 번에 보낸다. 1회. 코드가 조금 복잡해진다.
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다.
- **영향 범위**: `RedisRankingRepository.rankOf`, `RedisRankingRepositoryTest`

### E-69 랭킹 Redis 메모리 — dedupe Set 은 정원 크기, 표시 정보 Hash 와 누적 보드는 TTL 없이 계속 커짐 (현재 규모에서는 불필요)
- **구분**: 고도화 (Redis 메모리, 현재 규모에서는 불필요) (관련: E-42, E-57, E-39)
- **단계/위치**: phase-7, `rank.lua`, `RankingKeys`, `application.yml`(`ranking.*` TTL)
- **현상**:
  - `rank:dedupe:{dropId}`: member 수 = 그 드로우의 성공 응모 수(최대 정원 M). member 가 512개를 넘으면 intset 이 아니라 hashtable 이 되어 member 당 약 50~60B 다. M=1만이면 드로우당 약 0.6MB, closeAt + 7일 동안 산다.
  - `rank:user:weekly:*`: 그 주 응모자 수 × 약 70~100B, 35일 보관(최대 5~6개 주).
  - `rank:user:all`, `rank:drop:info`, `rank:user:info`, `rank:drop:closeAt`: TTL 없음. 가입자·드로우 수에 비례해 커진다.
- **문제/기회**: 동시에 살아 있는 드로우 50개 × M=1만이면 dedupe 가 약 30MB, 가입자 10만이면 `rank:user:all` + `rank:user:info` 가 약 15~20MB 다. **현재 규모에서는 불필요하다.** TTL 없는 표시 정보는 재집계(3.6)와 늦게 온 응모의 dedupe TTL 계산에 필요하므로, 지우는 쪽이 오히려 위험하다.
- **근거**: Redis 인코딩 규칙 기반 계산(추정, `MEMORY USAGE` 측정 안 함).
- **개선안**:
  1. 현행 유지(권장). 8단계 뒤 `MEMORY USAGE rank:dedupe:{id}`, `INFO memory` 를 리포트에 남긴다.
  2. `set-max-intset-entries` 를 정원 이상(예: 16384)으로 올려 dedupe 를 intset(member 당 4~8B)으로 유지한다. 메모리가 약 1/8 로 준다. 대가: intset 삽입이 O(n)(정렬 배열 memmove)이라 SADD 가 수십 µs 로 늘어나고, Redis 를 같이 쓰는 다른 서비스 Set 에도 적용된다(전역 설정).
  3. `rank:drop:closeAt`·`rank:drop:info` 를 드로우 마감 + N일 뒤에 지운다. 재집계 때 드로우 정보가 없어 dedupe 가 30일 fallback TTL 로 잡히고 제목이 `drop-{id}` 가 된다. 권하지 않는다.
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다.
- **영향 범위**: (2) `docker-compose.yml` Redis 설정, `infrastructure.md`

### E-70 랭킹 관측 지표 없음 — 건너뜀·중복·lazy 제거·lag 을 로그로만 알 수 있음
- **구분**: 운영·관측 (관련: E-13, E-38, E-47, E-60)
- **단계/위치**: phase-7, `KafkaConsumerConfig`(ERROR 로그), `RankingAggregateService.recordEntry`(중복은 false 반환만), `RankingQueryService.removeClosed`(WARN)
- **현상**: 메트릭이 하나도 없다. 건너뛴 레코드는 ERROR 로그와 스택, 중복 응모는 아무 흔적도 없고, lazy 제거 실패만 WARN 이다.
- **문제/기회**: E-66 부분 재처리를 하려면 "건너뛴 레코드가 있었는가"를 바로 알아야 하는데 지금은 로그 검색뿐이다. 8단계 리포트에 "응모 수 vs 랭킹 집계 수" 대조(정합성 확인)를 넣으려면 집계 결과 카운터가 필요하다.
- **근거**: 코드 분석.
- **개선안**:
  1. 최소 세트(권장): `ranking.record{result=counted|duplicate|invalid}` 카운터, `ranking.record.skipped` 카운터(recoverer 에서 증가), `ranking.drops.lazy_removed` 카운터, `rank.lua` 실행 Timer. 컨슈머 lag 은 Kafka UI(:8090) 또는 `kafka-consumer-groups.sh` 로 본다. 비용: Micrometer `MeterRegistry` 주입 몇 줄(application 에서 직접 쓰지 않도록 포트로 감싸거나 presentation/infrastructure 에서 증가).
  2. 로그만 유지하고 8단계에서 `grep` 으로 센다. 비용 0, 실시간성 없음.
  - 8단계 대조 방법(문서): `SUM(rank:drop:popular 의 점수)` 는 lazy 제거 때문에 응모 수와 맞지 않는다. 대신 드로우별 `SCARD rank:dedupe:{dropId}` 와 entry_db `count(*)` 를 비교한다(dedupe 생존 기간 안에서만 유효).
- **우선순위**: 중간 — E-13 측정 지표 준비와 같은 묶음으로, 8단계 전에 넣으면 리포트 정합성 확인에 바로 쓰인다.
- **영향 범위**: `KafkaConsumerConfig`, `RankingAggregateService` 또는 리스너, `RankingQueryService`, `phase-8-load-test.md` 지표 목록

### E-71 소수 시나리오 — 보상된 응모의 랭킹 집계, lazy 제거 뒤 점수 재시작, 닉네임·제목 변경 없음
- **구분**: 정책 메모 (정합성 영향 없음, 현재 규모에서는 불필요) (관련: E-40, E-18)
- **단계/위치**: phase-7, `rank.lua`, `RankingQueryService.removeClosed`, `RankingAggregateService.registerUser/registerDrop`
- **현상과 판단**:
  1. **보상된 응모도 랭킹에 집계된다.** E-40 처리 후에도 유령 이벤트(브로커는 기록, producer 는 실패 판단 → 보상 COMPENSATED → 저장 컨슈머는 claim 실패로 저장 안 함)는 `entry.created` 에 남는다. ranking 은 별도 컨슈머 그룹이라 이 레코드를 1점으로 센다. 사용자가 다시 응모해 성공하면 dedupe 로 중복이 걸러져 결과가 맞고, 다시 응모하지 않으면 **응모하지 않은 드로우가 1점** 남는다. 브로커 장애 순간에만 생기고 랭킹은 진실의 원천이 아니므로 허용한다. 6장 리스크에 한 줄 추가를 권한다. 막으려면 `entry.cancelled` 같은 보상 이벤트를 새로 발행해야 하는데 과하다.
  2. **lazy 제거 뒤 늦게 온 응모는 점수 1 부터 다시 쌓인다.** 마감 직전 응모가 lag 때문에 제거 뒤에 소비되면 `ZINCRBY` 가 member 를 새로 만든다. 종료 드로우라 결과에서는 다시 걸러지므로 보이지 않는다. 다만 나중에 인기 드로우 범위를 "전체 누적"으로 바꾸면(6장 비고 "필터만 제거") **이미 지운 드로우의 점수는 되살아나지 않는다**. 범위를 바꿀 때는 3.6 재집계(30일 한계)가 함께 필요하다는 점을 6장 비고에 적어 둔다.
  3. **닉네임·제목 변경 이벤트가 없다.** 지금은 변경 API 가 없으니(user·product 확인, drop 은 등록·재발행만) 문제가 없다. 생기면 같은 HSET 덮어쓰기 경로로 받을 수 있다. 재발행 API 로 다시 받은 `drop.created` 는 제목·closeAt 을 덮어쓰지만 dedupe TTL 은 바꾸지 않는다(PTTL ≥ 0 이면 유지). closeAt 이 바뀌지 않는 한 영향이 없다.
  4. **주간 키 TTL 은 재집계 시각 기준이다.** 3.6 재집계로 만든 지난 주차 키는 재집계 시점부터 35일 산다. 리스크에 "주차별 보관 기간이 며칠씩 다를 수 있다"로 이미 허용되어 있고, 재집계 때는 차이가 최대 30일로 커진다는 점만 덧붙이면 된다.
- **근거**: 코드 분석.
- **개선안**: 코드 변경 없음. 1·2·4 를 `phase-7-ranking.md` 6장 리스크/비고에 한 줄씩 추가한다(문서만).
- **우선순위**: 낮음 — 현재 규모에서는 불필요하다. 문서 보강만 한다.
- **영향 범위**: `phase-7-ranking.md` 6장, `docs/design/05-features/ranking.md` 리스크 절

---

## phase-8 (부하 테스트) 분석 — 2026-11-17

분석 범위: `git log dev..HEAD` (`test/phase-8-load-test`, 커밋 17개), `docs/report/load-test-report.md` v1.0, `phase-8-load-test.md` 3.9 P1~P4. 직접 측정은 하지 않았고 리포트 수치를 인용한다.
기존 항목에 덧붙인 8단계 결과: E-04·E-56(쿠폰 처리량 상한 B6), E-20(로그인 분산), E-22(gateway 풀·타임아웃), E-34(입장 배치와 `ENTRY_SOLD_OUT`), E-16(P3, 앞서 반영).

### E-72 gateway 업스트림 풀 대기 초과(`PoolAcquireTimeoutException`)가 500 으로 나감 — 의미상 503(재시도 가능)
- **구분**: 고도화 (응답 의미·장애 경로) (관련: E-22, E-75)
- **단계/위치**: phase-8, `gateway/src/main/resources/application.yml` `httpclient.pool`(`type: FIXED`, `max-connections: 200`, `acquire-timeout: 30000`), gateway 기본 오류 처리
- **현상**: 서비스당 연결 200개가 모두 쓰이면 나머지 요청은 gateway 에서 무제한 대기 큐에 들어가고, 30초를 넘기면 `PoolAcquireTimeoutException` 이 gateway 기본 오류 처리로 **500** 이 된다.
- **문제/기회**:
  - 이 예외는 연결을 얻기 전에 나므로 **요청이 업스트림에 전달되지 않았다**. 재시도해도 부작용이 없는 과부하 상황인데 500(서버 버그)으로 보인다. 클라이언트·k6 가 재시도 여부를 판단할 수 없고, 서비스의 503 `*_TEMPORARILY_FAILED`(재시도 가능)와도 의미가 어긋난다.
  - 30초 대기는 사용자가 이미 떠났을 시간이다. 대기 큐가 무제한이라 대기 요청 수만큼 gateway 메모리를 쓴다(E-73).
- **근거**: X1-1차(10,000명 동시 로그인, 분산 없음)에서 2,670건 500 (`PoolAcquireTimeoutException`). 로그인 상한 약 235건/초 × 30초 ≈ 7,000건만 30초 안에 처리된다는 계산과 대략 맞는다(실제 참여 7,330, 실패 10,000 − 7,330 = 2,670).
- **개선안**:
  1. `PoolAcquireTimeoutException` 만 골라 **503 + `Retry-After`** 로 바꾼다 (전역 필터의 `onErrorMap` 또는 `ErrorWebExceptionHandler`). 응답 본문은 공통 오류 형식(`GATEWAY_OVERLOADED` 등 코드 신설). 업스트림에 보낸 뒤의 오류(응답 대기 중 끊김 등)는 처리 여부를 알 수 없으므로 503 으로 바꾸지 않는다 — 재시도 시 중복 요청이 되지만 응모·쿠폰은 DUPLICATE 로 막히므로 정합성 문제는 없고, 의미만 구분한다. 비용: gateway 코드 수십 줄 + 테스트.
  2. 1 에 더해 `acquire-timeout` 을 5~10초로 줄이고 대기 수 상한(Reactor Netty `pendingAcquireMaxCount`, Spring Cloud Gateway 프로퍼티에 없으면 `HttpClientCustomizer` 로 — 확인 필요)을 둔다. 빨리 거절해 메모리와 사용자 대기를 줄인다. 대가: 짧은 버스트(쿠폰 0.5초 몰림)에서도 거절이 생길 수 있어 X3·X5 처럼 "늦더라도 다 처리" 되던 요청 일부가 503 이 된다. 값은 측정으로 정해야 한다.
  3. 경로별 rate limit(로그인 초당 N건)으로 앞단에서 자른다. 로컬 토큰 버킷이어야 한다(E-56 개선안 3과 같은 이유). 서비스 처리량을 알아야 N 을 정할 수 있다.
  - 1 을 권장한다. 정상 규모에서는 발생하지 않고, 발생하면 응답 의미만 바로잡는다.
- **우선순위**: 중간 — 기본 규모에서는 발생하지 않았지만(로그인 분산 후 0), 오픈 직전 로그인 몰림은 실제 서비스에서 흔하고 500 은 오류율 알림을 잘못 울린다.
- **영향 범위**: gateway 필터/오류 처리, `common.error` 오류 코드, `docs/design/` API 오류 코드 표, `phase-8-load-test.md` 3.9 P1 비고

### E-73 gateway 메모리·동시 연결 상한 — 15,000 동시에서 954/1024MiB, gateway 메트릭은 수집되지 않음
- **구분**: 고도화 (용량·관측) (관련: E-72, B2·B5)
- **단계/위치**: phase-8, `docker-compose.yml` gateway(`memory: 1024m`, `-XX:MaxRAMPercentage=50 -XX:MaxDirectMemorySize=256m`), `gateway/application.yml` `management.endpoints.web.exposure.include: health`, `infra/monitoring/prometheus.yml`(gateway 제외)
- **현상**:
  - 클라이언트 연결은 모두 gateway 한 인스턴스가 받는다. 동시 연결 수를 제한하는 장치가 없고, 업스트림 풀 대기 큐도 무제한이다.
  - 외부 공개 포트라 actuator 는 health 만 열었고 Prometheus 는 gateway 를 scrape 하지 않는다. 그래서 gateway 의 힙·다이렉트 메모리·풀 대기 수(`reactor.netty.connection.provider.pending.connections`)는 `docker stats` 로만 봤다.
- **문제/기회**:
  - P2 이후 X5(15,000 동시)에서 RSS 954/1024MiB(93%). 연결이 더 늘면 B2 와 같은 OOM kill 이 다시 날 수 있고, gateway 가 죽으면 **모든 서비스가 동시에 응답 불가**다(정합성은 B2 처럼 유지되지만 처리된 요청의 응답이 유실되어 사용자는 실패로 본다).
  - 대략 계산(추정): 기동 후 약 320MiB → 15,000 동시 954MiB, 연결당 약 40KB. 20,000 동시면 1,100MiB 이상으로 한도를 넘는다. X4(20,000명)는 k6 임시 포트 한계로 16,294명만 들어와 이 구간은 측정되지 않았다.
  - 풀 대기 수를 못 보므로 B6 의 "앞단 대기"가 gateway 풀 대기인지 포트 포워딩인지 직접 확인하지 못했다.
- **근거**: 리포트 B2, 5장 X5(P4 후) gateway RSS 954/1024MiB. 연결당 메모리는 추정.
- **개선안**:
  1. **관측 먼저**: `management.server.port` 를 따로 두고(compose 에서 호스트에 publish 하지 않음) 그 포트로만 prometheus 를 연다. `httpclient.pool.metrics: true` 로 풀 대기·활성 연결을 수집한다. 외부 노출 없이 지표를 얻는다. 비용: 설정 몇 줄.
  2. **동시 요청 상한 + 빠른 거절**: 진행 중 요청 수 세마포어 필터(초과 시 503, E-72 와 같은 응답)와 풀 대기 상한(E-72 개선안 2). 메모리 사용이 상한으로 묶인다. 대가: 상한을 넘는 버스트는 거절된다(지금은 늦게라도 처리).
  3. **메모리 한도 상향 또는 gateway 수평 확장**. 가장 단순하지만 상한을 옮길 뿐이다. 수평 확장은 앞단 L4 로드밸런서가 필요하다(과제 범위 밖).
  - 1 → 2 순서를 권장한다. 2 의 상한 값은 1 로 측정한 뒤 정한다.
- **우선순위**: 중간 — 기본 규모(3,000~4,000 동시, 최대 656MiB)는 여유가 있다. 단일 장애점이라 터지면 영향이 전체다.
- **영향 범위**: `docker-compose.yml`, `gateway/application.yml`, `infra/monitoring/prometheus.yml`, `docs/design/` 인프라·튜닝 표

### E-74 콜드 스타트(JIT) — 배포·재기동 직후 첫 버스트의 지연이 3~5배
- **구분**: 고도화 (운영 절차) (관련: B4)
- **단계/위치**: phase-8, `docker/service.Dockerfile`(JVM 옵션), 배포·오픈 운영 절차 (코드 위치 없음)
- **현상**: 드로우 오픈·쿠폰 시작은 트래픽이 0 에서 순간적으로 치솟는다. 서비스가 재기동된 뒤 첫 버스트가 오면 JIT 가 덜 끝난 상태에서 처리한다.
- **문제/기회**: 지연 목표(p99 500ms) 달성 여부가 "직전에 얼마나 데웠는가"에 달려 있다. 오픈 직전 배포나 장애 후 재기동이 겹치면 목표를 넘긴다.
- **근거** (리포트 B4·4장·5장):
  - 매진 쿠폰 발급(Kafka 없음) 4,000 VU, coupon 재기동 직후 1~4회차 p99 577 → 187 → 72 → 19ms.
  - S3 쿠폰 p99: 기동 직후 834ms vs 워밍업 후 153~204ms. S1 응모 p99 136.5 → 29.3ms, queue_enter p99 162.8 → 16.7ms.
  - X2·X4 queue_enter p99 443~506ms(서버 측 최대 69ms) — 재기동 후 워밍업 1회뿐. 충분히 데운 X1 은 43ms.
  - 가상 스레드·로그 끄기로는 콜드 상태가 나아지지 않았다(가설 기각).
- **개선안**:
  1. **운영 절차로 사전 요청**: 오픈 N분 전에 관리자 스크립트가 테스트용 드로우·쿠폰 이벤트에 대해 실제 경로(진입·상태·응모·발급·매진)를 수천 건 호출한다. 효과가 측정으로 확인됐고(위 1~4회차) 코드 변경이 없다. 대가: 테스트 데이터가 운영 DB·Redis·랭킹에 남지 않게 분리해야 한다(랭킹 집계에서 제외할 표시 등).
  2. **기동 시 자체 워밍업**: `ApplicationReadyEvent` 에서 핵심 경로를 내부 호출하고 끝난 뒤 readiness 를 UP 으로 바꾼다. 배포마다 자동이다. 대가: 기동이 수십 초 늘고, Kafka 발행 경로까지 데우려면 실제 메시지를 보내야 해서 부작용 없는 경로(매진·NOT_FOUND 판정, 조회)로 한정된다. readiness 를 보는 로드밸런서가 없는 compose 환경에서는 의미가 작다.
  3. **CDS/AOT 캐시**: Java 21 에서는 AppCDS(클래스 로딩·기동 시간 단축)만 쓸 수 있고 JIT 프로파일은 저장되지 않는다. JIT 프로파일까지 미리 담는 AOT 캐시는 JDK 24 이후 기능(JEP 483·515)이라 Java 버전 변경이 필요하다. 이번 증상(반복할수록 빨라짐)에는 효과가 제한적일 것으로 본다(추정).
  - 1 을 권장한다(문서·스크립트만). `load-test/` 의 워밍업 실행이 이미 같은 역할을 한다.
- **우선순위**: 중간 — 코드 변경 없이 목표 달성 여부를 좌우한다. 운영 문서에 절차로 남기는 정도가 과제 범위에 맞다.
- **영향 범위**: 운영 절차 문서(`docs/design/` 운영 절 또는 리포트 부록), (2) 각 서비스 기동 코드, (3) Dockerfile·Java 버전

### E-75 로그인 처리량 상한 — BCrypt(strength 10)로 user-service 1 인스턴스 약 235건/초
- **구분**: 고도화 (용량) (관련: E-16, E-20, E-72)
- **단계/위치**: phase-1/8, `UserService.login`, `PasswordEncoder` 설정(strength 10)
- **현상**: P3 로 DB 커넥션 점유는 없앴고(pending 189 → 0) 남은 상한은 CPU 다. 로그인 1건이 약 50ms CPU 를 쓴다.
- **문제/기회**: 오픈 직전 사용자가 한꺼번에 로그인하면 대기가 길어지고, gateway 풀 대기 30초를 넘으면 500 이 된다(E-72). 10,000명이면 약 42초다. 기본 규모(3,000명)는 약 13초라 오픈 전 대기 시간(90초) 안에 끝난다.
- **근거**: 리포트 3장 "관찰 — 로그인 처리량 상한"(user-service CPU 98%, 약 235건/초), X1-1차 2,670건 500, P3 후 3,000 동시 로그인 p99 2.92s.
- **개선안**:
  1. **user-service 수평 확장**: CPU 바운드라 인스턴스 수에 거의 비례한다(추정). 상태가 없어 정합성 영향이 없다. 대가: 로드밸런서 필요.
  2. **로그인 빈도 줄이기**: 토큰 수명(JWT 2시간)을 오픈 시각에 맞춰 넉넉히 두거나 refresh token 을 도입해, 오픈 직전 재로그인을 줄인다. 대가: refresh token 은 저장·폐기 설계가 새로 필요하다(과제 범위 밖).
  3. **strength 낮추기**(10 → 8 이면 약 4배): 보안 수준을 낮추므로 권하지 않는다(E-16 과 같은 판단).
  4. 앞단 rate limit + 503(E-72 개선안 3): 처리량은 그대로지만 실패가 빠르고 의미가 맞다.
  - 현재 규모에서는 불필요하다. 기록용으로 두고, 확대 규모가 요구사항이 되면 1 을 택한다.
- **우선순위**: 낮음 — 기본 규모는 오픈 전 대기 안에 처리되고, 실제 사용자는 시간에 나눠 로그인한다.
- **영향 범위**: (1) 배포 구성, (2) `user-service`·gateway 인증 설계

### E-76 커스텀 카운터 — 요청마다 `meterRegistry.counter(name, tags)` 조회, 발생 전에는 시리즈가 없음
- **구분**: 고도화 (관측성, 성능 비용은 현재 규모에서 불필요) (관련: E-60, E-47, E-38)
- **단계/위치**: phase-8, `CouponController.countOutcome`, `EntryController.countOutcome`, `RedisCouponGate.compensate`, `RedisEntryGate.compensate`, `RedisWaitingQueueRepository.admit`
- **현상**: 기록할 때마다 `counter(name, "outcome", x)` 로 `Meter.Id`·`Tags` 를 만들고 레지스트리 맵에서 찾는다. 카운터는 처음 기록될 때 생긴다.
- **문제/기회**:
  - **비용**: 조회 1회는 객체 할당 몇 개와 해시 조회로 수백 ns 수준이다(추정, 측정 안 함). 쿠폰 15,000 동시에서 서버 처리 평균 11~14ms 이고 GC 최대 정지 13ms 라 **측정에서 비용은 드러나지 않았다**. 성능 이유의 캐싱은 현재 규모에서는 불필요하다.
  - **관측**: 발생하지 않은 outcome 은 Prometheus 시리즈 자체가 없다. T8-13 에서 "보상 시리즈 자체가 생성되지 않음"을 0 으로 판정했는데, 이 상태는 "배선이 안 됐다"와 구분되지 않는다. `increase(...[5m]) > 0` 알림은 처음 증가하는 순간을 놓친다(시리즈가 0 없이 1 로 시작하므로).
- **근거**: 코드 확인, 리포트 2장 T8-13 행.
- **개선안**:
  1. 생성자(또는 `@PostConstruct`)에서 `EnumMap<CouponErrorCode, Counter>` / `EnumMap<CompensationResult, Counter>` 와 OK 카운터를 미리 등록한다. 시리즈가 0 부터 존재해 알림과 "0 판정"이 명확해지고, 요청당 조회도 없어진다. 태그 값 집합이 enum 으로 고정되는 장점도 있다. 대가: 코드 수십 줄, 기존 메트릭 테스트(`CouponIssueMetricsTest` 등)가 "증가 전 카운터 없음"을 가정하면 테스트도 손봐야 한다(장원영 소관).
  2. 그대로 두고 대시보드·알림 쿼리를 `or vector(0)` 로 작성한다. 코드 변경 없음. "배선 안 됨"과 "0" 의 구분은 여전히 안 된다.
  - 1 을 권장하되, 성능이 아니라 관측 명확성 때문이다.
- **우선순위**: 낮음 — 측정 결과에 영향이 없었고 알림 체계가 아직 없다.
- **영향 범위**: 위 5개 클래스, 메트릭 테스트, Grafana 대시보드 쿼리
