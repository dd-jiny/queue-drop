# queue-drop

한정판 선착순 드로우 서비스. 대기열 → 선착순 응모(정원 M) → 마감 시각 자동 추첨(K명). 결제 없음.
핵심 기능: **대기열, 선착순 쿠폰 발행, 랭킹**.

## 기술 스택
- Java 21, Spring Boot 4.1.2, Spring Cloud 2025.1.4, Gradle 9 멀티모듈 (wrapper 사용: `./gradlew`)
- PostgreSQL 17 (서비스별 DB, Flyway, `ddl-auto: validate`), Redis 7.4, Kafka 4.0 (KRaft)
- 인프라: `docker compose up -d` (PostgreSQL, Redis, Kafka, Kafka UI :8090)
- 모듈: `gateway`, `user-service`, `product-service`, `queue-service`, `entry-service`, `coupon-service`, `ranking-service`, `common`

## 문서 (작업 전에 반드시 읽기)
| 경로 | 내용 |
|------|------|
| `docs/design/` | 설계 — **무엇을, 왜**. 인덱스: `docs/design/README.md` |
| `docs/design/02-architecture/layered-architecture.md` | 4계층 · DIP · 모델 비노출 규칙 (ArchUnit R1~R7) |
| `docs/design/04-non-functional/concurrency-consistency.md` | 동시성·정합성 결정 D1~D7 |
| `docs/plan/implementation-plan.md` | 전체 순서(0~8단계), 진행 현황, 미결정 사항 처리 시점 |
| `docs/plan/phases/phase-{n}-*.md` | 단계별 구현 상세 — 파일 목록, 시그니처, Lua·SQL·설정, 테스트 ID, 커밋 계획 |
| `docs/plan/enhancement-backlog.md` | 고도화 백로그 (한지민이 작성, 없으면 생성) |

- 설계와 다르게 구현해야 하면 **설계 문서를 먼저 수정**(변경 이력 행 추가)하고 사용자 확인을 받는다.
- 미결정 사항: 플랜에 기본안이 있고 설계 방향을 바꾸지 않는 것은 Claude 가 기본안으로 정해 문서에 "확정(기본안)"으로 남기고 사후 보고한다. **설계 방향을 바꾸는 결정**(예: 5단계 추첨 건수 불일치 정책, 7단계 랭킹 범위·동점)은 선택지 + 트레이드오프를 제시하고 사용자가 결정한다.
- 명백한 버그 수정(정합성·장애 경로)은 설계 문서 변경 이력을 남기고 반영한 뒤 사후 보고한다.

## 아키텍처 규칙 (요약 — 상세는 layered-architecture.md)
- 패키지: `com.queuedrop.{service}.{presentation|application|domain|infrastructure}`
- presentation 은 domain 을 import 하지 않는다. application public 메서드는 Command/Result 만 사용 (도메인 모델 노출 금지)
- domain 은 순수 Java (Spring·JPA 금지, Lombok compileOnly 어노테이션만). 시각은 `now` 인자로 받는다
- 도메인 모델 ↔ JPA 엔티티 분리 + Mapper. 영속 포트는 `domain/repository`, 외부 연동 포트는 `application/port`
- Kafka Listener · Scheduler 는 presentation. application 은 `common.event` 메시지에 직접 의존하지 않는다

## 협업 규칙
- **Claude 가 브랜치 생성·커밋·push·PR 생성·`dev` 머지·머지된 브랜치 삭제까지 수행한다** (2026-11-16 사용자 위임). 커밋 메시지는 Conventional Commits, 한국어 본문.
- `main` 머지는 사용자가 요청할 때만 한다.
- 브랜치: `dev` 에서 단계별로 생성 → PR 로 `dev` 머지. 이름 `{type}/phase-{n}-{name}` (단계 문서의 브랜치명 사용)
- PR 본문 끝: `🤖 Generated with [Claude Code](https://claude.com/claude-code)`

## TDD 역할 분담
역할 이름은 호칭일 뿐이며 실제 인물을 흉내 내지 않는다.

| 역할 | 담당 | 정의 |
|------|------|------|
| **카리나** | 메인 세션. 구현(Green), 리팩터링, 브랜치 생성, 커밋 메시지·PR 본문 작성 | — |
| **장원영** | 실패하는 테스트 작성(Red), Green 이후 전체 실행·경계 케이스 보강. **테스트 코드 소유자** | `.claude/agents/jang-wonyoung.md` |
| **신민아** | 시니어 코드 검증. 설계·계층·정합성 기준으로 지적만 (수정 권한 없음) | `.claude/agents/shin-mina.md` |
| **한지민** | 시니어 고도화 분석 (조회 최적화, 인덱스, N+1, Redis/Kafka 효율). 백로그에만 기록 | `.claude/agents/han-jimin.md` |

### 사이클 (작업 단위 = 단계 문서 "커밋 계획"의 한 항목)
1. **장원영 (Red)**: 해당 테스트 ID 의 실패하는 테스트 작성 → 의도한 이유로 실패하는지 확인
2. **카리나 (Green)**: 테스트를 통과하는 최소 구현. **테스트를 통과시키려고 테스트 코드를 고치지 않는다** — 테스트가 틀렸다고 보면 근거와 함께 장원영에게 넘긴다
3. **카리나 (Refactor)**: 테스트가 통과하는 상태를 유지하며 정리
4. **장원영 (검증)**: 모듈 전체 테스트 + ArchitectureTest 실행, 빠진 경계 케이스 보강 → 실패 시 2로
5. **신민아 (검증)**: BLOCKER / MAJOR 는 카리나가 수정 후 재검증. **왕복 2회를 넘기면 멈추고 사용자에게 판단 요청**
6. **카리나**: 커밋

- 1~3단계처럼 단순한 단계는 신민아 검증을 단계 끝에 한 번만 해도 된다 (사용자와 합의 후). 4단계부터는 사이클마다 수행.
- **단계 종료 시**: 한지민 고도화 분석 → 백로그 추가 → 명백한 버그는 반영, 나머지는 해당 단계에서 처리. 이후 플랜 진행 현황 갱신, PR 생성 → `dev` 머지 → 브랜치 삭제.

## 현재 진행 상황
- 완료: 요구사항·설계 문서, 구현 플랜, 단계별 구현 상세, 역할 에이전트 정의, **0단계 — 공통 기반** (PR #4), **1단계 — 인증** (PR #5), **2단계 — 드로우**
- 다음: **3단계 — 대기열** (`feat/phase-3-queue`, `docs/plan/phases/phase-3-queue.md`)
- 진행 현황 표는 `docs/plan/implementation-plan.md` 2장을 기준으로 갱신한다.
