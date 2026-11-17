-- drop.created 로 받은 드로우 정보의 읽기 전용 사본 (erd.md entry_db)
CREATE TABLE drop_snapshots (
    drop_id        BIGINT       PRIMARY KEY,
    entry_capacity INT          NOT NULL,
    winner_count   INT          NOT NULL,
    open_at        TIMESTAMPTZ  NOT NULL,
    close_at       TIMESTAMPTZ  NOT NULL,
    status         VARCHAR(20)  NOT NULL,
    CONSTRAINT ck_drop_snapshots_capacity CHECK (entry_capacity >= winner_count AND winner_count >= 1),
    CONSTRAINT ck_drop_snapshots_period   CHECK (open_at < close_at),
    CONSTRAINT ck_drop_snapshots_status   CHECK (status IN ('OPEN', 'CLOSING', 'DRAWN'))
);

CREATE TABLE entries (
    id          BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    drop_id     BIGINT       NOT NULL,
    user_id     BIGINT       NOT NULL,
    entry_seq   INT          NOT NULL,
    status      VARCHAR(20)  NOT NULL,
    applied_at  TIMESTAMPTZ  NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT uk_entries_drop_user UNIQUE (drop_id, user_id),     -- 1인 1드로우 1응모
    CONSTRAINT uk_entries_drop_seq  UNIQUE (drop_id, entry_seq),   -- 응모 번호 중복 없음
    CONSTRAINT ck_entries_status    CHECK (status IN ('APPLIED', 'WON', 'LOST'))
);

-- 내 응모 목록 조회 (GET /api/entries/me)
CREATE INDEX idx_entries_user_applied ON entries (user_id, applied_at DESC);
