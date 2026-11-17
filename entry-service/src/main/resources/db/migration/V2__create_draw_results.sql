-- 추첨 기록 (erd.md entry_db draw_results). drop_id UK 가 드로우당 추첨 1회를 보장한다
CREATE TABLE draw_results (
    id              BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    drop_id         BIGINT       NOT NULL,
    seed            BIGINT       NOT NULL,
    applicant_count INT          NOT NULL,
    winner_count    INT          NOT NULL,
    drawn_at        TIMESTAMPTZ  NOT NULL,
    published       BOOLEAN      NOT NULL DEFAULT FALSE,   -- draw.completed 발행 여부 (false 면 재발행 대상)
    CONSTRAINT uk_draw_results_drop_id UNIQUE (drop_id),
    CONSTRAINT ck_draw_results_counts CHECK (winner_count >= 0 AND winner_count <= applicant_count)
);

-- 미발행 기록 재발행 조회 (findUnpublished)
CREATE INDEX idx_draw_results_unpublished ON draw_results (drop_id) WHERE published = FALSE;
