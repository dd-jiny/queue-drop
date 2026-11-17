CREATE TABLE coupon_events (
    id              BIGSERIAL     PRIMARY KEY,
    name            VARCHAR(100)  NOT NULL,
    total_quantity  INT           NOT NULL,
    issue_start_at  TIMESTAMPTZ   NOT NULL,
    created_at      TIMESTAMPTZ   NOT NULL,
    CONSTRAINT ck_coupon_events_quantity CHECK (total_quantity >= 1)
);
