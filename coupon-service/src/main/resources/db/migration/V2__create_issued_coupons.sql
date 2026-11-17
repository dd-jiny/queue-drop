CREATE TABLE issued_coupons (
    id               BIGSERIAL    PRIMARY KEY,
    coupon_event_id  BIGINT       NOT NULL REFERENCES coupon_events (id),
    user_id          BIGINT       NOT NULL,
    issued_at        TIMESTAMPTZ  NOT NULL,
    CONSTRAINT uk_issued_coupons_event_user UNIQUE (coupon_event_id, user_id)
);

CREATE INDEX idx_issued_coupons_user ON issued_coupons (user_id, issued_at DESC);   -- 내 쿠폰 조회
