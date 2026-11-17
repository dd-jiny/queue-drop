package com.queuedrop.common.event;

/** Kafka 토픽 이름. architecture.md 4장과 일치해야 한다. */
public final class Topics {
    public static final String USER_REGISTERED = "user.registered";
    public static final String DROP_CREATED    = "drop.created";
    public static final String ENTRY_CREATED   = "entry.created";
    public static final String DRAW_COMPLETED  = "draw.completed";
    public static final String COUPON_ISSUED   = "coupon.issued";

    /** 저장 컨슈머 실패 시 DefaultErrorHandler 가 3회 재시도 후 보내는 토픽 ({topic}.DLT 규칙) */
    public static final String DROP_CREATED_DLT  = DROP_CREATED + ".DLT";
    public static final String ENTRY_CREATED_DLT = ENTRY_CREATED + ".DLT";
    public static final String DRAW_COMPLETED_DLT = DRAW_COMPLETED + ".DLT";
    public static final String COUPON_ISSUED_DLT = COUPON_ISSUED + ".DLT";

    private Topics() {}
}
