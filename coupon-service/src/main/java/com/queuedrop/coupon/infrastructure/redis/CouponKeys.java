package com.queuedrop.coupon.infrastructure.redis;

/** coupon-service Redis 키 이름의 단일 출처 (architecture.md 5장). {eventId} = couponEventId */
public final class CouponKeys {

    private CouponKeys() {}

    public static String meta(long couponEventId)      { return "coupon:" + couponEventId + ":meta"; }
    /** 값은 항상 10진 정수 문자열 */
    public static String remaining(long couponEventId) { return "coupon:" + couponEventId + ":remaining"; }
    public static String issued(long couponEventId)    { return "coupon:" + couponEventId + ":issued"; }
    /** userId → 발급 자리를 잡은 요청 ID (E-40) */
    public static String requests(long couponEventId)  { return "coupon:" + couponEventId + ":requests"; }
    /** 저장 컨슈머가 DB 저장 전에 확정한 userId (E-40) */
    public static String persisted(long couponEventId) { return "coupon:" + couponEventId + ":persisted"; }
}
