package com.queuedrop.coupon.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.coupon.application.dto.Requester;

/** 관리자 권한 검사 (product·entry 의 AdminGuard 와 같은 형태). */
final class AdminGuard {

    private AdminGuard() {
    }

    static void requireAdmin(Requester requester) {
        if (requester == null || !requester.isAdmin()) {
            throw new ApplicationException(CommonErrorCode.FORBIDDEN);
        }
    }
}
