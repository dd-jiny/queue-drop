package com.queuedrop.product.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.error.CommonErrorCode;
import com.queuedrop.product.application.dto.Requester;

/** 관리자 권한 검사. ProductService·DropService 가 공유한다. */
final class AdminGuard {

    private AdminGuard() {
    }

    static void requireAdmin(Requester requester) {
        if (requester == null || !requester.isAdmin()) {
            throw new ApplicationException(CommonErrorCode.FORBIDDEN);
        }
    }
}
