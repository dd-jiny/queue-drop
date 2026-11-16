package com.queuedrop.product.application.dto;

/** 요청자 정보. presentation 이 gateway 주입 헤더(X-User-Id, X-User-Role)로 만든다. */
public record Requester(Long userId, String role) {
    public boolean isAdmin() {
        return "ADMIN".equals(role);
    }
}
