package com.queuedrop.product.application.dto;

/** status 는 nullable (null 이면 전체): SCHEDULED | OPEN | CLOSED | DRAWN */
public record DropSearchQuery(String status, int page, int size) {
}
