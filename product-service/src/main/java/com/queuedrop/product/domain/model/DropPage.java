package com.queuedrop.product.domain.model;

import java.util.List;

/** 드로우 페이지 조회 결과. domain 이 Spring Data Page 에 의존하지 않도록 둔 자체 타입. */
public record DropPage(List<Drop> content, long totalElements, int page, int size) {
    public DropPage {
        content = List.copyOf(content);
    }
}
