package com.queuedrop.product.application.dto;

import java.util.List;

public record DropPageResult(List<DropResult> content, long totalElements, int page, int size) {
}
