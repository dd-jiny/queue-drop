package com.queuedrop.product.application.dto;

import java.time.Instant;

public record DropRegisterCommand(Requester requester, Long productId, String title,
                                  int entryCapacity, int winnerCount, Instant openAt, Instant closeAt) {
}
