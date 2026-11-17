package com.queuedrop.queue.application.dto;

import java.time.Instant;

public record QueueMetaRegisterCommand(Long dropId, Instant openAt, Instant closeAt) {}
