package com.queuedrop.common.event;

/** user-service → ranking. key = userId */
public record UserRegistered(Long userId, String nickname) {}
