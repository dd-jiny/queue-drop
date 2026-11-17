package com.queuedrop.ranking.application.dto;

/** user.registered 필드를 그대로 담는다 (박싱 타입). 검증은 application 이 한다 */
public record UserInfoCommand(Long userId, String nickname) {}
