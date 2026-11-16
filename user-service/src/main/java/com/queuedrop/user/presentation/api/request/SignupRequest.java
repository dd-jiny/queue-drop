package com.queuedrop.user.presentation.api.request;

import com.queuedrop.user.application.dto.SignupCommand;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/** 비밀번호 정책(8~64자)은 입력 형식이라 여기서 검증한다. 이메일·닉네임의 최종 규칙은 domain 이 검증한다. */
public record SignupRequest(
        @NotBlank @Email @Size(max = 255) String email,
        @NotBlank @Size(min = 8, max = 64) String password,
        @NotBlank @Size(min = 2, max = 20) String nickname
) {
    public SignupCommand toCommand() {
        return new SignupCommand(email, password, nickname);
    }
}
