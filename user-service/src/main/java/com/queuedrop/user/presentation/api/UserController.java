package com.queuedrop.user.presentation.api;

import com.queuedrop.user.application.service.UserService;
import com.queuedrop.user.presentation.api.request.LoginRequest;
import com.queuedrop.user.presentation.api.request.SignupRequest;
import com.queuedrop.user.presentation.api.response.LoginResponse;
import com.queuedrop.user.presentation.api.response.SignupResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @PostMapping("/signup")
    public ResponseEntity<SignupResponse> signup(@Valid @RequestBody SignupRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(SignupResponse.from(userService.signup(request.toCommand())));
    }

    @PostMapping("/login")
    public LoginResponse login(@Valid @RequestBody LoginRequest request) {
        return LoginResponse.from(userService.login(request.toCommand()));
    }
}
