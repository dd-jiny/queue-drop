package com.queuedrop.product.presentation.api;

import com.queuedrop.product.application.dto.Requester;
import org.springframework.stereotype.Component;

/** gateway 주입 헤더 → Requester. ADMIN 여부 판단은 application 이 한다. */
@Component
public class RequesterResolver {

    public Requester resolve(Long userId, String role) {
        return new Requester(userId, role);
    }
}
