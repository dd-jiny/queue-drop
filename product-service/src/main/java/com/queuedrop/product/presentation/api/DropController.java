package com.queuedrop.product.presentation.api;

import com.queuedrop.product.application.dto.DropSearchQuery;
import com.queuedrop.product.application.service.DropQueryService;
import com.queuedrop.product.application.service.DropService;
import com.queuedrop.product.presentation.api.request.DropRegisterRequest;
import com.queuedrop.product.presentation.api.response.DropPageResponse;
import com.queuedrop.product.presentation.api.response.DropResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 경로 변수에 숫자 정규식을 붙여 /drops 고정 경로와 충돌하지 않게 한다. */
@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class DropController {

    private final DropService dropService;
    private final DropQueryService dropQueryService;
    private final RequesterResolver requesterResolver;

    @PostMapping("/{productId:\\d+}/drops")
    public ResponseEntity<DropResponse> register(
            @RequestHeader("X-User-Id") Long userId,
            @RequestHeader("X-User-Role") String role,
            @PathVariable Long productId,
            @Valid @RequestBody DropRegisterRequest request) {
        var result = dropService.register(request.toCommand(requesterResolver.resolve(userId, role), productId));
        return ResponseEntity.status(HttpStatus.CREATED).body(DropResponse.from(result));
    }

    @GetMapping("/drops")
    public DropPageResponse search(
            @RequestParam(required = false) String status,
            @RequestParam(defaultValue = "0") @Min(0) @Max(10000) int page,   // offset(page×size) 상한
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size) {
        return DropPageResponse.from(dropQueryService.search(new DropSearchQuery(status, page, size)));
    }

    @GetMapping("/drops/{dropId:\\d+}")
    public DropResponse get(@PathVariable Long dropId) {
        return DropResponse.from(dropQueryService.get(dropId));
    }

    @PostMapping("/drops/{dropId:\\d+}/republish")
    public ResponseEntity<Void> republish(
            @RequestHeader("X-User-Id") Long userId,
            @RequestHeader("X-User-Role") String role,
            @PathVariable Long dropId) {
        dropService.republish(requesterResolver.resolve(userId, role), dropId);
        return ResponseEntity.accepted().build();
    }
}
