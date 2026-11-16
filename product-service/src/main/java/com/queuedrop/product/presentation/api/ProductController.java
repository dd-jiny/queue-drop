package com.queuedrop.product.presentation.api;

import com.queuedrop.product.application.service.ProductService;
import com.queuedrop.product.presentation.api.request.ProductCreateRequest;
import com.queuedrop.product.presentation.api.response.ProductResponse;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/products")
@RequiredArgsConstructor
public class ProductController {

    private final ProductService productService;
    private final RequesterResolver requesterResolver;

    @PostMapping
    public ResponseEntity<ProductResponse> create(
            @RequestHeader("X-User-Id") Long userId,
            @RequestHeader("X-User-Role") String role,
            @Valid @RequestBody ProductCreateRequest request) {
        var result = productService.create(request.toCommand(requesterResolver.resolve(userId, role)));
        return ResponseEntity.status(HttpStatus.CREATED).body(ProductResponse.from(result));
    }
}
