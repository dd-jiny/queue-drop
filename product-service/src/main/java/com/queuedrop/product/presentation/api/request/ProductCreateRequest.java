package com.queuedrop.product.presentation.api.request;

import com.queuedrop.product.application.dto.ProductCreateCommand;
import com.queuedrop.product.application.dto.Requester;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record ProductCreateRequest(
        @NotBlank @Size(max = 100) String name,
        String description,
        @Size(max = 500) String imageUrl) {

    public ProductCreateCommand toCommand(Requester requester) {
        return new ProductCreateCommand(requester, name, description, imageUrl);
    }
}
