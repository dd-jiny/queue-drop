package com.queuedrop.product.application.dto;

public record ProductCreateCommand(Requester requester, String name, String description, String imageUrl) {
}
