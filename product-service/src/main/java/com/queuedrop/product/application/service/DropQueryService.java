package com.queuedrop.product.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.product.application.dto.DropPageResult;
import com.queuedrop.product.application.dto.DropResult;
import com.queuedrop.product.application.dto.DropSearchQuery;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.domain.model.Drop;
import com.queuedrop.product.domain.model.DropPage;
import com.queuedrop.product.domain.model.DropStatus;
import com.queuedrop.product.domain.repository.DropRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

/** 드로우 목록·상세 조회 (공개) */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class DropQueryService {

    private final DropRepository dropRepository;
    private final Clock clock;

    public DropPageResult search(DropSearchQuery query) {
        DropStatus status = parseStatus(query.status());
        Instant now = clock.instant();
        DropPage page = dropRepository.search(status, now, query.page(), query.size());
        return new DropPageResult(page.content().stream().map(d -> DropResult.of(d, now)).toList(),
                page.totalElements(), page.page(), page.size());
    }

    public DropResult get(Long dropId) {
        Drop drop = dropRepository.findById(dropId)
                .orElseThrow(() -> new ApplicationException(ProductErrorCode.DROP_NOT_FOUND));
        return DropResult.of(drop, clock.instant());
    }

    /** null·공백이면 전체 조회. 대소문자는 구분한다 (API 표기 그대로: OPEN 등). */
    private DropStatus parseStatus(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return DropStatus.valueOf(raw);
        } catch (IllegalArgumentException e) {
            throw new ApplicationException(ProductErrorCode.INVALID_STATUS_FILTER, e);
        }
    }
}
