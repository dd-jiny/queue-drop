package com.queuedrop.entry.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.DrawSummaryResult;
import com.queuedrop.entry.application.exception.DrawErrorCode;
import com.queuedrop.entry.domain.repository.DrawResultRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class DrawQueryService {

    private final DrawResultRepository results;

    public DrawSummaryResult summary(Long dropId) {
        return results.findByDropId(dropId)
                .map(DrawSummaryResult::from)
                .orElseThrow(() -> new ApplicationException(DrawErrorCode.NOT_COMPLETED));
    }
}
