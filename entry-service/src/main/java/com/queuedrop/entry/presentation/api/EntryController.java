package com.queuedrop.entry.presentation.api;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.EntryApplyCommand;
import com.queuedrop.entry.application.dto.EntryApplyResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.service.EntryApplyService;
import com.queuedrop.entry.application.service.EntryQueryService;
import com.queuedrop.entry.presentation.api.response.EntryApplyResponse;
import com.queuedrop.entry.presentation.api.response.MyEntryResponse;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 응모는 판정 성공 시 202 (DB 저장은 entry.created 컨슈머가 비동기로 한다).
 * 판정 결과는 entry.apply.outcome{outcome} 로 센다 — 태그는 ErrorCode 접두사 없는 판정 이름 (8단계 3.2).
 */
@RestController
@RequestMapping("/api/entries")
@RequiredArgsConstructor
public class EntryController {

    private static final String APPLY_OUTCOME = "entry.apply.outcome";

    private final EntryApplyService entryApplyService;
    private final EntryQueryService entryQueryService;
    private final MeterRegistry meterRegistry;

    @PostMapping("/drops/{dropId:\\d+}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public EntryApplyResponse apply(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        EntryApplyResult result;
        try {
            result = entryApplyService.apply(new EntryApplyCommand(dropId, userId));
        } catch (ApplicationException e) {
            if (e.errorCode() instanceof EntryErrorCode code) {
                countOutcome(code.name());
            }
            throw e;
        }
        countOutcome("OK");
        return EntryApplyResponse.from(result);
    }

    @GetMapping("/me")
    public List<MyEntryResponse> myEntries(@RequestHeader("X-User-Id") Long userId) {
        return entryQueryService.myEntries(userId).stream().map(MyEntryResponse::from).toList();
    }

    @GetMapping("/drops/{dropId:\\d+}/me")
    public MyEntryResponse myEntry(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        return MyEntryResponse.from(entryQueryService.myEntry(dropId, userId));
    }

    private void countOutcome(String outcome) {
        meterRegistry.counter(APPLY_OUTCOME, "outcome", outcome).increment();
    }
}
