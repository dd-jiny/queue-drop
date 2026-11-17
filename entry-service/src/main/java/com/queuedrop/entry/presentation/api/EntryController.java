package com.queuedrop.entry.presentation.api;

import com.queuedrop.entry.application.dto.EntryApplyCommand;
import com.queuedrop.entry.application.service.EntryApplyService;
import com.queuedrop.entry.application.service.EntryQueryService;
import com.queuedrop.entry.presentation.api.response.EntryApplyResponse;
import com.queuedrop.entry.presentation.api.response.MyEntryResponse;
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

/** 응모는 판정 성공 시 202 (DB 저장은 entry.created 컨슈머가 비동기로 한다) */
@RestController
@RequestMapping("/api/entries")
@RequiredArgsConstructor
public class EntryController {

    private final EntryApplyService entryApplyService;
    private final EntryQueryService entryQueryService;

    @PostMapping("/drops/{dropId:\\d+}")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public EntryApplyResponse apply(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        return EntryApplyResponse.from(entryApplyService.apply(new EntryApplyCommand(dropId, userId)));
    }

    @GetMapping("/me")
    public List<MyEntryResponse> myEntries(@RequestHeader("X-User-Id") Long userId) {
        return entryQueryService.myEntries(userId).stream().map(MyEntryResponse::from).toList();
    }

    @GetMapping("/drops/{dropId:\\d+}/me")
    public MyEntryResponse myEntry(@RequestHeader("X-User-Id") Long userId, @PathVariable Long dropId) {
        return MyEntryResponse.from(entryQueryService.myEntry(dropId, userId));
    }
}
