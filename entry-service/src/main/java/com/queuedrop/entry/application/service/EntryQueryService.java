package com.queuedrop.entry.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.entry.application.dto.MyEntryResult;
import com.queuedrop.entry.application.exception.EntryErrorCode;
import com.queuedrop.entry.application.port.EntryGate;
import com.queuedrop.entry.domain.repository.EntryRepository;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/** 내 응모 조회. Redis 보완은 단건 조회에만 적용하고 목록은 DB 만 본다. */
@Service
@RequiredArgsConstructor
public class EntryQueryService {

    private final EntryRepository entryRepository;
    private final EntryGate entryGate;

    public List<MyEntryResult> myEntries(long userId) {
        return entryRepository.findAllByUserId(userId).stream().map(MyEntryResult::from).toList();
    }

    public MyEntryResult myEntry(long dropId, long userId) {
        return entryRepository.findByDropIdAndUserId(dropId, userId)
                .map(MyEntryResult::from)
                .orElseGet(() -> {
                    if (entryGate.isApplicant(dropId, userId)) {
                        return MyEntryResult.pendingSave(dropId);     // 판정은 됐고 DB 저장 대기 중
                    }
                    throw new ApplicationException(EntryErrorCode.NOT_FOUND);
                });
    }
}
