package com.queuedrop.entry.domain.repository;

/** EntryRepository.saveIfAbsent 결과 (E-48). */
public enum EntrySaveOutcome {
    /** APPLIED 로 저장 */
    SAVED,
    /** 추첨이 끝난(DRAWN) 드로우라 LOST 로 저장 */
    SAVED_AS_LOST,
    /** 같은 (dropId, userId) 응모가 이미 있어 저장하지 않음 */
    ALREADY_EXISTS
}
