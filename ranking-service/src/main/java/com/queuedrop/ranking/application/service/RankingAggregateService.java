package com.queuedrop.ranking.application.service;

import com.queuedrop.ranking.application.dto.DropInfoCommand;
import com.queuedrop.ranking.application.dto.RankingRecordCommand;
import com.queuedrop.ranking.application.dto.UserInfoCommand;
import com.queuedrop.ranking.domain.exception.DomainException;
import com.queuedrop.ranking.domain.model.EventFields;
import com.queuedrop.ranking.domain.model.WeekKey;
import com.queuedrop.ranking.domain.repository.DisplayNameRepository;
import com.queuedrop.ranking.domain.repository.DropWindowRepository;
import com.queuedrop.ranking.domain.repository.RankingRepository;
import java.time.Instant;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** 집계·표시 정보 저장 (쓰기 경로). Redis 만 쓰므로 @Transactional 은 붙이지 않는다 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RankingAggregateService {

    private final RankingRepository rankingRepository;
    private final DisplayNameRepository displayNameRepository;
    private final DropWindowRepository dropWindowRepository;

    /** @return 이번 호출로 집계됐으면 true, 중복이거나 잘못된 메시지면 false */
    public boolean recordEntry(RankingRecordCommand cmd) {
        long dropId, userId;
        WeekKey week;
        try {
            dropId = EventFields.id(cmd.dropId(), "dropId");
            userId = EventFields.id(cmd.userId(), "userId");
            week = WeekKey.of(EventFields.instant(cmd.appliedAt(), "appliedAt"));   // 처리 시각이 아니라 응모 시각
        } catch (DomainException e) {
            log.warn("invalid entry.created skipped: dropId={} userId={} - {}", cmd.dropId(), cmd.userId(), e.getMessage());
            return false;
        }
        return rankingRepository.incrementOnce(dropId, userId, week);   // RankingStoreUnavailableException 은 전파
    }

    /** 모든 필드를 검증한 뒤에만 쓴다 — 제목만 저장되고 closeAt 이 빠지는 반쪽 저장이 없다 */
    public void registerDrop(DropInfoCommand cmd) {
        long dropId;
        String title;
        Instant closeAt;
        try {
            dropId = EventFields.id(cmd.dropId(), "dropId");
            title = EventFields.text(cmd.title(), "title");
            closeAt = EventFields.instant(cmd.closeAt(), "closeAt");
        } catch (DomainException e) {
            log.warn("invalid drop.created skipped: dropId={} - {}", cmd.dropId(), e.getMessage());
            return;
        }
        displayNameRepository.saveDropTitle(dropId, title);
        dropWindowRepository.save(dropId, closeAt);
    }

    public void registerUser(UserInfoCommand cmd) {
        long userId;
        String nickname;
        try {
            userId = EventFields.id(cmd.userId(), "userId");
            nickname = EventFields.text(cmd.nickname(), "nickname");
        } catch (DomainException e) {
            log.warn("invalid user.registered skipped: userId={} - {}", cmd.userId(), e.getMessage());
            return;
        }
        displayNameRepository.saveNickname(userId, nickname);
    }
}
