package com.queuedrop.product.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.product.application.dto.DropRegisterCommand;
import com.queuedrop.product.application.dto.DropResult;
import com.queuedrop.product.application.dto.MarkDrawnCommand;
import com.queuedrop.product.application.dto.Requester;
import com.queuedrop.product.application.exception.ProductErrorCode;
import com.queuedrop.product.application.port.DropCreatedPayload;
import com.queuedrop.product.application.port.DropEventPublisher;
import com.queuedrop.product.domain.exception.DomainException;
import com.queuedrop.product.domain.model.Drop;
import com.queuedrop.product.domain.repository.DropRepository;
import com.queuedrop.product.domain.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

/** 드로우 등록·재발행·추첨 완료 반영 유스케이스 */
@Service
@RequiredArgsConstructor
public class DropService {

    private final ProductRepository productRepository;
    private final DropRepository dropRepository;
    private final DropEventPublisher eventPublisher;
    private final Clock clock;

    @Transactional
    public DropResult register(DropRegisterCommand command) {
        AdminGuard.requireAdmin(command.requester());
        if (!productRepository.existsById(command.productId())) {
            throw new ApplicationException(ProductErrorCode.PRODUCT_NOT_FOUND);
        }
        Instant now = clock.instant();
        Drop drop;
        try {
            drop = Drop.create(command.productId(), command.title(), command.entryCapacity(),
                    command.winnerCount(), command.openAt(), command.closeAt(), now);
        } catch (DomainException e) {
            throw new ApplicationException(ProductErrorCode.INVALID_DROP, e);
        }
        Drop saved = dropRepository.save(drop);
        eventPublisher.publishCreated(toPayload(saved));   // 실제 전송은 커밋 후 (발행 어댑터 책임)
        return DropResult.of(saved, now);
    }

    /**
     * 발행 실패 복구용. 트랜잭션 밖에서 호출되어 즉시 발행된다.
     * 이미 오픈된 드로우도 허용한다 — 수신 측이 SET NX 로 멱등 처리해 잔여 정원을 덮어쓰지 않는다.
     */
    public void republish(Requester requester, Long dropId) {
        AdminGuard.requireAdmin(requester);
        Drop drop = dropRepository.findById(dropId)
                .orElseThrow(() -> new ApplicationException(ProductErrorCode.DROP_NOT_FOUND));
        eventPublisher.publishCreated(toPayload(drop));
    }

    /**
     * draw.completed 수신 시 추첨 완료 반영 (5단계 3.5). 재전달되면 이미 drawn 이라 무시한다 (멱등).
     * 도메인 모델은 영속 엔티티가 아니라 변경 감지가 없다 → 명시 save (E-30).
     */
    @Transactional
    public void markDrawn(MarkDrawnCommand command) {
        Drop drop = dropRepository.findById(command.dropId())
                .orElseThrow(() -> new ApplicationException(ProductErrorCode.DROP_NOT_FOUND));
        if (drop.isDrawn()) {
            return;
        }
        drop.markDrawn();
        dropRepository.save(drop);
    }

    private DropCreatedPayload toPayload(Drop d) {
        return new DropCreatedPayload(d.getId(), d.getProductId(), d.getTitle(), d.getEntryCapacity(),
                d.getWinnerCount(), d.getOpenAt(), d.getCloseAt());
    }
}
