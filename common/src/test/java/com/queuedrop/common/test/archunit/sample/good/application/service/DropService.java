package com.queuedrop.common.test.archunit.sample.good.application.service;

import com.queuedrop.common.error.ApplicationException;
import com.queuedrop.common.test.archunit.sample.good.application.dto.DropResult;
import com.queuedrop.common.test.archunit.sample.good.application.exception.DropErrorCode;
import com.queuedrop.common.test.archunit.sample.good.application.port.DropRepository;
import com.queuedrop.common.test.archunit.sample.good.domain.model.Drop;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Service;

/** public 시그니처는 Result 만 노출, domain 모델은 private·package-private 메서드에서만 사용 */
@Service
public class DropService {
    private final DropRepository dropRepository;

    public DropService(DropRepository dropRepository) { this.dropRepository = dropRepository; }

    public DropResult get(Long id) { return toResult(load(id)); }

    public List<DropResult> getAll() { return dropRepository.findAll().stream().map(DropService::toResult).toList(); }

    public Optional<DropResult> find(Long id) { return dropRepository.findById(id).map(DropService::toResult); }

    private Drop load(Long id) {
        return dropRepository.findById(id).orElseThrow(() -> new ApplicationException(DropErrorCode.DROP_NOT_FOUND));
    }

    private static DropResult toResult(Drop drop) { return new DropResult(drop.getId(), drop.getCapacity()); }

    Drop packagePrivateLoad(Long id) { return load(id); }
}
