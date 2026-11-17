package com.queuedrop.common.test.archunit.sample.good.application.port;

import com.queuedrop.common.test.archunit.sample.good.domain.model.Drop;
import java.util.List;
import java.util.Optional;

/** 포트는 domain 모델을 주고받아도 된다 (R6 범위 밖) */
public interface DropRepository {
    Optional<Drop> findById(Long id);
    List<Drop> findAll();
}
