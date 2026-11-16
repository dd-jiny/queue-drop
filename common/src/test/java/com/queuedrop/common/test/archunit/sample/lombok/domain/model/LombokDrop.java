package com.queuedrop.common.test.archunit.sample.lombok.domain.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** 컴파일 타임 Lombok 만 쓰는 domain 모델. 생성 코드에 @lombok.Generated(CLASS 보존)가 남는다 */
@Getter
@RequiredArgsConstructor
public class LombokDrop {
    private final Long id;
    private final int capacity;
}
