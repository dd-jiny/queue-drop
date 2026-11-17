package com.queuedrop.ranking.domain.model;

import com.queuedrop.ranking.domain.exception.DomainException;
import java.time.Instant;

/** Command 의 박싱 필드를 검증해 원시값으로 꺼낸다. 위반 시 DomainException */
public final class EventFields {

    private EventFields() {}

    public static long id(Long value, String name) {
        if (value == null || value <= 0) {
            throw new DomainException(name + " 가 올바르지 않음: " + value);
        }
        return value;
    }

    public static Instant instant(Instant value, String name) {
        if (value == null) {
            throw new DomainException(name + " 가 없음");
        }
        return value;
    }

    public static String text(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new DomainException(name + " 가 비어 있음");
        }
        return value;
    }
}
