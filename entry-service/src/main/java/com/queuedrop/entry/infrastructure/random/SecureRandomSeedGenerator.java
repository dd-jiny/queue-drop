package com.queuedrop.entry.infrastructure.random;

import com.queuedrop.entry.application.port.SeedGenerator;
import java.security.SecureRandom;
import org.springframework.stereotype.Component;

/** 추첨 시드 소스. 시드 재현은 java.util.Random 이 맡고, 시드 자체는 SecureRandom 으로 만든다 (NFR-04) */
@Component
public class SecureRandomSeedGenerator implements SeedGenerator {

    private final SecureRandom random = new SecureRandom();

    @Override
    public long next() {
        return random.nextLong();
    }
}
