package com.queuedrop.queue.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** 키 이름은 서비스 간 계약(architecture.md 5장, 단계 문서 3.0.1)이므로 문자열로 고정한다 */
class QueueKeysTest {

    @Test
    @DisplayName("[T3-25] Redis 키 이름이 단계 문서 3.0.1 표와 일치한다")
    void keyNames() {
        // When / Then
        assertThat(QueueKeys.meta(1L)).isEqualTo("queue:1:meta");
        assertThat(QueueKeys.seq(1L)).isEqualTo("queue:1:seq");
        assertThat(QueueKeys.waiting(1L)).isEqualTo("queue:1:waiting");
        assertThat(QueueKeys.tokenPrefix(1L)).isEqualTo("queue:1:token:");
        assertThat(QueueKeys.token(1L, 7L)).isEqualTo("queue:1:token:7");
        assertThat(QueueKeys.gate("admission")).isEqualTo("queue:admission:gate");
        assertThat(QueueKeys.entryRemaining(1L)).isEqualTo("entry:1:remaining");
        assertThat(QueueKeys.ACTIVE_DROPS).isEqualTo("queue:active-drops");
    }
}
