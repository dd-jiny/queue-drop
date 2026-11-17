package com.queuedrop.ranking.infrastructure.redis;

import static org.assertj.core.api.Assertions.assertThat;

import com.queuedrop.ranking.domain.exception.RankingStoreUnavailableException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Redis 저장소 테스트 공용 도우미 (coupon RedisCouponGateTest 와 같은 장애 주입 방식) */
final class RedisTestSupport {

    /** 명령 타임아웃(spring.data.redis.timeout=1s)보다 길게 */
    static final long PAUSE_MS = 1_500L;

    private RedisTestSupport() {}

    static void flushAll(StringRedisTemplate redis) {
        redis.execute((RedisCallback<Object>) connection -> {
            connection.serverCommands().flushAll();
            return null;
        });
    }

    /** 아무도 듣지 않는 포트로 연결하는 팩토리. 호출 측이 destroy 한다. */
    static LettuceConnectionFactory unreachableFactory(String host) {
        int closedPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            closedPort = socket.getLocalPort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        LettuceConnectionFactory factory = new LettuceConnectionFactory(
                new RedisStandaloneConfiguration(host, closedPort),
                LettuceClientConfiguration.builder().commandTimeout(Duration.ofSeconds(1)).build());
        factory.afterPropertiesSet();
        factory.start();
        return factory;
    }

    /** 별도 연결로 CLIENT PAUSE {PAUSE_MS} WRITE 를 보낸 뒤 action 을 실행한다 */
    static void withWritePause(LettuceConnectionFactory factory, Runnable action) {
        RedisClient client = RedisClient.create(RedisURI.create(factory.getHostName(), factory.getPort()));
        try (StatefulRedisConnection<String, String> pauser = client.connect()) {
            pauser.sync().dispatch(CommandType.CLIENT, new StatusOutput<>(StringCodec.UTF8),
                    new CommandArgs<>(StringCodec.UTF8).add("PAUSE").add(PAUSE_MS).add("WRITE"));
            action.run();
        } finally {
            client.shutdown();
        }
    }

    static void assertStoreUnavailable(String name, Throwable thrown) {
        assertThat(thrown).as(name).isInstanceOf(RankingStoreUnavailableException.class);
        assertThat(thrown.getCause()).as(name).isInstanceOf(DataAccessException.class);
    }

    static <T> void assertAllStoreUnavailable(T target, Map<String, Consumer<T>> calls) {
        calls.forEach((name, call) -> {
            Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(() -> call.accept(target));
            assertStoreUnavailable(name, thrown);
        });
    }
}
