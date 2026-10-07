package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * TPM 限流 Java 侧行为：Lua 由 mock {@code ReactiveRedisTemplate.execute} 替身，
 * 不断开真实 Redis。ARGV 契约对应 classpath 脚本，脚本本体不在此执行。
 */
class RateLimiterServiceTest {

    private ReactiveRedisTemplate<String, String> redis;
    private GatewayProperties props;
    private RateLimiterService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redis = mock(ReactiveRedisTemplate.class);
        props = new GatewayProperties();
        props.setTpmCapacity(100_000);
        props.setTpmRefillRatePerSecond(1667);
        service = new RateLimiterService(redis, props);
    }

    @Test
    void settle_noOpWhenDeltaIsZero() {
        StepVerifier.create(service.settle("u1", 40, 40))
                .verifyComplete();
        verify(redis, never()).execute(any(RedisScript.class), anyList(), anyList());
    }

    @Test
    @SuppressWarnings("unchecked")
    void tryAcquire_allowsWhenLuaReturnsOne_andPassesBucketArgs() {
        ArgumentCaptor<List<String>> keys = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        when(redis.execute(any(RedisScript.class), keys.capture(), args.capture()))
                .thenReturn(Flux.just(1L));

        StepVerifier.create(service.tryAcquire("tenant-a", 50))
                .expectNext(true)
                .verifyComplete();

        assertThat(keys.getValue()).containsExactly("tpm:tenant-a");
        assertThat(args.getValue()).containsExactly("50", "100000", "1667", "180");
    }

    @Test
    @SuppressWarnings("unchecked")
    void tryAcquire_usesConfiguredTtlWhenPositive() {
        props.setTpmBucketTtlSeconds(300);
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        when(redis.execute(any(RedisScript.class), anyList(), args.capture()))
                .thenReturn(Flux.just(1L));

        StepVerifier.create(service.tryAcquire("tenant-a", 50))
                .expectNext(true)
                .verifyComplete();

        assertThat(args.getValue().get(3)).isEqualTo("300");
    }

    @Test
    @SuppressWarnings("unchecked")
    void tryAcquire_rejectsWhenLuaReturnsZero() {
        when(redis.execute(any(RedisScript.class), anyList(), anyList()))
                .thenReturn(Flux.just(0L));

        StepVerifier.create(service.tryAcquire("u1", 90_000))
                .expectNext(false)
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void tryAcquire_failOpenOnRedisError() {
        when(redis.execute(any(RedisScript.class), anyList(), anyList()))
                .thenReturn(Flux.error(new RuntimeException("connection refused")));

        StepVerifier.create(service.tryAcquire("u1", 50))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    @SuppressWarnings("unchecked")
    void settle_refundsWhenOverEstimated_positiveDelta() {
        ArgumentCaptor<List<String>> keys = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        when(redis.execute(any(RedisScript.class), keys.capture(), args.capture()))
                .thenReturn(Flux.just(80L));

        StepVerifier.create(service.settle("u1", 100, 40))
                .verifyComplete();

        assertThat(keys.getValue()).containsExactly("tpm:u1");
        assertThat(args.getValue()).containsExactly("60", "100000", "180");
    }

    @Test
    @SuppressWarnings("unchecked")
    void settle_deductsWhenUnderEstimated_negativeDelta() {
        ArgumentCaptor<List<String>> args = ArgumentCaptor.forClass(List.class);
        when(redis.execute(any(RedisScript.class), anyList(), args.capture()))
                .thenReturn(Flux.just(10L));

        StepVerifier.create(service.settle("u1", 40, 90))
                .verifyComplete();

        assertThat(args.getValue()).containsExactly("-50", "100000", "180");
    }

    @Test
    @SuppressWarnings("unchecked")
    void settle_failOpenOnRedisError() {
        when(redis.execute(any(RedisScript.class), anyList(), anyList()))
                .thenReturn(Flux.error(new RuntimeException("redis down")));

        StepVerifier.create(service.settle("u1", 100, 40))
                .verifyComplete();
    }
}
