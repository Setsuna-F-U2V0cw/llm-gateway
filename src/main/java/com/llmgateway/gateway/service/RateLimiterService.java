package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

/**
 * 基于 Redis Lua 脚本的非阻塞 TPM 令牌桶限流服务
 *
 * [💎 面试亮点] 整个限流判断是一次原子的 Redis 操作：
 * - Lua 脚本在 Redis 单线程中执行，天然原子性，彻底解决分布式环境下的 Token 超卖问题
 * - ReactiveRedisTemplate（底层 Lettuce）以非阻塞方式发送命令，不占用 Netty EventLoop 线程
 * - Mono.flatMap 串联限流与转发，整条链路零阻塞
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RateLimiterService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final GatewayProperties props;

    // 预加载 Lua 脚本（启动时编译，运行时直接执行 EVALSHA，减少网络传输）
    private static final RedisScript<Long> TOKEN_BUCKET_SCRIPT = RedisScript.of(
            new ClassPathResource("scripts/token_bucket.lua"), Long.class
    );

    private static final String KEY_PREFIX = "tpm:";

    /**
     * 令牌桶限流检查：原子预扣指定数量的 Token
     *
     * [🚧 核心难点] 为什么必须用 Lua 而不是 GET + SET？
     * 高并发下，GET 读到剩余 100 Token，SET 扣减后写回，两步操作之间其他请求也读到 100，
     * 导致多个请求同时通过，实际消耗远超上限——这就是经典的"Token 超卖"问题。
     * Lua 脚本在 Redis 中原子执行，彻底消除这个竞态条件。
     *
     * @param userId    用户/租户 ID，令牌桶 Key 的一部分
     * @param tokenCost 本次预扣的 Token 数
     * @return Mono<Boolean> true=允许，false=限流拒绝
     */
    public Mono<Boolean> tryAcquire(String userId, int tokenCost) {
        String key = KEY_PREFIX + userId;
        long now = Instant.now().getEpochSecond();

        // [💎 面试亮点] 使用 EVALSHA 执行 Lua，参数动态传入（容量、补充速率、时间戳），
        // 令牌桶完全在 Redis 侧计算，网关无状态，天然支持水平扩展
        return reactiveRedisTemplate.execute(
                TOKEN_BUCKET_SCRIPT,
                List.of(key),
                List.of(
                        String.valueOf(tokenCost),
                        String.valueOf(props.getTpmCapacity()),
                        String.valueOf(props.getTpmRefillRatePerSecond()),
                        String.valueOf(now)
                )
        )
        .next()
        .map(result -> result == 1L)
        .doOnNext(allowed -> {
            if (!allowed) {
                log.warn("TPM 限流触发: userId={}, tokenCost={}, capacity={}",
                        userId, tokenCost, props.getTpmCapacity());
            }
        })
        // [💣 踩坑预警] Redis 不可用时不应阻断所有请求，降级为放行（fail-open 策略）
        .onErrorReturn(true);
    }

    /**
     * 结算真实消耗：将多扣的 Token 归还桶中（多退少补）
     *
     * @param userId        用户 ID
     * @param estimatedCost 预扣数量
     * @param actualCost    真实消耗数量
     */
    public Mono<Void> settle(String userId, int estimatedCost, int actualCost) {
        int refund = estimatedCost - actualCost;
        if (refund <= 0) {
            return Mono.empty();
        }
        // 将多扣的 Token 通过 HINCRBY 归还
        String key = KEY_PREFIX + userId;
        return reactiveRedisTemplate.opsForHash()
                .increment(key, "tokens", refund)
                .doOnNext(newTokens -> log.debug("Token 结算归还: userId={}, refund={}, newTokens={}",
                        userId, refund, newTokens))
                .then();
    }
}
