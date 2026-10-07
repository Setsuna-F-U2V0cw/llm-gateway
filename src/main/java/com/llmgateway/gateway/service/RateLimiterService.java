package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * 基于 Redis Lua 脚本的非阻塞 TPM 令牌桶限流服务
 *
 * [💎 面试亮点] 整个限流判断是一次原子的 Redis 操作：
 * - Lua 脚本在 Redis 单线程中执行，天然原子性，彻底解决分布式环境下的 Token 超卖问题
 * - ReactiveRedisTemplate（底层 Lettuce）以非阻塞方式发送命令，不占用 Netty EventLoop 线程
 * - Mono.flatMap 串联限流与转发，整条链路零阻塞
 * - refill 时钟用 Redis TIME，TTL 大于读超时；settle 在 key 过期时重建完整 hash
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

    private static final RedisScript<Long> SETTLE_SCRIPT = RedisScript.of(
            new ClassPathResource("scripts/token_bucket_settle.lua"), Long.class
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
        int ttl = props.resolveTpmBucketTtlSeconds();

        // [💎 面试亮点] EVALSHA 只传 cost/capacity/rate/ttl；now 由 Lua 调 Redis TIME，
        // 多实例网关时钟一致，桶完全在 Redis 侧计算。
        return reactiveRedisTemplate.execute(
                TOKEN_BUCKET_SCRIPT,
                List.of(key),
                List.of(
                        String.valueOf(tokenCost),
                        String.valueOf(props.getTpmCapacity()),
                        String.valueOf(props.getTpmRefillRatePerSecond()),
                        String.valueOf(ttl)
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
     * 结算真实消耗：多退少补。
     * <ul>
     *   <li>estimated &gt; actual → 归还差额</li>
     *   <li>estimated &lt; actual → 补扣差额（桶内 tokens 下限钳到 0）</li>
     *   <li>相等 → no-op</li>
     *   <li>桶已过期 → Lua 重建完整 hash（tokens + last_refill_time），禁止裸 HINCRBY</li>
     * </ul>
     */
    public Mono<Void> settle(String userId, int estimatedCost, int actualCost) {
        int delta = estimatedCost - actualCost;
        if (delta == 0) {
            return Mono.empty();
        }
        String key = KEY_PREFIX + userId;
        int ttl = props.resolveTpmBucketTtlSeconds();
        return reactiveRedisTemplate.execute(
                        SETTLE_SCRIPT,
                        List.of(key),
                        List.of(
                                String.valueOf(delta),
                                String.valueOf(props.getTpmCapacity()),
                                String.valueOf(ttl)
                        )
                )
                .next()
                .doOnNext(newTokens -> log.debug(
                        "Token 结算: userId={}, estimated={}, actual={}, delta={}, tokensAfter={}",
                        userId, estimatedCost, actualCost, delta, newTokens))
                // 结算失败不回灌主链路：预扣已发生，账单以尽力而为更新
                .onErrorResume(e -> {
                    log.error("Token 结算失败: userId={}, {}", userId, e.getMessage());
                    return Mono.empty();
                })
                .then();
    }
}
