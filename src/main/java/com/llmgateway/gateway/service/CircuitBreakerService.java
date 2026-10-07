package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import lombok.extern.slf4j.Slf4j;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;

/**
 * Resilience4j 熔断器服务（阶段四）
 *
 * 按 model 粒度维护 named CircuitBreaker（Q7）：一个 model 慢/挂不该连累别的 model 的 breaker。
 *
 * [🚧 核心难点] TTFT-as-failure（Q7 选 A）：
 *  - Resilience4j 自带的 slowCallDurationThreshold 测的是「整调用时长」，流式正常输出 30s 很常见，
 *    会被误判慢 → 关掉 slow-call 判定（slowCallRateThreshold=100f + 100 年时长，不可设 0），TTFT 由 timeout(Mono.delay, perChunk) 接管
 *  - TTFT 超时抛出的 TimeoutException 计为 failure（recordException 全收），正常完成计 success
 *  - 窗口内 failure 率超阈值 → breaker open → future 请求短路成 CallNotPermittedException → 走 fallback
 *
 * [💎 面试亮点] CircuitBreakerOperator 把 CircuitBreaker 状态机嫁接到 Reactor Flux 上：
 * 订阅时检查 breaker 状态（open 则直接发 CallNotPermittedException，不打下游），
 * onComplete 记 success，onError 记 failure，全程不阻塞 EventLoop。
 */
@Slf4j
@Service
public class CircuitBreakerService {

    private final GatewayProperties props;
    private final CircuitBreakerRegistry registry = CircuitBreakerRegistry.ofDefaults();

    public CircuitBreakerService(GatewayProperties props, MeterRegistry meterRegistry) {
        this.props = props;
        TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(registry).bindTo(meterRegistry);
    }

    /**
     * 取（或按需创建）指定 model 的 CircuitBreaker，包成 Reactor 适配算子。
     */
    public CircuitBreakerOperator<String> get(String modelId) {
        CircuitBreaker breaker = registry.circuitBreaker(modelId, buildConfig());
        return CircuitBreakerOperator.of(breaker);
    }

    private CircuitBreakerConfig buildConfig() {
        GatewayProperties.CircuitBreakerProps cb = props.getCb();
        return CircuitBreakerConfig.custom()
                .slidingWindowSize(cb.getSlidingWindowSize())
                .minimumNumberOfCalls(cb.getMinimumNumberOfCalls())
                .failureRateThreshold(cb.getFailureRateThreshold())
                .waitDurationInOpenState(Duration.ofMillis(cb.getWaitDurationInOpenStateMs()))
                .permittedNumberOfCallsInHalfOpenState(cb.getPermittedHalfOpenCalls())
                // 关掉 slow-call 判定：TTFT 由我们的 timeout 接管，整调用时长不作为慢判据
                // [💣 踩坑] Resilience4j 要求 slowCallRateThreshold 必须 ≥1，设 0 抛 IllegalArgumentException。
                // 设 100 + 100 年时长意味着：永不触发 slow-call。
                // 另：Duration.ofMillis(Long.MAX_VALUE) 内部乘 1_000_000 溢出 → ArithmeticException。
                .slowCallRateThreshold(100f)
                .slowCallDurationThreshold(Duration.ofDays(36500))
                // 所有异常计 failure（含 TTFT 的 TimeoutException、下游错误、CallNotPermitted）
                .recordException(e -> true)
                .build();
    }
}
