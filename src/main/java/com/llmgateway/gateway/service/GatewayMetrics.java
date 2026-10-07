package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 网关自定义指标的唯一入口。所有 tag 均为有限枚举或配置内 model，禁止 tenant/prompt/error message。
 */
@Service
public class GatewayMetrics {

    private static final String UNKNOWN_MODEL = "unknown";

    private final MeterRegistry registry;
    private final GatewayProperties props;
    private final AtomicInteger activeStreams = new AtomicInteger();

    public GatewayMetrics(MeterRegistry registry, GatewayProperties props) {
        this.registry = registry;
        this.props = props;
        Gauge.builder("llm.gateway.sse.active", activeStreams, AtomicInteger::get)
                .description("当前活跃的 SSE 请求数")
                .register(registry);
    }

    public StreamTracker startStream() {
        activeStreams.incrementAndGet();
        return new StreamTracker(System.nanoTime());
    }

    public void recordCacheLookup(boolean hit) {
        registry.counter("llm.gateway.cache.requests", "result", hit ? "hit" : "miss")
                .increment();
    }

    public void recordRateLimit(boolean allowed) {
        registry.counter("llm.gateway.rate.limit", "result", allowed ? "allowed" : "rejected")
                .increment();
    }

    public void recordFallbackAttempt(Throwable cause) {
        registry.counter("llm.gateway.fallback.attempts", "reason", fallbackReason(cause))
                .increment();
    }

    public void recordFallbackResult(String result) {
        registry.counter("llm.gateway.fallback.results", "result", result)
                .increment();
    }

    public void recordTokenSettlement(int estimated, int actual) {
        tokenSummary("estimated").record(estimated);
        tokenSummary("actual").record(actual);
        tokenSummary("delta").record(Math.abs(estimated - actual));
    }

    private DistributionSummary tokenSummary(String type) {
        return DistributionSummary.builder("llm.gateway.tokens")
                .tag("type", type)
                .baseUnit("tokens")
                .register(registry);
    }

    private String normalizeModel(String requested) {
        if (requested == null || requested.isBlank() || "auto".equalsIgnoreCase(requested)) {
            return "auto";
        }
        Map<String, GatewayProperties.ModelConfig> models = props.getModels();
        if (models == null || models.isEmpty()) {
            return UNKNOWN_MODEL;
        }
        if (models.containsKey(requested)) {
            return requested;
        }
        return models.entrySet().stream()
                .filter(e -> requested.equals(e.getValue().getRequestModel()))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(UNKNOWN_MODEL);
    }

    private String fallbackReason(Throwable cause) {
        if (cause instanceof TimeoutException) {
            return "ttft_timeout";
        }
        if (cause instanceof CallNotPermittedException) {
            return "circuit_open";
        }
        if (cause instanceof WebClientResponseException) {
            return "http_error";
        }
        return "other";
    }

    public final class StreamTracker {
        private final long startedNanos;
        private final AtomicBoolean firstTokenRecorded = new AtomicBoolean();
        private final AtomicBoolean finished = new AtomicBoolean();

        private StreamTracker(long startedNanos) {
            this.startedNanos = startedNanos;
        }

        public void firstToken(String model, String channel) {
            if (!firstTokenRecorded.compareAndSet(false, true)) {
                return;
            }
            Timer.builder("llm.gateway.ttft")
                    .tags("model", normalizeModel(model), "channel", channel)
                    .description("从网关订阅到首个 content/reasoning token 的耗时")
                    .register(registry)
                    .record(Duration.ofNanos(System.nanoTime() - startedNanos));
        }

        public void finish(String model, String channel, String outcome) {
            if (!finished.compareAndSet(false, true)) {
                return;
            }
            String normalized = normalizeModel(model);
            Timer.builder("llm.gateway.stream.duration")
                    .tags("model", normalized, "channel", channel, "outcome", outcome)
                    .description("网关 SSE 请求完整生命周期耗时")
                    .register(registry)
                    .record(Duration.ofNanos(System.nanoTime() - startedNanos));
            registry.counter("llm.gateway.requests",
                            "model", normalized,
                            "channel", channel,
                            "outcome", outcome)
                    .increment();
            activeStreams.decrementAndGet();
        }
    }
}
