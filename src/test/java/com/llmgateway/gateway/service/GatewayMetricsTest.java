package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayMetricsTest {

    @Test
    void recordsLowCardinalityLifecycleMetricsExactlyOnce() {
        GatewayProperties props = new GatewayProperties();
        props.setModels(Map.of("model-a", new GatewayProperties.ModelConfig()));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        GatewayMetrics metrics = new GatewayMetrics(registry, props);

        GatewayMetrics.StreamTracker tracker = metrics.startStream();
        tracker.firstToken("model-a", "primary");
        tracker.firstToken("model-a", "primary");
        tracker.finish("model-a", "primary", "completed");
        tracker.finish("model-a", "primary", "completed");

        assertThat(registry.get("llm.gateway.sse.active").gauge().value()).isZero();
        assertThat(registry.get("llm.gateway.ttft")
                .tags("model", "model-a", "channel", "primary")
                .timer().count()).isEqualTo(1);
        assertThat(registry.get("llm.gateway.requests")
                .tags("model", "model-a", "channel", "primary", "outcome", "completed")
                .counter().count()).isEqualTo(1);

        metrics.recordCacheLookup(true);
        metrics.recordRateLimit(false);
        metrics.recordFallbackAttempt(new TimeoutException());
        metrics.recordFallbackResult("success");
        metrics.recordTokenSettlement(100, 80);

        assertThat(registry.get("llm.gateway.cache.requests").tag("result", "hit")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("llm.gateway.rate.limit").tag("result", "rejected")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("llm.gateway.fallback.attempts").tag("reason", "ttft_timeout")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("llm.gateway.tokens").tag("type", "actual")
                .summary().count()).isEqualTo(1);
    }

    @Test
    void fallbackReasons_andModelNormalization() {
        GatewayProperties props = new GatewayProperties();
        GatewayProperties.ModelConfig cfg = new GatewayProperties.ModelConfig();
        cfg.setRequestModel("qwen3:1.7b");
        props.setModels(Map.of("qwen3-1-7b", cfg));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        GatewayMetrics metrics = new GatewayMetrics(registry, props);

        metrics.recordFallbackAttempt(CallNotPermittedException.createCallNotPermittedException(
                CircuitBreaker.ofDefaults("x")));
        metrics.recordFallbackAttempt(WebClientResponseException.create(
                500, "err", null, null, null));
        metrics.recordFallbackAttempt(new IllegalStateException("other"));

        assertThat(registry.get("llm.gateway.fallback.attempts").tag("reason", "circuit_open")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("llm.gateway.fallback.attempts").tag("reason", "http_error")
                .counter().count()).isEqualTo(1);
        assertThat(registry.get("llm.gateway.fallback.attempts").tag("reason", "other")
                .counter().count()).isEqualTo(1);

        GatewayMetrics.StreamTracker tracker = metrics.startStream();
        tracker.firstToken("qwen3:1.7b", "primary");
        tracker.finish("auto", "primary", "completed");
        assertThat(registry.get("llm.gateway.ttft")
                .tags("model", "qwen3-1-7b", "channel", "primary").timer().count()).isEqualTo(1);
        assertThat(registry.get("llm.gateway.requests")
                .tags("model", "auto", "channel", "primary", "outcome", "completed")
                .counter().count()).isEqualTo(1);
    }
}
