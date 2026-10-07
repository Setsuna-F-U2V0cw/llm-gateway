package com.llmgateway.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ChatRequest;
import com.llmgateway.gateway.model.Route;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LlmProxyServiceTest {

    @Test
    void errorFrame_escapesQuotesAndNewlinesAsJson() throws Exception {
        GatewayProperties props = new GatewayProperties();
        LlmProxyService service = new LlmProxyService(
                mock(WebClient.class),
                props,
                mock(TokenCountService.class),
                mock(RateLimiterService.class),
                mock(SemanticCacheService.class),
                mock(EmbeddingService.class),
                mock(Router.class),
                mock(CircuitBreakerService.class),
                new GatewayMetrics(new SimpleMeterRegistry(), props),
                new ObjectMapper());

        String frame = service.errorFrame("boom \"quoted\" \nand-line");
        var node = new ObjectMapper().readTree(frame);
        assertThat(node.get("error").asText()).isEqualTo("boom \"quoted\" \nand-line");
    }

    @Test
    void fallbackEndpoint_mustDifferFromPrimary() {
        assertThat(LlmProxyService.sameEndpoint(
                "http://localhost:11434/", "http://localhost:11434")).isTrue();
        assertThat(LlmProxyService.sameEndpoint(
                "http://localhost:11434", "http://localhost:11435")).isFalse();
        assertThat(LlmProxyService.sameEndpoint(
                "https://provider.example/v1", "https://provider.example/v1/")).isTrue();
    }

    @Test
    void differentiatedFallback_streamsFromIndependentEndpoint() throws Exception {
        try (MockWebServer fallback = new MockWebServer()) {
            fallback.start();
            fallback.enqueue(new MockResponse()
                    .setHeader("Content-Type", "text/event-stream")
                    .setBody("data:{\"choices\":[{\"delta\":{\"content\":\"fallback-ok\"}}]}\n\n"
                            + "data:[DONE]\n\n"));

            GatewayProperties props = new GatewayProperties();
            props.setTtftSloMs(1000);
            props.getFallback().setTargetUrl(
                    fallback.url("").toString().replaceAll("/$", ""));
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            GatewayMetrics metrics = new GatewayMetrics(registry, props);
            LlmProxyService service = new LlmProxyService(
                    WebClient.builder().build(),
                    props,
                    mock(TokenCountService.class),
                    mock(RateLimiterService.class),
                    mock(SemanticCacheService.class),
                    mock(EmbeddingService.class),
                    mock(Router.class),
                    mock(CircuitBreakerService.class),
                    metrics,
                    new ObjectMapper());
            Route primary = new Route(
                    "https://primary.example", "primary-model", "key",
                    false, 32000, "primary-model");

            StepVerifier.create(service.fallbackFlux(
                            primary,
                            new ChatRequest(),
                            metrics.startStream(),
                            new AtomicBoolean()))
                    .expectNextMatches(chunk -> chunk.contains("fallback-ok"))
                    .expectNext("[DONE]")
                    .verifyComplete();

            assertThat(registry.get("llm.gateway.fallback.results")
                    .tag("result", "success").counter().count()).isEqualTo(1);
            assertThat(fallback.takeRequest().getPath()).isEqualTo("/v1/chat/completions");
        }
    }

    @Test
    void cacheHitCancellation_settlesOnlyEmittedContent() {
        TokenCountService tokenCount = mock(TokenCountService.class);
        RateLimiterService rateLimiter = mock(RateLimiterService.class);
        ChatRequest request = new ChatRequest();
        when(tokenCount.actualTotalTokens(request, "A")).thenReturn(12);
        when(rateLimiter.settle("user-a", 100, 12)).thenReturn(Mono.empty());
        GatewayProperties props = new GatewayProperties();
        GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry(), props);

        LlmProxyService service = new LlmProxyService(
                mock(WebClient.class),
                props,
                tokenCount,
                rateLimiter,
                mock(SemanticCacheService.class),
                mock(EmbeddingService.class),
                mock(Router.class),
                mock(CircuitBreakerService.class),
                metrics,
                new ObjectMapper());

        Flux<String> cachedStream = Flux.just(
                "{\"choices\":[{\"delta\":{\"content\":\"A\"}}]}",
                "{\"choices\":[{\"delta\":{\"content\":\"B\"}}]}",
                "[DONE]");

        StepVerifier.create(service.attachSettlement(
                        cachedStream, "user-a", request, 100, new AtomicBoolean(),
                        metrics.startStream(), "auto").take(1))
                .expectNext("{\"choices\":[{\"delta\":{\"content\":\"A\"}}]}")
                .verifyComplete();

        verify(tokenCount).actualTotalTokens(eq(request), eq("A"));
        verify(rateLimiter).settle("user-a", 100, 12);
    }

    @Test
    void cancellationBeforeResponseBody_settlesInputOnly() {
        TokenCountService tokenCount = mock(TokenCountService.class);
        RateLimiterService rateLimiter = mock(RateLimiterService.class);
        EmbeddingService embedding = mock(EmbeddingService.class);
        ChatRequest request = requestWithPrompt("hello");

        when(tokenCount.countInputTokens(request)).thenReturn(40);
        when(tokenCount.estimateTotalTokens(request)).thenReturn(100);
        when(tokenCount.actualTotalTokens(request, "")).thenReturn(8);
        when(rateLimiter.tryAcquire("user-a", 100)).thenReturn(Mono.just(true));
        when(rateLimiter.settle("user-a", 100, 8)).thenReturn(Mono.empty());
        when(embedding.embed("hello")).thenReturn(Mono.never());
        GatewayProperties props = new GatewayProperties();
        GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry(), props);

        LlmProxyService service = new LlmProxyService(
                mock(WebClient.class),
                props,
                tokenCount,
                rateLimiter,
                mock(SemanticCacheService.class),
                embedding,
                mock(Router.class),
                mock(CircuitBreakerService.class),
                metrics,
                new ObjectMapper());

        StepVerifier.create(service.streamChat(request, "user-a"))
                .thenCancel()
                .verify();

        verify(tokenCount).actualTotalTokens(request, "");
        verify(rateLimiter).settle("user-a", 100, 8);
    }

    @Test
    void streamChat_lengthGuard_rejectsWithHttp413BeforeSseBody() {
        TokenCountService tokenCount = mock(TokenCountService.class);
        RateLimiterService rateLimiter = mock(RateLimiterService.class);
        EmbeddingService embedding = mock(EmbeddingService.class);
        SemanticCacheService cache = mock(SemanticCacheService.class);
        Router router = mock(Router.class);
        CircuitBreakerService breaker = mock(CircuitBreakerService.class);
        ChatRequest request = requestWithPrompt("hello");
        float[] vec = {1f};

        when(tokenCount.countInputTokens(request)).thenReturn(40);
        when(tokenCount.estimateTotalTokens(request)).thenReturn(100);
        when(tokenCount.actualTotalTokens(request, "")).thenReturn(8);
        when(rateLimiter.tryAcquire("user-a", 100)).thenReturn(Mono.just(true));
        when(rateLimiter.settle("user-a", 100, 8)).thenReturn(Mono.empty());
        when(embedding.embed("hello")).thenReturn(Mono.just(vec));
        when(cache.searchCache(any(), isNull(), eq("user-a")))
                .thenReturn(Mono.just(Optional.empty()));
        when(router.route(eq(request), eq(40), eq(vec))).thenReturn(Mono.error(
                new ResponseStatusException(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                        "prompt 超出所有 model 上下文上限")));

        GatewayProperties props = new GatewayProperties();
        GatewayMetrics metrics = new GatewayMetrics(new SimpleMeterRegistry(), props);
        LlmProxyService service = new LlmProxyService(
                mock(WebClient.class),
                props,
                tokenCount,
                rateLimiter,
                cache,
                embedding,
                router,
                breaker,
                metrics,
                new ObjectMapper());

        StepVerifier.create(service.streamChat(request, "user-a"))
                .verifyErrorSatisfies(e -> {
                    assertThat(e).isInstanceOf(ResponseStatusException.class);
                    assertThat(((ResponseStatusException) e).getStatusCode())
                            .isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
                });

        verify(rateLimiter).settle("user-a", 100, 8);
        verify(breaker, never()).get(any());
    }

    private static ChatRequest requestWithPrompt(String prompt) {
        ChatRequest.Message message = new ChatRequest.Message();
        message.setRole("user");
        message.setContent(prompt);
        ChatRequest request = new ChatRequest();
        request.setMessages(List.of(message));
        return request;
    }
}
