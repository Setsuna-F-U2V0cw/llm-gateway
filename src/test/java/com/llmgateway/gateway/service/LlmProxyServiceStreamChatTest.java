package com.llmgateway.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ChatRequest;
import com.llmgateway.gateway.model.Route;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * LlmProxyService.streamChat 主链路：缓存命中/未命中、429、TTFT fallback、结算门闩。
 * 下游一律 MockWebServer，禁止真实 LLM。
 */
class LlmProxyServiceStreamChatTest {

    @Test
    void streamChat_rateLimited_returns429WithoutDownstream() {
        TokenCountService tokenCount = mock(TokenCountService.class);
        RateLimiterService rateLimiter = mock(RateLimiterService.class);
        SemanticCacheService cache = mock(SemanticCacheService.class);
        Router router = mock(Router.class);
        ChatRequest request = requestWithPrompt("hello");
        when(tokenCount.countInputTokens(request)).thenReturn(40);
        when(tokenCount.estimateTotalTokens(request)).thenReturn(100);
        when(rateLimiter.tryAcquire("tenant-a", 100)).thenReturn(Mono.just(false));

        LlmProxyService service = newService(
                mock(WebClient.class), new GatewayProperties(),
                tokenCount, rateLimiter, cache, mock(EmbeddingService.class),
                router, mock(CircuitBreakerService.class),
                new GatewayMetrics(new SimpleMeterRegistry(), new GatewayProperties()));

        StepVerifier.create(service.streamChat(request, "tenant-a"))
                .verifyErrorSatisfies(e -> {
                    assertThat(e).isInstanceOf(ResponseStatusException.class);
                    assertThat(((ResponseStatusException) e).getStatusCode())
                            .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
                });

        verify(cache, never()).searchCache(any(), any(), any());
        verify(router, never()).route(any(), anyInt(), any());
        verify(rateLimiter, never()).settle(any(), anyInt(), anyInt());
    }

    @Test
    void streamChat_cacheHit_setsHeaderAndSettlesOnce() {
        TokenCountService tokenCount = mock(TokenCountService.class);
        RateLimiterService rateLimiter = mock(RateLimiterService.class);
        SemanticCacheService cache = mock(SemanticCacheService.class);
        EmbeddingService embedding = mock(EmbeddingService.class);
        Router router = mock(Router.class);
        ChatRequest request = requestWithPrompt("hello");
        request.setModel("deepseek-v4-flash");
        float[] vec = {1f};

        when(tokenCount.countInputTokens(request)).thenReturn(40);
        when(tokenCount.estimateTotalTokens(request)).thenReturn(100);
        when(tokenCount.actualTotalTokens(eq(request), any())).thenReturn(12);
        when(rateLimiter.tryAcquire("tenant-a", 100)).thenReturn(Mono.just(true));
        when(rateLimiter.settle("tenant-a", 100, 12)).thenReturn(Mono.empty());
        when(embedding.embed("hello")).thenReturn(Mono.just(vec));
        when(cache.searchCache(eq(vec), eq("deepseek-v4-flash"), eq("tenant-a")))
                .thenReturn(Mono.just(Optional.of(new SemanticCacheService.CacheHit(Flux.just(
                        "{\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}",
                        "[DONE]")))));

        LlmProxyService service = newService(
                mock(WebClient.class), new GatewayProperties(),
                tokenCount, rateLimiter, cache, embedding, router,
                mock(CircuitBreakerService.class),
                new GatewayMetrics(new SimpleMeterRegistry(), new GatewayProperties()));

        AtomicReference<ResponseEntity<Flux<String>>> entity = new AtomicReference<>();
        StepVerifier.create(service.streamChat(request, "tenant-a"))
                .assertNext(entity::set)
                .verifyComplete();

        assertThat(entity.get().getHeaders().getFirst("X-Cache-Hit")).isEqualTo("true");
        StepVerifier.create(entity.get().getBody())
                .expectNext("{\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}")
                .expectNext("[DONE]")
                .verifyComplete();

        verify(rateLimiter, times(1)).settle("tenant-a", 100, 12);
        verify(router, never()).route(any(), anyInt(), any());
    }

    @Test
    void streamChat_cacheMiss_streamsPrimary_writesCache_forcesStream() throws Exception {
        try (MockWebServer primary = new MockWebServer();
             MockWebServer fallback = new MockWebServer()) {
            primary.start();
            fallback.start();
            primary.enqueue(sse("hello"));

            Fixtures fx = fixtures(primary, fallback, false);
            when(fx.cache.searchCache(eq(fx.vec), eq("deepseek-v4-flash"), eq("tenant-a")))
                    .thenReturn(Mono.just(Optional.empty()));
            when(fx.router.route(eq(fx.request), eq(40), eq(fx.vec)))
                    .thenReturn(Mono.just(fx.primaryRoute));

            fx.request.setStream(false);
            fx.request.setModel("deepseek-v4-flash");

            AtomicReference<ResponseEntity<Flux<String>>> entity = new AtomicReference<>();
            StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a"))
                    .assertNext(entity::set)
                    .verifyComplete();

            assertThat(entity.get().getHeaders().getFirst("X-Cache-Hit")).isEqualTo("false");
            StepVerifier.create(entity.get().getBody())
                    .expectNextMatches(c -> c.contains("hello"))
                    .expectNext("[DONE]")
                    .verifyComplete();

            String body = primary.takeRequest().getBody().readUtf8();
            assertThat(body).contains("\"stream\":true");
            assertThat(body).contains("deepseek-v4-flash");
            verify(fx.cache, timeout(1000)).saveToCache(
                    eq(fx.vec), eq(fx.request), eq("hello"), eq("tenant-a"), eq("deepseek-v4-flash"));
            verify(fx.rateLimiter, times(1)).settle(eq("tenant-a"), eq(100), anyInt());
            assertThat(fallback.getRequestCount()).isZero();
        }
    }

    @Test
    void streamChat_embeddingError_failOpenRoutesWithNullVector() throws Exception {
        try (MockWebServer primary = new MockWebServer();
             MockWebServer fallback = new MockWebServer()) {
            primary.start();
            fallback.start();
            primary.enqueue(sse("ok"));

            Fixtures fx = fixtures(primary, fallback, false);
            when(fx.embedding.embed("hello")).thenReturn(Mono.error(new RuntimeException("ollama down")));
            when(fx.cache.searchCache(isNull(), isNull(), eq("tenant-a")))
                    .thenReturn(Mono.just(Optional.empty()));
            when(fx.router.route(eq(fx.request), eq(40), isNull()))
                    .thenReturn(Mono.just(fx.primaryRoute));

            StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a")
                            .flatMapMany(ResponseEntity::getBody))
                    .expectNextMatches(c -> c.contains("ok"))
                    .expectNext("[DONE]")
                    .verifyComplete();

            verify(fx.router).route(eq(fx.request), eq(40), isNull());
        }
    }

    @Test
    void streamChat_ttftTimeout_switchesToFallback() throws Exception {
        try (MockWebServer primary = new MockWebServer();
             MockWebServer fallback = new MockWebServer()) {
            primary.start();
            fallback.start();
            primary.enqueue(new MockResponse()
                    .setHeadersDelay(2, TimeUnit.SECONDS)
                    .setHeader("Content-Type", "text/event-stream")
                    .setBody("data:{\"choices\":[{\"delta\":{\"content\":\"late\"}}]}\n\ndata:[DONE]\n\n"));
            fallback.enqueue(sse("fallback-ok"));

            Fixtures fx = fixtures(primary, fallback, false);
            fx.props.setTtftSloMs(120);
            when(fx.cache.searchCache(any(), any(), any())).thenReturn(Mono.just(Optional.empty()));
            when(fx.router.route(any(), anyInt(), any())).thenReturn(Mono.just(fx.primaryRoute));

            StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a")
                            .flatMapMany(ResponseEntity::getBody))
                    .expectNextMatches(c -> c.contains("fallback-ok"))
                    .expectNext("[DONE]")
                    .expectComplete()
                    .verify(Duration.ofSeconds(5));

            verify(fx.cache, never()).saveToCache(any(), any(), any(), any(), any());
        }
    }

    @Test
    void streamChat_sameEndpointFallback_rejectedWithoutSecondCall() throws Exception {
        try (MockWebServer primary = new MockWebServer()) {
            primary.start();
            primary.enqueue(new MockResponse().setResponseCode(500).setBody("boom"));

            String url = baseUrl(primary);
            GatewayProperties props = new GatewayProperties();
            props.setTtftSloMs(3000);
            props.getFallback().setTargetUrl(url);

            TokenCountService tokenCount = mock(TokenCountService.class);
            RateLimiterService rateLimiter = mock(RateLimiterService.class);
            SemanticCacheService cache = mock(SemanticCacheService.class);
            EmbeddingService embedding = mock(EmbeddingService.class);
            Router router = mock(Router.class);
            ChatRequest request = requestWithPrompt("hello");
            float[] vec = {1f};
            stubAcquire(tokenCount, rateLimiter, request);
            when(embedding.embed("hello")).thenReturn(Mono.just(vec));
            when(cache.searchCache(any(), any(), any())).thenReturn(Mono.just(Optional.empty()));
            when(router.route(any(), anyInt(), any())).thenReturn(Mono.just(
                    new Route(url, "deepseek-v4-flash", "k", false, 64000, null)));

            LlmProxyService service = newService(
                    WebClient.builder().build(), props,
                    tokenCount, rateLimiter, cache, embedding, router,
                    new CircuitBreakerService(props, new SimpleMeterRegistry()),
                    new GatewayMetrics(new SimpleMeterRegistry(), props));

            StepVerifier.create(service.streamChat(request, "tenant-a")
                            .flatMapMany(ResponseEntity::getBody))
                    .expectNextMatches(c -> c.contains("fallback 配置与主通道相同"))
                    .verifyComplete();

            assertThat(primary.getRequestCount()).isEqualTo(1);
        }
    }

    @Test
    void streamChat_primaryAndFallbackFail_emitsDualErrorFrame() throws Exception {
        try (MockWebServer primary = new MockWebServer();
             MockWebServer fallback = new MockWebServer()) {
            primary.start();
            fallback.start();
            primary.enqueue(new MockResponse().setResponseCode(500).setBody("p"));
            fallback.enqueue(new MockResponse().setResponseCode(500).setBody("f"));

            Fixtures fx = fixtures(primary, fallback, false);
            when(fx.cache.searchCache(any(), any(), any())).thenReturn(Mono.just(Optional.empty()));
            when(fx.router.route(any(), anyInt(), any())).thenReturn(Mono.just(fx.primaryRoute));

            StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a")
                            .flatMapMany(ResponseEntity::getBody))
                    .expectNextMatches(c -> c.contains("主通道与备用通道均不可用"))
                    .verifyComplete();
        }
    }

    @Test
    void streamChat_circuitOpen_goesFallbackWithoutPrimaryCall() throws Exception {
        try (MockWebServer primary = new MockWebServer();
             MockWebServer fallback = new MockWebServer()) {
            primary.start();
            fallback.start();
            fallback.enqueue(sse("fb-ok"));

            Fixtures fx = fixtures(primary, fallback, true);
            CircuitBreaker open = CircuitBreaker.of("deepseek-v4-flash", CircuitBreakerConfig.custom()
                    .slidingWindowSize(2)
                    .minimumNumberOfCalls(1)
                    .failureRateThreshold(50f)
                    .waitDurationInOpenState(Duration.ofSeconds(60))
                    .slowCallRateThreshold(100f)
                    .slowCallDurationThreshold(Duration.ofDays(36500))
                    .build());
            open.transitionToOpenState();
            when(fx.breaker.get(any())).thenReturn(CircuitBreakerOperator.of(open));
            when(fx.cache.searchCache(any(), any(), any())).thenReturn(Mono.just(Optional.empty()));
            when(fx.router.route(any(), anyInt(), any())).thenReturn(Mono.just(fx.primaryRoute));

            StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a")
                            .flatMapMany(ResponseEntity::getBody))
                    .expectNextMatches(c -> c.contains("fb-ok"))
                    .expectNext("[DONE]")
                    .verifyComplete();

            assertThat(primary.getRequestCount()).isZero();
            verify(fx.cache, never()).saveToCache(any(), any(), any(), any(), any());
        }
    }

    @Test
    void streamChat_thinkingRoute_injectsReasoningParams() throws Exception {
        try (MockWebServer primary = new MockWebServer();
             MockWebServer fallback = new MockWebServer()) {
            primary.start();
            fallback.start();
            primary.enqueue(sse("think-ok"));

            Fixtures fx = fixtures(primary, fallback, false);
            Route thinking = new Route(baseUrl(primary), "deepseek-v4-flash-thinking", "k",
                    true, 64000, null);
            when(fx.cache.searchCache(any(), any(), any())).thenReturn(Mono.just(Optional.empty()));
            when(fx.router.route(any(), anyInt(), any())).thenReturn(Mono.just(thinking));

            StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a")
                            .flatMapMany(ResponseEntity::getBody))
                    .expectNextMatches(c -> c.contains("think-ok"))
                    .expectNext("[DONE]")
                    .verifyComplete();

            String body = primary.takeRequest().getBody().readUtf8();
            assertThat(body).contains("\"reasoning_effort\":\"high\"");
            assertThat(body).contains("\"type\":\"enabled\"");
        }
    }

    @Test
    void streamChat_afterContent_primaryError_emitsErrorFrameNotFallback() {
        WebClient webClient = webClientThatErrorsAfterSse(
                "data:{\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n",
                "SHOULD-NOT-SEE");

        Fixtures fx = fixturesWithClient(webClient, "http://primary.test", "http://fallback.test");
        when(fx.cache.searchCache(any(), any(), any())).thenReturn(Mono.just(Optional.empty()));
        when(fx.router.route(any(), anyInt(), any())).thenReturn(Mono.just(fx.primaryRoute));

        StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a")
                        .flatMapMany(ResponseEntity::getBody))
                .expectNextMatches(c -> c.contains("partial"))
                .expectNextMatches(c -> c.contains("主通道中断"))
                .verifyComplete();

        verify(fx.cache, never()).saveToCache(any(), any(), any(), any(), any());
    }

    @Test
    void streamChat_afterReasoningOnly_primaryError_shouldNotFallback() {
        WebClient webClient = webClientThatErrorsAfterSse(
                "data:{\"choices\":[{\"delta\":{\"reasoning_content\":\"think\"}}]}\n\n",
                "SHOULD-NOT-SEE");

        Fixtures fx = fixturesWithClient(webClient, "http://primary.test", "http://fallback.test");
        when(fx.cache.searchCache(any(), any(), any())).thenReturn(Mono.just(Optional.empty()));
        when(fx.router.route(any(), anyInt(), any())).thenReturn(Mono.just(fx.primaryRoute));

        StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a")
                        .flatMapMany(ResponseEntity::getBody))
                .expectNextMatches(c -> c.contains("reasoning_content"))
                .expectNextMatches(c -> c.contains("主通道中断"))
                .verifyComplete();

        verify(fx.cache, never()).saveToCache(any(), any(), any(), any(), any());
    }

    @Test
    void streamChat_embedTimeout_failOpenRoutesWithNullVector() throws Exception {
        try (MockWebServer primary = new MockWebServer();
             MockWebServer fallback = new MockWebServer()) {
            primary.start();
            fallback.start();
            primary.enqueue(sse("ok"));

            Fixtures fx = fixtures(primary, fallback, false);
            fx.props.setEmbedPipelineTimeoutMs(80);
            when(fx.embedding.embed("hello")).thenReturn(Mono.never());
            when(fx.cache.searchCache(isNull(), isNull(), eq("tenant-a")))
                    .thenReturn(Mono.just(Optional.empty()));
            when(fx.router.route(eq(fx.request), eq(40), isNull()))
                    .thenReturn(Mono.just(fx.primaryRoute));

            StepVerifier.create(fx.service.streamChat(fx.request, "tenant-a")
                            .flatMapMany(ResponseEntity::getBody))
                    .expectNextMatches(c -> c.contains("ok"))
                    .expectNext("[DONE]")
                    .expectComplete()
                    .verify(Duration.ofSeconds(3));

            verify(fx.router).route(eq(fx.request), eq(40), isNull());
        }
    }

    private static WebClient webClientThatErrorsAfterSse(String primarySse, String fallbackContent) {
        return webClientThatErrorsAfterSse(primarySse, fallbackContent, "reset");
    }

    private static WebClient webClientThatErrorsAfterSse(String primarySse, String fallbackContent,
                                                         String errorMessage) {
        DefaultDataBufferFactory factory = new DefaultDataBufferFactory();
        byte[] primaryBytes = primarySse.getBytes(StandardCharsets.UTF_8);
        String fallbackBody = "data:{\"choices\":[{\"delta\":{\"content\":\"" + fallbackContent
                + "\"}}]}\n\ndata:[DONE]\n\n";
        return WebClient.builder()
                .exchangeFunction(req -> {
                    String host = req.url().getHost();
                    if (host != null && host.contains("fallback")) {
                        return Mono.just(ClientResponse.create(HttpStatus.OK)
                                .header("Content-Type", "text/event-stream")
                                .body(fallbackBody)
                                .build());
                    }
                    return Mono.just(ClientResponse.create(HttpStatus.OK)
                            .header("Content-Type", "text/event-stream")
                            .body(Flux.concat(
                                    Mono.just(factory.wrap(primaryBytes)),
                                    Mono.error(new RuntimeException(errorMessage))))
                            .build());
                })
                .build();
    }

    private static Fixtures fixturesWithClient(WebClient webClient, String primaryUrl, String fallbackUrl) {
        GatewayProperties props = new GatewayProperties();
        props.setTtftSloMs(3000);
        props.getFallback().setTargetUrl(fallbackUrl);

        TokenCountService tokenCount = mock(TokenCountService.class);
        RateLimiterService rateLimiter = mock(RateLimiterService.class);
        SemanticCacheService cache = mock(SemanticCacheService.class);
        EmbeddingService embedding = mock(EmbeddingService.class);
        Router router = mock(Router.class);
        ChatRequest request = requestWithPrompt("hello");
        float[] vec = {1f};
        stubAcquire(tokenCount, rateLimiter, request);
        when(embedding.embed("hello")).thenReturn(Mono.just(vec));

        LlmProxyService service = newService(
                webClient, props,
                tokenCount, rateLimiter, cache, embedding, router,
                new CircuitBreakerService(props, new SimpleMeterRegistry()),
                new GatewayMetrics(new SimpleMeterRegistry(), props));

        Route primaryRoute = new Route(primaryUrl, "deepseek-v4-flash", "k", false, 64000, null);
        return new Fixtures(props, tokenCount, rateLimiter, cache, embedding, router,
                mock(CircuitBreakerService.class), request, vec, service, primaryRoute);
    }

    private static MockResponse sse(String content) {
        return new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("data:{\"choices\":[{\"delta\":{\"content\":\"" + content + "\"}}]}\n\n"
                        + "data:[DONE]\n\n");
    }

    private static String baseUrl(MockWebServer server) {
        return server.url("").toString().replaceAll("/$", "");
    }

    private static ChatRequest requestWithPrompt(String prompt) {
        ChatRequest.Message message = new ChatRequest.Message();
        message.setRole("user");
        message.setContent(prompt);
        ChatRequest request = new ChatRequest();
        request.setMessages(List.of(message));
        return request;
    }

    private static void stubAcquire(TokenCountService tokenCount, RateLimiterService rateLimiter,
                                    ChatRequest request) {
        when(tokenCount.countInputTokens(request)).thenReturn(40);
        when(tokenCount.estimateTotalTokens(request)).thenReturn(100);
        when(tokenCount.actualTotalTokens(eq(request), any())).thenReturn(10);
        when(rateLimiter.tryAcquire("tenant-a", 100)).thenReturn(Mono.just(true));
        when(rateLimiter.settle(anyString(), anyInt(), anyInt())).thenReturn(Mono.empty());
    }

    private static LlmProxyService newService(
            WebClient webClient,
            GatewayProperties props,
            TokenCountService tokenCount,
            RateLimiterService rateLimiter,
            SemanticCacheService cache,
            EmbeddingService embedding,
            Router router,
            CircuitBreakerService breaker,
            GatewayMetrics metrics) {
        return new LlmProxyService(
                webClient, props, tokenCount, rateLimiter, cache, embedding,
                router, breaker, metrics, new ObjectMapper());
    }

    private static Fixtures fixtures(MockWebServer primary, MockWebServer fallback,
                                     boolean mockBreaker) {
        GatewayProperties props = new GatewayProperties();
        props.setTtftSloMs(3000);
        props.getFallback().setTargetUrl(baseUrl(fallback));

        TokenCountService tokenCount = mock(TokenCountService.class);
        RateLimiterService rateLimiter = mock(RateLimiterService.class);
        SemanticCacheService cache = mock(SemanticCacheService.class);
        EmbeddingService embedding = mock(EmbeddingService.class);
        Router router = mock(Router.class);
        ChatRequest request = requestWithPrompt("hello");
        float[] vec = {1f};
        stubAcquire(tokenCount, rateLimiter, request);
        when(embedding.embed("hello")).thenReturn(Mono.just(vec));

        CircuitBreakerService breaker = mockBreaker
                ? mock(CircuitBreakerService.class)
                : new CircuitBreakerService(props, new SimpleMeterRegistry());

        LlmProxyService service = newService(
                WebClient.builder().build(), props,
                tokenCount, rateLimiter, cache, embedding, router, breaker,
                new GatewayMetrics(new SimpleMeterRegistry(), props));

        Route primaryRoute = new Route(baseUrl(primary), "deepseek-v4-flash", "k",
                false, 64000, null);
        return new Fixtures(props, tokenCount, rateLimiter, cache, embedding, router, breaker,
                request, vec, service, primaryRoute);
    }

    private record Fixtures(
            GatewayProperties props,
            TokenCountService tokenCount,
            RateLimiterService rateLimiter,
            SemanticCacheService cache,
            EmbeddingService embedding,
            Router router,
            CircuitBreakerService breaker,
            ChatRequest request,
            float[] vec,
            LlmProxyService service,
            Route primaryRoute
    ) {}
}
