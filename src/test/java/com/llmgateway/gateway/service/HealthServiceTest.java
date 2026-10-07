package com.llmgateway.gateway.service;

import com.google.common.util.concurrent.Futures;
import com.llmgateway.gateway.config.GatewayProperties;
import io.qdrant.client.QdrantClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveRedisCallback;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HealthServiceTest {

    private ReactiveRedisTemplate<String, String> redis;
    private QdrantClient qdrant;
    private IntentClassifier intentClassifier;
    private MockWebServer ollama;
    private HealthService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        redis = mock(ReactiveRedisTemplate.class);
        qdrant = mock(QdrantClient.class);
        intentClassifier = mock(IntentClassifier.class);
        when(intentClassifier.prototypeStatus()).thenReturn(
                new IntentClassifier.PrototypeStatus("ready", 6, 6, 1, null));
        ollama = new MockWebServer();
        ollama.start();

        GatewayProperties props = new GatewayProperties();
        props.setQdrantCollection("llm_cache");
        props.setOllamaBaseUrl(ollama.url("").toString().replaceAll("/$", ""));
        props.setOllamaEmbedModel("bge-m3");
        props.setTtftSloMs(3000);
        props.setTpmCapacity(100000);

        WebClient webClient = WebClient.builder().build();
        service = new HealthService(redis, qdrant, webClient, props, intentClassifier);
    }

    @AfterEach
    void tearDown() throws Exception {
        ollama.shutdown();
    }

    @Test
    void checkDependencies_allUp() {
        when(redis.execute(any(ReactiveRedisCallback.class))).thenReturn(Flux.just("PONG"));
        when(qdrant.collectionExistsAsync(anyString())).thenReturn(Futures.immediateFuture(true));
        ollama.enqueue(new MockResponse().setResponseCode(200).setBody("{\"models\":[]}"));

        StepVerifier.create(service.checkDependencies())
                .assertNext(h -> {
                    assertThat(h.status()).isEqualTo("up");
                    assertThat(h.components()).hasSize(3);
                    assertThat(h.components()).allMatch(c -> "up".equals(c.status()));
                    assertThat(h.gateway()).containsKey("ttftSloMs");
                    assertThat(h.gateway().get("intentClassifier"))
                            .isEqualTo(new IntentClassifier.PrototypeStatus("ready", 6, 6, 1, null));
                })
                .verifyComplete();
    }

    @Test
    void checkDependencies_redisDown_isDegraded() {
        when(redis.execute(any(ReactiveRedisCallback.class))).thenReturn(Flux.error(new RuntimeException("connection refused")));
        when(qdrant.collectionExistsAsync(anyString())).thenReturn(Futures.immediateFuture(true));
        ollama.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        StepVerifier.create(service.checkDependencies())
                .assertNext(h -> {
                    assertThat(h.status()).isEqualTo("degraded");
                    assertThat(h.components())
                            .anyMatch(c -> "redis".equals(c.name()) && "down".equals(c.status()));
                })
                .verifyComplete();
    }

    @Test
    void checkDependencies_qdrantCollectionMissing_isDegraded() {
        when(redis.execute(any(ReactiveRedisCallback.class))).thenReturn(Flux.just("PONG"));
        when(qdrant.collectionExistsAsync(anyString())).thenReturn(Futures.immediateFuture(false));
        ollama.enqueue(new MockResponse().setResponseCode(200).setBody("{}"));

        StepVerifier.create(service.checkDependencies())
                .assertNext(h -> {
                    assertThat(h.status()).isEqualTo("degraded");
                    assertThat(h.components())
                            .anyMatch(c -> "qdrant".equals(c.name())
                                    && "down".equals(c.status())
                                    && c.detail().contains("missing"));
                })
                .verifyComplete();
    }

    @Test
    void checkDependencies_ollama5xx_isDegraded() {
        when(redis.execute(any(ReactiveRedisCallback.class))).thenReturn(Flux.just("PONG"));
        when(qdrant.collectionExistsAsync(anyString())).thenReturn(Futures.immediateFuture(true));
        ollama.enqueue(new MockResponse().setResponseCode(500).setBody("down"));

        StepVerifier.create(service.checkDependencies())
                .assertNext(h -> {
                    assertThat(h.status()).isEqualTo("degraded");
                    assertThat(h.components())
                            .anyMatch(c -> "ollama".equals(c.name()) && "down".equals(c.status()));
                })
                .verifyComplete();
    }
}
