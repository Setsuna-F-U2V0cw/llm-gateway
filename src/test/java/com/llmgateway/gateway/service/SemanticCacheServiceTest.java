package com.llmgateway.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ChatRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SemanticCacheServiceTest {

    private VectorStoreService vectorStore;
    private GatewayProperties props;
    private SemanticCacheService service;

    @BeforeEach
    void setUp() {
        vectorStore = mock(VectorStoreService.class);
        props = new GatewayProperties();
        props.setStreamSimulateDelayMs(20);
        service = new SemanticCacheService(vectorStore, props, new ObjectMapper());
    }

    @Test
    void searchCache_passesTenantAndModel() {
        float[] vec = new float[]{0.1f, 0.2f};
        when(vectorStore.search(vec, "user-a", "deepseek-v4-flash"))
                .thenReturn(Mono.just("cached"));

        StepVerifier.create(service.searchCache(vec, "deepseek-v4-flash", "user-a"))
                .assertNext(opt -> assertThat(opt).isPresent())
                .verifyComplete();

        verify(vectorStore).search(vec, "user-a", "deepseek-v4-flash");
    }

    @Test
    void searchCache_nullEmbedding_skipsStore() {
        StepVerifier.create(service.searchCache(null, "m", "u"))
                .assertNext(opt -> assertThat(opt).isEmpty())
                .verifyComplete();
        verify(vectorStore, never()).search(any(), any(), any());
    }

    @Test
    void searchCache_miss_returnsEmptyOptional() {
        float[] vec = new float[]{1f};
        when(vectorStore.search(vec, "u", "m")).thenReturn(Mono.empty());

        StepVerifier.create(service.searchCache(vec, "m", "u"))
                .assertNext(opt -> assertThat(opt).isEmpty())
                .verifyComplete();
    }

    @Test
    void searchCache_storeError_failOpenEmpty() {
        float[] vec = new float[]{1f};
        when(vectorStore.search(vec, "u", "m"))
                .thenReturn(Mono.error(new RuntimeException("qdrant down")));

        StepVerifier.create(service.searchCache(vec, "m", "u"))
                .assertNext(opt -> assertThat(opt).isEmpty())
                .verifyComplete();
    }

    @Test
    void searchCache_hit_replaysTypewriterThenDone_withVirtualTime() {
        float[] vec = new float[]{1f};
        when(vectorStore.search(vec, "tenant-a", "deepseek-v4-flash"))
                .thenReturn(Mono.just("ab"));

        StepVerifier.withVirtualTime(() -> service.searchCache(vec, "deepseek-v4-flash", "tenant-a")
                        .flatMapMany(opt -> opt.orElseThrow().stream()))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(19))
                .thenAwait(Duration.ofMillis(1))
                .assertNext(chunk -> {
                    assertThat(chunk).contains("\"content\":\"a\"");
                    assertThat(chunk).contains("deepseek-v4-flash");
                    assertThat(chunk).doesNotStartWith("data:");
                })
                .thenAwait(Duration.ofMillis(20))
                .assertNext(chunk -> assertThat(chunk).contains("\"content\":\"b\""))
                .expectNext("[DONE]")
                .verifyComplete();
    }

    @Test
    void saveToCache_passesTenantAndModel() {
        when(vectorStore.save(any(), any(), any(), any(), any())).thenReturn(Mono.empty());
        float[] vec = new float[]{1f};
        ChatRequest req = requestWithPrompt("hi");

        service.saveToCache(vec, req, "answer text", "user-b", "deepseek-v4-flash");

        verify(vectorStore, timeout(1000))
                .save(eq(vec), eq("hi"), eq("answer text"), eq("user-b"), eq("deepseek-v4-flash"));
    }

    @Test
    void saveToCache_skipsWhenEmbeddingNullOrAnswerBlank() {
        ChatRequest req = requestWithPrompt("hi");
        service.saveToCache(null, req, "answer", "u", "m");
        service.saveToCache(new float[]{1f}, req, "  ", "u", "m");
        service.saveToCache(new float[]{1f}, req, null, "u", "m");
        verify(vectorStore, never()).save(any(), any(), any(), any(), any());
    }

    private static ChatRequest requestWithPrompt(String prompt) {
        ChatRequest.Message m = new ChatRequest.Message();
        m.setRole("user");
        m.setContent(prompt);
        ChatRequest req = new ChatRequest();
        req.setMessages(List.of(m));
        return req;
    }
}
