package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.Intent;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IntentClassifierTest {

    @Test
    void partialFailure_doesNotPublishIncompleteSnapshot_andUsesRules() {
        EmbeddingService embedding = mock(EmbeddingService.class);
        AtomicInteger calls = new AtomicInteger();
        when(embedding.embed(anyString())).thenAnswer(invocation ->
                calls.incrementAndGet() == 4
                        ? Mono.error(new RuntimeException("temporary failure"))
                        : Mono.just(new float[]{1f, 0f}));

        GatewayProperties props = retryProps(Duration.ofDays(1));
        IntentClassifier classifier = new IntentClassifier(embedding, props);

        StepVerifier.create(classifier.loadUntilReady())
                .thenAwait(Duration.ofMillis(10))
                .then(() -> {
                    assertThat(classifier.prototypeStatus().state()).isEqualTo("retrying");
                    assertThat(classifier.prototypeStatus().loaded()).isZero();
                    assertThat(classifier.classify(new float[]{1f, 0f}, "请写一段代码"))
                            .isEqualTo(Intent.CODE);
                })
                .thenCancel()
                .verify();
    }

    @Test
    void transientFailure_retriesThenPublishesCompleteSnapshot() {
        EmbeddingService embedding = mock(EmbeddingService.class);
        AtomicInteger calls = new AtomicInteger();
        when(embedding.embed(anyString())).thenAnswer(invocation ->
                calls.incrementAndGet() == 1
                        ? Mono.error(new RuntimeException("ollama starting"))
                        : Mono.just(new float[]{1f, 0f}));

        GatewayProperties props = retryProps(Duration.ofSeconds(1));
        IntentClassifier classifier = new IntentClassifier(embedding, props);

        StepVerifier.withVirtualTime(classifier::loadUntilReady)
                .thenAwait(Duration.ofSeconds(1))
                .verifyComplete();

        assertThat(classifier.prototypeStatus().state()).isEqualTo("ready");
        assertThat(classifier.prototypeStatus().loaded()).isEqualTo(6);
        assertThat(classifier.prototypeStatus().retries()).isEqualTo(1);
        assertThat(calls.get()).isGreaterThanOrEqualTo(19);
    }

    @Test
    void classify_usesCosineWhenSnapshotReady() {
        EmbeddingService embedding = mock(EmbeddingService.class);
        when(embedding.embed(anyString())).thenAnswer(invocation -> {
            String text = invocation.getArgument(0);
            if (text.contains("代码") || text.contains("bug") || text.contains("Python")
                    || text.contains("function")) {
                return Mono.just(new float[]{1f, 0f, 0f, 0f});
            }
            if (text.contains("翻译") || text.toLowerCase().contains("translate")) {
                return Mono.just(new float[]{0f, 1f, 0f, 0f});
            }
            return Mono.just(new float[]{0f, 0f, 1f, 0f});
        });

        IntentClassifier classifier = new IntentClassifier(embedding, retryProps(Duration.ofMillis(1)));
        StepVerifier.create(classifier.loadUntilReady()).verifyComplete();

        assertThat(classifier.classify(new float[]{1f, 0f, 0f, 0f}, "无关文本")).isEqualTo(Intent.CODE);
        assertThat(classifier.classify(new float[]{0f, 1f, 0f, 0f}, "无关文本")).isEqualTo(Intent.TRANSLATION);
        // 低置信：走 DEFAULT，即使 prompt 含代码关键词也不走规则（快照已 ready）
        assertThat(classifier.classify(new float[]{0f, 0f, 0f, 1f}, "请写一段代码"))
                .isEqualTo(Intent.DEFAULT);
    }

    @Test
    void classify_nullEmbeddingOrNotReady_usesRules() {
        IntentClassifier classifier = new IntentClassifier(mock(EmbeddingService.class), new GatewayProperties());
        assertThat(classifier.prototypeStatus().state()).isEqualTo("loading");
        assertThat(classifier.classify(null, "请写一段代码")).isEqualTo(Intent.CODE);
        assertThat(classifier.classify(new float[]{1f}, "把这段话翻译成英文")).isEqualTo(Intent.TRANSLATION);
        assertThat(classifier.classify(null, "你好")).isEqualTo(Intent.DEFAULT);
        assertThat(classifier.classify(null, null)).isEqualTo(Intent.DEFAULT);
    }

    @Test
    void classifyByRules_hasNoChitchatKeywordBranch() {
        IntentClassifier classifier = new IntentClassifier(null, new GatewayProperties());
        assertThat(classifier.classifyByRules("今天天气怎么样")).isEqualTo(Intent.DEFAULT);
        assertThat(classifier.classifyByRules("随便聊聊")).isEqualTo(Intent.DEFAULT);
    }

    private static GatewayProperties retryProps(Duration delay) {
        GatewayProperties props = new GatewayProperties();
        props.getPrototypeRetry().setInitialDelayMs(delay.toMillis());
        props.getPrototypeRetry().setMaxDelayMs(delay.toMillis());
        props.getPrototypeRetry().setJitter(0);
        return props;
    }
}
