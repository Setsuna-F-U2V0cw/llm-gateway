package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ChatRequest;
import com.llmgateway.gateway.model.Intent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Router 单元测试：intent → 生产模型名、LengthGuard 413、xlong 覆盖。
 * fixture 与 application.yml 对齐（deepseek-v4-flash 系列），不再使用 deepseek-chat。
 * {@code route(..., tokenCount, ...)} 的 tokenCount 是 input token，不是 TPM 预估总量。
 */
class RouterTest {

    private GatewayProperties props;
    private IntentClassifier classifier;
    private Router router;

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        props.setModels(new HashMap<>(Map.of(
                "deepseek-v4-flash", model("https://a", false, 64000, null),
                "deepseek-v4-flash-thinking", model("https://a", true, 64000, null),
                "deepseek-v4-pro", model("https://a", true, 128000, null),
                "qwen3-1-7b", model("http://ollama", false, 32000, "qwen3:1.7b")
        )));
        props.setRouting(new HashMap<>(Map.of(
                "code", "deepseek-v4-flash",
                "reasoning", "deepseek-v4-flash-thinking",
                "math", "deepseek-v4-pro",
                "chitchat", "qwen3-1-7b",
                "default", "deepseek-v4-flash"
        )));
        props.setXlongThreshold(32000);
        classifier = mock(IntentClassifier.class);
        router = new Router(classifier, props);
    }

    private GatewayProperties.ModelConfig model(String url, boolean thinking, int maxCtx, String requestModel) {
        GatewayProperties.ModelConfig m = new GatewayProperties.ModelConfig();
        m.setTargetUrl(url);
        m.setApiKey("k");
        m.setThinking(thinking);
        m.setMaxContext(maxCtx);
        m.setRequestModel(requestModel);
        return m;
    }

    @Test
    void classifyByRules_keywords() {
        IntentClassifier real = new IntentClassifier(null, new GatewayProperties());
        assertThat(real.classifyByRules("写一段 Python 代码")).isEqualTo(Intent.CODE);
        assertThat(real.classifyByRules("把这段话翻译成英文")).isEqualTo(Intent.TRANSLATION);
        assertThat(real.classifyByRules("计算 1+1 等于几")).isEqualTo(Intent.MATH);
        assertThat(real.classifyByRules("为什么天是蓝的")).isEqualTo(Intent.REASONING);
        assertThat(real.classifyByRules("写一篇文章")).isEqualTo(Intent.WRITING);
        assertThat(real.classifyByRules("你好")).isEqualTo(Intent.DEFAULT);
    }

    @Test
    void route_codeIntent_mapsToFlash() {
        when(classifier.classify(any(), any())).thenReturn(Intent.CODE);
        ChatRequest req = chatRequest("写代码");

        StepVerifier.create(router.route(req, 100, new float[]{1}))
                .assertNext(route -> {
                    assertThat(route.getModel()).isEqualTo("deepseek-v4-flash");
                    assertThat(route.isThinking()).isFalse();
                    assertThat(route.getTargetUrl()).isEqualTo("https://a");
                    assertThat(route.getBreakerName()).isEqualTo("deepseek-v4-flash");
                })
                .verifyComplete();
    }

    @Test
    void route_reasoningIntent_mapsToThinking() {
        when(classifier.classify(any(), any())).thenReturn(Intent.REASONING);
        ChatRequest req = chatRequest("证明素数无限");

        StepVerifier.create(router.route(req, 100, new float[]{1}))
                .assertNext(route -> {
                    assertThat(route.getModel()).isEqualTo("deepseek-v4-flash-thinking");
                    assertThat(route.isThinking()).isTrue();
                })
                .verifyComplete();
    }

    @Test
    void route_chitchatIntent_mapsToLocalQwen() {
        when(classifier.classify(any(), any())).thenReturn(Intent.CHITCHAT);

        StepVerifier.create(router.route(chatRequest("随便聊聊"), 100, new float[]{1}))
                .assertNext(route -> {
                    assertThat(route.getModel()).isEqualTo("qwen3-1-7b");
                    assertThat(route.getRequestModel()).isEqualTo("qwen3:1.7b");
                })
                .verifyComplete();
    }

    @Test
    void route_nullEmbedding_fallsBackToRulesPathOfClassifier() {
        when(classifier.classify(null, "写代码")).thenReturn(Intent.CODE);

        StepVerifier.create(router.route(chatRequest("写代码"), 100, null))
                .assertNext(route -> assertThat(route.getModel()).isEqualTo("deepseek-v4-flash"))
                .verifyComplete();
    }

    @Test
    void route_overSmallModel_xlongOverlaysToPro() {
        when(classifier.classify(any(), any())).thenReturn(Intent.CHITCHAT);
        ChatRequest req = chatRequest("超长闲聊");

        StepVerifier.create(router.route(req, 50_000, new float[]{1}))
                .assertNext(route -> {
                    assertThat(route.getModel()).isEqualTo("deepseek-v4-pro");
                    assertThat(route.getMaxContext()).isGreaterThanOrEqualTo(50_000);
                })
                .verifyComplete();
    }

    @Test
    void route_xlongDisabled_rejectsWhenOverChosenMaxContext() {
        props.setXlongThreshold(0);
        when(classifier.classify(any(), any())).thenReturn(Intent.CHITCHAT);

        StepVerifier.create(router.route(chatRequest("超长"), 50_000, new float[]{1}))
                .verifyErrorSatisfies(e -> {
                    assertThat(e).isInstanceOf(ResponseStatusException.class);
                    assertThat(((ResponseStatusException) e).getStatusCode())
                            .isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
                    assertThat(e.getMessage()).contains("超出 model 上下文上限");
                });
    }

    @Test
    void route_overAllContexts_rejectedWith413() {
        when(classifier.classify(any(), any())).thenReturn(Intent.DEFAULT);

        StepVerifier.create(router.route(chatRequest("超长"), 999_999, new float[]{1}))
                .verifyErrorSatisfies(e ->
                        assertThat(((ResponseStatusException) e).getStatusCode())
                                .isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE));
    }

    @Test
    void route_missingRoutingTarget_returns500() {
        props.getRouting().put("code", "not-registered");
        when(classifier.classify(any(), any())).thenReturn(Intent.CODE);

        StepVerifier.create(router.route(chatRequest("写代码"), 100, new float[]{1}))
                .verifyErrorSatisfies(e -> {
                    assertThat(((ResponseStatusException) e).getStatusCode())
                            .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
                    assertThat(e.getMessage()).contains("未配置的 model");
                });
    }

    @Test
    void route_explicitRegisteredModel_skipsIntent() {
        ChatRequest req = chatRequest("写代码");
        req.setModel("deepseek-v4-flash-thinking");

        StepVerifier.create(router.route(req, 100, new float[]{1}))
                .assertNext(route -> {
                    assertThat(route.getModel()).isEqualTo("deepseek-v4-flash-thinking");
                    assertThat(route.isThinking()).isTrue();
                })
                .verifyComplete();
        verify(classifier, never()).classify(any(), any());
    }

    @Test
    void route_explicitRequestModelTag_resolvesConfigKey() {
        ChatRequest req = chatRequest("hi");
        req.setModel("qwen3:1.7b");

        StepVerifier.create(router.route(req, 100, new float[]{1}))
                .assertNext(route -> assertThat(route.getModel()).isEqualTo("qwen3-1-7b"))
                .verifyComplete();
        verify(classifier, never()).classify(any(), any());
    }

    @Test
    void route_autoOrUnknownModel_usesIntent() {
        when(classifier.classify(any(), any())).thenReturn(Intent.CODE);
        ChatRequest auto = chatRequest("写代码");
        auto.setModel("auto");

        StepVerifier.create(router.route(auto, 100, new float[]{1}))
                .assertNext(route -> assertThat(route.getModel()).isEqualTo("deepseek-v4-flash"))
                .verifyComplete();
        verify(classifier).classify(any(), any());

        ChatRequest unknown = chatRequest("写代码");
        unknown.setModel("gpt-4o");
        StepVerifier.create(router.route(unknown, 100, new float[]{1}))
                .assertNext(route -> assertThat(route.getModel()).isEqualTo("deepseek-v4-flash"))
                .verifyComplete();
    }

    @Test
    void route_blankModel_usesIntent() {
        when(classifier.classify(any(), any())).thenReturn(Intent.MATH);
        ChatRequest req = chatRequest("求解方程");
        req.setModel("  ");

        StepVerifier.create(router.route(req, 100, new float[]{1}))
                .assertNext(route -> assertThat(route.getModel()).isEqualTo("deepseek-v4-pro"))
                .verifyComplete();
    }

    private ChatRequest chatRequest(String content) {
        ChatRequest req = new ChatRequest();
        ChatRequest.Message m = new ChatRequest.Message();
        m.setRole("user");
        m.setContent(content);
        req.setMessages(List.of(m));
        return req;
    }

    private static <T> T any() {
        return org.mockito.ArgumentMatchers.any();
    }
}
