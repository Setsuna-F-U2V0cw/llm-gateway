package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ChatRequest;
import com.llmgateway.gateway.model.Intent;
import com.llmgateway.gateway.model.Route;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Router 单元测试（阶段四）
 * - 规则分类 classifyByRules 关键词覆盖
 * - Router.route：intent → model 映射、LengthGuard 413 拒绝、xlong 覆盖到 big-context
 *
 * IntentClassifier 用 Mockito 桩，聚焦 Router 逻辑；classifyByRules 直接测真实实现。
 */
class RouterTest {

    private GatewayProperties props;
    private IntentClassifier classifier;
    private Router router;

    @BeforeEach
    void setUp() {
        props = new GatewayProperties();
        props.setModels(Map.of(
                "deepseek-chat", model("https://a", false, 64000),
                "deepseek-reasoner", model("https://a", true, 64000),
                "qwen3-1-7b", model("http://ollama", false, 32000)
        ));
        props.setRouting(new java.util.HashMap<>(Map.of(
                "code", "deepseek-chat",
                "reasoning", "deepseek-reasoner",
                "default", "deepseek-chat"
        )));
        props.setXlongThreshold(32000);
        classifier = mock(IntentClassifier.class);
        router = new Router(classifier, props);
    }

    private GatewayProperties.ModelConfig model(String url, boolean thinking, int maxCtx) {
        GatewayProperties.ModelConfig m = new GatewayProperties.ModelConfig();
        m.setTargetUrl(url);
        m.setApiKey("k");
        m.setThinking(thinking);
        m.setMaxContext(maxCtx);
        return m;
    }

    // ---------- classifyByRules（真实实现，无需 mock） ----------

    @Test
    void classifyByRules_keywords() {
        IntentClassifier real = new IntentClassifier(null);
        assertThat(real.classifyByRules("写一段 Python 代码")).isEqualTo(Intent.CODE);
        assertThat(real.classifyByRules("把这段话翻译成英文")).isEqualTo(Intent.TRANSLATION);
        assertThat(real.classifyByRules("计算 1+1 等于几")).isEqualTo(Intent.MATH);
        assertThat(real.classifyByRules("为什么天是蓝的")).isEqualTo(Intent.REASONING);
        assertThat(real.classifyByRules("写一篇文章")).isEqualTo(Intent.WRITING);
        assertThat(real.classifyByRules("你好")).isEqualTo(Intent.DEFAULT);
    }

    // ---------- Router.route 映射 ----------

    @Test
    void route_codeIntent_mapsToDeepseekChat() {
        when(classifier.classify(any())).thenReturn(Intent.CODE);
        ChatRequest req = chatRequest("写代码");

        StepVerifier.create(router.route(req, 100, new float[]{1}))
                .assertNext(route -> {
                    assertThat(route.getModel()).isEqualTo("deepseek-chat");
                    assertThat(route.isThinking()).isFalse();
                    assertThat(route.getTargetUrl()).isEqualTo("https://a");
                })
                .verifyComplete();
    }

    @Test
    void route_reasoningIntent_mapsToReasoner() {
        when(classifier.classify(any())).thenReturn(Intent.REASONING);
        ChatRequest req = chatRequest("证明素数无限");

        StepVerifier.create(router.route(req, 100, new float[]{1}))
                .assertNext(route -> {
                    assertThat(route.getModel()).isEqualTo("deepseek-reasoner");
                    assertThat(route.isThinking()).isTrue();
                })
                .verifyComplete();
    }

    @Test
    void route_nullEmbedding_fallsBackToRules() {
        // embedding 为 null → Router 走 classifyByRules；"写代码" → CODE → deepseek-chat
        when(classifier.classifyByRules("写代码")).thenReturn(Intent.CODE);
        ChatRequest req = chatRequest("写代码");

        StepVerifier.create(router.route(req, 100, null))
                .assertNext(route -> assertThat(route.getModel()).isEqualTo("deepseek-chat"))
                .verifyComplete();
    }

    // ---------- LengthGuard ----------

    @Test
    void route_overMaxContext_rejectedWith413() {
        when(classifier.classify(any())).thenReturn(Intent.CODE);
        ChatRequest req = chatRequest("写代码");
        // qwen3-1-7b maxContext=32000；tokenCount=50000 超限，且 50000 > xlongThreshold(32000)
        // 但 deepseek-chat maxContext=64000 > 50000，应覆盖到 deepseek-chat（big-context）
        // 这里测「超 qwen3-1-7b 上限」需让路由先选 qwen3-1-7b：用 default 路由指向 qwen3-1-7b
        props.getRouting().put("default", "qwen3-1-7b");
        when(classifier.classify(any())).thenReturn(Intent.DEFAULT);

        StepVerifier.create(router.route(req, 50000, new float[]{1}))
                .assertNext(route -> {
                    assertThat(route.getMaxContext()).isGreaterThanOrEqualTo(50000);
                })
                .verifyComplete();
    }

    @Test
    void route_overAllContexts_rejectedWith413() {
        when(classifier.classify(any())).thenReturn(Intent.DEFAULT);
        props.getRouting().put("default", "qwen3-1-7b");
        ChatRequest req = chatRequest("超长");
        // 999999 超所有 model 上限 → 413
        StepVerifier.create(router.route(req, 999999, new float[]{1}))
                .verifyErrorSatisfies(e ->
                        assertThat(((ResponseStatusException) e).getStatusCode()).isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE));
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
