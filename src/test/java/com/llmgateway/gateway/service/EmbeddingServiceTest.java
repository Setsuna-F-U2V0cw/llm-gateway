package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * EmbeddingService 单元测试：用 OkHttp MockWebServer 桩 Ollama {@code /api/embed}，
 * 无需真实 Ollama 进程。验证维度、L2 归一化、错误传播（交由上层 fail-open）。
 *
 * [reactive-backend-standards §4] 用 StepVerifier 驱动订阅，禁止 block()。
 */
class EmbeddingServiceTest {

    private MockWebServer server;
    private EmbeddingService embeddingService;
    private GatewayProperties props;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        props = new GatewayProperties();
        props.setOllamaEmbedModel("bge-large-zh-v1.5");
        props.setEmbeddingDimension(1024);
        // 去掉 MockWebServer URL 末尾斜杠，避免与 "/api/embed" 拼成双斜杠
        props.setOllamaBaseUrl(server.url("").toString().replaceAll("/$", ""));
        // 测试用普通 WebClient 即可，无需 ollamaWebClient bean 的超时配置
        WebClient webClient = WebClient.builder().build();
        embeddingService = new EmbeddingService(props, webClient);
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    void embed_shouldReturnNormalizedVectorOfConfiguredDimension() {
        // 构造 1024 维响应：前两个元素 0.6/0.8（范数已为 1），其余 0
        StringBuilder arr = new StringBuilder("[0.6,0.8");
        for (int i = 2; i < 1024; i++) arr.append(",0.0");
        arr.append("]");
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("{\"embeddings\":[" + arr + "]}"));

        StepVerifier.create(embeddingService.embed("你好"))
                .assertNext(vec -> {
                    assertThat(vec).hasSize(1024);
                    // L2 归一化后范数应 ≈ 1.0
                    double norm = 0;
                    for (float v : vec) norm += (double) v * v;
                    assertThat(Math.sqrt(norm)).isCloseTo(1.0, within(1e-5));
                    assertThat(vec[0]).isCloseTo(0.6f, within(1e-5f));
                    assertThat(vec[1]).isCloseTo(0.8f, within(1e-5f));
                })
                .verifyComplete();
    }

    @Test
    void embed_shouldPropagateErrorWhenOllamaReturns5xx() {
        // retrieve() 对 5xx 默认抛 WebClientResponseException，embed 不吞错，向上传播
        server.enqueue(new MockResponse().setResponseCode(500).setBody("internal error"));

        StepVerifier.create(embeddingService.embed("你好"))
                .verifyError();
    }
}
