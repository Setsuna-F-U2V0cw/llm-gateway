package com.llmgateway.gateway.controller;

import com.llmgateway.gateway.model.ChatRequest;
import com.llmgateway.gateway.model.DependencyHealth;
import com.llmgateway.gateway.model.ModelListResponse;
import com.llmgateway.gateway.service.HealthService;
import com.llmgateway.gateway.service.LlmProxyService;
import com.llmgateway.gateway.service.ModelCatalogService;
import com.llmgateway.gateway.security.GatewayAuthenticationFilter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * LLM 网关核心 Controller
 *
 * 对外暴露与 OpenAI 完全兼容的接口，客户端无需修改任何代码，
 * 只需将 base_url 从 https://api.openai.com 替换为网关地址即可。
 */
@Slf4j
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
public class ChatCompletionController {

    private final LlmProxyService llmProxyService;
    private final HealthService healthService;
    private final ModelCatalogService modelCatalogService;

    /**
     * SSE 流式聊天接口（核心端点）
     *
     * [面试亮点] 返回类型是 Mono<ResponseEntity<Flux<String>>>：
     * - 外层 Mono 在缓存命中判定完成后决出，用于写入 X-Cache-Hit 响应头
     * - 内层 Flux<String> 配合 produces = TEXT_EVENT_STREAM_VALUE，由 Spring WebFlux
     *   自动将每个元素以 SSE 格式（data: ...\n\n）推送，底层 Netty 使用 Chunked Transfer
     *   Encoding，数据产生一块就发送一块，实现真正的零拷贝流式透传
     *
     * [踩坑预警] 若改回 Mono<String>/String 或在 Service 中 .block()，Spring 会等待响应体
     * 完全接收后再返回，大模型长输出在网关层积压，破坏流式体验且极易 OOM。
     *
     * @param request 标准 OpenAI ChatCompletion 请求体
     * @param userId  由入口 API Key 映射出的可信 tenantId
     * @return        ResponseEntity 包装的 SSE 流；X-Cache-Hit 头标识是否命中语义缓存，
     *                每条消息格式为 "data: {json}\n\n"，结束时发送 "data: [DONE]\n\n"
     */
    @PostMapping(
        value = "/chat/completions",
        produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Mono<ResponseEntity<Flux<String>>> chatCompletions(
            @RequestBody ChatRequest request,
            @RequestAttribute(GatewayAuthenticationFilter.TENANT_ATTRIBUTE) String userId) {
        log.info("收到聊天请求: userId={}, model={}", userId, request.getModel());
        return llmProxyService.streamChat(request, userId);
    }

    /**
     * 进程存活探针。不探测 Redis/Qdrant/Ollama——三件套挂了网关仍 fail-open 接请求。
     * 依赖分项见 {@link #healthDeps()}。
     */
    @GetMapping("/health")
    public String health() {
        return "LLM Gateway is running";
    }

    /**
     * 依赖诊断：并行 ping Redis / Qdrant / Ollama。始终 HTTP 200，body.status 为 up 或 degraded。
     */
    @GetMapping("/health/deps")
    public Mono<DependencyHealth> healthDeps() {
        return healthService.checkDependencies();
    }

    /**
     * OpenAI 兼容模型列表。id 为配置 key（可直接作为 chat 请求的 model）；不含密钥。
     */
    @GetMapping("/models")
    public ModelListResponse models() {
        return modelCatalogService.list();
    }
}
