package com.llmgateway.gateway.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ChatRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

/**
 * 语义缓存服务：编排缓存命中/未命中的完整逻辑
 *
 * 阶段四重构（Q13）：embedding 计算上提到 {@code LlmProxyService.streamChat} 顶部算一次，
 * 本服务收窄为「按给定向量检索 + 伪装 SSE 流」。构造函数不再依赖 EmbeddingService——
 * 同一向量被缓存检索、意图分类、写缓存三处复用，省两次 embed 调用。
 *
 * 核心职责：
 * 1. 按给定 embedding 检索 Qdrant → 判断是否命中
 * 2. 命中：用 delayElements 伪装 SSE 打字机流，包装在 Optional 中返回
 * 3. 未命中：返回 Optional.empty()，由上层走路由 + 真实 LLM 转发路径
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SemanticCacheService {

    private final VectorStoreService vectorStoreService;
    private final GatewayProperties props;
    private final ObjectMapper objectMapper;

    /**
     * 缓存命中结果：流在订阅前即可拿到，供上层提前写 X-Cache-Hit。
     */
    public record CacheHit(Flux<String> stream) {}

    /**
     * 按给定 embedding 检索语义缓存（仅同一 tenant + 同一请求 model）。
     *
     * [💎 面试亮点] 判定建模为 Mono&lt;Optional&lt;CacheHit&gt;&gt;：命中标志在订阅 SSE 之前决出，
     * 上层写 X-Cache-Hit，并按实际回放给客户端的 content 做预扣结算。
     */
    public Mono<Optional<CacheHit>> searchCache(float[] embedding, String model, String userId) {
        if (embedding == null) {
            return Mono.just(Optional.empty());
        }

        return vectorStoreService.search(embedding, userId, model)
                .map(cachedAnswer -> Optional.of(
                        new CacheHit(simulateSseStream(cachedAnswer, model))))
                .defaultIfEmpty(Optional.empty())
                .onErrorResume(e -> {
                    log.warn("语义缓存查询异常，降级透传: {}", e.getMessage());
                    return Mono.just(Optional.empty());
                });
    }

    /**
     * 将 LLM 完整回答异步写入语义缓存（不阻塞主流程）
     *
     * [💎 面试亮点] subscribeOn(boundedElastic) + subscribe() 触发异步执行，
     * doOnComplete 回调结束后立刻返回，Qdrant 写入在后台线程完成，
     * 客户端的 SSE 连接不会因为落库操作而延迟关闭。
     *
     * 阶段四：embedding 由 pipeline 顶部传入复用，本方法不再调 EmbeddingService。
     *
     * @param embedding  prompt 向量（复用检索时的同一向量）；为 null 时跳过写入
     * @param request    原始请求（提取 prompt）
     * @param answer     只含 content 的完整回答
     * @param userId     租户，写入 payload.tenant_id
     * @param cacheModel 客户端请求的 model（路由改写之前），写入 payload.model
     */
    public void saveToCache(float[] embedding, ChatRequest request, String answer,
                            String userId, String cacheModel) {
        if (embedding == null || answer == null || answer.isBlank()) return;

        String prompt = extractPrompt(request);
        vectorStoreService.save(embedding, prompt, answer, userId, cacheModel)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        null,
                        e -> log.warn("语义缓存写入失败: {}", e.getMessage()),
                        () -> log.debug("语义缓存写入完成: tenant={}, model={}, promptLen={}, answerLen={}",
                                userId, cacheModel,
                                prompt == null ? 0 : prompt.length(),
                                answer.length())
                );
    }

    /**
     * SSE 流伪装：将完整字符串拆成字符数组，加延迟模拟打字机效果
     *
     * [💎 面试亮点] Flux.fromArray(chars).delayElements(Duration.ofMillis(20)) 让缓存命中的响应
     * 在前端看起来与真实 LLM 流式输出完全一致，用户无感知。
     * TTFB 从 3s+ 降至 ~80ms（Embedding + Qdrant 检索耗时），成本降为 0。
     *
     * [🚧 核心难点] delayElements 内部使用 Schedulers.parallel() 定时调度，
     * 每个字符的延迟是非阻塞的，不会占用任何线程等待。
     */
    private Flux<String> simulateSseStream(String answer, String model) {
        log.info("语义缓存命中，伪装 SSE 流输出: answerLen={}", answer.length());

        String[] chars = answer.split("");

        return Flux.fromArray(chars)
                .delayElements(Duration.ofMillis(props.getStreamSimulateDelayMs()))
                .map(c -> buildSseChunk(c, model))
                // 末尾补发 [DONE]，只发裸 payload，"data: " 前缀交给 ServerSentEventHttpMessageWriter
                .concatWith(Flux.just("[DONE]"));
    }

    /**
     * 构造 OpenAI 标准 SSE chunk 的 JSON payload
     *
     * [💣 踩坑预警] 只返回 JSON payload，不拼 "data: " 前缀和 "\n\n"。
     * Controller 返回 Mono&lt;ResponseEntity&lt;Flux&lt;String&gt;&gt;&gt; + produces=text/event-stream，Spring 的
     * ServerSentEventHttpMessageWriter 会对每个 String 元素自动加 "data:" 前缀和 "\n\n"。
     */
    private String buildSseChunk(String content, String model) {
        try {
            Map<String, Object> delta = Map.of("content", content, "role", "assistant");
            Map<String, Object> choice = Map.of("index", 0, "delta", delta, "finish_reason", "");
            Map<String, Object> chunk = Map.of(
                    "id", "cache-" + System.currentTimeMillis(),
                    "object", "chat.completion.chunk",
                    "model", model != null ? model : "unknown",
                    "choices", java.util.List.of(choice)
            );
            return objectMapper.writeValueAsString(chunk);
        } catch (Exception e) {
            return "{\"choices\":[{\"delta\":{\"content\":\"" + content + "\"}}]}";
        }
    }

    /**
     * 提取请求中最后一条 user 消息作为 Prompt
     */
    private String extractPrompt(ChatRequest request) {
        if (request.getMessages() == null || request.getMessages().isEmpty()) {
            return null;
        }
        return request.getMessages().stream()
                .filter(m -> "user".equals(m.getRole()))
                .reduce((first, second) -> second)
                .map(ChatRequest.Message::getContent)
                .orElse(null);
    }
}
