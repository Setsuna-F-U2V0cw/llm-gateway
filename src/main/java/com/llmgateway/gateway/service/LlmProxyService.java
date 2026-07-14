package com.llmgateway.gateway.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ChatRequest;
import com.llmgateway.gateway.model.Route;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * LLM 代理服务：将网关请求透传给下游 LLM，以 ResponseEntity 包装的 Flux&lt;String&gt; SSE 流返回。
 *
 * 阶段四完整请求链路：
 *   客户端
 *     → JTokkit 预估 Token
 *     → Redis Lua 令牌桶预扣（阶段二）
 *     → embed(prompt) 顶部算一次（阶段四上提，三处复用）
 *     → 语义缓存检索 Qdrant（阶段三，按向量检索）
 *         ├── 命中 → delayElements 伪装 SSE 流直接返回
 *         └── 未命中 → Router 按意图+长度选 model（阶段四）
 *                       → breaker.decorate( primary.timeout(TTFT-SLO, perChunk) )
 *                           ├── breaker open → CallNotPermitted ─┐
 *                           ├── TTFT > SLO → TimeoutException ──┤
 *                           └── 下游错误 ────────────────────────┤
 *                                                              ↓
 *                          .onErrorResume → fallback(Ollama qwen3:1.7b)
 *                             ├ 成功 → SSE 透传
 *                             └ 也挂 → data:{"error":"主通道与备用通道均不可用"}
 *                       → doOnComplete：Token 结算 + 异步写缓存（复用顶部 embedding）
 *
 * [💣 踩坑预警] fallback 只能在「首内容帧之前」切（Q5）。
 * 若主通道已吐 content 后中途出错，再切 fallback 会让客户端收到「半截 A + 半截 B」矛盾内容。
 * 用 contentStarted 标志区分：首内容前失败 → 切 fallback；首内容后失败 → 发 SSE 错误帧。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class LlmProxyService {

    private final WebClient webClient;
    private final GatewayProperties props;
    private final TokenCountService tokenCountService;
    private final RateLimiterService rateLimiterService;
    private final SemanticCacheService semanticCacheService;
    private final EmbeddingService embeddingService;
    private final Router router;
    private final CircuitBreakerService circuitBreakerService;
    private final ObjectMapper objectMapper;

    /**
     * 流式转发请求：限流 → embed（顶部一次）→ 语义缓存 → 路由 + LLM 转发
     *
     * 返回 Mono<ResponseEntity<Flux<String>>> 而非裸 Flux<String>，是为了在缓存命中/未命中
     * 决出后写入 X-Cache-Hit 响应头（前端 onopen 即可读到）。
     */
    public Mono<ResponseEntity<Flux<String>>> streamChat(ChatRequest request, String userId) {
        request.setStream(true);
        int estimatedTokens = tokenCountService.estimateTotalTokens(request);
        log.debug("请求转发: userId={}, model={}, estimatedTokens={}", userId, request.getModel(), estimatedTokens);

        return rateLimiterService.tryAcquire(userId, estimatedTokens)
                .flatMap(allowed -> {
                    if (!allowed) {
                        return Mono.<ResponseEntity<Flux<String>>>error(new ResponseStatusException(
                                HttpStatus.TOO_MANY_REQUESTS, "TPM 超限，请稍后重试"));
                    }
                    // [💎 面试亮点] embed 顶部算一次，三处复用：缓存检索、意图分类、写缓存。
                    // fail-open：embedding 不可用时返回 Optional.empty()，下游走规则分类 + 透传。
                    String prompt = extractPrompt(request);
                    return embedForPipeline(prompt)
                            .flatMap(embedding -> semanticCacheService.searchCache(embedding.orElse(null), request.getModel())
                                    .map(opt -> opt
                                            .map(cacheFlux -> ResponseEntity.ok()
                                                    .header("X-Cache-Hit", "true")
                                                    .<Flux<String>>body(cacheFlux))
                                            .orElseGet(() -> ResponseEntity.ok()
                                                    .header("X-Cache-Hit", "false")
                                                    .body(doStreamChat(request, userId, estimatedTokens, embedding)))));
                });
    }

    /**
     * 顶部 embedding：失败时 fail-open 返回 Optional.empty()（不阻断主链路）
     */
    private Mono<Optional<float[]>> embedForPipeline(String prompt) {
        if (prompt == null || prompt.isBlank()) {
            return Mono.just(Optional.empty());
        }
        return embeddingService.embed(prompt)
                .map(Optional::of)
                .onErrorResume(e -> {
                    log.warn("Embedding 失败，降级为规则分类 + 透传: {}", e.getMessage());
                    return Mono.just(Optional.empty());
                });
    }

    /**
     * 路由 → 主通道（带 TTFT 超时 + 熔断）→ fallback（必要时）→ 异步结算/落库
     *
     * 用 Flux.defer 包裹，确保每次订阅拿到独立的 buf / contentStarted / servedByFallback 状态。
     */
    private Flux<String> doStreamChat(ChatRequest request, String userId, int estimatedTokens,
                                      Optional<float[]> embedding) {
        return Flux.defer(() -> {
            StringBuilder responseBuffer = new StringBuilder();
            AtomicBoolean contentStarted = new AtomicBoolean(false);
            AtomicBoolean servedByFallback = new AtomicBoolean(false);

            return router.route(request, estimatedTokens, embedding.orElse(null))
                    .flatMapMany(route -> {
                        long slo = route.isThinking() ? props.getTtftSloThinkingMs() : props.getTtftSloMs();
                        // [🚧 核心难点] TTFT 超时赛跑（Q6 选 B）：
                        //   firstTimeout = Mono.delay(SLO) 订阅时启动
                        //   perChunkProvider：有 content/reasoning → Mono.never()（解除计时，首 token 到了）
                        //                     无 content（role 帧）→ Mono.delay(SLO)（重置计时继续等首 token）
                        //   超时抛 TimeoutException。role 帧透传，契约完整。
                        Flux<String> primaryWithTtft = callPrimary(route, request)
                                .timeout(Mono.delay(Duration.ofMillis(slo)),
                                        chunk -> hasContent(chunk)
                                                ? Mono.<Long>never()
                                                : Mono.delay(Duration.ofMillis(slo)));

                        // [💎 面试亮点] TTFT-as-failure（Q7 选 A）：
                        //   breaker.decorate 在内、onErrorResume(→fallback) 在外。
                        //   fallback 成功救用户，但不洗白 primary 的 failure 计数——窗口统计才准。
                        //   关掉 slow-call 判定（CircuitBreakerService 里配），TTFT 由上面的 timeout 接管。
                        return primaryWithTtft
                                .transform(circuitBreakerService.get(route.getModel()))
                                .onErrorResume(e -> {
                                    if (contentStarted.get()) {
                                        // 首内容已吐，中途错误不能切 fallback（避免半截 A + 半截 B）→ 错误帧
                                        log.warn("主通道中途错误（已吐内容，发错误帧不切 fallback）: {}", e.getMessage());
                                        return Flux.just("{\"error\": \"主通道中断: " + safeMsg(e) + "\"}");
                                    }
                                    // 首内容前失败（TTFT 超时 / breaker open / 早期错误）→ 切 fallback
                                    log.info("主通道失败，切 fallback: reason={}", e.getClass().getSimpleName());
                                    servedByFallback.set(true);
                                    return fallbackFlux(request);
                                })
                                .doOnNext(chunk -> {
                                    if (chunk == null || "[DONE]".equals(chunk.trim())) return;
                                    String text = extractContent(chunk);
                                    if (text != null && !text.isEmpty()) {
                                        contentStarted.set(true);
                                        responseBuffer.append(text);
                                    }
                                })
                                .doOnComplete(() -> {
                                    String fullResponse = responseBuffer.toString();
                                    // Token 结算（多退少补）
                                    int actualTokens = tokenCountService.countTokens(fullResponse);
                                    log.debug("Token 结算: userId={}, estimated={}, actual={}", userId, estimatedTokens, actualTokens);
                                    rateLimiterService.settle(userId, estimatedTokens, actualTokens)
                                            .subscribe(null, e -> log.error("Token 结算失败: {}", e.getMessage()));
                                    // 异步写缓存，复用顶部 embedding；fallback 服务的请求跳过（避免低质答案遮蔽主通道）
                                    if (!servedByFallback.get()) {
                                        semanticCacheService.saveToCache(embedding.orElse(null), request, fullResponse);
                                    } else {
                                        log.debug("fallback 服务，跳过缓存写入");
                                    }
                                });
                    });
        });
    }

    /**
     * 主通道调用：按 Route 的 targetUrl / apiKey / model 发 SSE 请求。
     * 下游按 body.model 路由，必须把 request.model 覆盖为 Route 的 model。
     * requestModel 优先于 model（解决 Ollama 等 config key ≠ API model tag 的场景）。
     *
     * 若路由判为思考模型（route.isThinking）且客户端未显式指定思考控制参数，
     * 自动注入 reasoning_effort="high" 和 thinking.type="enabled"。
     * 下游不认这些参数时会自动忽略，安全前向兼容。
     */
    private Flux<String> callPrimary(Route route, ChatRequest request) {
        request.setModel(route.getRequestModel() != null ? route.getRequestModel() : route.getModel());

        // [💎 面试亮点] 思考模型自动注入参数：isThinking=true 且客户端没设 →
        // 自动补 reasoning_effort="high" + thinking.type="enabled"。
        // 下游（如 OpenAI o-series）识别这些参数，DeepSeek 忽略——安全兼容。
        if (route.isThinking()) {
            if (request.getReasoningEffort() == null) {
                request.setReasoningEffort("high");
            }
            if (request.getThinking() == null) {
                ChatRequest.ThinkingConfig tc = new ChatRequest.ThinkingConfig();
                tc.setType("enabled");
                request.setThinking(tc);
            }
        }

        return webClient.post()
                .uri(route.getTargetUrl() + "/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + route.getApiKey())
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(request)
                .retrieve()
                .bodyToFlux(String.class);
    }

    /**
     * Fallback 通道（Q8）：Ollama OpenAI 兼容端点 qwen3:1.7b。
     * 不包 breaker（最后手段）；自带 TTFT 超时（复用非思考 SLO）；双重失败 → SSE 错误帧。
     */
    private Flux<String> fallbackFlux(ChatRequest request) {
        GatewayProperties.FallbackConfig fb = props.getFallback();
        request.setModel(fb.getRequestModel() != null ? fb.getRequestModel() : fb.getModel());
        long slo = props.getTtftSloMs();
        return webClient.post()
                .uri(fb.getTargetUrl() + "/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + fb.getApiKey())
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(request)
                .retrieve()
                .bodyToFlux(String.class)
                .timeout(Mono.delay(Duration.ofMillis(slo)),
                        chunk -> hasContent(chunk) ? Mono.<Long>never() : Mono.delay(Duration.ofMillis(slo)))
                // 双重失败 → 复用 SSE 错误帧契约
                .onErrorResume(e -> {
                    log.warn("fallback 也失败: {}", e.getMessage());
                    return Flux.just("{\"error\": \"主通道与备用通道均不可用: " + safeMsg(e) + "\"}");
                });
    }

    /**
     * TTFT 谓词（Q9）：delta.content **或** delta.reasoning_content 任一非空 → true。
     * 思考模型首 reasoning 帧也算"在吐字"，解除 TTFT 计时。
     * [💣 踩坑] 此谓词 ≠ extractContent（后者只读 content，用于缓存落库）。
     */
    private boolean hasContent(String chunk) {
        if (chunk == null || chunk.isBlank() || "[DONE]".equals(chunk.trim())) return false;
        try {
            JsonNode node = objectMapper.readTree(chunk);
            JsonNode choices = node.get("choices");
            if (choices == null || !choices.isArray() || choices.isEmpty()) return false;
            JsonNode delta = choices.get(0).get("delta");
            if (delta == null) return false;
            return nonEmpty(delta.get("content")) || nonEmpty(delta.get("reasoning_content"));
        } catch (Exception e) {
            return false;
        }
    }

    private boolean nonEmpty(JsonNode node) {
        return node != null && !node.asText("").isEmpty();
    }

    /**
     * 从下游 SSE chunk（裸 JSON payload）提取 delta.content 纯文本（缓存落库用）。
     * 只读 content，不读 reasoning_content——reasoning 非确定，不缓存（Q9）。
     * 跳过 [DONE]、无 content 的首帧 / reasoning 帧。
     */
    private String extractContent(String chunk) {
        try {
            JsonNode node = objectMapper.readTree(chunk);
            JsonNode choices = node.get("choices");
            if (choices == null || !choices.isArray() || choices.isEmpty()) return null;
            JsonNode delta = choices.get(0).get("delta");
            if (delta == null) return null;
            JsonNode content = delta.get("content");
            return content != null ? content.asText() : null;
        } catch (Exception e) {
            log.debug("chunk 解析失败，跳过: {}", chunk);
            return null;
        }
    }

    private String safeMsg(Throwable e) {
        String m = e.getMessage();
        return m == null ? e.getClass().getSimpleName() : m;
    }

    private String extractPrompt(ChatRequest request) {
        if (request.getMessages() == null || request.getMessages().isEmpty()) return null;
        return request.getMessages().stream()
                .filter(m -> "user".equals(m.getRole()))
                .reduce((first, second) -> second)
                .map(ChatRequest.Message::getContent)
                .orElse(null);
    }
}
