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
import reactor.core.publisher.SignalType;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
 *                       → doFinally：Token 结算（complete/cancel/error 都结）
 *                       → doOnComplete：异步写缓存（复用顶部 embedding；fallback 跳过）
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
    private final GatewayMetrics gatewayMetrics;
    private final ObjectMapper objectMapper;

    /**
     * 流式转发请求：限流 → embed（顶部一次）→ 语义缓存 → 路由 + LLM 转发
     *
     * 返回 Mono<ResponseEntity<Flux<String>>> 而非裸 Flux<String>，是为了在缓存命中/未命中
     * 决出后写入 X-Cache-Hit 响应头（前端 onopen 即可读到）。
     */
    public Mono<ResponseEntity<Flux<String>>> streamChat(ChatRequest request, String userId) {
        return Mono.defer(() -> {
            request.setStream(true);
            int inputTokens = tokenCountService.countInputTokens(request);
            int estimatedTokens = tokenCountService.estimateTotalTokens(request);
            // 路由会改写 request.model，缓存键必须用客户端原始 model，否则读写对不上。
            String cacheModel = request.getModel();
            GatewayMetrics.StreamTracker tracker = gatewayMetrics.startStream();
            log.debug("请求转发: userId={}, model={}, inputTokens={}, estimatedTokens={}",
                    userId, cacheModel, inputTokens, estimatedTokens);

            return rateLimiterService.tryAcquire(userId, estimatedTokens)
                    .flatMap(allowed -> {
                        gatewayMetrics.recordRateLimit(allowed);
                        if (!allowed) {
                            return Mono.<ResponseEntity<Flux<String>>>error(new ResponseStatusException(
                                    HttpStatus.TOO_MANY_REQUESTS, "TPM 超限，请稍后重试"));
                        }
                        AtomicBoolean settlementDone = new AtomicBoolean(false);
                        // [💎 面试亮点] embed 顶部算一次，三处复用：缓存检索、意图分类、写缓存。
                        // fail-open：embedding 不可用时返回 Optional.empty()，下游走规则分类 + 透传。
                        String prompt = extractPrompt(request);
                        return embedForPipeline(prompt)
                                .flatMap(embedding -> semanticCacheService.searchCache(
                                                embedding.orElse(null), cacheModel, userId)
                                        .flatMap(opt -> {
                                            gatewayMetrics.recordCacheLookup(opt.isPresent());
                                            if (opt.isPresent()) {
                                                return Mono.just(ResponseEntity.ok()
                                                        .header("X-Cache-Hit", "true")
                                                        .<Flux<String>>body(attachSettlement(
                                                                opt.get().stream(), userId, request,
                                                                estimatedTokens, settlementDone,
                                                                tracker, cacheModel)));
                                            }
                                            // [💣 踩坑] LengthGuard 必须在写 200 之前完成。
                                            // 若 route() 放进 body Flux，WebFlux 已发出 200 + text/event-stream，
                                            // 413 只能变成流上的 onError，客户端看不到 HTTP 413。
                                            return router.route(request, inputTokens, embedding.orElse(null))
                                                    .map(route -> ResponseEntity.ok()
                                                            .header("X-Cache-Hit", "false")
                                                            .<Flux<String>>body(doStreamChat(
                                                                    route, request, userId, estimatedTokens,
                                                                    embedding, cacheModel, settlementDone,
                                                                    tracker)));
                                        }))
                                // 响应体尚未交给 WebFlux 前就取消/报错时，body 的 doFinally 不会执行。
                                .doFinally(sig -> {
                                    if (sig != SignalType.ON_COMPLETE) {
                                        settleOnce(settlementDone, userId, request, estimatedTokens, "");
                                    }
                                });
                    })
                    .doFinally(sig -> {
                        if (sig != SignalType.ON_COMPLETE) {
                            tracker.finish(cacheModel, "pre_route", outcome(sig, false));
                        }
                    });
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
                .timeout(Duration.ofMillis(props.getEmbedPipelineTimeoutMs()))
                .onErrorResume(e -> {
                    log.warn("Embedding 失败/超时，降级为规则分类 + 透传: {}", e.toString());
                    return Mono.just(Optional.empty());
                });
    }

    /**
     * 主通道（带 TTFT 超时 + 熔断）→ fallback（必要时）→ 异步结算/落库。
     * Route 已在写 200 之前由 {@code router.route()} 决出（含 LengthGuard）。
     *
     * 用 Flux.defer 包裹，确保每次订阅拿到独立的 buf / contentStarted / servedByFallback 状态。
     */
    private Flux<String> doStreamChat(Route route, ChatRequest request, String userId, int estimatedTokens,
                                      Optional<float[]> embedding, String cacheModel,
                                      AtomicBoolean settlementDone,
                                      GatewayMetrics.StreamTracker tracker) {
        return Flux.defer(() -> {
            StringBuilder responseBuffer = new StringBuilder();
            AtomicBoolean contentStarted = new AtomicBoolean(false);
            AtomicBoolean servedByFallback = new AtomicBoolean(false);
            AtomicBoolean terminalError = new AtomicBoolean(false);
            AtomicReference<String> observedModel = new AtomicReference<>(route.getModel());
            AtomicReference<String> observedChannel = new AtomicReference<>("primary");

            long slo = route.isThinking() ? props.getTtftSloThinkingMs() : props.getTtftSloMs();
            // [🚧 核心难点] TTFT 超时赛跑（Q6 选 B）：
            //   firstTimeout = Mono.delay(SLO) 订阅时启动
            //   perChunkProvider：有 content/reasoning → Mono.never()（解除计时，首 token 到了）
            //                     无 content（role 帧）→ Mono.delay(SLO)（重置计时继续等首 token）
            //   超时抛 TimeoutException。role 帧透传，契约完整。
            Flux<String> primaryWithTtft = callPrimary(route, request)
                    .doOnNext(chunk -> {
                        if (hasContent(chunk)) {
                            tracker.firstToken(route.getModel(), "primary");
                        }
                    })
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
                            terminalError.set(true);
                            return Flux.just(errorFrame("主通道中断: " + safeMsg(e)));
                        }
                        // 首内容前失败（TTFT 超时 / breaker open / 早期错误）→ 切 fallback
                        log.info("主通道失败，切 fallback: reason={}", e.getClass().getSimpleName());
                        servedByFallback.set(true);
                        observedChannel.set("fallback");
                        gatewayMetrics.recordFallbackAttempt(e);
                        return fallbackFlux(route, request, tracker, terminalError);
                    })
                    .doOnNext(chunk -> {
                        if (chunk == null || "[DONE]".equals(chunk.trim())) {
                            return;
                        }
                        // 与 TTFT 同一谓词：reasoning 也算「已吐内容」，禁止再切 fallback
                        if (hasContent(chunk)) {
                            contentStarted.set(true);
                        }
                        String text = extractContent(chunk);
                        if (text != null && !text.isEmpty()) {
                            responseBuffer.append(text);
                        }
                    })
                    .doOnComplete(() -> {
                        // 只在正常完成时写缓存；fallback / 错误帧转 complete 都不落库。
                        if (!servedByFallback.get() && !terminalError.get()) {
                            semanticCacheService.saveToCache(
                                    embedding.orElse(null), request, responseBuffer.toString(),
                                    userId, cacheModel);
                        } else {
                            log.debug("跳过缓存写入: fallback={}, terminalError={}",
                                    servedByFallback.get(), terminalError.get());
                        }
                    })
                    // [💎 面试亮点] doFinally 覆盖 complete / cancel / error。
                    // LengthGuard 413 在写头之前抛，由 streamChat 外层 doFinally 结算。
                    // 取消生成不再把预扣挂到 120s EXPIRE；actual = input + 已产出 output。
                    .doFinally(sig -> {
                        log.debug("Token 结算信号: signal={}, userId={}", sig, userId);
                        settleOnce(settlementDone, userId, request,
                                estimatedTokens, responseBuffer.toString());
                        tracker.finish(
                                observedModel.get(),
                                observedChannel.get(),
                                outcome(sig, terminalError.get()));
                    });
        });
    }

    Flux<String> attachSettlement(Flux<String> stream, String userId, ChatRequest request,
                                  int estimatedTokens, AtomicBoolean settlementDone,
                                  GatewayMetrics.StreamTracker tracker, String cacheModel) {
        return Flux.defer(() -> {
            StringBuilder emittedContent = new StringBuilder();
            return stream
                    .doOnNext(chunk -> {
                        String text = extractContent(chunk);
                        if (text != null && !text.isEmpty()) {
                            tracker.firstToken(cacheModel, "cache");
                            emittedContent.append(text);
                        }
                    })
                    .doFinally(sig -> {
                        settleOnce(settlementDone, userId, request,
                                estimatedTokens, emittedContent.toString());
                        tracker.finish(cacheModel, "cache", outcome(sig, false));
                    });
        });
    }

    private void settleOnce(AtomicBoolean settlementDone, String userId, ChatRequest request,
                            int estimatedTokens, String outputText) {
        if (!settlementDone.compareAndSet(false, true)) {
            return;
        }
        int actualTokens = tokenCountService.actualTotalTokens(request, outputText);
        gatewayMetrics.recordTokenSettlement(estimatedTokens, actualTokens);
        log.debug("Token 结算: userId={}, estimated={}, actual={}", userId, estimatedTokens, actualTokens);
        rateLimiterService.settle(userId, estimatedTokens, actualTokens)
                .subscribe(null, e -> log.error("Token 结算失败: {}", e.getMessage()));
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
     * 差异化 fallback 通道。不包 breaker（最后手段）；若目标与本次主通道完全相同则拒绝伪 fallback。
     */
    Flux<String> fallbackFlux(Route primaryRoute, ChatRequest request,
                              GatewayMetrics.StreamTracker tracker,
                              AtomicBoolean terminalError) {
        GatewayProperties.FallbackConfig fb = props.getFallback();
        String fallbackModel = fb.getRequestModel() != null ? fb.getRequestModel() : fb.getModel();
        if (sameEndpoint(primaryRoute.getTargetUrl(), fb.getTargetUrl())) {
            terminalError.set(true);
            gatewayMetrics.recordFallbackResult("misconfigured");
            log.error("拒绝 fallback：主通道与备用通道目标完全相同: target={}, model={}",
                    fb.getTargetUrl(), fallbackModel);
            return Flux.just(errorFrame("fallback 配置与主通道相同，已拒绝重复调用"));
        }

        request.setModel(fallbackModel);
        long slo = props.getTtftSloMs();
        return webClient.post()
                .uri(fb.getTargetUrl() + "/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + fb.getApiKey())
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(request)
                .retrieve()
                .bodyToFlux(String.class)
                .doOnNext(chunk -> {
                    if (hasContent(chunk)) {
                        tracker.firstToken(primaryRoute.getModel(), "fallback");
                    }
                })
                .timeout(Mono.delay(Duration.ofMillis(slo)),
                        chunk -> hasContent(chunk) ? Mono.<Long>never() : Mono.delay(Duration.ofMillis(slo)))
                .doOnComplete(() -> gatewayMetrics.recordFallbackResult("success"))
                .doOnCancel(() -> gatewayMetrics.recordFallbackResult("cancelled"))
                // 双重失败 → 复用 SSE 错误帧契约
                .onErrorResume(e -> {
                    terminalError.set(true);
                    gatewayMetrics.recordFallbackResult("failure");
                    log.warn("fallback 也失败: {}", e.getMessage());
                    return Flux.just(errorFrame("主通道与备用通道均不可用: " + safeMsg(e)));
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

    /**
     * SSE 错误帧：用 ObjectMapper 序列化，避免异常消息里的引号/换行破坏 JSON。
     */
    String errorFrame(String message) {
        String msg = (message == null || message.isBlank()) ? "internal error" : message;
        try {
            return objectMapper.writeValueAsString(Map.of("error", msg));
        } catch (Exception e) {
            return "{\"error\":\"internal error\"}";
        }
    }

    private String safeMsg(Throwable e) {
        String m = e.getMessage();
        return m == null ? e.getClass().getSimpleName() : m;
    }

    private String outcome(SignalType signal, boolean terminalError) {
        if (terminalError || signal == SignalType.ON_ERROR) {
            return "error";
        }
        if (signal == SignalType.CANCEL) {
            return "cancelled";
        }
        return "completed";
    }

    static boolean sameEndpoint(String primaryUrl, String fallbackUrl) {
        return normalizeUrl(primaryUrl).equalsIgnoreCase(normalizeUrl(fallbackUrl));
    }

    private static String normalizeUrl(String value) {
        String normalized = normalizeValue(value);
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static String normalizeValue(String value) {
        return value == null ? "" : value.trim();
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
