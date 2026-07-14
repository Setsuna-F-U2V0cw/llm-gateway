package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ChatRequest;
import com.llmgateway.gateway.model.Intent;
import com.llmgateway.gateway.model.Route;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * 策略模式路由器（阶段四）
 *
 * 缓存未命中时介入，按「意图 + token 长度」决定打哪个下游 model。
 *
 * 组合方式（Q12 选 C）：intent 主导查路由表 → LengthGuard 守卫/覆盖。
 *  - IntentClassifier 产出 intent（embedding 复用，可换规则实现）→ 查 routing 表得 modelId
 *  - LengthGuard：超所选 model 的 maxContext 时，按 xlongThreshold 覆盖到 big-context model，或直接 413 拒
 *
 * 输出 Route 对象（Q10 选 10b）：封装 targetUrl / model / apiKey / isThinking / maxContext。
 *
 * [💎 面试亮点] 两个路由维度全部零额外计算——意图复用缓存检索的 embedding，token 长度复用限流的 JTokkit 计数。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class Router {

    private final IntentClassifier intentClassifier;
    private final GatewayProperties props;

    /**
     * 路由决策
     *
     * @param request    原始请求
     * @param tokenCount 已算好的 input token 数（复用限流阶段的 JTokkit 结果）
     * @param embedding  prompt 向量（复用缓存检索阶段；为 null 时走规则分类）
     * @return Mono<Route> 目标下游全部信息；超上下文上限时 emit 413 错误
     */
    public Mono<Route> route(ChatRequest request, int tokenCount, float[] embedding) {
        String prompt = extractPrompt(request);

        Intent intent = (embedding != null)
                ? intentClassifier.classify(embedding)
                : intentClassifier.classifyByRules(prompt);

        String modelId = resolveModelId(intent);
        log.info("路由决策: intent={}, model={}, tokenCount={}", intent, modelId, tokenCount);

        // LengthGuard：超所选 model 的上下文上限
        GatewayProperties.ModelConfig chosen = props.getModels().get(modelId);
        if (chosen == null) {
            return Mono.error(new ResponseStatusException(
                    HttpStatus.INTERNAL_SERVER_ERROR, "路由表指向未配置的 model: " + modelId));
        }
        if (tokenCount > chosen.getMaxContext()) {
            // 极长覆盖：xlongThreshold 配置且 token 超该阈值 → 覆盖到注册表里 maxContext 最大的 model
            if (props.getXlongThreshold() > 0 && tokenCount > props.getXlongThreshold()) {
                String bigModel = findBigContextModel(modelId);
                GatewayProperties.ModelConfig bigCfg = bigModel == null ? null : props.getModels().get(bigModel);
                // big model 必须存在、与原选不同、且真能容下 tokenCount，否则仍 413
                if (bigCfg != null && !bigModel.equals(modelId) && tokenCount <= bigCfg.getMaxContext()) {
                    log.info("LengthGuard 覆盖: {} → {} (tokenCount={} 超上限)", modelId, bigModel, tokenCount);
                    modelId = bigModel;
                    chosen = bigCfg;
                } else {
                    return Mono.error(new ResponseStatusException(
                            HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                            "prompt 超出所有 model 上下文上限: tokens=" + tokenCount));
                }
            } else {
                return Mono.error(new ResponseStatusException(
                        HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE,
                        "prompt 超出 model 上下文上限: tokens=" + tokenCount + ", max=" + chosen.getMaxContext()));
            }
        }

        return Mono.just(new Route(
                chosen.getTargetUrl(),
                modelId,
                chosen.getApiKey(),
                chosen.isThinking(),
                chosen.getMaxContext(),
                chosen.getRequestModel()
        ));
    }

    /** 查路由表，intent 名小写作 key；未命中走 default；default 也缺则取注册表第一个 */
    private String resolveModelId(Intent intent) {
        Map<String, String> routing = props.getRouting();
        String key = intent.name().toLowerCase();
        String modelId = routing.get(key);
        if (modelId == null) modelId = routing.get("default");
        if (modelId == null && props.getModels() != null && !props.getModels().isEmpty()) {
            modelId = props.getModels().keySet().iterator().next();
        }
        return modelId;
    }

    /** 找注册表里 maxContext 最大的 model（极长覆盖用） */
    private String findBigContextModel(String exclude) {
        String best = null;
        int bestCtx = 0;
        for (Map.Entry<String, GatewayProperties.ModelConfig> e : props.getModels().entrySet()) {
            if (e.getKey().equals(exclude)) continue;
            if (e.getValue().getMaxContext() > bestCtx) {
                bestCtx = e.getValue().getMaxContext();
                best = e.getKey();
            }
        }
        return best;
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
