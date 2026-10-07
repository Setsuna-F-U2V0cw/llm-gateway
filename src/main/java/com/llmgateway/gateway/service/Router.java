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
 * 缓存未命中时介入。客户端显式传入已注册 model 则跳过意图分类；否则按意图 + 长度路由。
 * 两条路径都走 LengthGuard。
 *
 * [💎 面试亮点] 意图复用缓存检索的 embedding，token 长度复用限流的 JTokkit 计数；下拉选中的 model 与自动路由互不打架。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class Router {

    private final IntentClassifier intentClassifier;
    private final GatewayProperties props;

    /**
     * 路由决策。
     * <ul>
     *   <li>客户端 {@code model} 命中注册表（config key 或 requestModel）→ 显式选用，跳过意图分类</li>
     *   <li>{@code model} 为空 / {@code auto} / 未注册 → 意图分类 + 路由表</li>
     *   <li>两种路径都走 LengthGuard</li>
     * </ul>
     *
     * @param tokenCount 本次 prompt 的 <strong>input</strong> token（LengthGuard / xlong），
     *                   不是 TPM 预估总量 {@code input + 1.5×input}
     */
    public Mono<Route> route(ChatRequest request, int tokenCount, float[] embedding) {
        String prompt = extractPrompt(request);
        String explicitId = resolveExplicitModelId(request.getModel());
        String modelId;
        if (explicitId != null) {
            modelId = explicitId;
            log.info("路由决策: explicitModel={}, tokenCount={}", modelId, tokenCount);
        } else {
            Intent intent = intentClassifier.classify(embedding, prompt);
            modelId = resolveModelId(intent);
            log.info("路由决策: intent={}, model={}, tokenCount={}", intent, modelId, tokenCount);
        }

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

    /**
     * 客户端显式选了已注册 model 则返回 config key；auto / 空 / 未注册返回 null（走意图）。
     */
    String resolveExplicitModelId(String requested) {
        if (requested == null || requested.isBlank() || "auto".equalsIgnoreCase(requested)) {
            return null;
        }
        Map<String, GatewayProperties.ModelConfig> models = props.getModels();
        if (models == null || models.isEmpty()) {
            return null;
        }
        if (models.containsKey(requested)) {
            return requested;
        }
        for (Map.Entry<String, GatewayProperties.ModelConfig> e : models.entrySet()) {
            String tag = e.getValue().getRequestModel();
            if (tag != null && tag.equals(requested)) {
                return e.getKey();
            }
        }
        return null;
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
