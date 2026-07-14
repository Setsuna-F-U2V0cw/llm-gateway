package com.llmgateway.gateway.model;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 路由决策结果（阶段四 Router 输出）
 *
 * 封装一次请求要打到的下游目标全部信息：
 * - targetUrl / model / apiKey：WebClient 调用参数
 * - isThinking：决定 TTFT SLO 档位（思考模型用 ttftSloThinkingMs）+ hasContent 谓词（含 reasoning_content）
 * - maxContext：LengthGuard 守卫依据
 *
 * breakerName = model（按 model 粒度 named CircuitBreaker）。
 */
@Data
@AllArgsConstructor
public class Route {
    private String targetUrl;
    /** 模型注册表 key，用作熔断器名和路由查找 */
    private String model;
    private String apiKey;
    private boolean isThinking;
    private int maxContext;
    /**
     * 发送给下游 API 的实际 model name。
     * 为 null 时使用 model（config key），适用于 key=name 的场景。
     */
    private String requestModel;

    /** CircuitBreaker 按 model 粒度，breaker 名直接用 model id */
    public String getBreakerName() {
        return model;
    }
}
