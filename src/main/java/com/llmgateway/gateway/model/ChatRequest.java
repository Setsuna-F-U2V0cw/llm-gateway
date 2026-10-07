package com.llmgateway.gateway.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

/**
 * OpenAI 标准格式的聊天请求体
 * 参考：https://platform.openai.com/docs/api-reference/chat/create
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChatRequest {

    /** 模型 ID，如 deepseek-v4-flash、deepseek-v4-flash-thinking；空 / auto 走意图路由 */
    private String model;

    /** 对话消息列表 */
    private List<Message> messages;

    /** 是否开启流式输出，网关层强制为 true 以支持 SSE */
    private Boolean stream;

    /** 最大生成 Token 数 */
    @JsonProperty("max_tokens")
    private Integer maxTokens;

    /** 采样温度 0~2 */
    private Double temperature;

    // ==========================================
    // 思考模式控制（OpenAI o-series 兼容）
    // ==========================================

    /**
     * 思考模式开关：{"type": "enabled"} 或 {"type": "disabled"}
     * 控制模型是否输出推理过程（reasoning_content）。
     * - enabled：强制开启思考（适用于默认不思考的模型）
     * - disabled：强制关闭思考（适用于默认思考的模型）
     * 为 null 时使用模型本身默认行为。
     */
    private ThinkingConfig thinking;

    /**
     * 思考强度（OpenAI reasoning_effort 参数）：
     * "low" / "medium" / "high" / "max"
     * 控制模型在推理上花费的 token 量。
     * 为 null 时使用模型本身默认值。
     */
    @JsonProperty("reasoning_effort")
    private String reasoningEffort;

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Message {
        /** 角色：system / user / assistant */
        private String role;

        /** 消息内容 */
        private String content;
    }

    @Data
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class ThinkingConfig {
        /** "enabled" 或 "disabled" */
        private String type;
    }
}
