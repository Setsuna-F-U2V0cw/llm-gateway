package com.llmgateway.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * LLM 网关配置属性，对应 application.yml 中的 llm.gateway.* 前缀
 *
 * 阶段四：用「模型注册表 + 路由表」替换单 targetUrl，支持按 intent/长度多模型路由。
 */
@Data
@Component
@ConfigurationProperties(prefix = "llm.gateway")
public class GatewayProperties {

    // ==========================================
    // 阶段四：模型注册表（替换单 targetUrl / apiKey）
    // ==========================================

    /**
     * 模型注册表：modelId → 模型配置。路由与 fallback 都从这里取目标。
     */
    private Map<String, ModelConfig> models;

    /**
     * 路由表：intent 名（小写）→ modelId。Router 按意图分类结果查此表。
     * 必须含 "default" 键作为低置信 / 兜底路由。
     */
    private Map<String, String> routing;

    /**
     * Fallback 通道配置：主通道 TTFT 超时 / 熔断 / 下游错误时切到此处。
     * 指向本地 Ollama OpenAI 兼容端点（qwen3-1-7b），不包 breaker，双重失败发 SSE 错误帧。
     */
    private FallbackConfig fallback = new FallbackConfig();

    /**
     * TTFT（首 content/reasoning 帧）超时阈值，非思考模型（毫秒）。
     * 超过即 cancel 主通道 + 切 fallback，并记一次 failure 进熔断器滑动窗口。
     */
    private long ttftSloMs = 3000L;

    /**
     * TTFT 超时阈值，思考模型（毫秒）。思考模型首 reasoning 延迟天然更高，给宽一档。
     */
    private long ttftSloThinkingMs = 10000L;

    /**
     * "极长 prompt" 覆盖阈值（token 数）。prompt 超过所选 model 的 maxContext 且
     * 超过此阈值时，覆盖到注册表里 maxContext 最大的 model；为 0 表示禁用覆盖，超上限直接拒。
     */
    private int xlongThreshold = 32000;

    /**
     * Resilience4j CircuitBreaker 配置（按 model 粒度各一个 named breaker）。
     * TTFT 超时当作 failure（关掉 slow-call 判定，由我们的 timeout 接管）。
     */
    private CircuitBreakerProps cb = new CircuitBreakerProps();

    // ==========================================
    // WebClient 通用超时（主链路 + fallback 共用主 webClient bean）
    // ==========================================

    /** TCP 连接超时（毫秒） */
    private int connectTimeoutMs = 5000;

    /** 读取超时（毫秒），流式输出可能很长，需较大值 */
    private int readTimeoutMs = 120000;

    // ==========================================
    // TPM 令牌桶限流配置
    // ==========================================

    /** 令牌桶容量上限（每分钟最大 Token 数） */
    private int tpmCapacity = 100000;

    /** 每秒补充的 Token 数（= tpmCapacity / 60） */
    private int tpmRefillRatePerSecond = 1667;

    // ==========================================
    // 语义缓存配置
    // ==========================================

    private String qdrantHost = "localhost";
    private int qdrantPort = 6334;
    private String qdrantCollection = "llm_cache";
    private int embeddingDimension = 1024;
    private float semanticCacheThreshold = 0.95f;
    private long streamSimulateDelayMs = 20L;

    // ==========================================
    // Embedding 模型配置（Ollama 本地部署）
    // ==========================================

    private String ollamaBaseUrl = "http://localhost:11434";
    private String ollamaEmbedModel = "bge-m3";
    private int ollamaReadTimeoutMs = 30000;

    // ==========================================
    // 静态内部配置类
    // ==========================================

    /** 单个模型的配置（注册表条目） */
    @Data
    public static class ModelConfig {
        /** 下游 Base URL（OpenAI 兼容） */
        private String targetUrl;
        /** 下游 API Key */
        private String apiKey;
        /** 是否思考模型（影响 TTFT SLO 档位 + hasContent 谓词 + 前端渲染） */
        private boolean isThinking;
        /** 上下文上限（token 数），LengthGuard 据此拒绝超长 prompt */
        private int maxContext = 32000;
        /**
         * 发送给下游 API 的实际 model name。
         * 为 null 时使用 config key（modelId），适用于 DeepSeek 等 key=name 的场景。
         * 本地模型（如 Ollama qwen3:1.7b）的 API model tag 含冒号，config key 不能用冒号，
         * 因此需要单独指定。
         */
        private String requestModel;
    }

    /** Fallback 通道配置 */
    @Data
    public static class FallbackConfig {
        private String targetUrl = "http://localhost:11434";
        /** 模型注册表 key（用于查找路由/熔断器） */
        private String model = "qwen3-1-7b";
        /** 发送给 API 的实际 model name；为 null 时用 model */
        private String requestModel = "qwen3:1.7b";
        private String apiKey = "ollama";
    }

    /** Resilience4j CircuitBreaker 滑动窗口参数 */
    @Data
    public static class CircuitBreakerProps {
        private int slidingWindowSize = 100;
        private int minimumNumberOfCalls = 20;
        private float failureRateThreshold = 50f;
        private long waitDurationInOpenStateMs = 60000L;
        private int permittedHalfOpenCalls = 10;
    }
}
