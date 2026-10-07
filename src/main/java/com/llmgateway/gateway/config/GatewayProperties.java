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

    /** Fallback 通道配置：必须与主通道使用不同目标实例或提供商。 */
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

    /**
     * 令牌桶 Redis key TTL（秒）。0 表示按 {@code max(120, readTimeoutMs/1000 + 60)} 自动计算，
     * 保证长 SSE 结束前 key 仍在，settle 能补账。
     */
    private int tpmBucketTtlSeconds = 0;

    // ==========================================
    // WebClient 连接池（主链路 SSE vs Embedding 隔离）
    // ==========================================

    /** 主链路（LLM SSE）出站连接池上限 */
    private int webClientMaxConnections = 4000;

    /** 主链路 pending acquire 上限 */
    private int webClientPendingAcquireMaxCount = 8000;

    /** Embedding / 健康检查用的独立小池 */
    private int ollamaWebClientMaxConnections = 200;

    /**
     * 解析实际写入 Redis EXPIRE 的 TTL。显式配置优先；否则大于读超时，避免流未结束 key 先过期。
     */
    public int resolveTpmBucketTtlSeconds() {
        if (tpmBucketTtlSeconds > 0) {
            return tpmBucketTtlSeconds;
        }
        return Math.max(120, readTimeoutMs / 1000 + 60);
    }

    // ==========================================
    // 语义缓存配置
    // ==========================================

    private String qdrantHost = "localhost";
    private int qdrantPort = 6334;
    private String qdrantCollection = "llm_cache";
    private int embeddingDimension = 1024;
    private float semanticCacheThreshold = 0.95f;
    private long streamSimulateDelayMs = 20L;
    /**
     * 语义缓存有效期（秒）。检索 must-filter {@code created_at >= now - ttl}。
     * 0 关闭过期过滤（仍写入 created_at，便于日后打开 TTL）。
     */
    private int semanticCacheTtlSeconds = 86400;

    // ==========================================
    // Embedding 模型配置（Ollama 本地部署）
    // ==========================================

    private String ollamaBaseUrl = "http://localhost:11434";
    private String ollamaEmbedModel = "bge-m3";
    private int ollamaReadTimeoutMs = 30000;
    /**
     * 主链路顶部 embedding 超时（毫秒）。超时与 HTTP 错误一律 fail-open（规则分类 + 透传）。
     * EmbeddingService 本身仍向上抛错，由 {@code embedForPipeline} 统一吞掉。
     */
    private long embedPipelineTimeoutMs = 3000L;
    private PrototypeRetryConfig prototypeRetry = new PrototypeRetryConfig();

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
        /** 默认指向第二个独立 Ollama 实例；生产建议通过环境变量改为不同提供商。 */
        private String targetUrl = "http://localhost:11435";
        /** 模型注册表 key（用于查找路由/熔断器） */
        private String model = "qwen3-1-7b";
        /** 发送给 API 的实际 model name；为 null 时用 model */
        private String requestModel = "qwen3:1.7b";
        private String apiKey = "ollama";
    }

    /** 意图原型后台加载重试参数 */
    @Data
    public static class PrototypeRetryConfig {
        private long initialDelayMs = 1000L;
        private long maxDelayMs = 30000L;
        private double jitter = 0.2;
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
