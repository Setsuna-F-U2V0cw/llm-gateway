package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Embedding 服务：将文本转换为高维向量，供 Qdrant 语义缓存检索。
 *
 * 实现方式：调用本地 Ollama 的 {@code POST /api/embed} 接口（模型 {@code bge-m3}，1024 维）。
 *
 * [💎 面试亮点] 接口设计为 {@code Mono<float[]>}——从 Mock 随机向量替换为真实 HTTP 调用时，
 * 调用方（SemanticCacheService）的响应式链路完全不需要改动，体现了响应式编程的接口稳定性。
 */
@Slf4j
@Service
public class EmbeddingService {

    private final GatewayProperties props;
    private final WebClient ollamaWebClient;

    // [💣 踩坑预警] 仓库内现在有两个 WebClient bean（主链路 webClient + ollamaWebClient），
    // 必须用 @Qualifier 显式指定。@RequiredArgsConstructor 不会把字段上的 @Qualifier 复制到
    // 构造参数（除非配 lombok.copyableAnnotations），会导致 qualifier 失效、启动报歧义 bean 异常。
    // 因此这里手写构造函数，在参数上标 @Qualifier，不用 @RequiredArgsConstructor。
    public EmbeddingService(GatewayProperties props,
                            @Qualifier("ollamaWebClient") WebClient ollamaWebClient) {
        this.props = props;
        this.ollamaWebClient = ollamaWebClient;
    }

    /**
     * 将输入文本转换为 Embedding 向量。
     *
     * WebClient 调用本身是非阻塞的，全部在 Netty EventLoop 上执行，
     * 无需（也不应）用 Schedulers.boundedElastic 切线程。
     *
     * @param text 需要向量化的文本（通常是用户 Prompt）
     * @return Mono<float[]> 归一化后的 Embedding 向量
     */
    public Mono<float[]> embed(String text) {
        OllamaEmbedRequest req = new OllamaEmbedRequest(props.getOllamaEmbedModel(), text);
        return ollamaWebClient.post()
                .uri(props.getOllamaBaseUrl() + "/api/embed")
                .bodyValue(req)
                .retrieve()
                .bodyToMono(OllamaEmbedResponse.class)
                .map(resp -> toFloatArray(resp.embeddings().get(0)))
                .map(this::l2Normalize)
                .doOnNext(vec -> log.debug("Embedding 生成: textLen={}, dim={}", text.length(), vec.length))
                // 只记日志后向上抛错：LlmProxyService.embedForPipeline 外层
                // onErrorResume → Optional.empty() 做 fail-open（降级为规则分类 + 透传），
                // 本层不吞错，保持错误链可观测。
                .onErrorResume(e -> {
                    log.warn("Ollama Embedding 调用失败，缓存将降级为未命中透传: {}", e.getMessage());
                    return Mono.error(e);
                });
    }

    /** Ollama /api/embed 请求体 */
    private record OllamaEmbedRequest(String model, String input) {}

    /** Ollama /api/embed 响应体：embeddings 为批量结果，取第一条 */
    private record OllamaEmbedResponse(List<List<Double>> embeddings) {}

    /** List<Double> → float[] */
    private float[] toFloatArray(List<Double> list) {
        float[] arr = new float[list.size()];
        for (int i = 0; i < list.size(); i++) {
            arr[i] = list.get(i).floatValue();
        }
        return arr;
    }

    /**
     * L2 归一化，使向量长度为 1，余弦相似度计算更准确。
     * BGE 模型本身已归一化，这里做防御性归一化，保证即使模型行为变化也安全。
     */
    private float[] l2Normalize(float[] vector) {
        float norm = 0f;
        for (float v : vector) {
            norm += v * v;
        }
        norm = (float) Math.sqrt(norm);
        if (norm == 0f) {
            return vector;
        }
        for (int i = 0; i < vector.length; i++) {
            vector[i] /= norm;
        }
        return vector;
    }
}
