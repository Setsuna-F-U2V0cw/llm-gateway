package com.llmgateway.gateway.service;

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingType;
import com.llmgateway.gateway.model.ChatRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 本地 Token 计算服务（基于 JTokkit）
 *
 * [💎 面试亮点] 使用 JTokkit 在网关层做本地 Token 计算，无需调用远程接口，
 * 计算延迟 < 1ms，在限流判断的热路径上几乎无开销。
 * 相比调用 /v1/tokenize 接口，节省了一次网络 RTT。
 */
@Slf4j
@Service
public class TokenCountService {

    // cl100k_base 编码适用于 GPT-4 / DeepSeek 等主流模型
    private final Encoding encoding;

    // 输出 Token 预估倍数（input * 1.5 作为保守预估）
    private static final double OUTPUT_MULTIPLIER = 1.5;
    // 每条消息的固定开销（role/name 等元数据）
    private static final int TOKENS_PER_MESSAGE = 4;

    public TokenCountService() {
        EncodingRegistry registry = Encodings.newDefaultEncodingRegistry();
        // [💎 面试亮点] cl100k_base 是当前主流大模型（GPT-4、DeepSeek）通用的 BPE 编码
        this.encoding = registry.getEncoding(EncodingType.CL100K_BASE);
    }

    /**
     * 计算请求的 Input Token 数
     */
    public int countInputTokens(ChatRequest request) {
        if (request.getMessages() == null || request.getMessages().isEmpty()) {
            return 0;
        }
        int total = 0;
        for (ChatRequest.Message msg : request.getMessages()) {
            total += TOKENS_PER_MESSAGE;
            if (msg.getContent() != null) {
                total += encoding.countTokens(msg.getContent());
            }
            if (msg.getRole() != null) {
                total += encoding.countTokens(msg.getRole());
            }
        }
        // 每次对话的固定前缀开销
        total += 3;
        return total;
    }

    /**
     * 预估本次请求总 Token 消耗（input + 预估 output），用于限流预扣
     *
     * [🚧 核心难点] 响应前无法知道真实 output token，
     * 采用 input * 1.5 保守预估，响应完成后再做多退少补结算。
     */
    public int estimateTotalTokens(ChatRequest request) {
        int inputTokens = countInputTokens(request);
        int estimatedOutput = (int) (inputTokens * OUTPUT_MULTIPLIER);

        // 如果请求指定了 max_tokens，取两者较小值作为 output 上限
        if (request.getMaxTokens() != null) {
            estimatedOutput = Math.min(estimatedOutput, request.getMaxTokens());
        }

        int total = inputTokens + estimatedOutput;
        log.debug("Token 预估: inputTokens={}, estimatedOutput={}, total={}", inputTokens, estimatedOutput, total);
        return total;
    }

    /**
     * 计算单条文本的 Token 数（用于响应完成后的真实结算）
     */
    public int countTokens(String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        return encoding.countTokens(text);
    }
}
