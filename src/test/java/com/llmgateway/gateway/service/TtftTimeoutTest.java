package com.llmgateway.gateway.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;

/**
 * TTFT 超时赛跑机制单元测试（阶段四 Q6）
 *
 * 验证 {@code Flux.timeout(Mono.delay(SLO), perChunkProvider)} 的关键行为：
 *  - role 帧（无 content）到达 → 重置计时，继续等首 token
 *  - content 帧到达 → 解除计时（首 token 到了）
 *  - SLO 内无 content → 抛 TimeoutException（触发上层切 fallback）
 *
 * 用 StepVerifier.withVirtualTime 控制时钟，避免真实等待。
 */
class TtftTimeoutTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String ROLE_FRAME =
            "{\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"\"}}]}";
    private static final String CONTENT_FRAME =
            "{\"choices\":[{\"delta\":{\"content\":\"好\"}}]}";
    private static final String REASONING_FRAME =
            "{\"choices\":[{\"delta\":{\"reasoning_content\":\"思考中\"}}]}";

    /** 复刻 LlmProxyService.hasContent：content 或 reasoning_content 非空 */
    private boolean hasContent(String chunk) {
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

    private boolean nonEmpty(JsonNode n) {
        return n != null && !n.asText("").isEmpty();
    }

    /** 复刻 LlmProxyService 的 TTFT 超时封装 */
    private Flux<String> withTtft(Flux<String> source, long sloMs) {
        return source.timeout(
                Mono.delay(Duration.ofMillis(sloMs)),
                chunk -> hasContent(chunk) ? Mono.<Long>never() : Mono.delay(Duration.ofMillis(sloMs))
        );
    }

    @Test
    void roleFrameResetsTimer_thenContentDisables_noTimeout() {
        // SLO=1000ms；role 帧在 200ms（重置计时到 1200ms），content 帧在 500ms（解除计时）
        Flux<String> source = Flux.concat(
                Mono.delay(Duration.ofMillis(200)).map(x -> ROLE_FRAME),
                Mono.delay(Duration.ofMillis(300)).map(x -> CONTENT_FRAME)
        );

        StepVerifier.withVirtualTime(() -> withTtft(source, 1000))
                .expectNext(ROLE_FRAME)
                .expectNext(CONTENT_FRAME)
                .verifyComplete();
    }

    @Test
    void onlyRoleFrame_noContent_timesOut() {
        // role 帧在 200ms（重置计时到 1200ms），发完后连接挂住（Flux.never 模拟不完成、无 content）
        // → 1200ms 抛 TimeoutException。现实里 DeepSeek 发完 role 帧会保持连接等 content，不会 onComplete。
        Flux<String> source = Flux.concat(
                Mono.delay(Duration.ofMillis(200)).map(x -> ROLE_FRAME),
                Flux.<String>never()
        );

        StepVerifier.withVirtualTime(() -> withTtft(source, 1000))
                .expectNext(ROLE_FRAME)
                .thenAwait(Duration.ofMillis(1200))
                .verifyError(TimeoutException.class);
    }

    @Test
    void noFrameAtAll_timesOutAtSlo() {
        // 无任何帧 → SLO(1000ms) 抛 TimeoutException
        StepVerifier.withVirtualTime(() -> withTtft(Flux.never(), 1000))
                .thenAwait(Duration.ofMillis(1000))
                .verifyError(TimeoutException.class);
    }

    @Test
    void reasoningFrameDisablesTimeout() {
        Flux<String> source = Flux.concat(
                Mono.delay(Duration.ofMillis(200)).map(x -> REASONING_FRAME),
                Flux.just("[DONE]")
        );

        StepVerifier.withVirtualTime(() -> withTtft(source, 1000))
                .expectNext(REASONING_FRAME)
                .expectNext("[DONE]")
                .verifyComplete();
    }
}
