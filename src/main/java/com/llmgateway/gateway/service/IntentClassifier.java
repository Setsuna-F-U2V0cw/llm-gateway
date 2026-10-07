package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.Intent;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 意图分类器（阶段四 Router 的核心组件）
 *
 * 两条路径：
 * 1. **embedding 复用**（主路径）：启动时用 EmbeddingService 给每个 intent 的几条典范短语 embed 后取均值，
 *    得到「意图原型向量」。请求到来时，复用 pipeline 顶部已算好的 prompt 向量，跟每个原型做余弦取 argmax。
 *    max 低于 {@link #CONFIDENCE_THRESHOLD} 时降级为 DEFAULT。零额外 embed 调用——向量是缓存检索的副产物。
 * 2. **规则分类**（fallback）：embedding 不可用（Ollama 挂 / 启动未就绪）时按关键词匹配粗分。
 *
 * [💎 面试亮点] 意图分类不花额外 LLM 调用、不花额外 embedding 调用——
 * 复用阶段三缓存检索已在算的 prompt 向量。两个路由维度（意图 + token 长度）全部零额外计算。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IntentClassifier {

    private final EmbeddingService embeddingService;
    private final GatewayProperties props;

    /** 余弦置信度阈值，低于此值判 DEFAULT（避免对无关 prompt 强行归类） */
    private static final double CONFIDENCE_THRESHOLD = 0.5;

    /** 每个 intent 的典范短语（启动时 embed 后取均值得原型向量） */
    private static final Map<Intent, List<String>> PROTOTYPE_PHRASES = Map.of(
            Intent.CODE, List.of("请帮我写一段代码", "这段代码有 bug 帮我调试", "用 Python 实现一个函数"),
            Intent.TRANSLATION, List.of("把这段话翻译成英文", "translate this to Chinese", "请翻译以下内容"),
            Intent.MATH, List.of("计算这道数学题", "求解这个方程", "求定积分的值"),
            Intent.REASONING, List.of("分析一下这个问题的原因", "请逐步推理", "为什么会出现这个现象"),
            Intent.WRITING, List.of("写一篇文章", "帮我创作一段文案", "写一首诗"),
            Intent.CHITCHAT, List.of("你好", "今天天气怎么样", "随便聊聊")
    );

    private static final int EXPECTED_PROTOTYPES = PROTOTYPE_PHRASES.size();

    /**
     * 只发布完整快照：任何一次加载失败，请求看到的仍是上一份完整快照或空 Map，
     * 不会拿半套意图参与分类。
     */
    private final AtomicReference<Map<Intent, float[]>> prototypes =
            new AtomicReference<>(Map.of());
    private final AtomicLong retryCount = new AtomicLong();
    private final AtomicReference<PrototypeStatus> prototypeStatus =
            new AtomicReference<>(new PrototypeStatus("loading", 0, EXPECTED_PROTOTYPES, 0, null));
    private volatile Disposable prototypeLoader;

    public record PrototypeStatus(
            String state,
            int loaded,
            int expected,
            long retries,
            String lastError
    ) {}

    /**
     * 应用就绪后启动唯一后台加载器。失败时指数退避重试，请求线程从不负责触发加载。
     */
    @EventListener(ApplicationReadyEvent.class)
    public synchronized void startPrototypeLoading() {
        if (prototypeLoader != null && !prototypeLoader.isDisposed()) {
            return;
        }
        prototypeLoader = loadUntilReady().subscribe(
                null,
                e -> log.error("意图原型后台加载器异常终止: {}", e.getMessage())
        );
    }

    Mono<Void> loadUntilReady() {
        GatewayProperties.PrototypeRetryConfig retry = props.getPrototypeRetry();
        Duration initial = Duration.ofMillis(Math.max(1L, retry.getInitialDelayMs()));
        Duration max = Duration.ofMillis(Math.max(initial.toMillis(), retry.getMaxDelayMs()));
        double jitter = Math.max(0.0, Math.min(1.0, retry.getJitter()));

        return Mono.defer(this::loadCompleteSnapshot)
                .retryWhen(Retry.backoff(Long.MAX_VALUE, initial)
                        .maxBackoff(max)
                        .jitter(jitter)
                        .doBeforeRetry(signal -> {
                            long retries = retryCount.incrementAndGet();
                            String error = safeMessage(signal.failure());
                            prototypeStatus.set(new PrototypeStatus(
                                    "retrying", 0, EXPECTED_PROTOTYPES, retries, error));
                            log.warn("意图原型加载失败，第 {} 次重试: {}", retries, error);
                        }))
                .doOnNext(snapshot -> {
                    prototypes.set(snapshot);
                    prototypeStatus.set(new PrototypeStatus(
                            "ready", snapshot.size(), EXPECTED_PROTOTYPES,
                            retryCount.get(), null));
                    log.info("意图原型加载完成并原子发布: {} 个", snapshot.size());
                })
                .then();
    }

    private Mono<Map<Intent, float[]>> loadCompleteSnapshot() {
        prototypeStatus.set(new PrototypeStatus(
                retryCount.get() == 0 ? "loading" : "retrying",
                0, EXPECTED_PROTOTYPES, retryCount.get(),
                prototypeStatus.get().lastError()));

        return Flux.fromIterable(PROTOTYPE_PHRASES.entrySet())
                // 单个 Ollama 实例避免 18 路同时冷启动；每个意图内最多并发 3 条短语。
                .concatMap(entry -> embedAndAverage(entry.getKey(), entry.getValue()))
                .doOnNext(e -> log.debug("意图原型计算完成: intent={}", e.getKey()))
                .collect(
                        () -> new EnumMap<Intent, float[]>(Intent.class),
                        (map, entry) -> map.put(entry.getKey(), entry.getValue()))
                .flatMap(map -> map.size() == EXPECTED_PROTOTYPES
                        ? Mono.just(Map.copyOf(map))
                        : Mono.error(new IllegalStateException(
                                "意图原型数量不完整: " + map.size() + "/" + EXPECTED_PROTOTYPES)));
    }

    private Mono<Map.Entry<Intent, float[]>> embedAndAverage(Intent intent, List<String> phrases) {
        return Flux.fromIterable(phrases)
                .flatMapSequential(embeddingService::embed, 3)
                .collectList()
                .map(vecs -> Map.entry(intent, average(vecs)));
    }

    /**
     * 分类的唯一公开接口。完整原型就绪且请求 embedding 可用时走余弦分类；
     * 其余情况在模块内部自动走规则 fallback。
     */
    public Intent classify(float[] embedding, String prompt) {
        Map<Intent, float[]> snapshot = prototypes.get();
        if (embedding == null || snapshot.size() != EXPECTED_PROTOTYPES) {
            return classifyByRules(prompt);
        }
        Intent best = Intent.DEFAULT;
        double bestSim = CONFIDENCE_THRESHOLD;
        for (Map.Entry<Intent, float[]> e : snapshot.entrySet()) {
            double sim = cosine(embedding, e.getValue());
            if (sim > bestSim) {
                bestSim = sim;
                best = e.getKey();
            }
        }
        log.debug("意图分类: best={}, sim={}", best, String.format("%.2f", bestSim));
        return best;
    }

    public PrototypeStatus prototypeStatus() {
        return prototypeStatus.get();
    }

    @PreDestroy
    public void stopPrototypeLoading() {
        Disposable loader = prototypeLoader;
        if (loader != null) {
            loader.dispose();
        }
    }

    /**
     * 规则分类（embedding 不可用时的 fallback）。关键词粗匹配。
     */
    Intent classifyByRules(String prompt) {
        if (prompt == null || prompt.isBlank()) return Intent.DEFAULT;
        String p = prompt.toLowerCase();
        if (containsAny(p, "代码", "编程", "function", "bug", "实现", "编译")) return Intent.CODE;
        if (containsAny(p, "翻译", "translate")) return Intent.TRANSLATION;
        if (containsAny(p, "计算", "求解", "数学", "方程", "积分", "概率")) return Intent.MATH;
        if (containsAny(p, "分析", "推理", "为什么", "原因", "证明")) return Intent.REASONING;
        if (containsAny(p, "写一篇", "创作", "文案", "写一首", "文章")) return Intent.WRITING;
        return Intent.DEFAULT;
    }

    private boolean containsAny(String p, String... keywords) {
        return Arrays.stream(keywords).anyMatch(p::contains);
    }

    private float[] average(List<float[]> vecs) {
        int dim = vecs.get(0).length;
        float[] avg = new float[dim];
        for (float[] v : vecs) {
            for (int i = 0; i < dim; i++) avg[i] += v[i];
        }
        for (int i = 0; i < dim; i++) avg[i] /= vecs.size();
        // 归一化，使余弦 = 点积
        double norm = 0;
        for (float v : avg) norm += v * v;
        norm = Math.sqrt(norm);
        if (norm > 0) for (int i = 0; i < dim; i++) avg[i] /= norm;
        return avg;
    }

    private double cosine(float[] a, float[] b) {
        if (a.length != b.length) {
            return 0;
        }
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) return 0;
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    private String safeMessage(Throwable error) {
        String message = error.getMessage();
        return message == null ? error.getClass().getSimpleName() : message;
    }
}
