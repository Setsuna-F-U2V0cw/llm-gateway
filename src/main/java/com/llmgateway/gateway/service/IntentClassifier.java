package com.llmgateway.gateway.service;

import com.llmgateway.gateway.model.Intent;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

    /** 余弦置信度阈值，低于此值判 DEFAULT（避免对无关 prompt 强行归类） */
    private static final double CONFIDENCE_THRESHOLD = 0.5;

    /**
     * 意图原型向量。volatile：启动时异步填充，填充前 classify 走规则 fallback。
     * 用 ConcurrentHashMap 保证可见性与并发安全。
     */
    private final Map<Intent, float[]> prototypes = new ConcurrentHashMap<>();

    /** 每个 intent 的典范短语（启动时 embed 后取均值得原型向量） */
    private static final Map<Intent, List<String>> PROTOTYPE_PHRASES = Map.of(
            Intent.CODE, List.of("请帮我写一段代码", "这段代码有 bug 帮我调试", "用 Python 实现一个函数"),
            Intent.TRANSLATION, List.of("把这段话翻译成英文", "translate this to Chinese", "请翻译以下内容"),
            Intent.MATH, List.of("计算这道数学题", "求解这个方程", "求定积分的值"),
            Intent.REASONING, List.of("分析一下这个问题的原因", "请逐步推理", "为什么会出现这个现象"),
            Intent.WRITING, List.of("写一篇文章", "帮我创作一段文案", "写一首诗"),
            Intent.CHITCHAT, List.of("你好", "今天天气怎么样", "随便聊聊")
    );

    /**
     * 启动时预 embed 意图原型。异步执行，不阻塞启动；Ollama 不可用则 prototypes 保持空，
     * classify 自动退化为规则分类（fail-open）。
     */
    @PostConstruct
    public void initPrototypes() {
        Flux.fromIterable(PROTOTYPE_PHRASES.entrySet())
                .flatMap(entry -> embedAndAverage(entry.getKey(), entry.getValue()))
                .doOnNext(e -> log.info("意图原型向量就绪: intent={}", e.getKey()))
                .collectList()
                .subscribe(
                        list -> log.info("意图原型加载完成: {} 个", list.size()),
                        e -> log.warn("意图原型加载失败，分类将退化为规则模式: {}", e.getMessage())
                );
    }

    private Mono<Map.Entry<Intent, float[]>> embedAndAverage(Intent intent, List<String> phrases) {
        return Flux.fromIterable(phrases)
                .flatMap(embeddingService::embed)
                .collectList()
                .map(vecs -> {
                    float[] avg = average(vecs);
                    prototypes.put(intent, avg);
                    return Map.entry(intent, avg);
                });
    }

    /**
     * 按给定 prompt embedding 分类意图（主路径，零额外调用）
     *
     * @return 最相似的 Intent；若 prototypes 未就绪或 max 余弦 < 阈值 → DEFAULT
     */
    public Intent classify(float[] embedding) {
        if (embedding == null || prototypes.isEmpty()) {
            return Intent.DEFAULT;
        }
        Intent best = Intent.DEFAULT;
        double bestSim = CONFIDENCE_THRESHOLD;
        for (Map.Entry<Intent, float[]> e : prototypes.entrySet()) {
            double sim = cosine(embedding, e.getValue());
            if (sim > bestSim) {
                bestSim = sim;
                best = e.getKey();
            }
        }
        log.debug("意图分类: best={}, sim={}", best, String.format("%.2f", bestSim));
        return best;
    }

    /**
     * 规则分类（embedding 不可用时的 fallback）。关键词粗匹配。
     */
    public Intent classifyByRules(String prompt) {
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
        double dot = 0, na = 0, nb = 0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) return 0;
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }
}
