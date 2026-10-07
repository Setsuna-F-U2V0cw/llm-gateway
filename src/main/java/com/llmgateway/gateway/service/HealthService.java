package com.llmgateway.gateway.service;

import com.google.common.util.concurrent.ListenableFuture;
import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ComponentHealth;
import com.llmgateway.gateway.model.DependencyHealth;
import io.qdrant.client.QdrantClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 依赖健康探测：Redis / Qdrant / Ollama 并行 ping，超时 fail-closed 记 down。
 *
 * [💎 面试亮点] 存活与就绪分离——GET /v1/health 只证明进程活着；本服务给诊断页看依赖。
 * 网关对三件套都是 fail-open，所以依赖挂了探测接口仍返回 200 + status=degraded，不把诊断请求打成 503。
 */
@Service
public class HealthService {

    private static final Duration PING_TIMEOUT = Duration.ofMillis(1500);

    private final ReactiveRedisTemplate<String, String> redis;
    private final QdrantClient qdrantClient;
    private final WebClient ollamaWebClient;
    private final GatewayProperties props;
    private final IntentClassifier intentClassifier;
    private final Executor callbackExecutor = Executors.newVirtualThreadPerTaskExecutor();

    public HealthService(ReactiveRedisTemplate<String, String> redis,
                         QdrantClient qdrantClient,
                         @Qualifier("ollamaWebClient") WebClient ollamaWebClient,
                         GatewayProperties props,
                         IntentClassifier intentClassifier) {
        this.redis = redis;
        this.qdrantClient = qdrantClient;
        this.ollamaWebClient = ollamaWebClient;
        this.props = props;
        this.intentClassifier = intentClassifier;
    }

    public Mono<DependencyHealth> checkDependencies() {
        return Mono.zip(checkRedis(), checkQdrant(), checkOllama())
                .map(t -> {
                    List<ComponentHealth> components = List.of(t.getT1(), t.getT2(), t.getT3());
                    boolean allUp = components.stream().allMatch(ComponentHealth::up);
                    return new DependencyHealth(allUp ? "up" : "degraded", components, gatewaySnapshot());
                });
    }

    private Mono<ComponentHealth> checkRedis() {
        return ping("redis",
                redis.execute(ReactiveRedisConnection::ping)
                        .next()
                        .map(pong -> pong == null ? "PONG" : pong));
    }

    private Mono<ComponentHealth> checkQdrant() {
        String collection = props.getQdrantCollection();
        return Mono.defer(() -> {
            long start = System.nanoTime();
            return fromListenable(qdrantClient.collectionExistsAsync(collection))
                    .map(exists -> exists
                            ? ComponentHealth.up("qdrant", elapsedMs(start), collection + " exists")
                            : ComponentHealth.down("qdrant", elapsedMs(start), collection + " missing"))
                    .timeout(PING_TIMEOUT)
                    .onErrorResume(e -> Mono.just(
                            ComponentHealth.down("qdrant", elapsedMs(start), safeMsg(e))));
        });
    }

    private Mono<ComponentHealth> checkOllama() {
        String base = props.getOllamaBaseUrl();
        return ping("ollama",
                ollamaWebClient.get()
                        .uri(base + "/api/tags")
                        .retrieve()
                        .toBodilessEntity()
                        .map(res -> base + " " + res.getStatusCode().value()));
    }

    private Mono<ComponentHealth> ping(String name, Mono<String> detail) {
        return Mono.defer(() -> {
            long start = System.nanoTime();
            return detail
                    .map(d -> ComponentHealth.up(name, elapsedMs(start), d))
                    .timeout(PING_TIMEOUT)
                    .onErrorResume(e -> Mono.just(
                            ComponentHealth.down(name, elapsedMs(start), safeMsg(e))));
        });
    }

    private Map<String, Object> gatewaySnapshot() {
        Map<String, Object> snap = new LinkedHashMap<>();
        snap.put("ttftSloMs", props.getTtftSloMs());
        snap.put("ttftSloThinkingMs", props.getTtftSloThinkingMs());
        snap.put("tpmCapacity", props.getTpmCapacity());
        snap.put("semanticCacheThreshold", props.getSemanticCacheThreshold());
        snap.put("qdrantCollection", props.getQdrantCollection());
        snap.put("embeddingDimension", props.getEmbeddingDimension());
        snap.put("ollamaEmbedModel", props.getOllamaEmbedModel());
        snap.put("fallbackModel", props.getFallback() == null ? null : props.getFallback().getRequestModel());
        snap.put("intentClassifier", intentClassifier.prototypeStatus());
        return snap;
    }

    private static long elapsedMs(long startNanos) {
        return Math.max(0, (System.nanoTime() - startNanos) / 1_000_000);
    }

    private static String safeMsg(Throwable e) {
        String m = e.getMessage();
        return m == null ? e.getClass().getSimpleName() : m;
    }

    private <T> Mono<T> fromListenable(ListenableFuture<T> future) {
        return Mono.create(sink -> {
            AtomicBoolean cancelled = new AtomicBoolean(false);
            future.addListener(() -> {
                if (cancelled.get()) {
                    return;
                }
                try {
                    sink.success(future.get());
                } catch (Exception e) {
                    if (cancelled.get()) {
                        return;
                    }
                    Throwable cause = e.getCause() == null ? e : e.getCause();
                    sink.error(cause);
                }
            }, callbackExecutor);
            sink.onDispose(() -> {
                cancelled.set(true);
                future.cancel(true);
            });
        });
    }
}
