package com.llmgateway.gateway.service;

import com.google.common.util.concurrent.ListenableFuture;
import com.llmgateway.gateway.config.GatewayProperties;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.VectorParams;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.SearchPoints;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import jakarta.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;
import static io.qdrant.client.WithPayloadSelectorFactory.enable;

/**
 * 向量存储服务：封装 Qdrant 的检索与写入操作
 *
 * [💎 面试亮点] Qdrant gRPC 接口返回 ListenableFuture（Guava），
 * 通过 Mono.create + addListener 将其桥接为 Mono，
 * 确保不阻塞 Netty EventLoop 线程。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class VectorStoreService {

    private final QdrantClient qdrantClient;
    private final GatewayProperties props;

    // 专用于 ListenableFuture 回调的执行器
    private final Executor callbackExecutor = Executors.newVirtualThreadPerTaskExecutor();

    /**
     * 应用启动时初始化 Qdrant 集合（幂等，已存在则跳过）
     */
    @PostConstruct
    public void initCollection() {
        fromListenable(qdrantClient.collectionExistsAsync(props.getQdrantCollection()))
                .flatMap(exists -> {
                    if (exists) {
                        log.info("Qdrant 集合已存在: {}", props.getQdrantCollection());
                        return Mono.empty();
                    }
                    log.info("创建 Qdrant 集合: {}, dim={}", props.getQdrantCollection(), props.getEmbeddingDimension());
                    return fromListenable(qdrantClient.createCollectionAsync(
                            props.getQdrantCollection(),
                            VectorParams.newBuilder()
                                    .setSize(props.getEmbeddingDimension())
                                    .setDistance(Distance.Cosine)
                                    .build()
                    ));
                })
                .subscribe(
                        result -> log.info("Qdrant 集合初始化完成"),
                        e -> log.warn("Qdrant 初始化失败（Qdrant 未启动？）: {}", e.getMessage())
                );
    }

    /**
     * 在向量库中搜索语义最相似的缓存记录
     *
     * @param vector 查询向量
     * @return 若找到相似度超过阈值的记录，返回缓存的完整回答；否则返回 Mono.empty()
     */
    public Mono<String> search(float[] vector) {
        List<Float> queryVector = toFloatList(vector);

        return fromListenable(qdrantClient.searchAsync(
                        SearchPoints.newBuilder()
                                .setCollectionName(props.getQdrantCollection())
                                .addAllVector(queryVector)
                                .setLimit(1)
                                .setWithPayload(enable(true))
                                .setScoreThreshold(props.getSemanticCacheThreshold())
                                .build()
                ))
                .flatMap(results -> {
                    if (results.isEmpty()) {
                        log.debug("语义缓存未命中");
                        return Mono.empty();
                    }
                    ScoredPoint top = results.get(0);
                    String cachedAnswer = top.getPayload().get("answer").getStringValue();
                    log.info("语义缓存命中: score={}, answerLen={}", top.getScore(), cachedAnswer.length());
                    return Mono.just(cachedAnswer);
                })
                .onErrorResume(e -> {
                    // [💣 踩坑预警] fail-open：Qdrant 不可用时降级为透传，不阻断用户请求
                    log.warn("Qdrant 检索失败，降级为透传: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * 将 Prompt 向量和对应的完整回答写入向量库
     */
    public Mono<Void> save(float[] vector, String prompt, String answer) {
        List<Float> floatList = toFloatList(vector);

        PointStruct point = PointStruct.newBuilder()
                .setId(id(UUID.randomUUID()))
                .setVectors(vectors(floatList))
                .putPayload("prompt", value(prompt))
                .putPayload("answer", value(answer))
                .build();

        return fromListenable(qdrantClient.upsertAsync(props.getQdrantCollection(), List.of(point)))
                .doOnSuccess(r -> log.debug("缓存写入成功: promptLen={}, answerLen={}", prompt.length(), answer.length()))
                .onErrorResume(e -> {
                    log.warn("缓存写入失败（不影响主流程）: {}", e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    /**
     * 将 Guava ListenableFuture 桥接为 Project Reactor Mono
     *
     * [🚧 核心难点] Qdrant Java 客户端返回的是 Guava ListenableFuture，
     * 而不是 JDK CompletableFuture。Mono.fromFuture 只接受 CompletableFuture，
     * 因此需要用 Mono.create 手动注册完成/失败回调来完成桥接。
     * callbackExecutor 使用 Java 21 虚拟线程，轻量且无阻塞。
     */
    private <T> Mono<T> fromListenable(ListenableFuture<T> future) {
        return Mono.create(sink -> future.addListener(
                () -> {
                    try {
                        sink.success(future.get());
                    } catch (Exception e) {
                        sink.error(e);
                    }
                },
                callbackExecutor
        ));
    }

    private List<Float> toFloatList(float[] vector) {
        List<Float> list = new ArrayList<>(vector.length);
        for (float v : vector) {
            list.add(v);
        }
        return list;
    }
}
