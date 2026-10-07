package com.llmgateway.gateway.service;

import com.google.common.util.concurrent.ListenableFuture;
import com.llmgateway.gateway.config.GatewayProperties;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Collections.Distance;
import io.qdrant.client.grpc.Collections.PayloadSchemaType;
import io.qdrant.client.grpc.Collections.VectorParams;
import io.qdrant.client.grpc.Points.Condition;
import io.qdrant.client.grpc.Points.FieldCondition;
import io.qdrant.client.grpc.Points.Filter;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.Range;
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.SearchPoints;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.qdrant.client.ConditionFactory.matchKeyword;
import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.value;
import static io.qdrant.client.VectorsFactory.vectors;
import static io.qdrant.client.WithPayloadSelectorFactory.enable;

/**
 * 向量存储服务：封装 Qdrant 的检索与写入操作。
 *
 * Qdrant 由 docker-compose 部署（gRPC 6334）。payload 增字段不改向量维度，
 * <strong>不必重建 collection</strong>；启动时幂等补齐 tenant_id / model 的 keyword 索引
 * 以及 created_at 的 integer 索引。
 * 升级前没有 tenant_id/model/created_at 的旧点会被 filter 自然排除（等同未命中）。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class VectorStoreService {

    static final String PAYLOAD_TENANT = "tenant_id";
    static final String PAYLOAD_MODEL = "model";
    static final String PAYLOAD_CREATED_AT = "created_at";

    private final QdrantClient qdrantClient;
    private final GatewayProperties props;

    private final Executor callbackExecutor = Executors.newVirtualThreadPerTaskExecutor();

    @PostConstruct
    public void initCollection() {
        String collection = props.getQdrantCollection();
        fromListenable(qdrantClient.collectionExistsAsync(collection))
                .flatMap(exists -> {
                    if (exists) {
                        log.info("Qdrant 集合已存在: {}（docker volume 保留数据，不重建）", collection);
                        return Mono.just(true);
                    }
                    log.info("创建 Qdrant 集合: {}, dim={}", collection, props.getEmbeddingDimension());
                    return fromListenable(qdrantClient.createCollectionAsync(
                            collection,
                            VectorParams.newBuilder()
                                    .setSize(props.getEmbeddingDimension())
                                    .setDistance(Distance.Cosine)
                                    .build()
                    )).thenReturn(true);
                })
                .then(ensurePayloadIndexes(collection))
                .subscribe(
                        null,
                        e -> log.warn("Qdrant 初始化失败（容器未启动？docker-compose up -d）: {}", e.getMessage()),
                        () -> log.info("Qdrant 集合初始化完成")
                );
    }

    /**
     * keyword 索引让 tenant_id / model 的 must-filter 走 payload index；
     * integer 索引服务 created_at 范围过滤。索引已存在时 Qdrant 会报错，fail-open 忽略。
     */
    private Mono<Void> ensurePayloadIndexes(String collection) {
        return ensureIndex(collection, PAYLOAD_TENANT, PayloadSchemaType.Keyword)
                .then(ensureIndex(collection, PAYLOAD_MODEL, PayloadSchemaType.Keyword))
                .then(ensureIndex(collection, PAYLOAD_CREATED_AT, PayloadSchemaType.Integer));
    }

    private Mono<Void> ensureIndex(String collection, String field, PayloadSchemaType type) {
        return fromListenable(qdrantClient.createPayloadIndexAsync(
                        collection, field, type, null, true, null, null))
                .onErrorResume(e -> {
                    log.debug("payload index {} 已存在或创建失败（可忽略）: {}", field, e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    /**
     * 仅在同一租户、同一请求 model、且未过期的点中检索语义缓存。
     * {@code semantic-cache-ttl-seconds=0} 关闭过期过滤。
     */
    public Mono<String> search(float[] vector, String userId, String model) {
        List<Float> queryVector = toFloatList(vector);
        String tenant = normalizeTenant(userId);
        String modelKey = normalizeModel(model);

        Filter.Builder filter = Filter.newBuilder()
                .addMust(matchKeyword(PAYLOAD_TENANT, tenant))
                .addMust(matchKeyword(PAYLOAD_MODEL, modelKey));
        int ttlSeconds = props.getSemanticCacheTtlSeconds();
        if (ttlSeconds > 0) {
            double minCreatedAt = Instant.now().getEpochSecond() - ttlSeconds;
            filter.addMust(Condition.newBuilder()
                    .setField(FieldCondition.newBuilder()
                            .setKey(PAYLOAD_CREATED_AT)
                            .setRange(Range.newBuilder().setGte(minCreatedAt).build())
                            .build())
                    .build());
        }

        return fromListenable(qdrantClient.searchAsync(
                        SearchPoints.newBuilder()
                                .setCollectionName(props.getQdrantCollection())
                                .addAllVector(queryVector)
                                .setLimit(1)
                                .setWithPayload(enable(true))
                                .setScoreThreshold(props.getSemanticCacheThreshold())
                                .setFilter(filter.build())
                                .build()
                ))
                .flatMap(results -> {
                    if (results.isEmpty()) {
                        log.debug("语义缓存未命中: tenant={}, model={}", tenant, modelKey);
                        return Mono.empty();
                    }
                    ScoredPoint top = results.get(0);
                    String cachedAnswer = top.getPayload().get("answer").getStringValue();
                    log.info("语义缓存命中: tenant={}, model={}, score={}, answerLen={}",
                            tenant, modelKey, top.getScore(), cachedAnswer.length());
                    return Mono.just(cachedAnswer);
                })
                .onErrorResume(e -> {
                    log.warn("Qdrant 检索失败，降级为透传: {}", e.getMessage());
                    return Mono.empty();
                });
    }

    /**
     * 写入时带上 tenant_id + model + created_at，供后续 filter 隔离与 TTL。
     */
    public Mono<Void> save(float[] vector, String prompt, String answer, String userId, String model) {
        List<Float> floatList = toFloatList(vector);
        String tenant = normalizeTenant(userId);
        String modelKey = normalizeModel(model);

        PointStruct.Builder point = PointStruct.newBuilder()
                .setId(id(UUID.randomUUID()))
                .setVectors(vectors(floatList))
                .putPayload("answer", value(answer))
                .putPayload(PAYLOAD_TENANT, value(tenant))
                .putPayload(PAYLOAD_MODEL, value(modelKey))
                .putPayload(PAYLOAD_CREATED_AT, value(Instant.now().getEpochSecond()));
        if (prompt != null) {
            point.putPayload("prompt", value(prompt));
        }

        return fromListenable(qdrantClient.upsertAsync(props.getQdrantCollection(), List.of(point.build())))
                .doOnSuccess(r -> log.debug("缓存写入成功: tenant={}, model={}, answerLen={}",
                        tenant, modelKey, answer.length()))
                .onErrorResume(e -> {
                    log.warn("缓存写入失败（不影响主流程）: {}", e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    static String normalizeTenant(String userId) {
        return (userId == null || userId.isBlank()) ? "anonymous" : userId;
    }

    static String normalizeModel(String model) {
        return (model == null || model.isBlank()) ? "unknown" : model;
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

    private List<Float> toFloatList(float[] vector) {
        List<Float> list = new ArrayList<>(vector.length);
        for (float v : vector) {
            list.add(v);
        }
        return list;
    }
}
