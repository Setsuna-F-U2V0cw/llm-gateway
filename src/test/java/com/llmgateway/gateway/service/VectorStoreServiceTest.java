package com.llmgateway.gateway.service;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.llmgateway.gateway.config.GatewayProperties;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.Points.PointStruct;
import io.qdrant.client.grpc.Points.ScoredPoint;
import io.qdrant.client.grpc.Points.SearchPoints;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static io.qdrant.client.ValueFactory.value;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class VectorStoreServiceTest {

    private QdrantClient client;
    private GatewayProperties props;
    private VectorStoreService service;

    @BeforeEach
    void setUp() {
        client = mock(QdrantClient.class);
        props = new GatewayProperties();
        props.setQdrantCollection("llm_cache");
        props.setSemanticCacheThreshold(0.95f);
        props.setSemanticCacheTtlSeconds(86400);
        service = new VectorStoreService(client, props);
    }

    @Test
    void search_attachesTenantModelAndCreatedAtFilter() {
        when(client.searchAsync(org.mockito.ArgumentMatchers.any(SearchPoints.class)))
                .thenReturn(Futures.immediateFuture(List.of()));

        StepVerifier.create(service.search(new float[]{0.1f, 0.2f}, "user-a", "deepseek-v4-flash"))
                .verifyComplete();

        ArgumentCaptor<SearchPoints> captor = ArgumentCaptor.forClass(SearchPoints.class);
        verify(client).searchAsync(captor.capture());
        SearchPoints sp = captor.getValue();
        assertThat(sp.getCollectionName()).isEqualTo("llm_cache");
        assertThat(sp.getLimit()).isEqualTo(1);
        assertThat(sp.getScoreThreshold()).isEqualTo(0.95f);
        assertThat(sp.getFilter().getMustCount()).isEqualTo(3);
        assertThat(sp.getFilter().getMustList())
                .anyMatch(c -> "tenant_id".equals(c.getField().getKey())
                        && "user-a".equals(c.getField().getMatch().getKeyword()))
                .anyMatch(c -> "model".equals(c.getField().getKey())
                        && "deepseek-v4-flash".equals(c.getField().getMatch().getKeyword()))
                .anyMatch(c -> "created_at".equals(c.getField().getKey()) && c.getField().hasRange());
        double gte = sp.getFilter().getMustList().stream()
                .filter(c -> "created_at".equals(c.getField().getKey()))
                .findFirst()
                .orElseThrow()
                .getField()
                .getRange()
                .getGte();
        assertThat(gte).isCloseTo(Instant.now().getEpochSecond() - 86400, within(5.0));
    }

    @Test
    void search_ttlZero_omitsCreatedAtFilter() {
        props.setSemanticCacheTtlSeconds(0);
        when(client.searchAsync(org.mockito.ArgumentMatchers.any(SearchPoints.class)))
                .thenReturn(Futures.immediateFuture(List.of()));

        StepVerifier.create(service.search(new float[]{0.1f}, "user-a", "m1"))
                .verifyComplete();

        ArgumentCaptor<SearchPoints> captor = ArgumentCaptor.forClass(SearchPoints.class);
        verify(client).searchAsync(captor.capture());
        assertThat(captor.getValue().getFilter().getMustCount()).isEqualTo(2);
        assertThat(captor.getValue().getFilter().getMustList())
                .noneMatch(c -> "created_at".equals(c.getField().getKey()));
    }

    @Test
    void search_hit_returnsAnswerPayload() {
        ScoredPoint hit = ScoredPoint.newBuilder()
                .setScore(0.99f)
                .putPayload("answer", value("cached-hit"))
                .build();
        when(client.searchAsync(org.mockito.ArgumentMatchers.any(SearchPoints.class)))
                .thenReturn(Futures.immediateFuture(List.of(hit)));

        StepVerifier.create(service.search(new float[]{0.1f}, "user-a", "m1"))
                .expectNext("cached-hit")
                .verifyComplete();
    }

    @Test
    void search_qdrantError_failOpenEmpty() {
        when(client.searchAsync(org.mockito.ArgumentMatchers.any(SearchPoints.class)))
                .thenReturn(Futures.immediateFailedFuture(new RuntimeException("unavailable")));

        StepVerifier.create(service.search(new float[]{0.1f}, "user-a", "m1"))
                .verifyComplete();
    }

    @Test
    void search_disposeCancelsListenableFuture() {
        SettableFuture<List<ScoredPoint>> future = SettableFuture.create();
        when(client.searchAsync(org.mockito.ArgumentMatchers.any(SearchPoints.class)))
                .thenReturn(future);

        StepVerifier.create(service.search(new float[]{0.1f}, "u", "m"))
                .thenCancel()
                .verify();

        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    void save_writesTenantModelAndCreatedAtPayload() {
        when(client.upsertAsync(anyString(), anyList()))
                .thenReturn(Futures.immediateFuture(null));

        StepVerifier.create(service.save(new float[]{0.1f}, "prompt", "answer", "user-b", "m1"))
                .verifyComplete();

        ArgumentCaptor<List<PointStruct>> captor = ArgumentCaptor.forClass(List.class);
        verify(client).upsertAsync(org.mockito.ArgumentMatchers.eq("llm_cache"), captor.capture());
        PointStruct point = captor.getValue().get(0);
        assertThat(point.getPayloadMap().get("tenant_id").getStringValue()).isEqualTo("user-b");
        assertThat(point.getPayloadMap().get("model").getStringValue()).isEqualTo("m1");
        assertThat(point.getPayloadMap().get("answer").getStringValue()).isEqualTo("answer");
        assertThat(point.getPayloadMap().get("prompt").getStringValue()).isEqualTo("prompt");
        assertThat(point.getPayloadMap().get("created_at").getIntegerValue())
                .isCloseTo(Instant.now().getEpochSecond(), within(5L));
    }

    @Test
    void save_upsertError_failOpenCompletes() {
        when(client.upsertAsync(anyString(), anyList()))
                .thenReturn(Futures.immediateFailedFuture(new RuntimeException("write failed")));

        StepVerifier.create(service.save(new float[]{0.1f}, "p", "a", "u", "m"))
                .verifyComplete();
    }

    @Test
    void normalize_blankFallsBack() {
        assertThat(VectorStoreService.normalizeTenant(null)).isEqualTo("anonymous");
        assertThat(VectorStoreService.normalizeTenant("  ")).isEqualTo("anonymous");
        assertThat(VectorStoreService.normalizeModel(null)).isEqualTo("unknown");
        assertThat(VectorStoreService.normalizeModel("")).isEqualTo("unknown");
    }
}
