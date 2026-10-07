package com.llmgateway.gateway.controller;

import com.llmgateway.gateway.config.GatewayAuthProperties;
import com.llmgateway.gateway.model.ChatRequest;
import com.llmgateway.gateway.model.ComponentHealth;
import com.llmgateway.gateway.model.DependencyHealth;
import com.llmgateway.gateway.model.ModelListResponse;
import com.llmgateway.gateway.security.GatewayAuthenticationFilter;
import com.llmgateway.gateway.service.HealthService;
import com.llmgateway.gateway.service.LlmProxyService;
import com.llmgateway.gateway.service.ModelCatalogService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Controller 切片：{@code WebTestClient.bindToController}，不起整个 Spring 容器。
 */
class ChatCompletionControllerTest {

    private LlmProxyService proxy;
    private HealthService health;
    private ModelCatalogService catalog;
    private ChatCompletionController controller;

    @BeforeEach
    void setUp() {
        proxy = mock(LlmProxyService.class);
        health = mock(HealthService.class);
        catalog = mock(ModelCatalogService.class);
        controller = new ChatCompletionController(proxy, health, catalog);
    }

    @Test
    void health_returnsPlainTextWithoutAuth() {
        WebTestClient.bindToController(controller).build()
                .get().uri("/v1/health")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).isEqualTo("LLM Gateway is running");
    }

    @Test
    void models_returnsCatalogPayload() {
        when(catalog.list()).thenReturn(ModelListResponse.of(List.of(
                new ModelListResponse.ModelItem(
                        "deepseek-v4-flash", "model", "llm-gateway",
                        null, false, 64000))));

        WebTestClient.bindToController(controller).build()
                .get().uri("/v1/models")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.object").isEqualTo("list")
                .jsonPath("$.data[0].id").isEqualTo("deepseek-v4-flash")
                .jsonPath("$.data[0].is_thinking").isEqualTo(false);
    }

    @Test
    void healthDeps_returnsJson() {
        when(health.checkDependencies()).thenReturn(Mono.just(new DependencyHealth(
                "up",
                List.of(ComponentHealth.up("redis", 1, "PONG")),
                Map.of("ttftSloMs", 3000))));

        WebTestClient.bindToController(controller).build()
                .get().uri("/v1/health/deps")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("up")
                .jsonPath("$.components[0].name").isEqualTo("redis");
    }

    @Test
    void chat_forwardsTenantAndWritesCacheHeader() {
        when(proxy.streamChat(any(ChatRequest.class), eq("tenant-a")))
                .thenReturn(Mono.just(ResponseEntity.ok()
                        .header("X-Cache-Hit", "true")
                        .body(Flux.just(
                                "{\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}",
                                "[DONE]"))));

        WebTestClient.bindToController(controller)
                .webFilter((exchange, chain) -> {
                    exchange.getAttributes().put(GatewayAuthenticationFilter.TENANT_ATTRIBUTE, "tenant-a");
                    return chain.filter(exchange);
                })
                .build()
                .post().uri("/v1/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals("X-Cache-Hit", "true");

        verify(proxy).streamChat(any(ChatRequest.class), eq("tenant-a"));
    }

    @Test
    void chat_lengthGuard_returnsHttp413() {
        when(proxy.streamChat(any(ChatRequest.class), eq("tenant-a")))
                .thenReturn(Mono.error(new ResponseStatusException(
                        HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE, "prompt 超出所有 model 上下文上限")));

        WebTestClient.bindToController(controller)
                .webFilter((exchange, chain) -> {
                    exchange.getAttributes().put(GatewayAuthenticationFilter.TENANT_ATTRIBUTE, "tenant-a");
                    return chain.filter(exchange);
                })
                .build()
                .post().uri("/v1/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"messages\":[{\"role\":\"user\",\"content\":\"huge\"}]}")
                .exchange()
                .expectStatus().isEqualTo(HttpStatus.REQUESTED_RANGE_NOT_SATISFIABLE);
    }

    @Test
    void models_withAuthFilter_missingKey_is401() {
        GatewayAuthProperties props = new GatewayAuthProperties();
        props.setApiKeys(Map.of("demo", "demo-secret"));
        GatewayAuthenticationFilter filter = new GatewayAuthenticationFilter(props);
        filter.initializeKeys();

        WebTestClient.bindToController(controller)
                .webFilter(filter)
                .build()
                .get().uri("/v1/models")
                .exchange()
                .expectStatus().isUnauthorized()
                .expectHeader().valueEquals(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
    }
}
