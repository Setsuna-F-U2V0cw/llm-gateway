package com.llmgateway.gateway.security;

import com.llmgateway.gateway.config.GatewayAuthProperties;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GatewayAuthenticationFilterTest {

    @Test
    void publicLiveness_doesNotRequireKey() {
        GatewayAuthenticationFilter filter = filter(Map.of("tenant-a", "secret-a"));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/v1/health").build());
        AtomicBoolean called = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, e -> {
                    called.set(true);
                    return Mono.empty();
                }))
                .verifyComplete();

        assertThat(called).isTrue();
    }

    @Test
    void missingKey_returnsOpenAiStyle401() {
        GatewayAuthenticationFilter filter = filter(Map.of("tenant-a", "secret-a"));
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/v1/models").build());

        StepVerifier.create(filter.filter(exchange, e -> Mono.empty()))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(exchange.getResponse().getHeaders().getFirst(HttpHeaders.WWW_AUTHENTICATE))
                .isEqualTo("Bearer");
        assertThat(exchange.getResponse().getHeaders().getContentType())
                .hasToString("application/json");
    }

    @Test
    void wrongKey_returns401WithoutCallingChain() {
        GatewayAuthenticationFilter filter = filter(Map.of("tenant-a", "secret-a"));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest
                .get("/v1/health/deps")
                .header(HttpHeaders.AUTHORIZATION, "Bearer wrong")
                .build());
        AtomicBoolean called = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, e -> {
                    called.set(true);
                    return Mono.empty();
                }))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(called).isFalse();
    }

    @Test
    void validKey_derivesTenantAndIgnoresSpoofedUserHeader() {
        GatewayAuthenticationFilter filter = filter(Map.of(
                "tenant-a", "secret-a",
                "tenant-b", "secret-b"));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest
                .post("/v1/chat/completions")
                .header(HttpHeaders.AUTHORIZATION, "Bearer secret-a")
                .header("X-User-Id", "tenant-b")
                .build());
        AtomicReference<String> tenant = new AtomicReference<>();

        StepVerifier.create(filter.filter(exchange, e -> {
                    tenant.set(e.getAttribute(GatewayAuthenticationFilter.TENANT_ATTRIBUTE));
                    return Mono.empty();
                }))
                .verifyComplete();

        assertThat(tenant).hasValue("tenant-a");
    }

    @Test
    void authDisabled_protectedPath_usesAnonymousTenant() {
        GatewayAuthProperties props = new GatewayAuthProperties();
        props.setEnabled(false);
        GatewayAuthenticationFilter filter = new GatewayAuthenticationFilter(props);
        filter.initializeKeys();
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/v1/models").build());
        AtomicReference<String> tenant = new AtomicReference<>();

        StepVerifier.create(filter.filter(exchange, e -> {
                    tenant.set(e.getAttribute(GatewayAuthenticationFilter.TENANT_ATTRIBUTE));
                    return Mono.empty();
                }))
                .verifyComplete();

        assertThat(tenant).hasValue("anonymous");
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    void prometheus_requiresKey() {
        GatewayAuthenticationFilter filter = filter(Map.of("tenant-a", "secret-a"));
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/actuator/prometheus").build());

        StepVerifier.create(filter.filter(exchange, e -> Mono.empty()))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void blankBearer_returns401() {
        GatewayAuthenticationFilter filter = filter(Map.of("tenant-a", "secret-a"));
        var exchange = MockServerWebExchange.from(MockServerHttpRequest
                .get("/v1/models")
                .header(HttpHeaders.AUTHORIZATION, "Bearer    ")
                .build());

        StepVerifier.create(filter.filter(exchange, e -> Mono.empty()))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void trailingSlashChatPath_isNotProtected_currentContract() {
        // 精确 path 匹配的已知弱点：尾斜杠不在 PROTECTED_PATHS 内。
        GatewayAuthenticationFilter filter = filter(Map.of("tenant-a", "secret-a"));
        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/v1/chat/completions/").build());
        AtomicBoolean called = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, e -> {
                    called.set(true);
                    return Mono.empty();
                }))
                .verifyComplete();

        assertThat(called).isTrue();
        assertThat(exchange.getResponse().getStatusCode()).isNull();
    }

    @Test
    void duplicateOrBlankKeys_failFast() {
        assertThatThrownBy(() -> filter(Map.of()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("未配置");

        assertThatThrownBy(() -> filter(Map.of(
                "tenant-a", "same",
                "tenant-b", "same")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("相同");

        assertThatThrownBy(() -> filter(Map.of("tenant-a", "")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("未配置");
    }

    private static GatewayAuthenticationFilter filter(Map<String, String> keys) {
        GatewayAuthProperties props = new GatewayAuthProperties();
        props.setApiKeys(keys);
        GatewayAuthenticationFilter filter = new GatewayAuthenticationFilter(props);
        filter.initializeKeys();
        return filter;
    }
}
