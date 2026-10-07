package com.llmgateway.gateway.security;

import com.llmgateway.gateway.config.GatewayAuthProperties;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 多租户 API Key 入口鉴权。
 *
 * <p>认证成功后将 tenantId 写入 exchange attribute。业务代码只信任该属性，
 * 不再信任可伪造的 X-User-Id。</p>
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
public class GatewayAuthenticationFilter implements WebFilter {

    public static final String TENANT_ATTRIBUTE = "llmGatewayTenantId";

    private static final Set<String> PROTECTED_PATHS = Set.of(
            "/v1/chat/completions",
            "/v1/models",
            "/v1/health/deps",
            "/actuator/prometheus"
    );
    private static final byte[] UNAUTHORIZED_BODY = (
            "{\"error\":{\"message\":\"Invalid API key\","
                    + "\"type\":\"authentication_error\",\"code\":\"invalid_api_key\"}}")
            .getBytes(StandardCharsets.UTF_8);

    private final GatewayAuthProperties props;
    private volatile Map<String, byte[]> tenantKeyHashes = Map.of();

    @PostConstruct
    public void initializeKeys() {
        if (!props.isEnabled()) {
            tenantKeyHashes = Map.of();
            return;
        }
        if (props.getApiKeys() == null || props.getApiKeys().isEmpty()) {
            throw new IllegalStateException("网关鉴权已启用，但未配置 llm.gateway.auth.api-keys");
        }

        Map<String, byte[]> hashes = new LinkedHashMap<>();
        Set<String> uniqueHashes = new java.util.HashSet<>();
        props.getApiKeys().forEach((tenantId, apiKey) -> {
            if (tenantId == null || tenantId.isBlank()) {
                throw new IllegalStateException("网关 API Key 的 tenantId 不能为空");
            }
            if (apiKey == null || apiKey.isBlank()) {
                throw new IllegalStateException("租户 " + tenantId + " 的网关 API Key 未配置");
            }
            byte[] hash = sha256(apiKey);
            if (!uniqueHashes.add(Base64.getEncoder().encodeToString(hash))) {
                throw new IllegalStateException("不同租户不能配置相同的网关 API Key");
            }
            hashes.put(tenantId, hash);
        });
        tenantKeyHashes = Map.copyOf(hashes);
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String path = exchange.getRequest().getPath().value();
        if (!PROTECTED_PATHS.contains(path)) {
            return chain.filter(exchange);
        }
        if (!props.isEnabled()) {
            exchange.getAttributes().put(TENANT_ATTRIBUTE, "anonymous");
            return chain.filter(exchange);
        }

        String bearer = extractBearer(exchange.getRequest());
        String tenantId = authenticate(bearer);
        if (tenantId == null) {
            return unauthorized(exchange);
        }
        exchange.getAttributes().put(TENANT_ATTRIBUTE, tenantId);
        return chain.filter(exchange);
    }

    private String authenticate(String rawKey) {
        if (rawKey == null) {
            return null;
        }
        byte[] candidate = sha256(rawKey);
        String matchedTenant = null;
        // 不因首个匹配提前退出，减少不同 Key 位置带来的计时差异。
        for (Map.Entry<String, byte[]> entry : tenantKeyHashes.entrySet()) {
            if (MessageDigest.isEqual(candidate, entry.getValue())) {
                matchedTenant = entry.getKey();
            }
        }
        return matchedTenant;
    }

    private String extractBearer(ServerHttpRequest request) {
        String authorization = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (authorization == null || !authorization.regionMatches(true, 0, "Bearer ", 0, 7)) {
            return null;
        }
        String key = authorization.substring(7).trim();
        return key.isEmpty() ? null : key;
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange) {
        var response = exchange.getResponse();
        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        response.getHeaders().set(HttpHeaders.WWW_AUTHENTICATE, "Bearer");
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        return response.writeWith(Mono.just(response.bufferFactory().wrap(UNAUTHORIZED_BODY)));
    }

    private static byte[] sha256(String value) {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM 不支持 SHA-256", e);
        }
    }
}
