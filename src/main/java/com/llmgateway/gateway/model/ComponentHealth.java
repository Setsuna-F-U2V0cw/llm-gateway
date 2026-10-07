package com.llmgateway.gateway.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * GET /v1/health/deps 单个依赖探测结果。
 * status: up / down。网关自身 fail-open，依赖挂了仍返回 HTTP 200，由 status 字段表达。
 */
public record ComponentHealth(
        String name,
        String status,
        @JsonProperty("latency_ms") long latencyMs,
        @JsonInclude(JsonInclude.Include.NON_NULL) String detail
) {
    public static ComponentHealth up(String name, long latencyMs, String detail) {
        return new ComponentHealth(name, "up", latencyMs, detail);
    }

    public static ComponentHealth down(String name, long latencyMs, String detail) {
        return new ComponentHealth(name, "down", latencyMs, detail);
    }

    public boolean up() {
        return "up".equals(status);
    }
}
