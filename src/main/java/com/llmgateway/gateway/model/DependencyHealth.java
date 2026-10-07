package com.llmgateway.gateway.model;

import java.util.List;
import java.util.Map;

/**
 * GET /v1/health/deps 聚合结果。
 * status=up 三件套都可达；status=degraded 至少一个挂了。
 * HTTP 始终 200：存活探针继续用 GET /v1/health 纯文本。
 */
public record DependencyHealth(
        String status,
        List<ComponentHealth> components,
        Map<String, Object> gateway
) {}
