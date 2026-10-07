package com.llmgateway.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 网关入口鉴权配置。Key 只用于入口认证，不得出现在模型目录、健康快照或日志中。
 */
@Data
@Component
@ConfigurationProperties(prefix = "llm.gateway.auth")
public class GatewayAuthProperties {

    /** 安全默认：未显式关闭时必须提供至少一个租户 Key。 */
    private boolean enabled = true;

    /** tenantId -> API Key。认证成功后 tenantId 成为限流和缓存隔离的可信身份。 */
    private Map<String, String> apiKeys = new LinkedHashMap<>();
}
