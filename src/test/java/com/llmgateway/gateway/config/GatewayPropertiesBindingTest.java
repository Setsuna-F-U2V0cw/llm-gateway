package com.llmgateway.gateway.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayPropertiesBindingTest {

    @Test
    void applicationYaml_bindsThinkingModels() throws Exception {
        var environment = new StandardEnvironment();
        var sources = new YamlPropertySourceLoader()
                .load("application.yml", new ClassPathResource("application.yml"));
        sources.forEach(environment.getPropertySources()::addFirst);

        GatewayProperties properties = Binder.get(environment)
                .bind("llm.gateway", Bindable.of(GatewayProperties.class))
                .orElseThrow(() -> new AssertionError("llm.gateway 配置未绑定"));

        assertThat(properties.getModels().get("deepseek-v4-flash").isThinking()).isFalse();
        assertThat(properties.getModels().get("deepseek-v4-flash-thinking").isThinking()).isTrue();
        assertThat(properties.getModels().get("deepseek-v4-pro").isThinking()).isTrue();
        assertThat(properties.getModels().get("qwen3-1-7b").isThinking()).isFalse();
        assertThat(properties.getModels().get("qwen3-1-7b").getRequestModel()).isEqualTo("qwen3:1.7b");
        assertThat(properties.getRouting().get("code")).isEqualTo("deepseek-v4-flash");
        assertThat(properties.getRouting().get("default")).isEqualTo("deepseek-v4-flash");
        assertThat(properties.getRouting().get("chitchat")).isEqualTo("qwen3-1-7b");
        assertThat(properties.getTtftSloMs()).isEqualTo(3000L);
        assertThat(properties.getTtftSloThinkingMs()).isEqualTo(10000L);
        assertThat(properties.getFallback().getTargetUrl()).contains("11435");
        assertThat(properties.getTpmCapacity()).isEqualTo(100000);
        assertThat(properties.getSemanticCacheThreshold()).isEqualTo(0.95f);
        assertThat(properties.getSemanticCacheTtlSeconds()).isEqualTo(86400);
        assertThat(properties.getEmbedPipelineTimeoutMs()).isEqualTo(3000L);
        assertThat(properties.getWebClientMaxConnections()).isEqualTo(4000);
        assertThat(properties.getOllamaWebClientMaxConnections()).isEqualTo(200);
        assertThat(properties.resolveTpmBucketTtlSeconds()).isEqualTo(180);
    }
}
