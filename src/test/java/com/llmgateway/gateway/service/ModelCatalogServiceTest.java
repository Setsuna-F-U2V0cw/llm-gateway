package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ModelListResponse;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ModelCatalogServiceTest {

    @Test
    void list_omitsSecretsAndExposesCapabilities() {
        GatewayProperties props = new GatewayProperties();
        GatewayProperties.ModelConfig cfg = new GatewayProperties.ModelConfig();
        cfg.setTargetUrl("https://secret.example");
        cfg.setApiKey("sk-secret");
        cfg.setThinking(true);
        cfg.setMaxContext(128000);
        cfg.setRequestModel("qwen3:1.7b");
        props.setModels(Map.of("qwen3-1-7b", cfg));

        ModelCatalogService catalog = new ModelCatalogService(props);
        var list = catalog.list();

        assertThat(list.object()).isEqualTo("list");
        assertThat(list.data()).hasSize(1);
        var item = list.data().get(0);
        assertThat(item.id()).isEqualTo("qwen3-1-7b");
        assertThat(item.requestModel()).isEqualTo("qwen3:1.7b");
        assertThat(item.thinking()).isTrue();
        assertThat(item.maxContext()).isEqualTo(128000);
        assertThat(item.toString()).doesNotContain("sk-secret");
        assertThat(item.toString()).doesNotContain("secret.example");
    }

    @Test
    void list_emptyWhenNoModels() {
        ModelCatalogService catalog = new ModelCatalogService(new GatewayProperties());
        assertThat(catalog.list().data()).isEmpty();
    }

    @Test
    void list_includesAllRegisteredIds() {
        GatewayProperties props = new GatewayProperties();
        props.setModels(Map.of(
                "deepseek-v4-flash", new GatewayProperties.ModelConfig(),
                "deepseek-v4-pro", new GatewayProperties.ModelConfig()));
        var ids = new ModelCatalogService(props).list().data().stream()
                .map(ModelListResponse.ModelItem::id)
                .toList();
        assertThat(ids).containsExactlyInAnyOrder("deepseek-v4-flash", "deepseek-v4-pro");
    }
}
