package com.llmgateway.gateway.service;

import com.llmgateway.gateway.config.GatewayProperties;
import com.llmgateway.gateway.model.ModelListResponse;
import com.llmgateway.gateway.model.ModelListResponse.ModelItem;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 已注册模型目录。只暴露 id / 能力字段，不带 apiKey 和 targetUrl。
 */
@Service
@RequiredArgsConstructor
public class ModelCatalogService {

    private final GatewayProperties props;

    public ModelListResponse list() {
        if (props.getModels() == null || props.getModels().isEmpty()) {
            return ModelListResponse.of(List.of());
        }
        List<ModelItem> items = props.getModels().entrySet().stream()
                .map(e -> new ModelItem(
                        e.getKey(),
                        "model",
                        "llm-gateway",
                        e.getValue().getRequestModel(),
                        e.getValue().isThinking(),
                        e.getValue().getMaxContext()))
                .toList();
        return ModelListResponse.of(items);
    }
}
