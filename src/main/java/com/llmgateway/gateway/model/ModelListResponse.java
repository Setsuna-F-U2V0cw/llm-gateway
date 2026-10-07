package com.llmgateway.gateway.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * OpenAI 兼容 GET /v1/models。额外字段给前端下拉用，不含 apiKey / targetUrl。
 */
public record ModelListResponse(String object, List<ModelItem> data) {

    public static ModelListResponse of(List<ModelItem> data) {
        return new ModelListResponse("list", data);
    }

    public record ModelItem(
            String id,
            String object,
            @JsonProperty("owned_by") String ownedBy,
            @JsonInclude(JsonInclude.Include.NON_NULL)
            @JsonProperty("request_model") String requestModel,
            @JsonProperty("is_thinking") boolean thinking,
            @JsonProperty("max_context") int maxContext
    ) {}
}
