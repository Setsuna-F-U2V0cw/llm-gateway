package com.llmgateway.gateway.model;

/**
 * 意图分类枚举（阶段四 Router）
 *
 * 6 个具体意图 + 1 个 DEFAULT 兜底（embedding 分类低置信或规则未命中时用）。
 * 名称小写后作为 routing 表的 key 查 modelId。
 */
public enum Intent {
    CODE,
    TRANSLATION,
    MATH,
    REASONING,
    WRITING,
    CHITCHAT,
    DEFAULT
}
