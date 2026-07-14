package com.llmgateway.gateway.config;

import io.qdrant.client.QdrantClient;
import io.qdrant.client.QdrantGrpcClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Qdrant 向量数据库客户端配置
 *
 * [💎 面试亮点] Qdrant Java 客户端底层基于 gRPC，相比 HTTP REST 接口：
 * - 二进制协议，传输向量数据效率更高（1536 维 float 向量约 6KB）
 * - 长连接复用，避免每次检索建立 TCP 握手的开销
 * - 原生支持异步调用（ListenableFuture），与响应式链路天然适配
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class QdrantConfig {

    private final GatewayProperties props;

    @Bean
    public QdrantClient qdrantClient() {
        log.info("初始化 Qdrant 客户端: {}:{}", props.getQdrantHost(), props.getQdrantPort());
        return new QdrantClient(
                QdrantGrpcClient.newBuilder(props.getQdrantHost(), props.getQdrantPort(), false)
                        .build()
        );
    }
}
