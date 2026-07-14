package com.llmgateway.gateway.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * WebClient 配置类
 *
 * 核心设计要点：
 * 1. 使用 Reactor Netty 的 HttpClient 作为底层连接器
 * 2. 配置连接超时、读写超时，防止下游 LLM 响应过慢导致连接泄漏
 * 3. WebClient 本身是非阻塞的，所有 IO 操作都在 Netty EventLoop 线程上执行
 */
@Configuration
public class WebClientConfig {

    /**
     * 构建全局 WebClient Bean
     *
     * [面试亮点] WebClient 底层使用 Reactor Netty，基于 NIO 多路复用。
     * 相比 RestTemplate 的线程-per-请求模型，单个 EventLoop 线程可同时
     * 处理数千个并发连接，彻底解决大模型长连接场景下的 C10K 问题。
     */
    @Bean
    public WebClient webClient(GatewayProperties props) {
        HttpClient httpClient = HttpClient.create()
            // TCP 连接超时：防止下游服务不可达时无限等待
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, props.getConnectTimeoutMs())
            // 响应超时：流式场景需要较长时间
            .responseTimeout(Duration.ofMillis(props.getReadTimeoutMs()))
            .doOnConnected(conn ->
                conn
                    // 读超时 Handler：在 Netty Pipeline 级别控制，比应用层更精准
                    .addHandlerLast(new ReadTimeoutHandler(props.getReadTimeoutMs(), TimeUnit.MILLISECONDS))
                    // 写超时 Handler
                    .addHandlerLast(new WriteTimeoutHandler(props.getConnectTimeoutMs(), TimeUnit.MILLISECONDS))
            );

        return WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            // 调大 buffer 上限，防止大 JSON 响应体被截断
            .codecs(configurer ->
                configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)
            )
            .build();
    }

    /**
     * Ollama 专用 WebClient Bean（Embedding 调用）
     *
     * [面试亮点] 与主链路 WebClient 区分：Embedding 是短非流式 POST，
     * 用独立的更短读取超时（ollamaReadTimeoutMs=30s），避免 Ollama 卡住时
     * 连接被占满 120s。两个 WebClient 各司其职，超时策略按场景定制。
     *
     * 命名 bean，注入处需用 @Qualifier("ollamaWebClient") 显式指定。
     */
    @Bean("ollamaWebClient")
    public WebClient ollamaWebClient(GatewayProperties props) {
        HttpClient httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, props.getConnectTimeoutMs())
            // Embedding 短请求，用更短响应超时
            .responseTimeout(Duration.ofMillis(props.getOllamaReadTimeoutMs()))
            .doOnConnected(conn ->
                conn
                    .addHandlerLast(new ReadTimeoutHandler(props.getOllamaReadTimeoutMs(), TimeUnit.MILLISECONDS))
                    .addHandlerLast(new WriteTimeoutHandler(props.getConnectTimeoutMs(), TimeUnit.MILLISECONDS))
            );

        return WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .codecs(configurer ->
                configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024)
            )
            .build();
    }
}
