package com.llmgateway.gateway.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * WebClient 配置类
 *
 * 核心设计要点：
 * 1. 使用 Reactor Netty 的 HttpClient 作为底层连接器
 * 2. 主链路与 Embedding 使用独立 ConnectionProvider，避免长 SSE 占满短请求池
 * 3. WebClient 本身是非阻塞的，所有 IO 操作都在 Netty EventLoop 线程上执行
 */
@Configuration
public class WebClientConfig {

    /**
     * 主链路 WebClient（LLM SSE 透传 + fallback）。
     *
     * [面试亮点] 显式 ConnectionProvider：默认 {@code HttpClient.create()} 池大约
     * maxConnections=500，并发活跃 SSE 超过后会在 pendingAcquire 上排队，表现为 TTFT
     * 飙升或 fallback 风暴，而 EventLoop 本身并未饱和。
     */
    @Bean
    public WebClient webClient(GatewayProperties props) {
        ConnectionProvider provider = ConnectionProvider.builder("llm-primary")
                .maxConnections(props.getWebClientMaxConnections())
                .pendingAcquireMaxCount(props.getWebClientPendingAcquireMaxCount())
                .pendingAcquireTimeout(Duration.ofSeconds(10))
                .maxIdleTime(Duration.ofSeconds(60))
                .maxLifeTime(Duration.ofMinutes(10))
                .evictInBackground(Duration.ofSeconds(30))
                .metrics(true)
                .build();
        HttpClient httpClient = buildHttpClient(
                provider, props.getConnectTimeoutMs(), props.getReadTimeoutMs());
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer ->
                        configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
                .build();
    }

    /**
     * Ollama 专用 WebClient（Embedding / 健康检查）。独立小池，不与 SSE 争抢。
     *
     * 命名 bean，注入处需用 {@code @Qualifier("ollamaWebClient")} 显式指定。
     */
    @Bean("ollamaWebClient")
    public WebClient ollamaWebClient(GatewayProperties props) {
        int max = props.getOllamaWebClientMaxConnections();
        ConnectionProvider provider = ConnectionProvider.builder("llm-ollama")
                .maxConnections(max)
                .pendingAcquireMaxCount(Math.max(max * 2, 400))
                .pendingAcquireTimeout(Duration.ofSeconds(5))
                .maxIdleTime(Duration.ofSeconds(30))
                .maxLifeTime(Duration.ofMinutes(5))
                .evictInBackground(Duration.ofSeconds(30))
                .metrics(true)
                .build();
        HttpClient httpClient = buildHttpClient(
                provider, props.getConnectTimeoutMs(), props.getOllamaReadTimeoutMs());
        return WebClient.builder()
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer ->
                        configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
                .build();
    }

    private static HttpClient buildHttpClient(ConnectionProvider provider,
                                              int connectTimeoutMs,
                                              int ioTimeoutMs) {
        return HttpClient.create(provider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeoutMs)
                .responseTimeout(Duration.ofMillis(ioTimeoutMs))
                .doOnConnected(conn -> conn
                        .addHandlerLast(new ReadTimeoutHandler(ioTimeoutMs, TimeUnit.MILLISECONDS))
                        // 写超时与读超时对齐：大 JSON body 在慢上行上不能用 5s connect 超时卡住
                        .addHandlerLast(new WriteTimeoutHandler(ioTimeoutMs, TimeUnit.MILLISECONDS)));
    }
}
