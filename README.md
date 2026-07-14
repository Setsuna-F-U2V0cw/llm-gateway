# LLM Gateway

> 基于 **Java 21 + Spring Boot 3.x + WebFlux + Netty** 构建的高性能大模型 API 网关。

[![Java](https://img.shields.io/badge/Java-21-%23ED8B00?logo=openjdk)](https://www.java.com)
[![Spring Boot](https://img.shields.io/badge/Spring_Boot-3.2-%236DB33F?logo=spring)](https://spring.io/projects/spring-boot)

LLM Gateway 定位在"多个客户端"与"多个 LLM 提供商"之间，解决每个 LLM 客户端都需要但不应各自重造的横切关注点：**限流、缓存、熔断、智能路由**。

```
Client (Spring AI / LangChain / curl)
       ↓
  ┌── LLM Gateway ──┐
  │ 限流 → 缓存 → 路由 → 熔断 │
  └─────────────────┘
       ↓
  DeepSeek / OpenAI / Ollama / 任意 OpenAI 兼容 API
```

---

## 功能特性

- **<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='16' height='16' style='vertical-align:middle' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><polygon points='13 2 3 14 12 14 11 22 21 10 12 10'/></svg> 非阻塞 SSE 流式透传** — 基于 Netty EventLoop 替代 Tomcat 线程每请求模型。网关不拼装完整响应，逐块零拷贝转发下游 LLM 输出到客户端，单机支持万级并发长连接。
- **<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='16' height='16' style='vertical-align:middle' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><rect x='3' y='11' width='18' height='11' rx='2' ry='2'/><path d='M7 11V7a5 5 0 0 1 10 0v4'/></svg> TPM 原子令牌桶限流** — 摒弃传统每分钟请求数 (RPM) 限流，基于 JTokkit 本地计算 Token 消耗 + Redis Lua 原子脚本实现每分钟 Token 数 (TPM) 预扣与结算。Redis 不可用时自动放行。
- **<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='16' height='16' style='vertical-align:middle' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><ellipse cx='12' cy='5' rx='9' ry='3'/><path d='M21 12c0 1.66-4 3-9 3s-9-1.34-9-3'/><path d='M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5'/></svg> 语义缓存** — 通过 Ollama Embedding + Qdrant 向量数据库做余弦相似度检索 (threshold 0.95)，命中时用 `delayElements` 伪装 SSE 流式回放。缓存命中首字延迟 ~0.24s（对比未命中 ~5.67s），提升约 **23×**。
- **<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='16' height='16' style='vertical-align:middle' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><path d='M12 2l7 4.5v5.5c0 4.5-3.5 8.5-7 9.5-3.5-1-7-5-7-9.5V6.5L12 2z'/></svg> TTFT 熔断 + 策略路由** — 首字超时 (Time To First Token) 监控：`Flux.timeout(Mono.delay(SLO), perChunk)` 精确控制 role 帧重置/content 帧解除计时，超时后熔断器记失败并切换回退通道。智能路由按"意图分类 (Embedding 余弦 argmax) + Token 长度"选择目标模型，配置驱动无需改代码。
- **<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' width='16' height='16' style='vertical-align:middle' fill='none' stroke='currentColor' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'><polyline points='23 4 23 10 17 10'/><path d='M20.49 15a9 9 0 1 1-2.12-9.36L23 10'/></svg> 无感回退** — 主通道超时/熔断/错误时自动切换到本地 Ollama 模型 (qwen3:1.7b)，`contentStarted` 标志保证只在首内容帧前切换，避免半截 A + 半截 B。双重失败返回 SSE 错误帧而非 HTTP 500。

---

## 快速开始

### 前提条件

- JDK 21+, Maven 3.8+
- Docker（用于运行 Redis 和 Qdrant）
- Ollama: `ollama pull bge-m3`（Embedding 模型，必须）
- Ollama: `ollama pull qwen3:1.7b`（可选，用于本地回退）

### 启动

```bash
# 1. 基础设施
docker-compose up -d

# 2. 网关
LLM_API_KEY=your-deepseek-api-key mvn spring-boot:run

# 3. 打开前端控制台
open http://localhost:8080
```

> 不配 `LLM_API_KEY` 也能启动——语义缓存、智能路由、本地回退均可独立工作，只是 DeepSeek 主通道不可用。

### 使用 curl 测试

```bash
curl -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: test-user' \
  -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"用一句话介绍 Java"}],"stream":true}'
```

响应头 `X-Cache-Hit: true` 表示返回的是缓存结果。

---

## 架构

### 请求生命周期

```
Request
  │
  ▼
Token 预估 (JTokkit, < 1ms)
  │
  ▼
Redis Lua 令牌桶限流 (TPM)
  │
  ▼
Embedding + Qdrant 语义检索 (cosine > 0.95)
  │
  ├── 命中 → delayElements 伪装 SSE 流
  │            X-Cache-Hit: true, 首字延迟 ~0.24s
  │
  └── 未命中 → Router (意图 + Token 长度)
                 │
                 ▼
          breaker.decorate( primary.timeout(首字SLO) )
              ├ breaker open → CallNotPermitted
              ├ 首字超时 → TimeoutException
              └ 下游错误
                    │
                    ▼
              onErrorResume → fallback (Ollama)
                    │
                    ▼
              doOnComplete: Token 结算 + 异步写缓存
```

### 关键设计决策

| 决策 | 做法 |
|------|------|
| SSE 透传 | `bodyToFlux(String.class)` + `Flux<String>`，逐帧转发，零内存积压 |
| 缓存命中判定 | `Mono<Optional<Flux<String>>>`——订阅前决出命中/未命中，据此写 `X-Cache-Hit` 响应头 |
| Embedding 复用 | 顶部算一次向量，向下分发给 Qdrant 检索、意图分类、缓存落库 |
| TTFT 计时 | `Flux.timeout(Mono.delay(SLO), perChunk)`——role 帧重置、content 帧解除 |
| 熔断器接法 | `CircuitBreakerOperator` 在内、`onErrorResume(→fallback)` 在外——fallback 成功不洗白失败计数 |

---

## 性能基准

> 实测 2026-07-12，模拟 LLM 首字延迟 4-6s

| 场景 | 指标 | 结果 |
|----------|--------|--------|
| 语义缓存首字延迟 | 命中对比未命中 | **0.24s vs 5.67s → 23×** |
| 熔断回退 | 主通道不可用时 | **10/10 HTTP 200 (100%)** |
| 回退首字延迟 | 热模型对比排队 | 0.56s – 28.51s (单点瓶颈，见已知限制) |

---

## API 参考

| 接口 | 方法 | 描述 |
|----------|--------|-------------|
| `/v1/chat/completions` | POST | 流式聊天 (SSE, OpenAI 兼容格式) |
| `/v1/health` | GET | 健康检查 |

### Request Format

```json
{
  "model": "deepseek-v4-flash",
  "messages": [{ "role": "user", "content": "Hello" }],
  "stream": true
}
```

### 请求头

| 请求头 | 说明 |
|--------|-------------|
| `X-User-Id` | 多租户限流隔离键，默认 `anonymous` |
| `X-Cache-Hit` | `true` / `false`，由后端在响应头写入 |

---

## 配置

关键配置项 (`application.yml` → `llm.gateway.*`)：

| 配置项 | 默认值 | 说明 |
|-----|---------|-------------|
| `models` | — | 模型注册表: `{model-id: {target-url, api-key, is-thinking, max-context}}` |
| `routing` | — | 路由表: `{intent: model-id}`，含 `default` 兜底 |
| `fallback` | Ollama qwen3:1.7b | 主通道不可用时的备用通道 |
| `ttft-slo-ms` | `3000` | 首字超时阈值 (ms)，非思考模型 |
| `ttft-slo-thinking-ms` | `10000` | 首字超时阈值 (ms)，思考模型 |
| `tpm-capacity` | `100000` | 令牌桶容量 (每用户每分钟 Token) |
| `semantic-cache-threshold` | `0.95` | 余弦命中阈值 |
| `circuit-breaker` | — | 熔断器滑动窗口参数 |

完整配置见 [`src/main/resources/application.yml`](src/main/resources/application.yml)。

---

## 项目结构

```
src/main/java/com/llmgateway/gateway/
├── LlmGatewayApplication.java       # Spring Boot 入口
├── config/
│   ├── GatewayProperties.java        # 全量配置 (模型、路由、限流、缓存)
│   ├── QdrantConfig.java             # Qdrant gRPC 客户端
│   └── WebClientConfig.java          # 两个 WebClient bean (主链路 / Embedding)
├── controller/
│   └── ChatCompletionController.java # POST /v1/chat/completions + GET /v1/health
├── model/
│   ├── ChatRequest.java              # OpenAI 兼容请求体
│   ├── Intent.java                   # 意图枚举
│   └── Route.java                    # 路由结果
└── service/
    ├── LlmProxyService.java          # 主链路编排
    ├── TokenCountService.java        # JTokkit Token 计算
    ├── RateLimiterService.java       # Redis Lua 令牌桶
    ├── EmbeddingService.java         # Ollama Embedding
    ├── VectorStoreService.java       # Qdrant 检索/写入
    ├── SemanticCacheService.java     # 缓存命中判定 + 伪装流
    ├── IntentClassifier.java         # 意图分类 (Embedding / 规则)
    ├── Router.java                   # 策略路由 (意图 + 长度)
    └── CircuitBreakerService.java    # 按 model 粒度熔断器
```

---

## 已知不足（未来改进）

| 限制 | 影响 | 改进 |
|-----------|--------|-------------|
| 主备同节点部署 | 冷启动双倍超时 | 差异化回退路径 |
| 缺乏监控与可观测性 | 缓存命中率需 grep 日志 | 接入 Micrometer + Prometheus |
| 无身份认证 | X-User-Id 可被伪造 | 认证代理（Kong / JWT）|
| 熔断器状态为本地 | 多副本间状态不一致 | Redis PubSub 同步 |
| 无缓存投毒防护 | 恶意回答可能污染缓存 | 输出过滤 |

