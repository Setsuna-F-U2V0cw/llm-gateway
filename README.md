# AI-Native 高性能多租户 LLM API 网关 (LLM Gateway)

基于 **Java 21 + Spring Boot 3.x + WebFlux + Netty** 响应式架构构建的高性能大模型 API 网关，旨在解决 LLM 调用中的**长连接并发耗竭、按 Token 计费防刷、请求高延迟及调用成本高昂**等企业级痛点。

---

## 项目技术栈

| 层次 | 技术选型 |
|------|----------|
| **核心框架** | Java 21, Spring Boot 3.x, Spring WebFlux, Project Reactor, Netty |
| **数据中间件** | Redis 7 (Lettuce 响应式客户端) |
| **AI 基础设施** | JTokkit (Token 极速计算), Qdrant (向量数据库), Ollama bge-m3 (本地 Embedding 模型, 1024 维) |
| **高可用组件** | Resilience4j (熔断、降级与超时控制) |
| **可观测性** | Micrometer + Spring Boot Actuator + Prometheus |

---

## 快速演示

### 浏览器前端

启动后访问 **http://localhost:8080/**——一个同源托管的 Vue 3 控制台：

| Tab | 功能 |
|-----|------|
| 聊天 | 流式对话；模型下拉（自动=意图路由 / 已注册 model 显式选用）；缓存命中徽章 |
| 诊断 | 并行探测 Redis / Qdrant / Ollama（`GET /v1/health/deps`） |
| 配置 | 租户 API Key（仅存 sessionStorage）+ 运行时参数 + 已注册模型 |

### 命令行测试

```bash
# 基础流式聊天
curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $GATEWAY_DEMO_API_KEY" \
  -d '{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"用一句话介绍 Java"}],"stream":true}' \
  --no-buffer
```

预期输出（逐帧推送）：

```
data: {"choices":[{"delta":{"role":"assistant"}}]}
data: {"choices":[{"delta":{"content":"Java"}}]}
data: {"choices":[{"delta":{"content":"是一种"}}]}
...
data: [DONE]
```

响应头会包含 `X-Cache-Hit: true`（命中缓存）或 `false`（透传）。

### 差异化 Fallback 验证

```bash
# 默认 fallback 使用第二个独立 Ollama 端口，避免与 11434 主通道同一进程
ollama pull qwen3:1.7b
OLLAMA_HOST=127.0.0.1:11435 ollama serve
GATEWAY_DEMO_API_KEY=your-gateway-key LLM_API_KEY=your-key mvn spring-boot:run
```

Prometheus 指标（受同一 Gateway Key 保护）：

```bash
curl -H "Authorization: Bearer $GATEWAY_DEMO_API_KEY" \
  http://localhost:8080/actuator/prometheus
```

---

## 术语说明

| 缩写 | 全称 | 说明 |
|------|------|------|
| SSE | Server-Sent Events | 服务端推送事件，逐帧推送文本的流式协议 |
| TPM | Tokens Per Minute | 每分钟 Token 数，限流的计量单位 |
| RPM | Requests Per Minute | 每分钟请求数，传统限流单位 |
| TTFT | Time To First Token | 首 Token 延迟——从发出请求到收到第一个有效字的耗时 |
| TTFB | Time To First Byte | 首字节延迟——从发出请求到收到第一个字节的耗时 |
| SLO | Service Level Objective | 服务等级目标，此处指 TTFT 超时阈值 |
| C10K | Connecting 10,000 Clients | 万级并发连接问题 |

---

## 核心请求生命周期

```
Authorization: Bearer API Key → 映射可信 tenantId
    │
    ▼
Token 预估计算 (JTokkit, < 1ms)
    │
    ▼
Redis Lua 分布式限流 (TPM 令牌桶)
    │
    ▼
embed(prompt) 顶部算一次 ──→ 三处复用
    │
    ▼
Semantic Cache 语义检索 (Qdrant, cosine > 0.95)
    │
    ├── 命中 ──→ delayElements 伪装流 ⚡
    │               X-Cache-Hit: true, 首字延迟 ~0.24s
    │
    └── 未命中 ──→ Router 策略模式
                      │
                      ├── IntentClassifier (复用 embedding，零额外调用)
                      ├── LengthGuard (超 maxContext 拒/覆盖)
                      └── Route {targetUrl, model, breakerName}
                            │
                            ▼
                    breaker.decorate( primary.timeout(首字SLO, perChunk) )
                        ├─ breaker open → CallNotPermitted ─┐
                        ├─ 首字延迟 > SLO  → TimeoutException ─┤
                        └─ 下游错误 ────────────────────────┤
                                                           ▼
                              .onErrorResume → fallbackFlux (Ollama qwen3:1.7b)
                                  ├─ 成功 → 透传
                                  └─ 也挂 → 错误帧
                            │
                            ▼
                    doFinally: Token 结算（complete/cancel/error）
                    doOnComplete: 异步写缓存（tenant_id + 原始 model）
```

---

## 性能基准测试

> 实测数据（2026-07-12，假下游模拟 LLM 首字延迟 4-6s）

| 场景 | 指标 | 结果 |
|------|------|------|
| **P1 — 语义缓存首字延迟** | 缓存命中 vs 未命中 | **0.24s vs 5.67s → 提升 23×** 🚀 |
| **P2 — 熔断 Fallback** | 主通道不可用时的成功率 | **10/10 HTTP 200 (100%)** ✅ |
| **P2 — Fallback 首字延迟** | 热模型 vs 排队 | 0.56s – 28.51s（单点容量瓶颈，见局限） |

---

## 实现进度

| 阶段 | 状态 | 核心功能 |
|------|------|----------|
| 阶段一：WebFlux 极速透传通道 | ✅ 已完成 | Netty + WebFlux SSE 零拷贝透传，解决万级并发连接 (C10K) |
| 阶段二：TPM 细粒度分布式限流 | ✅ 已完成 | JTokkit 本地计算 + Redis Lua 令牌桶 + 多退少补结算 |
| 阶段三：语义缓存 | ✅ 已完成 | Qdrant 向量检索 + Ollama bge-m3 Embedding + delayElements 伪装流；端到端验证通过 (2026-07-06) |
| 阶段四：熔断、路由与 Fallback | ✅ 已完成 | Resilience4j 首字延迟熔断 + 策略模式意图路由 + 差异化 fallback；14 个单元测试类（含 Metrics / Auth / Health / Binding） |

---

## 项目结构

```
src/main/
├── java/com/llmgateway/gateway/
│   ├── LlmGatewayApplication.java              # Spring Boot 启动入口
│   ├── config/
│   │   ├── GatewayProperties.java               # 模型/路由/限流/缓存/原型重试配置
│   │   ├── GatewayAuthProperties.java           # tenantId → API Key 鉴权配置
│   │   ├── QdrantConfig.java                    # Qdrant gRPC 客户端（非阻塞，6334 端口）
│   │   └── WebClientConfig.java                 # 两个 WebClient bean（主链路 120s / Embedding 30s）
│   ├── security/
│   │   └── GatewayAuthenticationFilter.java     # Bearer Key 鉴权 + 可信 tenantId 注入
│   ├── controller/
│   │   └── ChatCompletionController.java        # POST /v1/chat/completions + GET /v1/health|/health/deps|/models
│   ├── model/
│   │   ├── ChatRequest.java                     # OpenAI 标准格式请求体 DTO
│   │   ├── Intent.java                          # 意图枚举（6 意图 + DEFAULT）
│   │   ├── Route.java                           # 路由决策结果（targetUrl/model/apiKey/isThinking）
│   │   ├── ComponentHealth.java                 # /health/deps 单组件状态
│   │   ├── DependencyHealth.java                # 依赖诊断聚合
│   │   └── ModelListResponse.java               # GET /v1/models（不含密钥）
│   └── service/
│       ├── HealthService.java                   # Redis/Qdrant/Ollama 并行探测
│       ├── ModelCatalogService.java             # /v1/models 目录（不含密钥）
│       ├── GatewayMetrics.java                  # TTFT/缓存/fallback/SSE/Token 低基数指标
│       ├── LlmProxyService.java                 # ⭐ 主链路编排（限流→embed→缓存→路由→首字超时→fallback→结算）
│       ├── TokenCountService.java               # JTokkit 本地 Token 计算
│       ├── RateLimiterService.java              # Redis Lua 原子令牌桶 + fail-open + 多退少补
│       ├── EmbeddingService.java                # Ollama /api/embed 调用 + L2 归一化
│       ├── VectorStoreService.java              # Qdrant gRPC 检索/写入 + tenant/model filter
│       ├── SemanticCacheService.java            # 缓存命中判定 + delayElements 伪装流 + 租户隔离写入
│       ├── IntentClassifier.java                # 原型原子发布 + 指数退避自愈 + 规则 fallback
│       ├── Router.java                          # 策略模式路由（意图→路由表→LengthGuard→Route）
│       └── CircuitBreakerService.java           # 按 model 粒度 named breaker + 首字延迟计失败
├── resources/
│   ├── application.yml                          # 全量配置（模型/路由/fallback/首字SLO/缓存/限流）
│   ├── static/index.html                        # Vue 3 CDN 前端控制台
│   └── scripts/
│       ├── token_bucket.lua              # 令牌桶 Lua 脚本（原子预扣）
│       └── token_bucket_settle.lua       # 结算 Lua（多退少补，桶过期 no-op）
```

---

## 阶段详解

### 阶段一：WebFlux 极速透传通道 ✅

**核心目标：** 解决传统 Tomcat `Thread-per-request` 模型在长连接场景下的线程枯竭问题。

| 方案 | 线程模型 | 1000 并发 × 30s 连接 | 内存占用 |
|------|---------|---------------------|---------|
| Tomcat | 1 请求 = 1 线程 | 1000 线程 | ~1GB 栈空间 |
| **WebFlux + Netty** | EventLoop 事件驱动 | 8 线程（4 核×2） | ~64MB |

Controller 返回 `Mono<ResponseEntity<Flux<String>>>`：外层在缓存命中判定后写入 `X-Cache-Hit`，内层 `Flux<String>` 由 Spring WebFlux 以 `data: ...\n\n` 推送。Netty 使用 Chunked Transfer Encoding 逐块发送，无需在内存中积累完整响应（零拷贝透传）。

### 阶段二：每分钟 Token 数 (TPM) 细粒度分布式限流 ✅

摒弃传统每分钟请求数 (RPM) 限流，实现适配大模型特性的 **每分钟 Token 数 (TPM)** 预扣费与防刷单：

```
JTokkit 计算预估 Token (<1ms, 零网络)
  → Redis Lua 原子令牌桶（非阻塞）
    → 通过 → 预扣 → 转发
    → 拒绝 → 429 Too Many Requests
      → doFinally → 结算（多退少补，含取消）
```

**原子性保障**：Lua 脚本在 Redis 单线程中原子执行，消除 GET+SET 两步操作的竞态条件。Redis 不可用时 `onErrorReturn(true)` 放行（fail-open）。

### 阶段三：语义缓存 ✅

**Embedding + Qdrant 语义检索**——用"相似度"替代"精确匹配"：

```
embed(prompt) → Qdrant 余弦检索 (threshold=0.95)
  ├─ 命中 → delayElements(20ms) 伪装流
  │          首字延迟: ~0.24s (vs 未命中 ~5.67s)
  │          前端零改动：命中/未命中都是 SSE 流
  └─ 未命中 → 走路由 + 真实 LLM 调用
                → doOnComplete 异步写缓存（tenant_id + model）
```

| 维度 | 精确 KV 缓存 | 语义缓存（本项目） |
|------|-------------|------------------|
| 命中条件 | prompt 完全相同 | 语义 cosine > 0.95 |
| 典型命中率 | ~5-10% | ~30-50% |
| Embedding | 不需要 | Ollama bge-m3 (1024d，本地) |

### 阶段四：熔断、路由与 Fallback ✅

**三层韧性架构**：

```
主通道 (DeepSeek)
  → 首字超时 (3s 无首 content 或 reasoning → 超时异常)
    → 熔断器记失败 (窗口 > 50% → 断开)
      → fallback 通道 (Ollama qwen3:1.7b)
        → 双重失败 → 错误帧
```

**智能路由**（缓存未命中时介入）：
- **意图分类**：原型完整后原子发布；Ollama 启动未就绪时指数退避后台重试，请求走规则 fallback；恢复后自动切回 embedding 分类
- **长度守卫**：超所选模型 `maxContext` 时覆盖到大窗口模型（`deepseek-v4-pro`）；仍超所有窗口则在写 SSE 头之前返回 **HTTP 413**
- **配置驱动**：加新模型只需改 YAML，`/v1/models` 与前端下拉自动同步，**0 行 Java/前端代码**

**技术亮点**：
- `Flux.timeout(Mono.delay(首字SLO), perChunk)`——role 帧重置计时、content 或 reasoning 帧解除、精准首字超时控制
- `contentStarted` 标志——**只在首个 content 或 reasoning 帧之前切 fallback**，避免半截 A + 半截 B
- 熔断器在内、`onErrorResume` 在外——fallback 成功不洗白 primary 的失败计数
- 关掉 Resilience4j 自带的慢调用判定（首字超时由业务的 timeout 接管），踩了 `Duration.ofMillis(Long.MAX_VALUE)` 溢出坑
- Micrometer 记录 P95/P99 TTFT、缓存命中、fallback 成功率、SSE 活跃数及 Token 结算；Prometheus 从受保护端点抓取
- fallback 默认指向第二个 Ollama 实例（11435），也可通过环境变量切到另一云提供商；运行时拒绝与主通道完全相同的目标

---

## 已知局限

> 知道项目边界，说明知道改进方向

| 局限 | 影响 | 改进方向 |
|------|------|----------|
| **默认 fallback 仍需部署第二实例** | 11435 未启动时备用通道不可用 | 独立 Ollama 实例或配置不同云提供商 |
| **尚无现成 Grafana Dashboard** | 指标已导出但需自行写查询和告警 | 增加 Dashboard JSON 与 SLO 告警规则 |
| **API Key 注册表来自静态配置** | 大规模租户增删需重启 | 后续接数据库 / 配置中心并只保存 Key 哈希 |
| **熔断器状态单机内存** | 多副本时熔断不一致 | Redis PubSub 同步 / 集中式计数器 |
| **无缓存防中毒** | 恶意回答可被缓存扩散 | 输出过滤 + 内容审核 API |
| **网关错误帧不补 `[DONE]`** | 双重失败等路径 `Flux.just({"error":...})` 后 complete | 前端以 `parsed.error` 终止；缓存命中 / 下游成功仍有 `[DONE]` |

---

## 快速启动

### 前置条件

- JDK 21+, Maven 3.8+
- Docker（启动 Redis + Qdrant）
- Ollama（Embedding 用）：`ollama pull bge-m3`
- Ollama（Fallback 用，可选）：`ollama pull qwen3:1.7b`

### 启动

```bash
# 1. 启动基础设施
docker-compose up -d

# 2. 启动网关
GATEWAY_DEMO_API_KEY=your-gateway-key \
LLM_API_KEY=your-deepseek-key \
FALLBACK_TARGET_URL=http://localhost:11435 \
mvn spring-boot:run

# 3. 打开前端
open http://localhost:8080
```

> 鉴权默认启用，`GATEWAY_DEMO_API_KEY` 为空会 fail-fast 拒绝启动；不配 `LLM_API_KEY` 仍可使用语义缓存、路由和本地 fallback。

---

## 简历亮点

1. **【架构重构】** 利用 WebFlux + Netty 取代 Tomcat 阻塞模型，零拷贝透传流式数据，彻底解决大模型长响应导致的网关线程枯竭瓶颈
2. **【高阶并发】** 基于 Redis Lua 脚本与 `ReactiveRedisTemplate` 结合，设计非阻塞令牌桶限流，解决大模型按 Token 计费场景下的高并发超卖与资损难题
3. **【AI-Native】** 创新引入语义缓存（Semantic Cache），向量数据库检索结合 `delayElements` 算子伪装流式输出，基准测试首字延迟提升 **23×**（5.67s → 0.24s）
4. **【高可用基建】** 基于 Resilience4j 实现网关层的首字超时 (Time To First Token) 监控与无感平滑熔断，熔断 Fallback 成功率 **100%**（10/10），引入策略模式进行成本感知智能路由

---

## 面试 Q&A

行号级整合底稿：**[`docs/interview-qa.md`](docs/interview-qa.md)**（17 章共 71 组问答 + 12 条弱点评估，每组含追问链 / 源码行号 / 考察点 / 常见错误 / 追问应对）。

历史 61 题分册仍在 [`docs/interview-qa/`](docs/interview-qa/)，仅作对照；过时说法以整合稿与源码为准。
