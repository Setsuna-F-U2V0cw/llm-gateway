# LLM Gateway 端到端测试命令

> 更新时间：2026-07-12
> 前置条件：Java 21, Maven, Docker, Ollama（bge-m3）, DeepSeek API Key

---

## 环境启动

```bash
# 1. Ollama（embedding 模型）
ollama serve                                            # 后台启动 Ollama 服务
ollama pull bge-m3                                      # 拉取 embedding 模型（1024 维，FP16 ~1.8GB）
ollama list                                             # 确认 bge-m3 已在

# 2. Docker 基础设施
docker-compose up -d                                    # 启动 Redis(6379) + Qdrant(6333/6334)

# 3. 首次启动或切换 embedding 模型后，需重建 Qdrant collection（维度匹配）
#    删除后重启网关，initCollection 自动按当前 dimension 重建
curl -X DELETE http://localhost:6333/collections/llm_cache
# 预期: {"result":true,"status":"ok","time":0.0}

# 4. 启动网关
LLM_API_KEY=你的DeepSeek_API_Key mvn spring-boot:run

# 启动时日志关键确认:
#   - VectorStoreService.initCollection 以 size=N（当前 1024）创建 collection
#   - 监听在 8080 端口
#   - QdrantClient 连接成功
```

---

---
## 建议验证顺序

建议按以下**渐进式**顺序执行，从基础链路开始逐层叠加，便于定位问题：

| 顺序 | 测试 | 验证什么 | 快速验证（5 分钟） |
|------|------|----------|:---:|
| ① | 1.1 → 1.2 → 1.3 | 基础 SSE 透传，确认网关本身正常运行 | ✅ |
| ② | 3.1 → 3.2 → 3.3 | 语义缓存未命中 / 命中 / 相似命中 | ✅ |
| ③ | 4.3 策略路由 | 意图分类+路由映射正确 | ✅ |
| ④ | 4.1 → 4.2 TTFT 熔断+Fallback | 主通道挂时平滑切换 | ⏳（需改配置重启） |
| ⑤ | 2.1 → 2.2 TPM 限流 | 令牌桶并发限流+多租户隔离 | ⏳（需调配置+并行脚本）|

> 快速验证（✅）可在一次 `mvn spring-boot:run` 内连续跑完，无需重启。
> ⏳ 标记的测试需改 `application.yml` 配置并重启网关。

---

## Stage 1：SSE 透传 — 验证核心 SSE 流式透传

**目标**：确认 WebFlux + Netty 零拷贝将下游 LLM 的 SSE 流直接透传回客户端，
每条 chunk 格式正确，末尾带 `[DONE]` 终止信号。

### 测试 1.1：基础 SSE 流式透传

```bash
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: test-user' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"用一句话介绍 Java"}],"stream":true}'
```

| 检查点 | 期望 | 排查 |
|--------|------|------|
| HTTP 状态码 | `200 OK` | 检查 API Key / 网络 |
| Content-Type | `text/event-stream;charset=UTF-8` | Controller 注解 |
| SSE 格式 | `data: {"choices":[{"delta":{"content":"..."}}]}\n\n` | 每条 `data:` 一行，空行分隔 |
| 末尾 | `data: [DONE]\n\n` | OpenAI 标准终止符 |
| 响应头 | `X-Cache-Hit: true/false` | §3 契约 |
| 无 buffer | 输出逐字到达 | `--no-buffer` 或 `-N` |

### 测试 1.2：错误 chunk 格式

```bash
# 用错误 API Key 触发下游 401
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: test-user' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

| 检查点 | 期望 |
|--------|------|
| 状态码 | `200 OK`（网关不因下游错而 500） |
| SSE body | `data: {"error": "..."}\n\n`（错误包装在 SSE 帧内） |
| 末尾 | `data: [DONE]\n\n` |

### 测试 1.3：非流式请求被强制转流式

```bash
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"你好"}],"stream":false}'
```

| 检查点 | 期望 |
|--------|------|
| 响应 | 仍然是 SSE 流式，不受 `stream: false` 影响 |

---

## Stage 2：TPM 限流 — 验证 Redis Lua 令牌桶

**目标**：确认 JTokkit 本地预估 Token → Redis Lua 原子令牌桶预扣费，
超限返回 429，超额部分在 doOnComplete 多退少补。

### 测试 2.1：基础限流（调小容量 + 并发触发）

```yaml
# 临时修改 application.yml
tpm-capacity: 120
tpm-refill-rate-per-second: 1
```

> 容量 120、每秒只补 1 token：中文 prompt 的预估消耗约 50~55 token，桶满时够过 2 个，
> 后续请求因 tokens < cost 全部 429。refill=1 保证请求间隔内桶不会回满。
> **必须并发**：顺序 for 循环每个 curl 等 SSE 流结束才发下一个（间隔 5~15s），
> 桶早已按 refill=1 补回，永远看到满桶全 200。

```bash
# 并发测试脚本 test_llm_gateway.sh（已包含在仓库）
# 5 个 curl 同时 fork，在微秒级内全部到达 Redis
for i in $(seq 1 5); do
  (curl -s -o /dev/null -w "Request $i: %{http_code} | X-Cache-Hit: %{header{X-Cache-Hit}}\n" \
    -X POST http://localhost:8080/v1/chat/completions \
    -H 'Content-Type: application/json' \
    -H 'X-User-Id: limiter-test' \
    -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"请你介绍一下JavaScript编程语言"}],"stream":true}'
  ) &
done
wait
```

```bash
# 跑前清 Redis 保证桶从满开始
docker exec llm-gateway-redis redis-cli FLUSHALL
./test_llm_gateway.sh
```

| 检查点 | 期望 |
|--------|------|
| 通过的个数 | 恰好 2 个 200（容量 120 ÷ 预估 ~55 ≈ 2.18） |
| 其余 | `HTTP 429`（TPM 超限，请稍后重试） |
| 哪两个通过 | **不确定**（并发竞争，Redis 单线程排队，每次跑的编号不同——正常现象） |

> 验证结果（2026-07-06）：`tpm-capacity: 120` + `refill-rate: 1`，并发 5 发，恰好 2 个 200、3 个 429 ✓

验证完记得改回：`tpm-capacity: 100000` + `tpm-refill-rate-per-second: 1667`

### 测试 2.2：多租户隔离

```bash
for i in $(seq 1 5); do
  echo "user-a: $(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8080/v1/chat/completions -H 'X-User-Id: user-a' -d '{"messages":[{"role":"user","content":"hi"}],"stream":true}')"
  echo "user-b: $(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8080/v1/chat/completions -H 'X-User-Id: user-b' -d '{"messages":[{"role":"user","content":"hi"}],"stream":true}')"
done
```

| 检查点 | 期望 |
|--------|------|
| user-a 超限时 | user-b 仍正常（令牌桶 key 按 X-User-Id 隔离） |

### 测试 2.3：Redis fail-open

```bash
# 停掉 Redis
docker stop llm-gateway-redis
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"你好"}],"stream":true}'
```

| 检查点 | 期望 |
|--------|------|
| Redis 不可用时 | 请求正常通过（降级透传），不阻断主链路 |

```bash
docker start llm-gateway-redis  # 恢复
```

---

## Stage 3：语义缓存 — 验证 Embedding + Qdrant + 命中伪装流

**目标**：确认完整缓存链路 Embedding→Qdrant 检索→命中/未命中→回放/透传。

### 测试 3.1：缓存未命中（第一轮）

```bash
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: cache-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"用一句话介绍 Java"}],"stream":true}'
```

| 检查点 | 期望 | 日志关键字 |
|--------|------|-----------|
| X-Cache-Hit | `false` | — |
| SSE 流 | 正常逐字输出 | `Ollama Embedding 生成: textLen=..., dim=1024` |
| 落库 | 无报错 | `语义缓存写入完成: promptLen=..., answerLen=` |

### 测试 3.2：缓存命中（第二轮 — 相同 prompt）

```bash
# 重复完全相同请求
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: cache-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"用一句话介绍 Java"}],"stream":true}'
```

| 检查点 | 期望 | 日志关键字 |
|--------|------|-----------|
| X-Cache-Hit | `true` | `语义缓存命中，伪装 SSE 流输出: answerLen=N` |
| TTFB | ~80-150ms（远快于第一轮 3s+） | — |
| 流式输出 | **纯文本**逐字到达（非 JSON 字面量） | — |
| SSE 格式 | `data: {"choices":[{"delta":{"content":"每"}}]}`（无重复 data: 前缀） | — |

### 测试 3.3：语义相似性（第三轮 — 相似但不同措辞）

```bash
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: cache-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"请用一句话简单介绍一下 Java 编程语言"}],"stream":true}'
```

| 检查点 | 期望 |
|--------|------|
| X-Cache-Hit | `true`（余弦 > 0.95）或 `false`（措辞差异大时） |

> 阈值 0.95 较严，验证可临时降到 `semantic-cache-threshold: 0.85` 观察。

### 测试 3.4：Embedding fail-open

```bash
# 停掉 Ollama
pkill -f "ollama serve"
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"讲一个故事"}],"stream":true}'
# 日志: "Ollama Embedding 调用失败，缓存将降级为未命中透传"
# 仍正常流式输出（fail-open，不阻断主链路）
```

### 测试 3.5：清空缓存

```bash
# 删除 Qdrant collection（重启网关自动重建）
curl -X DELETE http://localhost:6333/collections/llm_cache
# 重启网关后 initCollection 自动重建
```

---

## Stage 4：熔断与智能路由

> 前置：`ollama pull qwen3:1.7b`（fallback chat 模型）+ 已有 `bge-m3`（embedding）。启动网关 `LLM_API_KEY=xxx mvn spring-boot:run`。

### 测试 4.1：TTFT 熔断 + Fallback 切换

**目标**：主通道首 token 超过 SLO（3s）或不可达 → cancel + 切 Ollama qwen3:1.7b fallback，SSE 不中断；累计失败率超阈值 → 熔断器 open，后续请求直接跳主通道。

> ⚠️ 此测试需要修改 `application.yml` 并重启网关。测试完务必改回。

**Fallback 切换工作流**：
```
请求 → breaker.decorate( primary.timeout(TTFT-SLO, perChunk) )
         ├ 主通道正常 → SSE 透传 DeepSeek 输出
         ├ TTFT > 3s → TimeoutException ─┐
         ├ breaker open → CallNotPermitted ┤
         └ 下游错误 ──────────────────────┘
                                          ↓
              .onErrorResume → fallback（Ollama qwen3:1.7b，首内容前才切）
                 ├ 成功 → SSE 透传 qwen3:1.7b 输出
                 └ 也挂 → data:{"error":"主通道与备用通道均不可用"}
```

```bash
# 1. 临时把 deepseek-chat 指向不可达地址（模拟主通道挂）
#    编辑 application.yml: models.deepseek-chat.target-url: http://10.255.255.1
#    重启网关

# 2. 发请求：3s 内首 token 到不了 → 切 fallback qwen3:1.7b，仍返回完整 SSE 流
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -H 'X-User-Id: ttft-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"用一句话介绍 Java"}],"stream":true}'

# 预期：
#   - 约 3s 后开始收到 SSE 流（fallback qwen3:1.7b 的输出，不是 DeepSeek）
#   - 日志: "主通道失败，切 fallback: reason=TimeoutException"
#   - Content-Type: text/event-stream;charset=UTF-8，SSE 格式正确，data:[DONE] 收尾

# 3. 连发多次（> minimum-number-of-calls=20）触发熔断器 open
for i in $(seq 1 25); do
  curl -s -o /dev/null -w "Request $i: %{http_code}\n" -X POST http://localhost:8080/v1/chat/completions \
    -H 'Content-Type: application/json' -H 'X-User-Id: ttft-test' \
    -d '{"messages":[{"role":"user","content":"hi"}],"stream":true}'
done
# 预期：后期请求日志出现 "CircuitBreaker 'deepseek-chat' is OPEN" → 直接走 fallback，不再等 3s
```

| 检查点 | 期望 |
|--------|------|
| 单次 TTFT 超时 | 3s 后切 fallback，SSE 流不中断 |
| 持续失败 | 熔断器 open，后续请求跳过 3s 等待直接 fallback |
| 双重失败 | fallback 也挂时 → `data: {"error":"主通道与备用通道均不可用: ..."}` |

> 验证完改回 `models.deepseek-chat.target-url: https://api.deepseek.com`。

### 测试 4.2：Fallback 转发（主通道正常 + 主动触发）

```bash
# 主通道正常时，发请求应走 deepseek-chat（不经 fallback）
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'X-User-Id: fb-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"你好"}],"stream":true}'
# 日志: "路由决策: intent=..., model=deepseek-chat" → 走主通道，无 "切 fallback" 日志

# 触发 fallback：见 4.1 的不可达地址法；或临时把 ttft-slo-ms 调到 1（极严）让正常延迟也超时
```

| 检查点 | 期望 |
|--------|------|
| 主通道正常 | 走 deepseek-chat，X-Cache-Hit: false（未命中缓存时） |
| Fallback 切换 | SSE 内容来自 qwen3:1.7b，流式正常 |

### 测试 3.4 复用：Embedding fail-open（思考模型路由验证）

```bash
# 思考模型路由：发"证明素数无限"应路由到 deepseek-reasoner（reasoning 意图）
curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'X-User-Id: route-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"证明素数有无限多个"}],"stream":true}'
# 日志: "路由决策: intent=REASONING, model=deepseek-reasoner"
# 前端: 🧠 推理过程块（置灰斜体）+ 正文气泡
```

### 测试 4.3：策略模式路由

**目标**：不同意图的 prompt 路由到不同 model，**观察网关终端日志**验证 intent + model 决策。

> 💡 跑此测试前建议先清一次缓存（测试 3.5），避免缓存命中短路跳过路由决策日志。

```bash
# ---------- 代码意图 → deepseek-chat ----------
curl -s -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'X-User-Id: route-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"用 Python 写一个快排"}],"stream":true}' > /dev/null
# 网关日志: "路由决策: intent=CODE, model=deepseek-chat"

# ---------- 推理意图 → deepseek-reasoner（思考模型）----------
curl -s -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'X-User-Id: route-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"证明素数有无限多个"}],"stream":true}' > /dev/null
# 网关日志: "路由决策: intent=REASONING, model=deepseek-reasoner"
# 浏览器前端: 🧠 推理过程块（置灰斜体）+ 正文气泡

# ---------- 闲聊意图 → qwen3:1.7b（本地省钱）----------
curl -s -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'X-User-Id: route-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"今天天气怎么样"}],"stream":true}' > /dev/null
# 网关日志: "路由决策: intent=CHITCHAT, model=qwen3:1.7b"

# ---------- 数学意图 → deepseek-reasoner ----------
curl -s -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'X-User-Id: route-test' \
  -d '{"model":"deepseek-chat","messages":[{"role":"user","content":"计算 ∫x²dx 从 0 到 1"}],"stream":true}' > /dev/null
# 网关日志: "路由决策: intent=MATH, model=deepseek-reasoner"

# ---------- 超长 prompt 守卫 ----------
# 构造超过 model 上限的 prompt（deepseek-chat maxContext=64000）
# 可用脚本生成长文本：
python3 -c "
import json
text = '你好 ' * 50000
req = json.dumps({'model':'deepseek-chat','messages':[{'role':'user','content':text}],'stream':true})
print(req)
" | curl -i -N -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' -H 'X-User-Id: route-test' -d @-
# 预期: HTTP 413 REQUESTED_RANGE_NOT_SATISFIABLE
# 如果 llm.gateway.xlong-threshold 生效且存在 big-context model，
# 日志会显示 "LengthGuard 覆盖: ... → ... (tokenCount=... 超上限)"
```

| 检查点 | 期望 | 关键日志 |
|--------|------|----------|
| 代码意图 | model=deepseek-chat | `路由决策: intent=CODE, model=deepseek-chat` |
| 推理意图 | model=deepseek-reasoner，前端显示 🧠 推理块 | `路由决策: intent=REASONING, model=deepseek-reasoner` |
| 闲聊意图 | model=qwen3:1.7b（本地省钱） | `路由决策: intent=CHITCHAT, model=qwen3:1.7b` |
| 数学意图 | model=deepseek-reasoner | `路由决策: intent=MATH, model=deepseek-reasoner` |
| 超长 prompt | HTTP 413 或 xlong 覆盖 | `LengthGuard 覆盖` 或 `413` |
| 缓存命中（复用 3.2） | 不路由，日志无"路由决策" | 只有 `语义缓存命中，伪装 SSE 流输出` |

> 注：意图分类复用 embedding（缓存检索的副产物）。若 Ollama embedding 不可用，退化为规则分类（关键词匹配），日志仍可见 intent 判定。

---

## 预期结果一览

| 测试 | 关键检查点 | 通过标准 |
|------|-----------|---------|
| **1.1 基础 SSE 透传** | SSE 流输出 + `[DONE]` | 逐字到达，末尾终止符，`X-Cache-Hit: false` |
| **1.2 错误 chunk** | 下游 401 不导致网关 500 | SSE 错误帧 `data: {"error":"..."}` + `[DONE]` |
| **1.3 强制流式** | `stream:false` 仍返回 SSE | 和 1.1 一样是流式响应 |
| **2.1 基础限流** | 并发 5 发，容量 120 | 恰好 2 个 200、3 个 429 |
| **2.2 多租户隔离** | user-a 超限不影响 user-b | 隔离子桶独立计数 |
| **2.3 Redis fail-open** | Redis 停掉请求正常过 | 降级透传，不 500 |
| **3.1 缓存未命中** | 首轮 `X-Cache-Hit: false` | TTFB ~3s+，走 DeepSeek |
| **3.2 缓存命中** | 相同 prompt 第二轮 `X-Cache-Hit: true` | TTFB ~80-150ms，伪装打字机流 |
| **3.3 语义相似** | 相似措辞命中 | `X-Cache-Hit: true`（余弦 > 0.95 时） |
| **3.4 Embedding fail-open** | Ollama 停掉请求正常过 | 降级规则分类 + 透传，不 500 |
| **4.1 TTFT 熔断 + Fallback** | 主通道不可达，3s 后切 qwen3:1.7b | SSE 不中断，日志 `切 fallback` |
| **4.1 熔断器 open** | 连发 25 次，后期跳过 3s 等待 | 日志 `CircuitBreaker '...' is OPEN` |
| **4.1 双重失败** | fallback 也挂 | `data: {"error":"主通道与备用通道均不可用: ..."}` |
| **4.2 Fallback 转发** | 主通道正常时不走 fallback | 日志无 `切 fallback` |
| **4.3 代码意图** | → `deepseek-chat` | 日志 `intent=CODE` |
| **4.3 推理意图** | → `deepseek-reasoner` | 日志 `intent=REASONING`，前端 🧠 推理块 |
| **4.3 闲聊意图** | → `qwen3:1.7b`（本地省钱） | 日志 `intent=CHITCHAT` |
| **4.3 数学意图** | → `deepseek-reasoner` | 日志 `intent=MATH` |
| **4.3 超长 prompt** | → HTTP 413 或 xlong 覆盖 | `LengthGuard 覆盖` 日志或 `413` |
| **3.2 + 4.3 缓存命中短路** | 命中时不管意图 | 无 `路由决策` 日志，只 `缓存命中` |

---

## 配置回滚

测试完 Stage 4 后确认以下配置已恢复：

```yaml
# application.yml → models.deepseek-chat.target-url
target-url: https://api.deepseek.com        # 不是 http://10.255.255.1

# application.yml → tpm-capacity / tpm-refill-rate-per-second 已恢复
tpm-capacity: 100000
tpm-refill-rate-per-second: 1667
```

清理环境：
```bash
docker-compose down                          # 停 Redis + Qdrant
pkill -f "spring-boot:run"                   # 停网关（或终端 Ctrl+C）
ollama serve                                 # 若停过 Ollama 则重启
```

---

## 性能基准测试

### 测试 P1：语义缓存 TTFB 对比

**目标**：量化缓存命中相对于未命中的 TTFB（首字节延迟）提升。

**方案**：在本地起一个模拟下游 LLM（响应首字延迟 4-6s），通过网关发两轮相同请求，对比 TTFB。

#### 环境准备

```bash
# 1. 假下游脚本
cat > /tmp/fake_llm.py << 'PYEOF'
#!/usr/bin/env python3
import http.server, json, time, random
RESPONSE = "二分查找（Binary Search）是一种在有序数组中查找目标元素的高效算法..."
SLOW_MIN, SLOW_MAX = 4.0, 6.0
class Handler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        time.sleep(random.uniform(SLOW_MIN, SLOW_MAX))
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        for ch in RESPONSE:
            chunk = {"choices":[{"index":0,"delta":{"role":"assistant","content":ch},"finish_reason":None}]}
            self.wfile.write(f'data: {json.dumps(chunk, ensure_ascii=False)}\n\n'.encode())
            self.wfile.flush(); time.sleep(0.05)
        self.wfile.write(b'data: [DONE]\n\n'); self.wfile.flush()
    def log_message(self, *a): pass
http.server.HTTPServer(('0.0.0.0', 11435), Handler).serve_forever()
PYEOF

# 2. 启动假下游
python3 /tmp/fake_llm.py &

# 3. application.yml 中 deepseek-chat 指向假下游
# models.deepseek-chat.target-url: http://localhost:11435
# ttft-slo-ms: 15000（避免 SLO 超时切到 fallback）

# 4. 启动网关
mvn clean spring-boot:run
```

#### 测试命令

```bash
# 第一轮（缓存未命中 → 走假下游，首字 4-6s）
curl -N -s -D - -o /dev/null -w "TTFB: %{time_starttransfer}s\n" \
  -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"写一个二分查找"}],"stream":true}' 2>&1 | grep -E "Cache|TTFB"

sleep 3

# 第二轮（缓存命中 → Embedding + Qdrant 检索 200-300ms 回写）
curl -N -s -D - -o /dev/null -w "TTFB: %{time_starttransfer}s\n" \
  -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"写一个二分查找"}],"stream":true}' 2>&1 | grep -E "Cache|TTFB"
```

#### 测试结果（2026-07-12 实测）

| 指标 | 缓存未命中 | 缓存命中 | 提升倍数 |
|------|-----------|---------|---------|
| X-Cache-Hit | `false` | `true` | — |
| TTFB（首字节） | **5.67s** | **0.24s** | **~23×** |

> 说明：未命中时首字延迟来自假下游的模拟响应（4-6s）。缓存命中时 TTFB = Embedding（~50ms）+ Qdrant 检索（~5ms）+ delayElements 伪装流启动（~20ms），实测约 240ms。

---

### 测试 P2：熔断 Fallback 成功率

**目标**：主通道正常 → 主通道宕机 → 请求全由 fallback（qwen3:1.7b）接管，成功率 100%，SSE 不中断。

**前置条件**：
- `application.yml` 中 `deepseek-chat` 和 `deepseek-reasoner` 的 `target-url` 已指向本地假下游 `http://localhost:11435`
- `ttft-slo-ms` 足够宽松（当前 `15000`，使假下游 4-6s 首字延迟不会误触超时）
- Ollama 已运行，`qwen3:1.7b` 已拉取

```bash
# =================================================
# 第 1 步：预热 qwen3:1.7b（避免冷启动干扰测试）
# =================================================
# 发一条闲聊请求 → Router 分类为 CHITCHAT → 路由到 qwen3:1.7b（本地 Ollama）
# 让模型加载进 GPU 显存，后续 fallback 秒响应
curl -N -s -o /dev/null -w "预热 TTFB: %{time_starttransfer}s\n" \
  -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"你好"}],"stream":true}' \
  --max-time 30

# =================================================
# 第 2 步：启动假下游（模拟 DeepSeek 主通道）
# =================================================
python3 /home/cary/.claude/jobs/0fd5fff7/tmp/fake_llm.py &
sleep 2

# 验证假下游存活（--max-time 需大于 6s 首字延迟 + 2s 逐字输出）
curl -s -o /dev/null -w "假下游 HTTP: %{http_code}\n" \
  -X POST http://localhost:11435/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"test","messages":[{"role":"user","content":"hi"}],"stream":false}' \
  --max-time 15

# =================================================
# 第 3 步：验证主通道正常（假下游代理 deepseek-chat）
# =================================================
# prompt "写一个二分查找" → Router CODE → deepseek-chat → localhost:11435（假下游）
curl -N -s -D - -X POST http://localhost:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"写一个二分查找"}],"stream":true}' \
  --max-time 20 2>&1 | grep -E "X-Cache-Hit|HTTP|TTFB|假下游"
# 预期: X-Cache-Hit: false, TTFB ≈ 5-6s（来自假下游的模拟首字延迟）

echo "主通道正常，按 Enter 继续..."; read

# =================================================
# 第 4 步：停掉假下游（模拟主通道宕机）
# =================================================
kill $(lsof -ti :11435) 2>/dev/null
sleep 1
ss -tlnp | grep 11435 && echo "⚠️ 假下游仍在运行" || echo "✅ 假下游已停止"

# =================================================
# 第 5 步：连发 10 次请求，全部由 fallback 接管
# =================================================
# 此时主通道（localhost:11435）Connection refused → onErrorResume → 
# fallbackFlux(Ollama qwen3:1.7b)，TTFB 来自 qwen 本地响应（约 1-3s）
for i in $(seq 1 10); do
  echo "--- Request $i ---"
  curl -N -s -D - -o /dev/null -w "TTFB: %{time_starttransfer}s | HTTP: %{http_code}\n" \
    -X POST http://localhost:8080/v1/chat/completions \
    -H 'Content-Type: application/json' \
    -d '{"messages":[{"role":"user","content":"写一个二分查找"}],"stream":true}' \
    --max-time 30 2>&1 | grep -E "Cache|TTFB|HTTP"
  sleep 1
done

# =================================================
# 第 6 步：验证结果
# =================================================
# 检查项：
#   - 所有 10 次请求 HTTP 200 ✓
#   - TTFB 来自 fallback qwen3:1.7b（不是 5-6s 的假下游延迟，而是 ~1-3s 本地响应）
#   - 网关日志有 "主通道失败，切 fallback: reason=..."（每条请求1次）
#   - 网关日志无 "双重失败" / "主通道与备用通道均不可用"
#   - 成功率 = 10/10 = 100% ✓
#   - SSE 流完整，以 data:[DONE] 结尾

# 查看网关 fallback 日志
# Spring Boot 默认输出到终端 stdout，看启动网关的终端窗口即可
# 例如日志行：
#   2026-07-12 10:00:00 [o-8080-exec-?] INFO  c.l.gateway.service.LlmProxyService - 主通道失败，切 fallback: reason=ConnectException: Connection refused
# 快速统计：
#   macOS/Linux: ps aux | grep 'spring-boot:run\|llm-gateway' 找到 PID
#   或者直接看运行 mvn spring-boot:run 的终端
```

| 检查点 | 预期 | 验证方法 |
|--------|------|----------|
| 预热请求 | HTTP 200，TTFB 包含 qwen3:1.7b 冷启动（首次 ~5-10s，之后 ~1-3s） | 观察 `time_starttransfer` |
| 假下游存活 | HTTP 200 | curl 状态码 |
| 主通道正常 | `X-Cache-Hit: false`，TTFB 5-6s（假下游首字延迟） | `-D -` 输出头 |
| 停掉假下游 | `localhost:11435` 无监听 | `ss -tlnp` |
| Fallback 接管 | 10/10 HTTP 200，TTFB ~1-3s（非 5-6s） | 循环输出 |
| SSE 完整性 | `data: [DONE]` 收尾 | 网关日志无异常 |
| 网关日志 | 10 条 `切 fallback`，无 `双重失败` | `grep "切 fallback"` |

#### 测试结果（2026-07-12 实测）

**第一轮——相同 prompt（缓存验证）**：假下游停掉后，10 次请求全部命中缓存（`X-Cache-Hit: true`），TTFB ~0.23s-0.31s，HTTP 200。

| 指标 | 值 |
|------|----|
| 成功率 | 10/10 ✅ |
| 缓存命中率 | 10/10 ✅ |
| TTFB 范围 | 0.23s – 0.31s |
| 中位数 TTFB | ~0.23s |
| 网关日志 | 无 `切 fallback`（缓存短路路由） |

**第二轮——不同 prompt（fallback 验证）**：每条请求使用不同 prompt（`variant 1..10`），缓存不命中 → 走路由 → Connection refused → fallback qwen3:1.7b。10/10 HTTP 200，终端打印 `切 fallback`。

| 请求 | TTFB | 分析 |
|------|------|------|
| Req 1 | 7.48s | 首次 fallback，qwen3:1.7b 冷加载（停掉假下游后模型已 idle） |
| Req 2 | 3.13s | 模型已加载，响应正常 |
| Req 3 | **0.56s** | 热响应，预期本地模型 TTFB |
| Req 4-10 | 15.58s – 28.51s | ⚠️ 高 TTFB——fallback 排队严重 |

| 指标 | 值 |
|------|----|
| 成功率 | 10/10 ✅ |
| 缓存命中率 | 0/10（不同 prompt，合理）|
| TTFB 范围 | 0.56s – 28.51s |
| 中位数 TTFB | ~19s |
| 网关日志 | 10 条 `切 fallback: reason=...` ✅ |

**分析结论**

| 检查项 | 结果 | 评价 |
|--------|------|------|
| 成功率 10/10 | ✅ HTTP 200 | 熔断+fallback 机制正常 |
| SSE 不中断 | ✅ 未出现错误帧 | `onErrorResume` 切流路径正确 |
| `切 fallback` 日志 | ✅ 终端有打印 | 日志链路完整，按请求有记录 |
| TTFB 稳定性 | ⚠️ 方差极大（0.56s-28.51s） | **单点 fallback 扛不住突发流量**——`ttft-slo-ms: 15000` 让每个请求先等 15s 才发现主通道挂了再切（实际 Connection refused 更快，但 Ollama 单实例串行处理排队加剧了延迟） |

**结论**：Fallback 功能正确，成功率 100%。TTFB 方差大是 **单点 fallback 的容量瓶颈**（Ollama 单实例 + `qwen3:1.7b` 串行推理），不是架构问题。生产环境需配置多个 fallback 副本或升级更大模型实例。
