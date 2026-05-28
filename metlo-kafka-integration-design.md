# Metlo 消息队列 Kafka 化设计文档

> 版本: v2.0  
> 日期: 2026-05-28  
> 作者: 架构组  
> 状态: 方案评审  
> 变更: v1.0(v2) → v2.0 合并第三方 Topic 数据模型分析 + Zread 产品视角

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [现状分析](#2-现状分析)
3. [目标架构](#3-目标架构)
4. [详细设计](#4-详细设计)
5. [数据模型（完整定义）](#5-数据模型完整定义)
6. [消费与背压控制](#6-消费与背压控制)
7. [改造清单](#7-改造清单)
8. [迁移策略](#8-迁移策略)
9. [监控与运维](#9-监控与运维)
10. [风险与缓解](#10-风险与缓解)

---

## 1. 背景与动机

### 1.1 现状问题

Metlo 当前使用 **Redis List** 作为 Ingestor ↔ Analyzer 之间的消息通道，存在以下问题：

| 问题 | 影响 |
|------|------|
| 消息无持久化 | `LPOP` 即删除，消费崩溃 = 数据永久丢失 |
| 无消费组 | 仅支持单消费者，Analyzer 无法水平扩展 |
| 无回溯能力 | Trace 被消费后不可重放，无法事后回溯分析 |
| 可观测性差 | Redis 队列长度是唯一指标，无延迟、Lag、吞吐等维度 |
| 无多路消费 | 无法实现「实时分析 + 离线归档 + 独立审计」并行消费 |
| 背压机制弱 | `LPOP` + `sleep` 轮询，空转浪费且延迟不可控 |

### 1.2 预期收益

引入 Kafka 作为消息中间件后：

- **消息不丢失**: 基于 offset 提交，at-least-once 语义
- **水平扩展**: Consumer Group 机制，Analyzer 实例可独立扩缩
- **回溯分析**: 重置 offset 即可重放历史流量
- **多路消费**: 实时分析、离线归档、独立审计三条链路共享同一 topic
- **天然可观测**: Lag、吞吐、消费延迟直接通过 Kafka Metrics 暴露

---

## 2. 现状分析

### 2.1 现有消息流

```
┌──────────────────────────────────────────────────────────────┐
│                      采集层 (Ingestors)                        │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │
│  │ Node SDK │ │ Python   │ │ Java     │ │ Go SDK   │        │
│  │ Express  │ │ Django   │ │ Spring   │ │ Gin      │        │
│  │ Fastify  │ │ Flask    │ │ WebFlux  │ │ Gorilla  │        │
│  │ Koa      │ │ FastAPI  │ │          │ │          │        │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘        │
│       │            │            │            │               │
│       └────────────┴─────┬──────┴────────────┘               │
│                          │                                    │
│                    还有：AWS/GCP 流量镜像、K8s DaemonSet       │
└──────────────────────────┼───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                  Ingestor (collector.ts, Port 8081)            │
│                                                                │
│  V1: log-request/index.ts         → { ctx, version:1, trace } │
│  V2: log-request/v2/index.ts      → { ctx, version:2, trace } │
│  V2: log-request/v2/index.ts      → { ctx, version:2, traces }│
│       (批量 PARTIAL)                                           │
│                          │                                     │
│                   Redis LPUSH "traces_queue"                   │
└──────────────────────────┼───────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│                 Redis List: "traces_queue"                     │
│  Message Format: JSON.stringify({ ctx, version, trace|traces })│
└──────────────────────────┼───────────────────────────────────┘
                           │
                    LPOP + sleep(100ms) 轮询
                           │
                           ▼
┌──────────────────────────────────────────────────────────────┐
│              Analyzer (analyzer.ts, Piscina Worker Pool)      │
│                                                                │
│  分流逻辑 (analyzer.ts#L322-L340):                              │
│    traceTask.trace  (FULL 单条)    → analyze(traceTask.trace)  │
│    traceTask.traces (PARTIAL 批量) → forEach(analyze)          │
│                                                                │
│  GraphQL 自循环: createGraphQlTraces → LPUSH back to Redis     │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 关键发现：消息已隐式包含两种分析类型

源码 `analyzer.ts#L322-L340` 揭示了一个关键事实——消息天然存在两条消费路径：

```typescript
// analyzer.ts 实际逻辑
interface TraceTask {
  trace?: QueuedApiTrace         // FULL: 单条敏感追踪
  traces?: QueuedApiTrace[]      // PARTIAL: 批量低优先级追踪
  ctx: MetloContext
  version: number                // 1 或 2，决定字段集合
  hasValidEnterpriseLicense?: boolean
}
```

这决定了 **Kafka Topic 必须拆分为 2 个**，否则同一条消费链路需要反复判断 `trace` vs `traces`，且分区策略冲突（PARTIAL 的 `traces` 数组含多个不同 `host`）。

### 2.3 V1 vs V2 数据源共存

当前 Ingestor 并行支持 V1 和 V2 两套采集逻辑，字段集合不同：

| 字段 | V1 (log-request/index.ts) | V2 (log-request/v2/index.ts) |
|------|--------------------------|------------------------------|
| `path`, `host`, `method`, `createdAt` | ✅ | ✅ |
| `requestHeaders`, `requestParameters`, `requestBody` | ✅ | ✅ |
| `responseHeaders`, `responseBody`, `responseStatus` | ✅ | ✅ |
| `meta` (网络元数据) | ✅ | ✅ |
| `sessionMeta` (认证元数据) | ✅ | ✅ |
| `processedTraceData` | ❌ undefined | ✅ SDK 预分析结果 |
| `endpointPath` | ❌ undefined | ✅ SDK 参数化路径 |
| `redacted` | ❌ undefined | ✅ 脱敏标记 |
| `analysisType` | ❌ undefined | ✅ FULL/PARTIAL |
| `graphqlPaths` | ❌ undefined | ✅ |
| `originalHost` | ❌ undefined | ✅ |
| `encryption` | ❌ undefined | ✅ |

Kafka Consumer 必须兼容 V1 消息（无 `processedTraceData` 时 Analyzer 自行扫描）。

---

## 3. 目标架构

### 3.1 改造后消息流

```
                              ┌──────────────────────────────────────────┐
                              │            Kafka Cluster                  │
                              │                                          │
┌──────────┐                  │  ┌────────────────────────────────────┐  │
│ Ingestor │── produce ──────→│  │  metlo.traces.full (12 partitions) │  │
│ :8081    │                  │  │  Key=host, Value=FullTraceMessage   │  │
│          │                  │  └────────────┬───────────────────────┘  │
│          │                  │               │                          │
│          │── produce ──────→│  ┌────────────▼───────────────────────┐  │
│          │   (batch)        │  │  metlo.traces.partial (6 partitions)│  │
│          │                  │  │  Key=hosts[0], Value=BatchMessage   │  │
└──────────┘                  │  └────────────┬───────────────────────┘  │
                              │               │                          │
                              └───────────────┼──────────────────────────┘
                                              │
                               Consumer Group: metlo-analyzer
                                              │
                 ┌────────────────────────────┼──────────────────────────┐
                 │                            │                          │
          ┌──────▼──────┐            ┌────────▼───────┐          ┌──────▼──────┐
          │ Analyzer #1 │            │  Analyzer #2    │          │ 冷数据归档   │
          │ (Piscina-4) │            │  (Piscina-4)    │          │(独立Consumer)│
          │ FULL+PARTIAL │           │  FULL+PARTIAL   │          │ 只消费 FULL  │
          └─────────────┘            └────────────────┘          └─────────────┘
```

### 3.2 组件变更总览

| 组件 | 变更 |
|------|------|
| Ingestor | Redis `LPUSH` → Kafka Producer（双 topic 路由） |
| Analyzer | Redis `LPOP` 轮询 → Kafka Consumer 推模式（双 topic 订阅） |
| GraphQL 展开 | **从 Analyzer 前移至 Ingestor/LogRequest V2** |
| Piscina Worker Pool | 不变，仍作为并发分析载体 |
| Scanner / Alert / OpenAPI / DataField / DataClass | **零改动**，完全不感知消息来源 |
| Frontend / CLI / SDKs | **零改动** |

---

## 4. 详细设计

### 4.1 Topic 设计：双 Topic 策略

#### 设计依据

当前 Analyzer 消费逻辑中，`traceTask.trace` (单数) 和 `traceTask.traces` (复数) 走完全不同的代码分支。FULL 是安全关键路径（用户数据、认证端点），PARTIAL 是低优先级批量路径（health check、ping）。混在一个 topic 导致：
- 消息结构不一致（`trace` vs `traces` 字段互斥）
- 分区策略冲突（PARTIAL 的批内多 host 无法按 host 分区）
- 无法做差异化 QoS

#### Topic 规格

```
Topic 1: metlo.traces.full
────────────────────────────
用途:        FULL 分析类型，单条推送
来源:        log-request/index.ts (V1), log-request/v2/index.ts (V2 single)
Partitions:  12
Replication: 3
Retention:   7 days
Compression: snappy

Message Key:   trace.host（MD5 哈希取模）
Message Value: FullTraceMessage = { ctx, version, trace: QueuedApiTrace }
               注意: trace.analysisType === "full" 或 undefined (V1)

Topic 2: metlo.traces.partial
──────────────────────────────
用途:        PARTIAL 分析类型，批量推送
来源:        log-request/v2/index.ts (V2 batch)
Partitions:  6（流量远低于 FULL）
Replication: 3
Retention:   3 days（低价值数据，保留更短）
Compression: snappy

Message Key:   traces[0].host 或 hosts 中最频繁的 host
Message Value: PartialTraceBatchMessage = { ctx, version:2, traces: QueuedApiTraceV2[] }
               注意: 每条 trace.analysisType === "partial"
```

#### 分区策略（两个 topic 一致）

选择 `host` 作为 partition key：

1. **局部性**: 同一 host 的 trace 有序到达，端点发现算法的路径参数化依赖时序连续性
2. **负载均衡**: 按 host 哈希天然分散，避免热点分区
3. **扩展性**: 新增 host 自动分散，无需额外路由逻辑

#### PARTIAL 批量的分区策略

`PartialTraceBatchMessage.traces` 可能含多个不同 host。解决方案：

```
batch 内取第一个 trace.host 作为 Key
→ 同一 host 的批大概率落在同一 partition
→ 跨 host 的批（极少见）允许散列到不同 partition，不影响端点发现
```

#### 预估吞吐

```
单 Trace 平均大小: ~2 KB（含 requestBody/responseBody）
中等规模 API:      5,000 req/s × 2KB  = 10 MB/s 写入
高峰 API:         50,000 req/s × 2KB = 100 MB/s 写入

FULL : PARTIAL ≈ 7:3（安全关键 vs 健康检查）
12+6 partition × 标准 SSD Broker 可轻松承载 200+ MB/s
```

### 4.2 Producer 设计

#### 配置

```typescript
// backend/src/kafka/producer.ts
import { Kafka, Producer } from "kafkajs"

const kafka = new Kafka({
  clientId: "metlo-ingestor",
  brokers: process.env.KAFKA_BROKERS?.split(",") || ["localhost:9092"],
  retry: {
    initialRetryTime: 100,
    retries: 3,
  },
})

const producer = kafka.producer({
  allowAutoTopicCreation: false,
  maxInFlightRequests: 5,
  idempotent: true,
})

export const FULL_TOPIC = "metlo.traces.full"
export const PARTIAL_TOPIC = "metlo.traces.partial"
```

#### 双 Topic 路由逻辑

```typescript
// 替换 LogRequest V1 和 V2 中的 Redis lpush
// V1 (log-request/index.ts) — 只有 FULL
async function publishTraceV1(trace: QueuedApiTraceV1): Promise<void> {
  await producer.send({
    topic: FULL_TOPIC,
    messages: [{
      key: trace.host,
      value: JSON.stringify({ ctx: {}, version: 1, trace }),
    }],
  })
}

// V2 (log-request/v2/index.ts) — FULL 或 PARTIAL
async function publishTraceV2(
  traces: QueuedApiTraceV2[],
  analysisType: AnalysisType,
): Promise<void> {
  if (analysisType === AnalysisType.PARTIAL) {
    // 批量推送
    const key = traces[0]?.host || "unknown"
    await producer.send({
      topic: PARTIAL_TOPIC,
      messages: [{
        key,
        value: JSON.stringify({ ctx: {}, version: 2, traces }),
      }],
    })
  } else {
    // 单条推送 (FULL)
    const messages = traces.map(t => ({
      key: t.host,
      value: JSON.stringify({ ctx: {}, version: 2, trace: t }),
    }))
    await producer.send({ topic: FULL_TOPIC, messages })
  }
}
```

### 4.3 GraphQL 展开逻辑前移

**现有问题**（analyzer.ts 内）：

```
Analyzer 收到一个 GraphQL trace
  → createGraphQlTraces() 展开为 N 个子 trace
  → 每个子 trace 再 push 回 Redis TRACES_QUEUE（消费者自循环）
  → 在 Kafka 架构下不可接受（Consumer 做 Producer 破坏语义）
```

**改造**（前移至 LogRequest V2）：

```typescript
// 在 log-request/v2/index.ts 中，构造 QueuedApiTrace 之后、发送前
if (isGraphQlEndpoint(trace) && trace.processedTraceData?.graphqlPaths) {
  const expandedTraces = expandGraphQlTrace(trace)
  // 展开后的子 trace 均为 FULL 类型
  await publishTraceV2(expandedTraces, AnalysisType.FULL)
  return
}
// 否则正常路由
await publishTraceV2([trace], trace.analysisType || AnalysisType.FULL)
```

Analyzer 端同步删除 `createGraphQlTraces` 调用和自循环逻辑。

### 4.4 Consumer 设计

#### 配置

```typescript
// backend/src/kafka/consumer.ts
const consumer = kafka.consumer({
  groupId: "metlo-analyzer",
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxBytesPerPartition: 1048576,
})
```

#### 双 Topic 订阅 + 分流消费

```typescript
async function startAnalyzerConsumer(): Promise<void> {
  await consumer.connect()
  // 同时订阅两个 topic
  await consumer.subscribe({ topic: FULL_TOPIC, fromBeginning: false })
  await consumer.subscribe({ topic: PARTIAL_TOPIC, fromBeginning: false })

  const pool = new Piscina({
    filename: path.resolve(__dirname, "analyze-traces.js"),
    maxThreads: parseInt(process.env.NUM_WORKERS || "4"),
    idleTimeout: 10 * 60 * 1000,
    maxQueue: 4096,
  })

  let paused = false

  await consumer.run({
    autoCommit: false,
    eachBatchAutoResolve: false,

    eachBatch: async ({
      batch, resolveOffset, heartbeat,
      pause: pauseConsumer, commitOffsetsIfNecessary,
    }) => {
      for (const message of batch.messages) {
        if (pool.queueSize >= 4096 && !paused) {
          pauseConsumer()
          paused = true
        }

        const raw = JSON.parse(message.value.toString())

        // ─── 分流逻辑（对应原始 analyzer.ts#L322-L340） ───
        if (raw.trace) {
          // FULL: 单条 trace
          pool.run({
            trace: raw.trace,
            ctx: raw.ctx,
            version: raw.version,
            hasValidEnterpriseLicense,
          })
        } else if (raw.traces) {
          // PARTIAL: 批量 traces
          for (const t of raw.traces) {
            pool.run({
              trace: t,
              ctx: raw.ctx,
              version: raw.version,
              hasValidEnterpriseLicense,
            })
          }
        }

        resolveOffset(message.offset)
      }

      // 等待 Piscina 消化
      while (pool.queueSize > 0) {
        await sleep(100)
        await heartbeat()
      }

      if (paused && pool.queueSize < 1024) {
        paused = false
      }

      await commitOffsetsIfNecessary()
    },
  })
}
```

### 4.5 `ctx` 扩展建议（Zread 视角）

当前 `MetloContext` 是空接口 `{}`。第三方分析和 Zread 均建议扩展：

```typescript
// backend/src/types.ts
interface MetloContext {
  traceId?: string       // 分布式追踪 ID，关联 Ingestor → Analyzer → Alert
  apiKeyId?: string      // API Key 标识，用于多租户路由和消费者鉴权
  ingestTimestamp?: number  // 采集时间戳，用于计算端到端延迟
}
```

这不需要修改 Kafka 消息格式（JSON 天然兼容新增字段），Consumer 侧渐进升级即可。

---

## 5. 数据模型（完整定义）

> 以下类型定义基于 `common/src/types.ts`、`common/src/enums.ts` 源码级还原，标注源码行号。所有字段均经过 `topic.txt` 交叉验证。

### 5.1 枚举类型

```typescript
// 来源: common/src/enums.ts

enum RestMethod {
  GET="GET", HEAD="HEAD", POST="POST", PUT="PUT",
  PATCH="PATCH", DELETE="DELETE", CONNECT="CONNECT",
  OPTIONS="OPTIONS", TRACE="TRACE"
}

enum AnalysisType {
  FULL = "full",
  PARTIAL = "partial"
}

enum AuthType {
  BASIC = "basic",
  HEADER = "header",
  JWT = "jwt",
  SESSION_COOKIE = "session_cookie"
}

enum DataSection {
  REQUEST_PATH    = "reqPath",
  REQUEST_QUERY   = "reqQuery",
  REQUEST_HEADER  = "reqHeaders",
  REQUEST_BODY    = "reqBody",
  RESPONSE_HEADER = "resHeaders",
  RESPONSE_BODY   = "resBody"
}

enum DataType {
  INTEGER="integer", NUMBER="number", STRING="string",
  BOOLEAN="boolean", OBJECT="object", ARRAY="array", UNKNOWN="unknown"
}

enum RiskScore {
  NONE="none", LOW="low", MEDIUM="medium", HIGH="high"
}
```

### 5.2 基础结构

```typescript
// 来源: common/src/types.ts

interface PairObject {
  name: string
  value: string
}

interface Meta {
  incoming: boolean           // 是否入站请求
  source: string              // 源 IP
  sourcePort: string
  destination: string         // 目标 IP
  destinationPort: string
  originalSource?: string     // 原始来源（可选）
}

interface SessionMeta {
  authenticationProvided: boolean
  authenticationSuccessful: boolean
  authType: AuthType
  uniqueSessionKey?: string
  user?: string
}
```

### 5.3 预分析数据（仅 V2）

```typescript
// 来源: common/src/types.ts#L94-L105

interface ProcessedTraceData {
  block: boolean                                      // 是否应阻断（需企业 License）
  attackDetections?: Record<string, string[]>         // 攻击检测结果
  xssDetected?: Record<string, string>                // XSS 检测结果
  sqliDetected?: Record<string, [string, string]>     // SQL 注入检测结果
  sensitiveDataDetected: Record<string, string[]>     // 敏感数据检测结果
  dataTypes: Record<string, string[]>                 // 字段数据类型推断
  requestContentType: string                          // 请求 Content-Type
  responseContentType: string                         // 响应 Content-Type
  graphqlPaths?: string[]                             // GraphQL 操作路径
  validationErrors?: Record<string, string[]>         // OpenAPI 验证错误
}

interface Encryption {
  key: string
  generatedIvs: Record<string, number[]>
}
```

### 5.4 追踪主体（V1 vs V2 对比）

```typescript
// V1 — 来源: backend/src/services/log-request/index.ts#L56-L69
interface QueuedApiTraceV1 {
  path: string
  createdAt: Date
  host: string
  method: RestMethod
  requestParameters: PairObject[]
  requestHeaders: PairObject[]
  requestBody: string
  responseStatus: number
  responseHeaders: PairObject[]
  responseBody: string
  meta: Meta
  sessionMeta: SessionMeta
  // V1 不携带以下字段
  processedTraceData?: undefined
  endpointPath?: undefined
  redacted?: undefined
  analysisType?: undefined
  graphqlPaths?: undefined
  originalHost?: undefined
  encryption?: undefined
}

// V2 — 来源: common/src/types.ts#L139-L159
interface QueuedApiTraceV2 {
  path: string
  endpointPath?: string              // ★ SDK 提供的参数化路径
  createdAt: Date
  host: string
  method: RestMethod
  requestParameters: PairObject[]
  requestHeaders: PairObject[]
  requestBody: string
  responseStatus: number
  responseHeaders: PairObject[]
  responseBody: string
  meta: Meta
  sessionMeta: SessionMeta
  processedTraceData?: ProcessedTraceData  // ★ SDK 预分析
  redacted?: boolean                       // ★ 脱敏标记
  originalHost?: string                    // ★ Analyzer 注入
  encryption?: Encryption                  // ★ 加密信息
  analysisType?: AnalysisType              // ★ FULL/PARTIAL
  graphqlPaths?: string[]                  // ★ GraphQL 路径
}
```

### 5.5 Kafka Message Value（外层包装）

```typescript
// Topic: metlo.traces.full

// V1 消息
interface FullTraceMessageV1 {
  ctx: MetloContext     // 当前为 {}
  version: 1
  trace: QueuedApiTraceV1
}

// V2 消息
interface FullTraceMessageV2 {
  ctx: MetloContext
  version: 2
  trace: QueuedApiTraceV2
}

// Topic: metlo.traces.partial
interface PartialTraceBatchMessage {
  ctx: MetloContext
  version: 2            // PARTIAL 仅 V2 支持
  traces: QueuedApiTraceV2[]  // 每条 analysisType === "partial"
}
```

### 5.6 消息示例

#### FULL V2（最完整场景）

```json
{
  "ctx": {},
  "version": 2,
  "trace": {
    "path": "/api/v1/users/42/orders",
    "endpointPath": "/api/v1/users/{param1}/orders",
    "createdAt": "2026-05-28T08:30:00.000Z",
    "host": "api.example.com",
    "method": "GET",
    "requestParameters": [
      { "name": "page", "value": "1" },
      { "name": "limit", "value": "20" }
    ],
    "requestHeaders": [
      { "name": "Authorization", "value": "Bearer eyJhbG..." },
      { "name": "Content-Type", "value": "application/json" }
    ],
    "requestBody": "",
    "responseStatus": 200,
    "responseHeaders": [
      { "name": "Content-Type", "value": "application/json; charset=utf-8" }
    ],
    "responseBody": "{\"data\":[{\"id\":1,\"email\":\"user@test.com\",\"ssn\":\"123-45-6789\"}]}",
    "meta": {
      "incoming": true,
      "source": "10.0.1.5",
      "sourcePort": "443",
      "destination": "10.0.2.10",
      "destinationPort": "8080"
    },
    "sessionMeta": {
      "authenticationProvided": true,
      "authenticationSuccessful": true,
      "authType": "jwt",
      "uniqueSessionKey": "user_42",
      "user": "alice"
    },
    "processedTraceData": {
      "block": false,
      "attackDetections": {},
      "xssDetected": {},
      "sqliDetected": {},
      "sensitiveDataDetected": {
        "resBody.data.email": ["Email"],
        "resBody.data.ssn": ["Social Security Number"]
      },
      "dataTypes": {
        "resBody.data": ["array"],
        "resBody.data.id": ["integer"],
        "resBody.data.email": ["string"]
      },
      "requestContentType": "application/json",
      "responseContentType": "application/json",
      "graphqlPaths": [],
      "validationErrors": {}
    },
    "redacted": false,
    "analysisType": "full",
    "graphqlPaths": []
  }
}
```

#### PARTIAL Batch V2

```json
{
  "ctx": {},
  "version": 2,
  "traces": [
    {
      "path": "/api/v1/health",
      "createdAt": "2026-05-28T08:30:01.000Z",
      "host": "api.example.com",
      "method": "GET",
      "requestParameters": [],
      "requestHeaders": [],
      "requestBody": "",
      "responseStatus": 200,
      "responseHeaders": [],
      "responseBody": "{\"status\":\"ok\"}",
      "meta": { "incoming": true, "source": "10.0.1.5", "sourcePort": "80",
                "destination": "10.0.2.10", "destinationPort": "8080" },
      "sessionMeta": { "authenticationProvided": false,
                       "authenticationSuccessful": false, "authType": "basic" },
      "processedTraceData": { "block": false, "sensitiveDataDetected": {},
                              "dataTypes": {}, "requestContentType": "*/*",
                              "responseContentType": "application/json" },
      "redacted": true,
      "analysisType": "partial"
    },
    {
      "path": "/api/v1/ping",
      "createdAt": "2026-05-28T08:30:01.100Z",
      "host": "api.example.com",
      "method": "GET",
      "requestParameters": [],
      "requestHeaders": [],
      "requestBody": "",
      "responseStatus": 200,
      "responseHeaders": [],
      "responseBody": "pong",
      "meta": { "incoming": true, "source": "10.0.1.5", "sourcePort": "80",
                "destination": "10.0.2.10", "destinationPort": "8080" },
      "sessionMeta": { "authenticationProvided": false,
                       "authenticationSuccessful": false, "authType": "basic" },
      "processedTraceData": { "block": false, "sensitiveDataDetected": {},
                              "dataTypes": {}, "requestContentType": "*/*",
                              "responseContentType": "text/plain" },
      "redacted": true,
      "analysisType": "partial"
    }
  ]
}
```

### 5.7 序列化注意事项

| 关注点 | 说明 |
|--------|------|
| `Date` 类型 | `createdAt` 序列化后变为 ISO 8601 字符串，Consumer 反序列化后需 `new Date(str)` 还原 |
| `ctx` 空对象 | 当前 `MetloContext` 为空接口，序列化为 `{}`。扩展 `traceId`/`apiKeyId` 后 Consumer 渐进升级 |
| 空 `processedTraceData` (V1) | 该字段为 `undefined` 时不入 JSON；Consumer 需判空后走 Analyzer 自行扫描路径 |
| `block` 与 License | 无企业 License 时始终为 `false`；Consumer 可据此跳过阻断逻辑减少无效判断 |
| 单条消息上限 | 1 MB（Kafka 默认）。超限时 Ingestor 截断 body（保留前 512KB）+ 标记 `truncated: true` |

---

## 6. 消费与背压控制

### 6.1 消费语义

```
语义: at-least-once（先分析，后提交 offset）
幂等: Analyzer 内部通过 trace UUID + endpoint UUID 去重
```

### 6.2 背压流控

```
                    ┌──────────────────────────┐
                    │   Kafka Consumer          │
                    │   max.poll.records = 500  │
                    │   订阅 FULL + PARTIAL      │
                    └──────────┬───────────────┘
                               │
                    ┌──────────▼───────────────┐
                    │   Piscina Queue            │
                    │   maxQueue = 4096          │
                    │                            │
                    │  queueSize < 4096  → 正常  │
                    │  queueSize >= 4096 → pause │
                    │  queueSize < 1024  → resume│
                    └──────────┬───────────────┘
                               │
                    ┌──────────▼───────────────┐
                    │   Worker Pool              │
                    │   maxThreads = CPU 核心数  │
                    │   idleTimeout = 10 min     │
                    └──────────────────────────┘
```

关键：两个 topic 共享同一个 Piscina Pool 和背压信号。任一 topic 消费致队列满时，Consumer 整体 pause，两个 topic 同步暂停。

### 6.3 异常分类处理

| 类型 | 策略 | 行为 |
|------|------|------|
| 单条分析失败 | 跳过 | `resolveOffset(offset)`，不阻塞 |
| JSON 反序列化失败 | 跳过 + 日志 | 记录死信 offset 到 `metlo.dlq` |
| DB 连接断开 | 暂停重试 | `consumer.pause()`，DB 恢复后 resume |
| `createGraphQlTraces` 残留调用 | 告警 | 该逻辑已前移至 Ingestor，Consumer 端不应出现 |
| 消费 Lag 持续 > 5000 | 告警 + 扩容 | 触发 on-call，按需增加 Analyzer 实例 |

---

## 7. 改造清单

### 7.1 新增文件

| 文件 | 职责 |
|------|------|
| `backend/src/kafka/index.ts` | Kafka Client 初始化，Producer/Consumer 工厂 |
| `backend/src/kafka/producer.ts` | 双 topic 路由 `publishTraceV1()` / `publishTraceV2()` |
| `backend/src/kafka/consumer.ts` | 双 topic 订阅 + 分流消费 + 背压控制 |

### 7.2 修改文件

| 文件 | 变更 | 行数 |
|------|------|------|
| `backend/src/services/log-request/index.ts` | Redis LPUSH → `publishTraceV1()` | ~5 |
| `backend/src/services/log-request/v2/index.ts` | Redis LPUSH → `publishTraceV2()`；新增 GraphQL 展开 | ~30 |
| `backend/src/analyzer.ts` | 删除 LPOP 轮询 + sleep + GraphQL 自循环；改为 Consumer 初始化 | ~50(删为主) |
| `backend/src/constants.ts` | 新增 Kafka 常量；保留原常量标记 `@deprecated` | ~15 |
| `backend/package.json` | 新增 `kafkajs` 依赖 | 1 行 |
| `docker-compose.yaml` | 新增 Kafka + Zookeeper（开发环境） | ~30 |
| `backend/src/types.ts` | 可选：`MetloContext` 扩展 `traceId`/`apiKeyId` | ~5 |

### 7.3 零改动模块

以下模块完全不感知消息队列变迁：

```
backend/src/services/scanner/          # 敏感数据扫描
backend/src/services/analyze/v2/       # 分析 Pipeline
backend/src/services/alert/            # 告警生成
backend/src/services/spec/             # OpenAPI 差分
backend/src/services/data-field/       # 数据字段管理
backend/src/services/data-classes/     # 数据类管理
backend/src/services/endpoint/         # 端点发现
backend/src/services/webhook/          # Webhook 通知
backend/src/api/                       # REST API 路由（10 个路由注册器）
frontend/                              # Next.js Web UI
ingestors/                             # 多语言 SDK (Node/Python/Java/Go/K8s)
cli/                                   # 部署 CLI + 安全测试
common/                                # 共享类型/枚举/映射
deploy/                                # CloudFormation/Bicep/K8s 模板
```

### 7.4 环境变量

```bash
# 新增
KAFKA_BROKERS=kafka:9092
KAFKA_FULL_TOPIC=metlo.traces.full
KAFKA_PARTIAL_TOPIC=metlo.traces.partial
KAFKA_CONSUMER_GROUP=metlo-analyzer

# 保留（Analyzer 仍使用 Piscina）
NUM_WORKERS=4

# 保留（Redis 另有他用：配置缓存、会话存储）
REDIS_URL=redis://...
```

---

## 8. 迁移策略

### 8.1 阶段一：并行运行（零风险）

```
                    ┌──────────┐
                    │ Ingestor │
                    └────┬─────┘
                         │
              ┌──────────┼──────────┐
              ▼                     ▼
        ┌──────────┐         ┌──────────────┐
        │  Redis   │         │    Kafka      │
        │ (旧链路)  │         │ FULL+PARTIAL  │
        └────┬─────┘         └──────┬───────┘
             │                      │
             ▼                      ▼
        ┌──────────┐         ┌──────────────┐
        │ Analyzer │         │ Analyzer 新   │
        │ (旧版)   │         │ (Kafka版)     │
        └──────────┘         └──────────────┘

切换: KAFKA_ENABLED=true → Ingestor 双写, 新 Analyzer 读 Kafka
回滚: KAFKA_ENABLED=false → 全量回 Redis
```

### 8.2 阶段二：切换与对比（1 周）

1. Ingestor 双写 Redis + Kafka
2. 新 Analyzer 消费 Kafka，旧 Analyzer 保留作为对照组
3. 每日对比告警产出量、类型分布、误报率，偏差 < 1%

### 8.3 阶段三：下线旧链路

1. 移除 Ingestor Redis 写入逻辑
2. 移除 `analyzer.ts` LPOP 轮询 + GraphQL 自循环 + sleep
3. 清理 `constants.ts` 中 `TRACES_QUEUE` 常量（标记 `@deprecated` 后一个版本删除）

---

## 9. 监控与运维

### 9.1 核心指标

| 指标 | 来源 | 告警阈值 |
|------|------|----------|
| `metlo.traces.full` Consumer Lag | Kafka Exporter | > 5000 持续 5min |
| `metlo.traces.partial` Consumer Lag | Kafka Exporter | > 20000 持续 5min（低优先级） |
| 消费吞吐 (msg/s) | Consumer Metrics | 趋势监控 |
| Piscina Queue Size | 应用 Metrics | > 3000 持续 2min |
| 端到端延迟 (ingest→analyze) | `ctx.ingestTimestamp` | p99 > 30s |
| 分析失败率 | 应用 Metrics | > 1% |
| Kafka 写入失败率 | Producer Metrics | > 0.1% |

### 9.2 关键日志事件

```
[KAFKA] producer connected to brokers: kafka:9092
[KAFKA] consumer joined group: metlo-analyzer, subscribed: [metlo.traces.full, metlo.traces.partial]
[KAFKA] consumer paused (queueSize=4096)
[KAFKA] consumer resumed (queueSize=512)
[KAFKA] produce failed (retries exhausted), trace dropped, host=api.example.com
[KAFKA] V1 trace consumed (no processedTraceData), host=legacy.example.com
[KAFKA] GraphQL expansion in ingestor: 1→5 traces, host=graphql.example.com
```

### 9.3 运维操作

```bash
# 回溯分析最近 1 小时 FULL 流量
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group metlo-analyzer --topic metlo.traces.full \
  --reset-offsets --to-datetime 2026-05-28T20:00:00.000 --execute

# 查看消费延迟
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group metlo-analyzer --describe

# 扩容 FULL topic 分区
kafka-topics --bootstrap-server kafka:9092 \
  --alter --topic metlo.traces.full --partitions 24
```

---

## 10. 风险与缓解

### 10.1 风险矩阵

| 风险 | 概率 | 影响 | 缓解 |
|------|------|------|------|
| Kafka 集群不可用 | 低 | 高 | Ingestor 本地 buffer 降级（≤10000 条）；Redis 旧链路保留作紧急回退 |
| Consumer Lag 持续增长 | 中 | 中 | HPA 自动扩容 Analyzer；PARTIAL topic 可临时降低消费优先级 |
| Piscina 背压过载 | 中 | 中 | 调优 `maxThreads`；监控队列深度 |
| V1 消息无 `processedTraceData` 导致 NullRef | 低 | 中 | Consumer 端显式判空，走 Analyzer 自行扫描路径 |
| `xssDetected`/`sqliDetected` 类型兼容 | 低 | 低 | JSON 不校验类型，Consumer 端 `typeof` 防护 |
| `PartialTraceBatchMessage` 分区键跨 host | 低 | 低 | 取 `traces[0].host`，跨 host 批极少见 |
| GraphQL 展开遗漏（某条路径未前移） | 低 | 中 | Consumer 端加守卫：若仍检测到 GraphQL 未展开 trace，记录 WARN + 降级处理 |
| `kafkajs` 版本兼容 | 低 | 中 | 锁定版本；CI 集成测试覆盖 |

### 10.2 Kafka 不可用降级

```typescript
class TracePublisher {
  private buffer: QueuedApiTrace[] = []
  private readonly MAX_BUFFER = 10000

  async publish(trace: QueuedApiTrace, analysisType: AnalysisType): Promise<void> {
    try {
      const topic = analysisType === AnalysisType.PARTIAL ? PARTIAL_TOPIC : FULL_TOPIC
      await producer.send({
        topic,
        messages: [{ key: trace.host, value: JSON.stringify({ ctx: {}, version: 2, trace }) }],
      })
      await this.flushBuffer()
    } catch (err) {
      mlog.withErr(err).warn("Kafka unavailable, buffering trace")
      if (this.buffer.length < this.MAX_BUFFER) {
        this.buffer.push(trace)
      } else {
        mlog.error("Buffer overflow, dropping trace")
      }
    }
  }
}
```

### 10.3 测试策略

| 类型 | 内容 | 触发 |
|------|------|------|
| 单元测试 | Producer/Consumer 封装；V1/V2 消息序列化兼容 | PR |
| 集成测试 | Docker Compose 全链路 (Kafka + Analyzer + DB)；双 topic 分流正确性 | PR |
| 性能测试 | 10,000 msg/s FULL + 3,000 msg/s PARTIAL，持续 30min | 发版前 |
| 数据对比 | 新旧链路告警产出量，按 AlertType 分组对比 | 迁移期间每日 |
| 混沌测试 | 随机 Kill Broker → 验证降级 buffer → 恢复后自动刷盘 | 按周 |

---

## 附录

### A. 依赖

```json
{ "kafkajs": "^2.2.4" }
```

### B. Docker Compose 补充

```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports: ["9092:9092"]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

### C. 改造代码量预估

| 类别 | 行数 |
|------|------|
| 新增（Kafka Producer/Consumer 模块） | ~250 |
| 修改（LogRequest V1 + V2） | ~40 |
| 修改（Analyzer 删除旧逻辑） | ~60（删为主） |
| 修改（配置/常量） | ~20 |
| 修改（docker-compose） | ~30 |
| 测试 | ~350 |
| **合计** | **~750 行** |

### D. 信息源

| 来源 | 贡献 |
|------|------|
| DeepWiki (metlo-labs/metlo) | 架构 Pipeline、服务职责、DB Schema |
| Zread (zread.ai/metlo-labs/metlo) | 产品视角、SDK 框架矩阵、企业功能边界 |
| 第三方 topic.txt | V1/V2 精确类型差异、FULL/PARTIAL 分流依据、源码行号 |
| GitHub API 源码逆向 | 消费逻辑、Scanner 引擎、告警系统、Piscina 并发模型 |
| Starlog 评测 | 竞品对比、开源局限性、适用场景判断 |
