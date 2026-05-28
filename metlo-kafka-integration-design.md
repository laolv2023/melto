# Metlo 消息队列 Kafka 化设计文档

> 版本: v1.0  
> 日期: 2026-05-28  
> 作者: 架构组  
> 状态: 方案评审

---

## 目录

1. [背景与动机](#1-背景与动机)
2. [现状分析](#2-现状分析)
3. [目标架构](#3-目标架构)
4. [详细设计](#4-详细设计)
5. [改造清单](#5-改造清单)
6. [数据模型](#6-数据模型)
7. [消费与背压控制](#7-消费与背压控制)
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
| 无多路消费 | 无法实现"实时分析 + 离线归档"双路并行消费 |
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
┌──────────┐   QueuedApiTrace    ┌─────────┐   QueuedApiTrace    ┌──────────┐
│ Ingestor │ ──── LPUSH ──────→  │  Redis  │ ──── LPOP ──────→  │ Analyzer │
│ :8081    │                     │  List   │     + sleep(100ms)  │ (Piscina)│
└──────────┘                     └─────────┘                     └──────────┘

Queue Key: "traces_queue"
Message Format: JSON.stringify(QueuedApiTrace)
```

### 2.2 关键代码路径

#### 写入侧
```
collector.ts
  └→ services/log-request/v2/index.ts
       └→ RedisClient.lpush("traces_queue", JSON.stringify(trace))
```

#### 读取侧
```
analyzer.ts
  └→ getQueuedApiTrace()
       └→ RedisClient.lpop("traces_queue")
            └→ JSON.parse(traceString)
                 └→ pool.run(traceTask)  // Piscina Worker
```

#### GraphQL 展开逻辑（自循环）

```typescript
// analyzer.ts  ~Line 80
// 注意：该逻辑会从 Analyzer 再 push 回 Redis
for (const graphQlTrace of createGraphQlTraces(trace)) {
  await redis.lpush(TRACES_QUEUE, JSON.stringify(graphQlTrace))
}
```

### 2.3 现有数据模型

消息体为 `QueuedApiTrace`，来自 `@common/types`，结构见 [§6 数据模型](#6-数据模型)。

---

## 3. 目标架构

### 3.1 改造后消息流

```
                              ┌────────────────────────────────────────┐
                              │            Kafka Cluster               │
                              │                                        │
┌──────────┐  QueuedApiTrace  │  ┌──────────────────────────────────┐ │
│ Ingestor │ ── produce ────→ │  │  metlo.traces (topic)            │ │
│ :8081    │                  │  │  P0  P1  P2  ...  P11            │ │
└──────────┘                  │  └───┬──────────┬───────────────────┘ │
                              │      │          │                      │
                              └──────┼──────────┼──────────────────────┘
                                     │          │
                          Consumer Group: metlo-analyzer
                                     │          │
                    ┌────────────────┼─────┐  ┌──┴──────────────┐
                    │  Analyzer #1   │     │  │  Analyzer #2     │
                    │  (Piscina-4)   │     │  │  (Piscina-4)     │
                    └────────────────┘     │  └─────────────────┘
                                           │
                    ┌────────────────┐     │  ┌─────────────────┐
                    │ Analyzer #3    │     │  │ 冷数据归档       │
                    │ (Piscina-4)    │     │  │ (独立 Consumer)  │
                    └────────────────┘     │  └─────────────────┘
                                           │
                    ┌────────────────┐     │
                    │ 实时审计服务    │     │
                    │ (独立 Consumer)│ ←───┘
                    └────────────────┘
```

### 3.2 组件变更

| 组件 | 变更 |
|------|------|
| Ingestor | Redis `LPUSH` → Kafka Producer |
| Analyzer | Redis `LPOP` 轮询 → Kafka Consumer 推模式 |
| GraphQL 展开 | 从 Analyzer 前移至 Ingestor/LogRequest |
| Piscina Worker Pool | 不变，仍作为并发分析载体 |
| Scanner / Alert / OpenAPI | 不变，完全不感知消息来源 |

---

## 4. 详细设计

### 4.1 Kafka Topic 设计

```
Topic Name:    metlo.traces
Partitions:    12（初期，可根据 host 数量动态扩）
Replication:   3
Retention:     7 days

Message Key:   trace.host（MD5 哈希取模分区）
Message Value: JSON.stringify(QueuedApiTrace)
Compression:   snappy

Cleanup Policy: delete
Segment Size:  1 GB
```

#### 分区策略

选择 `host` 作为 partition key 的理由：

1. **局部性**: 同一 host 的 trace 有序到达，端点发现算法的路径参数化依赖时序连续性
2. **负载均衡**: 按 host 哈希天然分散，避免热点分区
3. **扩展性**: 新增 host 自动分散，无需额外路由逻辑

#### 预估吞吐

```
单 Trace 平均大小: ~2 KB（含 requestBody/responseBody）
中等规模 API: 5,000 req/s × 2KB = 10 MB/s 写入
高峰 API: 50,000 req/s × 2KB = 100 MB/s 写入

12 partition × 标准 SSD Broker 可轻松承载 200+ MB/s 写入
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
  idempotent: true,               // 幂等生产者，防止重复
})

export const TRACE_TOPIC = "metlo.traces"
```

#### 发送逻辑

```typescript
// 替换 LogRequest V2 中 Redis lpush 调用
async function publishTrace(trace: QueuedApiTrace): Promise<void> {
  try {
    await producer.send({
      topic: TRACE_TOPIC,
      messages: [{
        key: trace.host,
        value: JSON.stringify(trace),
      }],
    })
  } catch (err) {
    mlog.withErr(err).error("Kafka produce failed")
    // 降级策略见 §10.2
    throw err
  }
}
```

#### 批量优化（可选）

高吞吐场景下，可启用批量发送：

```typescript
// Ingestor 内维护一个 buffer，达到阈值或超时后 flush
const BATCH_SIZE = 100
const BATCH_TIMEOUT_MS = 50
// → producer.send({ messages: [...batch] })
```

### 4.3 Consumer 设计

#### 配置

```typescript
// backend/src/kafka/consumer.ts
const consumer = kafka.consumer({
  groupId: "metlo-analyzer",
  sessionTimeout: 30000,
  heartbeatInterval: 3000,
  maxBytesPerPartition: 1048576,   // 1 MB
  readUncommitted: false,
})
```

#### 消费逻辑（核心）

```typescript
// 替换 analyzer.ts 中 getQueuedApiTrace() + sleep 循环
async function startAnalyzerConsumer(): Promise<void> {
  await consumer.connect()
  await consumer.subscribe({ topic: TRACE_TOPIC, fromBeginning: false })

  // Piscina Worker Pool（复用现有）
  const pool = new Piscina({
    filename: path.resolve(__dirname, "analyze-traces.js"),
    maxThreads: parseInt(process.env.NUM_WORKERS || "4"),
    idleTimeout: 10 * 60 * 1000,
    maxQueue: 4096,
  })

  let paused = false

  await consumer.run({
    autoCommit: false,            // 手动提交，at-least-once
    eachBatchAutoResolve: false,

    eachBatch: async ({
      batch,
      resolveOffset,
      heartbeat,
      pause: pauseConsumer,
      commitOffsetsIfNecessary,
    }) => {
      const { topic, partition, messages } = batch

      for (const message of messages) {
        // ---- 背压控制 ----
        // Piscina maxQueue=4096，超出时暂停消费
        if (pool.queueSize >= 4096 && !paused) {
          pauseConsumer()
          paused = true
        }

        const traceTask: TraceTask = JSON.parse(message.value.toString())
        traceTask.ctx = ctx
        traceTask.version = 2
        traceTask.hasValidEnterpriseLicense = hasValidLicense

        pool.run(traceTask)
          .then(() => resolveOffset(message.offset))
          .catch((err) => {
            mlog.withErr(err).error(`Analysis failed, offset: ${message.offset}`)
            resolveOffset(message.offset) // 不阻塞后续消费
          })
      }

      // 等待 Piscina 队列消化
      while (pool.queueSize > 0) {
        await sleep(100)
        await heartbeat()
      }

      if (paused) {
        // Piscina 队列已空，恢复消费
        paused = false
      }

      await commitOffsetsIfNecessary()
    },
  })
}
```

#### 背压机制

```
Kafka Consumer (max.poll.records=500)
       │
       │  batch push
       ▼
Piscina Worker Pool (maxQueue=4096, maxThreads=4)
       │
       ├── queueSize < 4096  → 继续 poll，正常消费
       │
       └── queueSize >= 4096 → pause()，等待 pool 消化
                                  ↓
                              queueSize < 1024  → resume()
```

关键参数调优：

| 参数 | 值 | 说明 |
|------|-----|------|
| `max.poll.records` | 500 | 每批次拉取上限，过大增加暂停延迟 |
| Piscina `maxQueue` | 4096 | Worker 背压阈值 |
| Piscina `maxThreads` | CPU 核心数 | 取决于单 trace 平均分析耗时 |
| 暂停阈值 | queueSize ≥ 4096 | 触发消费暂停 |
| 恢复阈值 | queueSize < 1024 | 恢复消费，留缓冲避免频繁启停 |

### 4.4 GraphQL 展开逻辑前移

**现有逻辑**（Analyzer 内，不可接受）：

```
Analyzer 收到一个 GraphQL trace
  → createGraphQlTraces() 展开为多个子 trace
  → 每个子 trace 再 push 回 Redis TRACES_QUEUE（自循环）
```

**改造后**（LogRequest V2 / Ingestor 内）：

```typescript
// services/log-request/v2/index.ts
async function handleTrace(rawRequest: RawRequest): Promise<void> {
  const trace = buildQueuedApiTrace(rawRequest)

  if (isGraphQlEndpoint(trace)) {
    // GraphQL 操作展开
    const traces = expandGraphQlTrace(trace)

    // 批量生产到 Kafka
    const messages = traces.map(t => ({
      key: `${t.host}.${t.path}`,
      value: JSON.stringify(t),
    }))
    await producer.send({ topic: TRACE_TOPIC, messages })
  } else {
    // 普通 REST API
    await producer.send({
      topic: TRACE_TOPIC,
      messages: [{ key: trace.host, value: JSON.stringify(trace) }],
    })
  }
}
```

Analyzer 端移除 `createGraphQlTraces` 及其自循环逻辑。

---

## 5. 改造清单

### 5.1 新增文件

| 文件 | 职责 |
|------|------|
| `backend/src/kafka/index.ts` | Kafka Client 初始化，Producer/Consumer 工厂 |
| `backend/src/kafka/producer.ts` | Trace Producer 封装，含重试与降级 |
| `backend/src/kafka/consumer.ts` | Analyzer Consumer 封装，含背压控制 |

### 5.2 修改文件

| 文件 | 变更内容 | 影响范围 |
|------|----------|----------|
| `backend/src/services/log-request/v2/index.ts` | Redis LPUSH → Kafka send；新增 GraphQL 展开 | Ingestor 写入路径 |
| `backend/src/analyzer.ts` | 删除 LPOP 轮询，改为 Kafka Consumer；删除 GraphQL 自循环逻辑 | Analyzer 消费路径 |
| `backend/src/constants.ts` | 新增 Kafka 相关常量（topic、group 等） | 全局常量 |
| `backend/package.json` | 新增 `kafkajs` 依赖 | 依赖管理 |
| `backend/src/collector.ts` | 如直接操作 Redis，改为 Kafka 初始化 | Ingestor 入口 |
| `docker-compose.yaml` | 新增 Kafka + Zookeeper 容器（开发环境） | 部署编排 |
| `deploy/backend/Dockerfile` | 无需改动，仅确认 Node 版本兼容 | 镜像构建 |

### 5.3 不变部分

以下模块**零改动**，彻底不感知底层消息队列变迁：

```
backend/src/services/scanner/          # 敏感数据扫描
backend/src/services/analyze/v2/       # 分析 Pipeline
backend/src/services/alert/            # 告警生成
backend/src/services/spec/             # OpenAPI 差分
backend/src/services/data-field/       # 数据字段管理
backend/src/services/data-classes/     # 数据类管理
backend/src/services/endpoint/         # 端点发现
backend/src/api/                       # REST API 路由
frontend/                              # Web UI
ingestors/                             # 各语言 SDK
cli/                                   # 部署 CLI
common/                                # 共享类型/枚举
```

### 5.4 环境变量

```bash
# 新增
KAFKA_BROKERS=kafka:9092
KAFKA_TRACE_TOPIC=metlo.traces
KAFKA_CONSUMER_GROUP=metlo-analyzer

# 保留（Analyzer 仍使用 Piscina）
NUM_WORKERS=4

# 可废弃（不再使用 Redis 做 trace queue）
# REDIS_URL 仍保留，用于配置缓存等其他用途
```

---

## 6. 数据模型

### 6.1 Kafka Message Value 完整定义

```typescript
// @common/types.ts — QueuedApiTrace（现有类型，无需修改）

interface QueuedApiTrace {
  // ─── 请求标识 ───
  path: string                              // 原始路径，如 /api/users/42/orders
  method: RestMethod                        // HTTP 方法枚举
  host: string                              // 目标主机
  createdAt: Date                           // ISO 8601 时间戳

  // ─── 请求内容 ───
  requestHeaders: Array<{
    name: string                            // 如 "Content-Type"
    value: string                           // 如 "application/json"
  }>
  requestParameters: Array<{
    name: string                            // 如 "page"
    value: string                           // 如 "1"
  }>
  requestBody: string                       // 原始请求体

  // ─── 响应内容 ───
  responseStatus: number                    // HTTP 状态码
  responseHeaders: Array<{ name: string; value: string }>
  responseBody: string                      // 原始响应体

  // ─── 预处理器输出 ───
  processedTraceData: ProcessedTraceData

  // ─── 元信息 ───
  meta?: Array<{
    environment: string                     // "production"|"staging"|"development"
    agent: string                           // "go-sdk"|"python-sdk"|"aws-mirror"|...
    agentVersion: string
    sourceIp?: string
  }>
}

interface ProcessedTraceData {
  requestContentType: string
  responseContentType: string

  graphqlPaths?: string[]                   // 如 ["reqBody.query.getUser"]
  xssDetected?: Record<string, string[]>
  sqliDetected?: Record<string, string[]>
  attackDetections?: Record<string, string[]>
  sensitiveDataDetected?: Record<string, string[]>
  dataTypes?: Record<string, DataType[]>
}
```

### 6.2 Schema 兼容性策略

| 策略 | 说明 |
|------|------|
| 新增字段 | 向后兼容，旧 Consumer 忽略未知字段 |
| 枚举扩展 | 新增枚举值不影响现有逻辑 |
| 字段删除 | 不删除，标记 `@deprecated`，一个版本后移入 `legacy` 字段 |
| Schema Registry | 可选——当前 JSON 序列化足够，后期可引入 Avro |

### 6.3 消息大小控制

```
单条 trace 上限: 1 MB（Kafka topic 默认 max.message.bytes）
超过上限策略: Ingestor 截断 requestBody/responseBody（保留前 512KB）+ 标记 truncated=true
```

---

## 7. 消费与背压控制

### 7.1 消费语义

```
语义: at-least-once（先分析、后提交 offset）
幂等保证: Analyzer 内部通过 trace.uuid + endpoint 去重
```

### 7.2 背压流控

```
                    ┌─────────────────────┐
                    │   Kafka Consumer     │
                    │   max.poll.records   │
                    │        = 500         │
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │   Piscina Queue       │
                    │   maxQueue = 4096     │
                    │                       │
                    │  queueSize < 4096     │──── 正常消费
                    │  queueSize >= 4096    │──── consumer.pause()
                    │  queueSize < 1024     │──── consumer.resume()
                    └──────────┬───────────┘
                               │
                    ┌──────────▼───────────┐
                    │   Worker Pool         │
                    │   maxThreads = 4      │
                    │   idleTimeout = 10min │
                    └───────────────────────┘
```

### 7.3 异常处理

```
┌─────────────────────────────────────────────────┐
│  消费异常分类                                     │
├───────────────┬──────────┬───────────────────────┤
│  类型          │  策略     │  行为                 │
├───────────────┼──────────┼───────────────────────┤
│  单条分析失败  │  跳过     │  resolveOffset(offset) │
│  反序列化失败  │  跳过     │  记录死信日志          │
│  DB 连接断开   │  重试     │  consumer.pause()      │
│  持续消费失败  │  告警     │  触发 on-call + 人工   │
│  消费 Lag 过高 │  告警     │  按需扩容 Analyzer     │
└───────────────┴──────────┴───────────────────────┘
```

---

## 8. 迁移策略

### 8.1 阶段一：并行运行（无风险）

```
                    ┌─────────┐
                    │ Ingestor │
                    └────┬────┘
                         │
              ┌──────────┼──────────┐
              ▼                     ▼
        ┌──────────┐         ┌──────────┐
        │  Redis   │         │  Kafka   │
        │  (旧)    │         │  (新)    │
        └────┬─────┘         └────┬─────┘
             │                    │
             ▼                    ▼
        ┌──────────┐         ┌──────────┐
        │ Analyzer │         │ Analyzer │
        │ (旧版)   │         │ (新版)   │
        └──────────┘         └──────────┘

切换方式: 环境变量 KAFKA_ENABLED=true 切换消费路径
回滚方式: KAFKA_ENABLED=false 切回 Redis
```

**验证标准**：
- Kafka Consumer Lag 稳定 < 1000
- Analyzer 告警产出量与旧版一致（偏差 < 1%）
- 端到端延迟无明显劣化（p99 < 旧版 p99 × 1.2）

### 8.2 阶段二：切换与观察

**时长**: 1 周

1. 新 Ingestor 双写 Redis + Kafka（一周数据累积）
2. 新 Analyzer 消费 Kafka，旧 Analyzer 关闭
3. 持续对比新旧告警数量、误报率

### 8.3 阶段三：下线旧链路

1. 移除 Ingestor 的 Redis 写入逻辑
2. 移除 `analyzer.ts` 中的 `LPOP` 轮询 + GraphQL 自循环
3. 清理 `constants.ts` 中 `TRACES_QUEUE` 常量

---

## 9. 监控与运维

### 9.1 核心指标

| 指标 | 来源 | 告警阈值 |
|------|------|----------|
| Consumer Lag | Kafka Exporter | > 5000 持续 5min |
| 消费吞吐 (msg/s) | Consumer Metrics | 无（趋势监控） |
| Piscina Queue Size | 应用 Metrics | > 3000 持续 2min |
| 端到端延迟 (ingest→analyze) | 应用 Metrics | p99 > 30s |
| 分析失败率 | 应用 Metrics | > 1% |
| Kafka 写入失败率 | Producer Metrics | > 0.1% |

### 9.2 日志关键事件

```
[KAFKA] producer connected to brokers: kafka:9092
[KAFKA] consumer joined group: metlo-analyzer, assigned partitions: [0,1,2,3]
[KAFKA] consumer paused (queueSize=4096), partition=5
[KAFKA] consumer resumed (queueSize=512)
[KAFKA] produce failed (retries exhausted), trace dropped, host=api.example.com
[KAFKA] consumer offset committed, partition=0, offset=12345
```

### 9.3 运维操作手册

```bash
# 重置 Consumer Group offset（回溯分析最近 1 小时流量）
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group metlo-analyzer \
  --topic metlo.traces \
  --reset-offsets --to-datetime 2026-05-28T20:00:00.000 \
  --execute

# 查看 Consumer Group 状态
kafka-consumer-groups --bootstrap-server kafka:9092 \
  --group metlo-analyzer --describe

# 扩容分区（需要停止 Consumer 后操作）
kafka-topics --bootstrap-server kafka:9092 \
  --alter --topic metlo.traces --partitions 24
```

---

## 10. 风险与缓解

### 10.1 风险矩阵

| 风险 | 概率 | 影响 | 缓解措施 |
|------|------|------|----------|
| Kafka 集群不可用 | 低 | 高 | Ingestor 本地写 buffer 降级；双链路保底 |
| Consumer Lag 持续增长 | 中 | 中 | HPA 自动扩容 Analyzer；分区动态扩展 |
| Piscina 背压过载 | 中 | 中 | 调优 `maxThreads`；监控队列深度 |
| JSON 序列化性能瓶颈 | 低 | 低 | 后期引入 Avro + Schema Registry |
| 消息乱序导致端点发现偏差 | 低 | 低 | 按 host 分区保证局部有序 |
| `kafkajs` 与 Node 版本兼容 | 低 | 中 | 锁定版本；CI 集成测试 |

### 10.2 Kafka 不可用降级策略

```typescript
// Ingestor 侧降级
class TracePublisher {
  private buffer: QueuedApiTrace[] = []
  private readonly MAX_BUFFER = 10000

  async publish(trace: QueuedApiTrace): Promise<void> {
    try {
      await producer.send({ topic: TRACE_TOPIC, messages: [{ key: trace.host, value: JSON.stringify(trace) }] })
      await this.flushBuffer()  // 恢复后刷盘
    } catch (err) {
      mlog.withErr(err).warn("Kafka unavailable, buffering trace")
      if (this.buffer.length < this.MAX_BUFFER) {
        this.buffer.push(trace)
      } else {
        mlog.error("Buffer overflow, dropping trace")
      }
    }
  }

  private async flushBuffer(): Promise<void> {
    while (this.buffer.length > 0) {
      const batch = this.buffer.splice(0, 500)
      await producer.send({
        topic: TRACE_TOPIC,
        messages: batch.map(t => ({ key: t.host, value: JSON.stringify(t) })),
      })
    }
  }
}
```

### 10.3 测试策略

| 测试类型 | 内容 | 触发条件 |
|----------|------|----------|
| 单元测试 | Producer/Consumer 封装函数 | PR |
| 集成测试 | Docker Compose 全链路 (Kafka + Analyzer + DB) | PR |
| 性能测试 | 10,000 msg/s 持续 30min，验证 Lag 稳定 | 发版前 |
| 混沌测试 | 随机 Kill Kafka Broker，验证降级与恢复 | 按周 |
| 数据对比 | 新旧链路告警产出量对比，偏差 < 1% | 迁移期间每日 |

---

## 附录

### A. 依赖版本锁定

```json
{
  "kafkajs": "^2.2.4"
}
```

### B. Docker Compose 开发环境补充

```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

### C. 改造总代码量预估

| 类别 | 行数 |
|------|------|
| 新增（Kafka 模块） | ~200 |
| 修改（Ingestor） | ~30 |
| 修改（Analyzer） | ~50（删除为主） |
| 修改（配置/常量） | ~20 |
| 测试 | ~300 |
| **合计** | **~600 行** |
