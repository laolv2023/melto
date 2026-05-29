# Metlo Kafka 集成方案

> 基于 [metlo-labs/metlo](https://github.com/metlo-labs/metlo) 的 Kafka 消息队列集成方案 — 入向数据适配 + 核心分析输出。

---

## 目录

1. [项目概述](#项目概述)
2. [文件结构](#文件结构)
3. [架构总览](#架构总览)
4. [快速开始](#快速开始)
5. [组件说明](#组件说明)
6. [测试](#测试)
7. [配置参考](#配置参考)
8. [下游集成](#下游集成)

---

## 项目概述

将 Metlo API 安全平台的消息通道从 **Redis List** 迁移到 **Kafka**，并扩展核心分析模块的输出能力。

### 能力矩阵

| 能力 | 说明 | 涉及文件 |
|------|------|----------|
| **入向适配** | 流量镜像原始日志 → QueuedApiTrace | `metlo-kafka-adapter.ts` |
| **数据模型** | 完整类型定义 + 序列化/反序列化 | `metlo-kafka-data-model.ts` |
| **输出推送** | Analyzer 产出 → 4 个 Kafka Topic | `metlo-kafka-output-design.md` |
| **集成测试** | Python 全链路模拟测试 | `metlo-integration-test.py` |
| **设计文档** | 入向改造方案 (双 Topic) | `metlo-kafka-integration-design.md` |

### 数据流

```
                      ┌─────────────┐
                      │  Kafka       │
                      │  Cluster     │
                      │             │
RawMirrorLog ──┤      │  ┌─────────┐│      ┌──────────────┐
  (Akto dump)  │─适配→│  │FULL     ││─消费→│  Analyzer    │
               │      │  │PARTIAL  ││      │  (Piscina)   │
               │      │  └─────────┘│      └──┬───┬───┬───┘
               │      │             │         │   │   │
               │      │  ┌─────────┐│    ┌────┘   │   └────┐
               │      │  │alerts   ││◄───┤        │        │
               │      │  │endpoints││    │   ┌────┘   ┌────┘
               │      │  │datafields││   │   │        │
               │      │  │spec.diffs││   │   │        │
               │      │  └─────────┘│   │   │        │
                      └─────────────┘   │   │        │
                                        ▼   ▼        ▼
                                     SIEM  CMDB  合规审计
```

---

## 文件结构

```
laolv2023/melto/
├── README.md                              ← 本文件
│
├── 设计文档
│   ├── metlo-kafka-integration-design.md  ← 入向改造方案 (v2.1, 双Topic)
│   └── metlo-kafka-output-design.md       ← 出向Producer方案 (v1.0, 4Topic)
│
├── 代码
│   ├── metlo-kafka-data-model.ts          ← 完整类型定义 (9段, ~350行)
│   └── metlo-kafka-adapter.ts             ← 流量镜像适配层 (9段, ~400行)
│
├── 测试
│   ├── metlo-kafka-adapter.test.ts        ← 适配器单元测试 (15组, 71项)
│   ├── metlo-integration-test.py          ← Python 集成测试 (全链路)
│   └── metlo-output-samples.json          ← 4 Topic 输出样例
│
└── 报告
    └── metlo-alert-report.md              ← 集成测试告警产出报告
```

---

## 架构总览

### 入向链路

| 步骤 | 输入 | 输出 | 文件 |
|------|------|------|------|
| 1. 日志采集 | Akto 流量镜像 dump | `RawMirrorLog` | — |
| 2. 适配 | `RawMirrorLog` | `QueuedApiTraceV1` | `metlo-kafka-adapter.ts` |
| 3. 序列化 | `QueuedApiTraceV1` | `{ctx, version:1, trace}` | `metlo-kafka-data-model.ts` §9 |
| 4. 推送 | Kafka message | `metlo.traces.full` | 入向设计 §4.2 |
| 5. 消费 | Kafka Consumer | `TraceTask` | 入向设计 §4.4 |
| 6. 分析 | `TraceTask` → Analyzer Pipeline | Alert / Endpoint / DataField / SpecDiff | — |

### 出向链路

| 步骤 | 输入 | 输出 Topic | 消息格式 |
|------|------|-----------|----------|
| 告警产出 | `Alert` 实体 | `metlo.alerts` | `KafkaAlertMessage` |
| 端点发现 | `ApiEndpoint` 实体 | `metlo.endpoints` | `KafkaEndpointMessage` |
| 敏感数据 | `DataField` 实体 | `metlo.datafields` | `KafkaDataFieldMessage` |
| 规范差分 | OpenAPI diff | `metlo.spec.diffs` | `KafkaSpecDiffMessage` |

---

## 快速开始

### 前置条件

- Node.js ≥ 18
- TypeScript ≥ 4.7
- Kafka ≥ 3.0 (或 Docker `confluentinc/cp-kafka:7.5.0`)

### 1. 安装依赖

```bash
cd backend/
npm install kafkajs
npm install --save-dev typescript ts-node @types/node
```

### 2. 复制文件到项目

```bash
# 类型定义 → backend/src/kafka/
cp metlo-kafka-data-model.ts backend/src/kafka/types.ts

# 适配器 → backend/src/services/ingestor/
cp metlo-kafka-adapter.ts backend/src/services/ingestor/akto-adapter.ts
```

### 3. 启动 Kafka (开发环境)

```bash
docker compose -f docker-compose.yaml up -d kafka zookeeper
```

### 4. 初始化 Producer

```typescript
// backend/src/index.ts
import { OutputProducer } from "kafka/output-producer"

await OutputProducer.init(
  process.env.KAFKA_BROKERS?.split(",") || ["localhost:9092"]
)
```

### 5. 配置环境变量

```bash
# .env
KAFKA_BROKERS=localhost:9092
KAFKA_OUTPUT_ENABLED=true
NUM_WORKERS=4
```

### 6. 运行测试

```bash
# TypeScript 单元测试
npx ts-node metlo-kafka-adapter.test.ts

# Python 集成测试
python3 metlo-integration-test.py
```

---

## 组件说明

### metlo-kafka-data-model.ts

完整的 TypeScript 类型定义文件，可直接放入 `backend/src/kafka/types.ts`。

**结构:**

| 段落 | 内容 | 主要导出 |
|------|------|----------|
| §1 | 枚举类型 | `RestMethod`, `AnalysisType`, `AuthType`, `RiskScore`, etc. |
| §2 | 基础结构 | `PairObject`, `Meta`, `SessionMeta` |
| §3 | 预分析数据 | `ProcessedTraceData`, `Encryption` |
| §4 | 追踪主体 | `QueuedApiTraceV1`, `QueuedApiTraceV2`, `QueuedApiTrace` |
| §5 | Kafka Message | `FullTraceMessageV1/V2`, `PartialTraceBatchMessage` |
| §6 | Analyzer 任务 | `TraceTask`, `IgnoredDetection` |
| §7 | 辅助类型 | `DataClassConfig`, `BufferedMessage` |
| §8 | 类型守卫 | `isV2Trace()`, `isFullMessage()`, `isPartialMessage()` |
| §9 | 序列化 | `serializeFullMessage()`, `deserializeMessage()` |

**使用示例:**

```typescript
import { adapt } from "./services/ingestor/akto-adapter"
import { serializeFullMessage } from "./kafka/types"

// 适配
const trace = adapt(rawAktoLog)
// 序列化
const kafkaValue = serializeFullMessage(trace, 1)
// 发送
await producer.send({
  topic: "metlo.traces.full",
  messages: [{ key: trace.host, value: kafkaValue }],
})
```

### metlo-kafka-adapter.ts

流量镜像原始日志 → `QueuedApiTraceV1` 的转换器，可直接放入 `backend/src/services/ingestor/`。

**输入格式:**

```json
{
  "path": "/api/users/123",
  "method": "POST",
  "requestHeaders": "{\"Content-Type\":\"application/json\",\"Cookie\":\"...\"}",
  "responseHeaders": "{\"Content-Type\":\"application/json\"}",
  "requestPayload": "{\"key\":\"value\"}",
  "responsePayload": "{\"data\":[]}",
  "ip": "10.0.0.1",
  "destIp": "10.0.0.100",
  "time": "1779867214",
  "statusCode": "200",
  "source": "MIRRORING",
  "direction": "REQUEST",
  "tag": "{\"env\":\"dev\",\"host\":\"...\",\"url\":\"...\"}"
}
```

**核心函数:**

| 函数 | 用途 |
|------|------|
| `adapt(raw)` | 单条转换 |
| `adaptBatch(raws)` | 批量转换 + 错误隔离 |
| `adaptAndProduce(raws, producer)` | 转换 + 分组 → Kafka |
| `parseHeaders(str)` | JSON 字符串 → `PairObject[]` |
| `sanitizeHeaders(headers)` | 敏感 Header 脱敏 |
| `buildSessionMeta(headers)` | Cookie/Auth 解析 → 认证上下文 |

**脱敏规则:**

自动脱敏的 Header: `cookie`, `authorization`, `set-cookie`, `x-api-key`, `x-auth-token`, `proxy-authorization`

脱敏后 value 统一替换为 `[REDACTED]`，name 保留（端点发现需要识别 auth 类型）。

### 输出 Topic 消息格式

详见 `metlo-kafka-output-design.md` §3。

**快速参考:**

```json
// metlo.alerts
{
  "meta": { "messageId": "uuid", "timestamp": "ISO8601", "eventType": "alert_created" },
  "alert": { "uuid": "...", "type": "PII Data Detected", "riskScore": "medium", ... },
  "context": { "endpoint": {...}, "dataFields": [...], "traceSummary": {...} }
}

// metlo.endpoints
{
  "meta": { "eventType": "endpoint_discovered" },
  "endpoint": { "uuid": "...", "path": "/api/users/{param1}", "method": "GET", "host": "..." }
}

// metlo.datafields
{
  "meta": { "eventType": "datafield_detected" },
  "dataField": { "dataPath": "resBody.data.email", "dataClasses": ["Email"], ... },
  "endpointSummary": { "path": "...", "host": "..." }
}

// metlo.spec.diffs
{
  "meta": { "eventType": "spec_diff_detected" },
  "diff": { "diffType": "unexpected_status_code", "detail": "HTTP 401 on ...", ... },
  "endpointSummary": { "path": "...", "host": "..." }
}
```

---

## 测试

### 单元测试 (TypeScript)

```bash
npx ts-node metlo-kafka-adapter.test.ts
```

覆盖 15 组 71 项：字段映射、时间戳转换、脱敏、边界处理、批量适配、序列化。

### 集成测试 (Python)

```bash
python3 metlo-integration-test.py
```

模拟完整链路：

```
69 条 Akto dump 日志
  → 适配 (69/69 成功)
  → 端点发现 (30 个唯一端点)
  → 敏感数据扫描 (64 次命中)
  → 告警生成 (137 条)
  → 输出 4 个 Kafka Topic (231 条消息)
```

**最近一次测试结果:**

```
输入: 69 条 → 输出: 231 条 (4 Topic)
适配: 69/69 (100%)
告警: HIGH=6, MEDIUM=64, LOW=67
脱敏: 38/38 Authorization → [REDACTED]
数据质量: 全部通过
```

### 告警产出报告

```bash
# 查看集成测试生成的告警报告
cat metlo-alert-report.md
```

---

## 配置参考

### Kafka Topic 配置

| Topic | Partitions | Retention | 用途 |
|-------|-----------|-----------|------|
| `metlo.traces.full` | 12 | 7 days | 入向 FULL 分析 |
| `metlo.traces.partial` | 6 | 3 days | 入向 PARTIAL 批量 |
| `metlo.alerts` | 12 | 30 days | 出向告警 |
| `metlo.endpoints` | 6 | 90 days | 出向端点 |
| `metlo.datafields` | 6 | 30 days | 出向数据字段 |
| `metlo.spec.diffs` | 3 | 7 days | 出向规范差分 |

### 环境变量

```bash
# 入向
KAFKA_BROKERS=kafka:9092                    # Kafka 集群地址
KAFKA_FULL_TOPIC=metlo.traces.full          # FULL topic 名
KAFKA_PARTIAL_TOPIC=metlo.traces.partial    # PARTIAL topic 名
KAFKA_CONSUMER_GROUP=metlo-analyzer          # Consumer Group
NUM_WORKERS=4                                # Piscina Worker 数

# 出向
KAFKA_OUTPUT_BROKERS=kafka:9092             # 出向 Broker (默认复用入向)
KAFKA_OUTPUT_ENABLED=true                   # 出向开关
```

### Docker Compose (开发环境)

```yaml
# 追加到 docker-compose.yaml
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
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

---

## 下游集成

### SIEM (安全信息与事件管理)

```bash
# 消费 HIGH 风险告警
kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic metlo.alerts --group siem-forwarder
```

### 钉钉/飞书通知

```python
# Consumer 伪代码
for msg in consumer:
    alert = msg["alert"]
    if alert["riskScore"] == "high":
        webhook.send(f"🔴 HIGH: {alert['description']}")
```

### API 资产同步

```bash
# 消费端点变更
kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic metlo.endpoints --group cmdb-sync
```

### 数据湖归档

```bash
# 批量导出到 S3
kafka-connect-s3 --topics metlo.alerts,metlo.endpoints,metlo.datafields
```

---

## 版本历史

| 版本 | 日期 | 内容 |
|------|------|------|
| v1.0 | 2026-05-28 | 初版 — 单 Topic 入向方案 |
| v2.0 | 2026-05-28 | 双 Topic + V1/V2 完整类型 + GraphQL 前移 |
| v2.1 | 2026-05-29 | Consumer 背压/offset/降级格式修正 |
| — | 2026-05-29 | 适配器 + 单元测试 + 集成测试 |
| — | 2026-05-29 | 4 Topic 出向 Producer 方案 |
