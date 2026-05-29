# Metlo 核心分析输出 Kafka Producer 服务 — 设计方案

> 版本: v1.0  
> 日期: 2026-05-29  
> 状态: 方案评审  
> 依赖: metlo-kafka-data-model.ts, metlo-kafka-adapter.ts

---

## 目录

1. [设计目标](#1-设计目标)
2. [输出 Topic 设计](#2-输出-topic-设计)
3. [数据格式定义](#3-数据格式定义)
4. [Producer 服务设计](#4-producer-服务设计)
5. [代码集成点](#5-代码集成点)
6. [下游消费场景](#6-下游消费场景)

---

## 1. 设计目标

将 Metlo 核心分析模块的输出（告警、端点、敏感数据字段、OpenAPI 差分）**实时推送至 Kafka**，使下游系统可以：

- **SIEM 集成**: 安全告警直接接入 Splunk/ELK/安全运营中心
- **实时告警通知**: 独立 Consumer 监听 HIGH 告警 → 钉钉/飞书/邮件
- **数据湖归档**: 所有端点变更 + 数据字段写入数据湖做长期分析
- **API 资产同步**: 自动发现的端点同步到 CMDB/API Gateway
- **合规审计**: DataField 变更记录满足 GDPR/等保审计追溯

---

## 2. 输出 Topic 设计

```
┌───────────────────────────────────────────────────────────┐
│                    Metlo Backend                           │
│                                                           │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │ Analyzer │  │  Alert   │  │ Endpoint │  │DataField │ │
│  │ Pipeline │  │ Service  │  │ Service  │  │ Service  │ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘ │
│       │             │             │             │        │
└───────┼─────────────┼─────────────┼─────────────┼────────┘
        │             │             │             │
        │    ┌────────┴────────┐    │    ┌────────┴────────┐
        │    │  KafkaProducer  │    │    │  KafkaProducer  │
        │    │  (event-driven) │    │    │  (event-driven) │
        │    └────────┬────────┘    │    └────────┬────────┘
        │             │             │             │
        ▼             ▼             ▼             ▼
┌───────────────────────────────────────────────────────────┐
│                     Kafka Cluster                          │
│                                                           │
│  metlo.alerts           metlo.endpoints                   │
│  (12 partitions)        (6 partitions)                    │
│  Key: apiEndpointUuid   Key: host                         │
│                                                           │
│  metlo.datafields       metlo.spec.diffs                  │
│  (6 partitions)         (3 partitions)                    │
│  Key: apiEndpointUuid   Key: apiEndpointUuid              │
└───────────────────────────────────────────────────────────┘
```

| Topic | 用途 | Partitions | Retention | Message Key | 数据来源 |
|-------|------|-----------|-----------|-------------|----------|
| `metlo.alerts` | 安全告警事件 | 12 | 30 days | `apiEndpointUuid` | `services/alert/` |
| `metlo.endpoints` | 端点发现/变更 | 6 | 90 days | `host` | `services/endpoint/` |
| `metlo.datafields` | 敏感数据字段 | 6 | 30 days | `apiEndpointUuid` | `services/data-field/` |
| `metlo.spec.diffs` | OpenAPI 规范差分 | 3 | 7 days | `apiEndpointUuid` | `services/spec/` |

### 分区策略

- **`metlo.alerts`**: `apiEndpointUuid` 分区 — 同一端点的告警有序到达，方便消费者做告警聚合和去重
- **`metlo.endpoints`**: `host` 分区 — 同一 host 的端点变更有序，保持端点发现的时序连续性
- **`metlo.datafields`**: `apiEndpointUuid` 分区 — 同一端点的数据字段变更有序
- **`metlo.spec.diffs`**: `apiEndpointUuid` 分区 — 量小，3 分区足够

---

## 3. 数据格式定义

### 3.1 `metlo.alerts` — 告警事件

```typescript
// Topic: metlo.alerts
// Key: alert.apiEndpointUuid
// Value: KafkaAlertMessage

interface KafkaAlertMessage {
  /** 消息元信息 */
  meta: {
    /** 消息唯一 ID (幂等去重) */
    messageId: string              // uuid v4
    /** 事件时间戳 */
    timestamp: string              // ISO 8601
    /** 事件类型 */
    eventType: "alert_created" | "alert_updated" | "alert_resolved" | "alert_ignored"
  }

  /** 告警实体 */
  alert: {
    /** 告警唯一 ID */
    uuid: string
    /** 告警类型 */
    type: AlertType                // "New Endpoint Detected" | "PII Data Detected" | ...
    /** 风险评分 */
    riskScore: RiskScore           // "none" | "low" | "medium" | "high"
    /** 关联端点 */
    apiEndpointUuid: string
    /** 告警描述 */
    description: string
    /** 告警状态 */
    status: "Open" | "Resolved" | "Ignored"
    /** 解决说明 (Resolved 时填充) */
    resolutionMessage?: string
    /** 创建时间 */
    createdAt: string              // ISO 8601
    /** 更新时间 */
    updatedAt: string              // ISO 8601
  }

  /** 关联上下文 (按需填充，减少 Consumer 额外查询) */
  context?: {
    /** 端点摘要 */
    endpoint?: {
      path: string                 // 参数化路径, 如 /api/users/{param1}
      method: RestMethod
      host: string
      riskScore: RiskScore
      firstDetected?: string
      lastActive?: string
    }
    /** 关联的数据字段 (仅 PII_DATA_DETECTED 类型) */
    dataFields?: Array<{
      dataPath: string             // 如 "resBody.user.email"
      dataSection: DataSection
      dataClasses: string[]        // ["Email", "Credit Card Number"]
    }>
    /** 触发告警的 Trace 摘要 (不含敏感 body) */
    traceSummary?: {
      traceUuid: string
      responseStatus: number
      sourceIp?: string
      truncated: boolean
    }
  }
}
```

#### 消息示例

```json
{
  "meta": {
    "messageId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "timestamp": "2026-05-29T08:32:44.711Z",
    "eventType": "alert_created"
  },
  "alert": {
    "uuid": "f9e8d7c6-b5a4-3210-9876-543210fedcba",
    "type": "PII Data Detected",
    "riskScore": "medium",
    "apiEndpointUuid": "12345678-1234-1234-1234-123456789abc",
    "description": "Sensitive data of type Email detected in field 'resBody.data.email' of Response Body.",
    "status": "Open",
    "createdAt": "2026-05-29T08:32:44.000Z",
    "updatedAt": "2026-05-29T08:32:44.000Z"
  },
  "context": {
    "endpoint": {
      "path": "/api/v1/users/{param1}",
      "method": "GET",
      "host": "api.example.com",
      "riskScore": "medium",
      "firstDetected": "2026-05-20T00:00:00.000Z",
      "lastActive": "2026-05-29T08:30:00.000Z"
    },
    "dataFields": [
      {
        "dataPath": "resBody.data.email",
        "dataSection": "resBody",
        "dataClasses": ["Email"]
      }
    ],
    "traceSummary": {
      "traceUuid": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
      "responseStatus": 200,
      "sourceIp": "10.0.1.5",
      "truncated": false
    }
  }
}
```

### 3.2 `metlo.endpoints` — 端点变更事件

```typescript
// Topic: metlo.endpoints
// Key: endpoint.host
// Value: KafkaEndpointMessage

interface KafkaEndpointMessage {
  meta: {
    messageId: string
    timestamp: string
    eventType: "endpoint_discovered" | "endpoint_updated" | "endpoint_risk_changed"
  }

  endpoint: {
    uuid: string
    path: string                    // 参数化路径
    pathRegex: string               // 路径匹配正则
    method: RestMethod
    host: string
    numberParams: number            // 路径参数数量
    riskScore: RiskScore
    isGraphQl: boolean

    /** 是否为认证端点 */
    isAuthenticated?: boolean
    /** 是否为敏感数据端点 */
    isSensitive?: boolean

    firstDetected?: string
    lastActive?: string
    createdAt: string
    updatedAt: string
  }

  /** 变更详情 (eventType=endpoint_updated 时填充) */
  changeDetail?: {
    /** 变更前的风险评分 */
    previousRiskScore?: RiskScore
    /** 变更前的认证状态 */
    previousAuthStatus?: boolean
    /** 变更原因 */
    reason?: string
  }
}
```

#### 消息示例

```json
{
  "meta": {
    "messageId": "11111111-1111-1111-1111-111111111111",
    "timestamp": "2026-05-29T08:33:00.000Z",
    "eventType": "endpoint_discovered"
  },
  "endpoint": {
    "uuid": "12345678-1234-1234-1234-123456789abc",
    "path": "/api/v1/users/{param1}",
    "pathRegex": "^/api/v1/users/[^/]+$",
    "method": "GET",
    "host": "api.example.com",
    "numberParams": 1,
    "riskScore": "low",
    "isGraphQl": false,
    "isAuthenticated": true,
    "isSensitive": false,
    "firstDetected": "2026-05-29T08:33:00.000Z",
    "lastActive": "2026-05-29T08:33:00.000Z",
    "createdAt": "2026-05-29T08:33:00.000Z",
    "updatedAt": "2026-05-29T08:33:00.000Z"
  }
}
```

### 3.3 `metlo.datafields` — 敏感数据字段事件

```typescript
// Topic: metlo.datafields
// Key: dataField.apiEndpointUuid
// Value: KafkaDataFieldMessage

interface KafkaDataFieldMessage {
  meta: {
    messageId: string
    timestamp: string
    eventType: "datafield_detected" | "datafield_updated" | "datafield_falsepositive"
  }

  dataField: {
    uuid: string
    /** 字段路径, 如 "resBody.data.email" */
    dataPath: string
    /** 字段所在区域 */
    dataSection: DataSection
    /** 检测到的数据类 */
    dataClasses: string[]
    /** 误报标记的数据类 */
    falsePositives: string[]
    /** Scanner 识别的数据类型 */
    scannerIdentified: string[]
    /** 推断的 JSON 数据类型 */
    dataType: DataType
    /** 关联端点 */
    apiEndpointUuid: string
    /** 数据标签 (如 "PII") */
    dataTag?: string
    createdAt: string
    updatedAt: string
  }

  /** 端点摘要 (减少 Consumer 二次查询) */
  endpointSummary?: {
    path: string
    method: RestMethod
    host: string
    riskScore: RiskScore
  }
}
```

### 3.4 `metlo.spec.diffs` — OpenAPI 规范差分事件

```typescript
// Topic: metlo.spec.diffs
// Key: diff.apiEndpointUuid
// Value: KafkaSpecDiffMessage

interface KafkaSpecDiffMessage {
  meta: {
    messageId: string
    timestamp: string
    eventType: "spec_diff_detected"
  }

  diff: {
    /** 关联端点 */
    apiEndpointUuid: string
    /** 关联的 OpenAPI Spec ID */
    specId: string
    /** 差异类型 */
    diffType: "unexpected_parameter" | "missing_parameter"
              | "type_mismatch" | "missing_endpoint"
              | "unexpected_status_code"
    /** 差异详情 */
    detail: string
    /** 实际观察到的值 */
    observedValue?: string
    /** 规范期望的值 */
    expectedValue?: string
    createdAt: string
  }

  endpointSummary?: {
    path: string
    method: RestMethod
    host: string
  }
}
```

---

## 4. Producer 服务设计

### 4.1 架构

```
backend/src/kafka/
├── index.ts              # Kafka Client 工厂 (已存在)
├── producer.ts           # Trace Producer (已存在, 入向)
├── consumer.ts           # Trace Consumer (已存在, 入向)
├── output-producer.ts    # ★ 新增: 出向 Producer 服务
└── types.ts              # 类型定义 (已存在)
```

### 4.2 OutputProducer 核心实现

```typescript
// backend/src/kafka/output-producer.ts

import { Kafka, Producer } from "kafkajs"
import { v4 as uuidv4 } from "uuid"
import { Alert } from "models/alert"
import { ApiEndpoint } from "models/api-endpoint"
import { DataField } from "models/data-field"
import mlog from "logger"

// ── Topic 常量 ──
export const OUTPUT_TOPICS = {
  ALERTS:      "metlo.alerts",
  ENDPOINTS:   "metlo.endpoints",
  DATAFIELDS:  "metlo.datafields",
  SPEC_DIFFS:  "metlo.spec.diffs",
} as const

// ── 消息元信息工厂 ──
function createMeta(eventType: string): KafkaMessageMeta {
  return {
    messageId: uuidv4(),
    timestamp: new Date().toISOString(),
    eventType,
  }
}

export interface KafkaMessageMeta {
  messageId: string
  timestamp: string
  eventType: string
}

// ════════════════════════════════════════════════════════════
// OutputProducer — 全局单例
// ════════════════════════════════════════════════════════════

export class OutputProducer {
  private producer: Producer
  private enabled: boolean
  private static instance: OutputProducer

  private constructor() {}

  static async init(brokers: string[]): Promise<void> {
    const instance = new OutputProducer()
    const kafka = new Kafka({
      clientId: "metlo-output-producer",
      brokers,
      retry: { initialRetryTime: 100, retries: 3 },
    })
    instance.producer = kafka.producer({
      allowAutoTopicCreation: false,
      maxInFlightRequests: 5,
      idempotent: true,
    })
    instance.enabled = brokers.length > 0
    if (instance.enabled) {
      await instance.producer.connect()
      mlog.info("OutputProducer connected")
    }
    OutputProducer.instance = instance
  }

  static getInstance(): OutputProducer {
    if (!OutputProducer.instance) {
      throw new Error("OutputProducer not initialized. Call OutputProducer.init() first.")
    }
    return OutputProducer.instance
  }

  // ── 告警事件 ──

  async publishAlertCreated(
    alert: Alert,
    endpoint: ApiEndpoint,
    dataFields?: DataField[],
    traceSummary?: { traceUuid: string; responseStatus: number; sourceIp?: string; truncated: boolean },
  ): Promise<void> {
    if (!this.enabled) return
    const msg: KafkaAlertMessage = {
      meta: createMeta("alert_created"),
      alert: {
        uuid: alert.uuid,
        type: alert.type,
        riskScore: alert.riskScore,
        apiEndpointUuid: alert.apiEndpointUuid,
        description: alert.description,
        status: alert.status,
        resolutionMessage: alert.resolutionMessage,
        createdAt: alert.createdAt.toISOString(),
        updatedAt: alert.updatedAt.toISOString(),
      },
      context: {
        endpoint: endpoint ? {
          path: endpoint.path,
          method: endpoint.method,
          host: endpoint.host,
          riskScore: endpoint.riskScore,
          firstDetected: endpoint.firstDetected?.toISOString(),
          lastActive: endpoint.lastActive?.toISOString(),
        } : undefined,
        dataFields: dataFields?.map(df => ({
          dataPath: df.dataPath,
          dataSection: df.dataSection,
          dataClasses: df.dataClasses,
        })),
        traceSummary,
      },
    }
    await this.send(OUTPUT_TOPICS.ALERTS, alert.apiEndpointUuid, msg)
  }

  async publishAlertUpdated(alert: Alert, eventType: string): Promise<void> {
    if (!this.enabled) return
    const msg: KafkaAlertMessage = {
      meta: createMeta(eventType),
      alert: {
        uuid: alert.uuid,
        type: alert.type,
        riskScore: alert.riskScore,
        apiEndpointUuid: alert.apiEndpointUuid,
        description: alert.description,
        status: alert.status,
        resolutionMessage: alert.resolutionMessage,
        createdAt: alert.createdAt.toISOString(),
        updatedAt: alert.updatedAt.toISOString(),
      },
    }
    await this.send(OUTPUT_TOPICS.ALERTS, alert.apiEndpointUuid, msg)
  }

  // ── 端点事件 ──

  async publishEndpointDiscovered(endpoint: ApiEndpoint): Promise<void> {
    if (!this.enabled) return
    const msg: KafkaEndpointMessage = {
      meta: createMeta("endpoint_discovered"),
      endpoint: this.serializeEndpoint(endpoint),
    }
    await this.send(OUTPUT_TOPICS.ENDPOINTS, endpoint.host, msg)
  }

  async publishEndpointUpdated(
    endpoint: ApiEndpoint,
    changeReason: string,
    previousRiskScore?: RiskScore,
    previousAuthStatus?: boolean,
  ): Promise<void> {
    if (!this.enabled) return
    const msg: KafkaEndpointMessage = {
      meta: createMeta("endpoint_updated"),
      endpoint: this.serializeEndpoint(endpoint),
      changeDetail: { previousRiskScore, previousAuthStatus, reason: changeReason },
    }
    await this.send(OUTPUT_TOPICS.ENDPOINTS, endpoint.host, msg)
  }

  // ── 数据字段事件 ──

  async publishDataFieldDetected(
    dataField: DataField,
    endpoint: ApiEndpoint,
  ): Promise<void> {
    if (!this.enabled) return
    const msg: KafkaDataFieldMessage = {
      meta: createMeta("datafield_detected"),
      dataField: {
        uuid: dataField.uuid,
        dataPath: dataField.dataPath,
        dataSection: dataField.dataSection,
        dataClasses: dataField.dataClasses,
        falsePositives: dataField.falsePositives,
        scannerIdentified: dataField.scannerIdentified,
        dataType: dataField.dataType,
        apiEndpointUuid: dataField.apiEndpointUuid,
        dataTag: dataField.dataTag,
        createdAt: dataField.createdAt.toISOString(),
        updatedAt: dataField.updatedAt.toISOString(),
      },
      endpointSummary: {
        path: endpoint.path,
        method: endpoint.method,
        host: endpoint.host,
        riskScore: endpoint.riskScore,
      },
    }
    await this.send(OUTPUT_TOPICS.DATAFIELDS, dataField.apiEndpointUuid, msg)
  }

  // ── Spec 差分事件 ──

  async publishSpecDiff(
    specDiff: {
      apiEndpointUuid: string
      specId: string
      diffType: string
      detail: string
      observedValue?: string
      expectedValue?: string
    },
    endpoint: ApiEndpoint,
  ): Promise<void> {
    if (!this.enabled) return
    const msg: KafkaSpecDiffMessage = {
      meta: createMeta("spec_diff_detected"),
      diff: {
        ...specDiff,
        createdAt: new Date().toISOString(),
      },
      endpointSummary: {
        path: endpoint.path,
        method: endpoint.method,
        host: endpoint.host,
      },
    }
    await this.send(OUTPUT_TOPICS.SPEC_DIFFS, specDiff.apiEndpointUuid, msg)
  }

  // ── 内部发送 ──

  private async send(topic: string, key: string, value: object): Promise<void> {
    try {
      await this.producer.send({
        topic,
        messages: [{ key, value: JSON.stringify(value) }],
      })
    } catch (err) {
      // 出向推送失败不阻塞分析主流程 — 仅记录日志
      mlog.withErr(err).warn(`OutputProducer: failed to send to ${topic}`)
    }
  }

  private serializeEndpoint(ep: ApiEndpoint) {
    return {
      uuid: ep.uuid,
      path: ep.path,
      pathRegex: ep.pathRegex,
      method: ep.method,
      host: ep.host,
      numberParams: ep.numberParams,
      riskScore: ep.riskScore,
      isGraphQl: ep.isGraphQl,
      isAuthenticated: ep.isAuthenticated,
      isSensitive: ep.isSensitive,
      firstDetected: ep.firstDetected?.toISOString(),
      lastActive: ep.lastActive?.toISOString(),
      createdAt: ep.createdAt.toISOString(),
      updatedAt: ep.updatedAt.toISOString(),
    }
  }
}
```

### 4.3 初始化

在 `backend/src/index.ts` 和 `backend/src/analyzer.ts` 启动时调用：

```typescript
// backend/src/index.ts (API 服务启动)
import { OutputProducer } from "kafka/output-producer"

const main = async () => {
  // ... existing init ...
  await OutputProducer.init(
    process.env.KAFKA_OUTPUT_BROKERS?.split(",") ||
    process.env.KAFKA_BROKERS?.split(",") || []
  )
  // ...
}

// backend/src/analyzer.ts (Analyzer 服务启动 — 同样)
```

### 4.4 配置开关

```bash
# 出向 Kafka (默认复用入向 KAFKA_BROKERS，可独立配置)
KAFKA_OUTPUT_BROKERS=kafka:9092

# 出向推送开关 (默认启用。设为 false 完全禁用出向推送)
KAFKA_OUTPUT_ENABLED=true

# 出向失败不影响分析主流程（始终非阻塞）
```

---

## 5. 代码集成点

### 5.1 Alert 服务集成

**文件**: `backend/src/services/alert/sensitive-data.ts`

```diff
  // 在 createSensitiveDataAlerts() 底部，alert 保存后
+ const outputProducer = OutputProducer.getInstance()
+ await outputProducer.publishAlertCreated(newAlert, apiEndpoint, [dataField], {
+   traceUuid: apiTrace.uuid,
+   responseStatus: apiTrace.responseStatus,
+   sourceIp: apiTrace.meta?.source,
+   truncated: false,
+ })
```

**文件**: `backend/src/api/alert/index.ts` (Alert 状态变更 API)

```diff
  // 在 resolve/ignore 操作后
+ if (alert.status === Status.RESOLVED) {
+   await OutputProducer.getInstance().publishAlertUpdated(alert, "alert_resolved")
+ } else if (alert.status === Status.IGNORED) {
+   await OutputProducer.getInstance().publishAlertUpdated(alert, "alert_ignored")
+ }
```

### 5.2 Endpoint 服务集成

**文件**: `backend/src/services/analyze/v2/index.ts`

```diff
  // 在 createNewEndpointAlert() 之后
+ if (newEndpoint) {
+   await OutputProducer.getInstance().publishEndpointDiscovered(apiEndpoint)
+ }
```

**文件**: 端点风险评分更新处 (Risk Score update logic)

```diff
+ if (prevRiskScore !== apiEndpoint.riskScore) {
+   await OutputProducer.getInstance().publishEndpointUpdated(
+     apiEndpoint,
+     "risk_score_recalculated",
+     prevRiskScore,
+   )
+ }
```

### 5.3 DataField 服务集成

**文件**: `backend/src/services/data-field/v2/analyze.ts`

```diff
  // 在 findDataFieldsToSave() 返回后，保存 dataFields 时
+ const outputProducer = OutputProducer.getInstance()
+ for (const df of newDataFields) {
+   await outputProducer.publishDataFieldDetected(df, apiEndpoint)
+ }
```

### 5.4 Spec 差分集成

**文件**: `backend/src/services/spec/v2.ts`

```diff
  // 在 findOpenApiSpecDiff() 返回后
+ for (const diff of specDiffs) {
+   await OutputProducer.getInstance().publishSpecDiff({
+     apiEndpointUuid: apiEndpoint.uuid,
+     specId: spec.id,
+     diffType: diff.type,
+     detail: diff.message,
+     observedValue: diff.observed,
+     expectedValue: diff.expected,
+   }, apiEndpoint)
+ }
```

---

## 6. 下游消费场景

### 6.1 SIEM 集成

```
metlo.alerts (HIGH only)
  → Consumer: alert-siem-forwarder
  → 过滤 riskScore=high
  → 转换 → Splunk/ELK/Sentinel
  → SOC 值班响应
```

### 6.2 实时通知

```
metlo.alerts
  → Consumer: alert-notifier
  → 匹配 severity + host
  → 钉钉 Webhook / 飞书 / 企业微信 / PagerDuty
```

### 6.3 API 资产同步

```
metlo.endpoints
  → Consumer: api-asset-sync
  → 写入 CMDB / API Gateway 配置
  → 自动注册/注销 API 路由
```

### 6.4 数据湖归档

```
metlo.alerts + metlo.endpoints + metlo.datafields
  → Consumer: lake-sink
  → 批量写入 S3/OSS parquet
  → Athena/Presto 分析
```

---

## 附录 A. 消息类型索引

| 类型文件 | 路径 | 包含 |
|----------|------|------|
| `KafkaAlertMessage` | `kafka/output-types.ts` | 告警 + 端点上下文 + 数据字段 + Trace 摘要 |
| `KafkaEndpointMessage` | `kafka/output-types.ts` | 端点实体 + 变更详情 |
| `KafkaDataFieldMessage` | `kafka/output-types.ts` | 数据字段 + 端点摘要 |
| `KafkaSpecDiffMessage` | `kafka/output-types.ts` | Spec 差分 + 端点摘要 |

## 附录 B. 与现有设计的衔接

```
入向 (已有):                   出向 (本次新增):
                                
RawMirrorLog                   Alert      → metlo.alerts
  └→ adapter.ts                Endpoint   → metlo.endpoints
      └→ metlo.traces.full     DataField  → metlo.datafields
      └→ metlo.traces.partial  SpecDiff   → metlo.spec.diffs
           └→ Analyzer ──────────────────────┘
                │
                └── 分析产出 → OutputProducer
```
