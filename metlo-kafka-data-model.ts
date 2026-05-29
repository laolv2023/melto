/**
 * Metlo Kafka 消息数据模型 — 完整类型定义
 *
 * 来源: metlo-labs/metlo (MIT License)
 * 提取自: common/src/enums.ts, common/src/types.ts, backend/src/types.ts
 *
 * 本文件为 Kafka 化的消息数据模型规范，所有字段均与原始代码保持一致。
 * 标注了 V1/V2 差异、FULL/PARTIAL 分流、字段来源行号。
 *
 * 版本: v2.1
 * 日期: 2026-05-29
 */

// ============================================================================
// §1 枚举类型 — 来源: common/src/enums.ts
// ============================================================================

/** HTTP 方法枚举 */
export enum RestMethod {
  GET = "GET",
  HEAD = "HEAD",
  POST = "POST",
  PUT = "PUT",
  PATCH = "PATCH",
  DELETE = "DELETE",
  CONNECT = "CONNECT",
  OPTIONS = "OPTIONS",
  TRACE = "TRACE",
}

/** 分析类型 — 决定消息路由到 FULL 还是 PARTIAL topic */
export enum AnalysisType {
  /** 单条推送，安全关键路径 (用户数据、认证端点) */
  FULL = "full",
  /** 批量推送，低优先级路径 (health check、ping) */
  PARTIAL = "partial",
}

/** 认证类型 */
export enum AuthType {
  /** 无认证信息 (适配层扩展, 原始枚举中不存在) */
  NONE = "none",
  BASIC = "basic",
  HEADER = "header",
  JWT = "jwt",
  SESSION_COOKIE = "session_cookie",
}

/** 数据分区 — 标识字段属于请求/响应的哪个部分 */
export enum DataSection {
  REQUEST_PATH = "reqPath",
  REQUEST_QUERY = "reqQuery",
  REQUEST_HEADER = "reqHeaders",
  REQUEST_BODY = "reqBody",
  RESPONSE_HEADER = "resHeaders",
  RESPONSE_BODY = "resBody",
}

/** 推断的字段数据类型 */
export enum DataType {
  INTEGER = "integer",
  NUMBER = "number",
  STRING = "string",
  BOOLEAN = "boolean",
  OBJECT = "object",
  ARRAY = "array",
  UNKNOWN = "unknown",
}

/** 风险评分等级 */
export enum RiskScore {
  NONE = "none",
  LOW = "low",
  MEDIUM = "medium",
  HIGH = "high",
}

/** 告警类型 */
export enum AlertType {
  NEW_ENDPOINT = "New Endpoint Detected",
  PII_DATA_DETECTED = "PII Data Detected",
  OPEN_API_SPEC_DIFF = "Open API Spec Diff",
  QUERY_SENSITIVE_DATA = "Sensitive Data in Query Params",
  PATH_SENSITIVE_DATA = "Sensitive Data in Path Params",
  BASIC_AUTHENTICATION_DETECTED = "Basic Authentication Detected",
  UNSECURED_ENDPOINT_DETECTED = "Endpoint not secured by SSL",
  UNAUTHENTICATED_ENDPOINT_SENSITIVE_DATA = "Unauthenticated Endpoint returning Sensitive Data",
}

/** 内置数据类 — 用于敏感数据检测分类 */
export enum DataClass {
  EMAIL = "Email",
  CREDIT_CARD = "Credit Card Number",
  SSN = "Social Security Number",
  PHONE_NUMBER = "Phone Number",
  IP_ADDRESS = "IP Address",
  COORDINATE = "Geographic Coordinates",
  VIN = "Vehicle Identification Number",
  ADDRESS = "Address",
  DOB = "Date of Birth",
  DL_NUMBER = "Driver License Number",
  AADHAR_NUMBER = "Aadhar Number",
  BRAZIL_CPF = "Brazil CPF",
}

/** 攻击检测类型 */
export enum AttackType {
  HIGH_USAGE_SENSITIVE_ENDPOINT = "High Usage on Sensitive Endpoint",
  HIGH_ERROR_RATE = "High Error Rate",
  ANOMALOUS_CALL_ORDER = "Anomalous Call Order",
  UNAUTHENTICATED_ACCESS = "Unauthenticated Access",
  BOLA = "Broken Object Level Authorization",
}

/** 连接/采集器类型 */
export enum ConnectionType {
  AWS = "AWS",
  GCP = "GCP",
  PYTHON = "PYTHON",
  NODE = "NODE",
  JAVA = "JAVA",
  GOLANG = "GOLANG",
  KUBERNETES = "KUBERNETES",
  DOCKERCOMPOSE = "DOCKERCOMPOSE",
  BURPSUITE = "BURPSUITE",
}

/** Kafka Topic 常量 */
export const KafkaTopic = {
  /** FULL 分析类型 topic */
  FULL: "metlo.traces.full",
  /** PARTIAL 分析类型 topic */
  PARTIAL: "metlo.traces.partial",
} as const;

// ============================================================================
// §2 基础结构 — 来源: common/src/types.ts
// ============================================================================

/** 键值对 (Header / Query Parameter 通用结构) */
export interface PairObject {
  name: string;
  value: string;
}

/** 网络层元数据 — 用于攻击溯源 */
export interface Meta {
  /** 是否入站请求 */
  incoming: boolean;
  /** 源 IP */
  source: string;
  /** 源端口 */
  sourcePort: string;
  /** 目标 IP */
  destination: string;
  /** 目标端口 */
  destinationPort: string;
  /** 原始来源 (可选，如经过代理时记录真实客户端 IP) */
  originalSource?: string;
}

/** 会话认证元数据 — 直接影响告警类型判定 */
export interface SessionMeta {
  /** 是否提供了认证信息 */
  authenticationProvided: boolean;
  /** 认证是否成功 */
  authenticationSuccessful: boolean;
  /** 认证类型 */
  authType: AuthType;
  /** 会话唯一标识 — 用于 BOLA 攻击检测 */
  uniqueSessionKey?: string;
  /** 用户标识 */
  user?: string;
}

// ============================================================================
// §3 预分析数据 (仅 V2) — 来源: common/src/types.ts#L94-L105
// ============================================================================

/**
 * SDK 端预分析结果。
 * V1 消息不携带此字段 (undefined)。
 * 无企业 License 时 `block` 始终为 false。
 */
export interface ProcessedTraceData {
  /** 是否应阻断 (需企业 License 才可为 true) */
  block: boolean;
  /** 通用攻击检测命中 — key: 字段路径, value: [攻击类型] */
  attackDetections?: Record<string, string[]>;
  /** XSS 检测命中 — key: 字段路径, value: 载荷类型 */
  xssDetected?: Record<string, string>;
  /** SQL 注入检测命中 — key: 字段路径, value: [注入类型, 载荷]
   *  注意: JSON 序列化后为 string[], 需 Consumer 端判长校验 */
  sqliDetected?: Record<string, string[]>;
  /** 敏感数据检测命中 — key: 字段路径, value: [DataClass 枚举值] */
  sensitiveDataDetected: Record<string, string[]>;
  /** 字段数据类型推断 — key: 字段路径, value: [DataType 枚举值] */
  dataTypes: Record<string, string[]>;
  /** 请求 Content-Type (经 essence 提取) */
  requestContentType: string;
  /** 响应 Content-Type (经 essence 提取) */
  responseContentType: string;
  /** GraphQL 操作路径列表, 如 ["reqBody.query.getUser"] */
  graphqlPaths?: string[];
  /** OpenAPI Spec 验证错误 — key: 字段路径, value: [错误信息] */
  validationErrors?: Record<string, string[]>;
}

/** 加密信息 */
export interface Encryption {
  /** 加密密钥 */
  key: string;
  /** 各字段生成的初始化向量 — key: 字段路径, value: [IV 字节] */
  generatedIvs: Record<string, number[]>;
}

// ============================================================================
// §4 追踪主体 — V1 与 V2 对比
// ============================================================================

/**
 * V1 版本 — 来源: backend/src/services/log-request/index.ts#L56-L69
 *
 * V1 不携带 SDK 预分析结果，Analyzer 需从原始 body 自行扫描敏感数据。
 * V1 消息仅路由到 metlo.traces.full (不支持 PARTIAL 批量)。
 */
export interface QueuedApiTraceV1 {
  /** 请求路径 (原始, 未经参数化), 如 /api/users/123 */
  path: string;
  /** 创建时间 (ISO 8601) */
  createdAt: Date;
  /** 目标主机 */
  host: string;
  /** HTTP 方法 */
  method: RestMethod;
  /** URL 查询参数 */
  requestParameters: PairObject[];
  /** 请求头 */
  requestHeaders: PairObject[];
  /** 请求体 (JSON 字符串或空) */
  requestBody: string;
  /** 响应状态码 */
  responseStatus: number;
  /** 响应头 */
  responseHeaders: PairObject[];
  /** 响应体 (JSON 字符串、纯文本、或空) */
  responseBody: string;
  /** 网络层元数据 */
  meta: Meta;
  /** 会话认证元数据 */
  sessionMeta: SessionMeta;

  // ── 以下字段 V1 不携带, 始终为 undefined ──
  processedTraceData?: undefined;
  endpointPath?: undefined;
  redacted?: boolean;   // 脱敏标记 — V1 原本不携带, 适配层可注入
  analysisType?: undefined;
  graphqlPaths?: undefined;
  originalHost?: undefined;
  encryption?: undefined;
}

/**
 * V2 版本 — 来源: common/src/types.ts#L139-L159
 *
 * V2 相比 V1 新增了 8 个字段，由 SDK 端预填充或 Analyzer 端注入。
 * V2 支持 FULL 和 PARTIAL 两种分析类型。
 */
export interface QueuedApiTraceV2 {
  /** 请求路径 (原始), 如 /api/users/123 */
  path: string;
  /** SDK 提供的参数化路径 (★ V2 新增), 如 /api/users/{param1} */
  endpointPath?: string;
  /** 创建时间 (ISO 8601) */
  createdAt: Date;
  /** 目标主机 */
  host: string;
  /** HTTP 方法 */
  method: RestMethod;
  /** URL 查询参数 */
  requestParameters: PairObject[];
  /** 请求头 */
  requestHeaders: PairObject[];
  /** 请求体 */
  requestBody: string;
  /** 响应状态码 */
  responseStatus: number;
  /** 响应头 */
  responseHeaders: PairObject[];
  /** 响应体 */
  responseBody: string;
  /** 网络层元数据 */
  meta: Meta;
  /** 会话认证元数据 */
  sessionMeta: SessionMeta;
  /** SDK 端预分析结果 (★ V2 新增) — V1 无此字段 */
  processedTraceData?: ProcessedTraceData;
  /** 是否已脱敏 (★ V2 新增) */
  redacted?: boolean;
  /** 映射前的原始主机 (★ V2 新增) — Analyzer 的 host 映射后注入 */
  originalHost?: string;
  /** 加密信息 (★ V2 新增) */
  encryption?: Encryption;
  /** 分析类型 (★ V2 新增) — FULL 或 PARTIAL, 默认 FULL */
  analysisType?: AnalysisType;
  /** GraphQL 路径列表 (★ V2 新增) */
  graphqlPaths?: string[];
}

/** 联合类型 — Consumer 端兼容处理 V1/V2 */
export type QueuedApiTrace = QueuedApiTraceV1 | QueuedApiTraceV2;

// ============================================================================
// §5 Kafka Message Value — 外层包装 (Topic 路由)
// ============================================================================

/**
 * 请求上下文。
 * 带 Kafka 扩展建议 (traceId/apiKeyId/ingestTimestamp)。
 * 无字段时序列化为 {}。
 * 来源: backend/src/types.ts#L3
 */
export interface MetloContext {
  /** 分布式追踪 ID (新增建议) — 关联 Ingestor → Analyzer → Alert */
  traceId?: string;
  /** API Key 标识 (新增建议) — 用于多租户路由和消费者鉴权 */
  apiKeyId?: string;
  /** 采集时间戳 (新增建议) — 用于计算端到端延迟 */
  ingestTimestamp?: number;
}

// ── Topic: metlo.traces.full ──

/** V1 全量追踪消息 — 路由到 metlo.traces.full */
export interface FullTraceMessageV1 {
  ctx: MetloContext;
  version: 1;
  trace: QueuedApiTraceV1;
}

/** V2 全量追踪消息 — 路由到 metlo.traces.full, trace.analysisType === "full" */
export interface FullTraceMessageV2 {
  ctx: MetloContext;
  version: 2;
  trace: QueuedApiTraceV2;
}

/** FULL topic 消息联合类型 */
export type FullTraceMessage = FullTraceMessageV1 | FullTraceMessageV2;

// ── Topic: metlo.traces.partial ──

/** PARTIAL 批量追踪消息 — 路由到 metlo.traces.partial, 每条 traces[i].analysisType === "partial" */
export interface PartialTraceBatchMessage {
  ctx: MetloContext;
  /** PARTIAL 仅 V2 支持 */
  version: 2;
  /** 批量追踪数组 */
  traces: QueuedApiTraceV2[];
}

// ============================================================================
// §6 Analyzer 消费任务 — 来源: analyzer.ts TraceTask
// ============================================================================

/** Worker Pool 分析任务 — 对应原始 analyzer.ts#L322-L340 的分流逻辑 */
export interface TraceTask {
  /** FULL: 单条追踪 (与 traces 互斥) */
  trace?: QueuedApiTrace;
  /** PARTIAL: 批量追踪 (与 trace 互斥) */
  traces?: QueuedApiTrace[];
  /** 请求上下文 */
  ctx: MetloContext;
  /** V1 或 V2, 决定字段集合 */
  version: 1 | 2;
  /** 是否持有有效企业 License — 进程级常量, 不在消息体中 */
  hasValidEnterpriseLicense: boolean;
  /** 用户忽略的检测项 — 从 MetloConfig 获取, 不在消息体中 */
  ignoredDetections?: IgnoredDetection[];
}

// ============================================================================
// §7 辅助类型
// ============================================================================

/** 忽略的检测项 — 来源: services/metlo-config/types.ts */
export interface IgnoredDetection {
  /** 检测类型匹配规则 */
  detection_type: string;
  /** 匹配的 host 正则 */
  host_regex?: string;
  /** 匹配的 path 正则 */
  path_regex?: string;
}

/** 自定义数据类 — 来源: metlo-config.yaml */
export interface DataClassConfig {
  /** 数据类名称, 如 "Internal API Key" */
  className: string;
  /** 值的正则表达式 */
  regex?: string;
  /** 键的正则表达式 */
  keyRegex?: string;
  /** 风险等级 */
  riskScore: RiskScore;
  /** 数据标签 */
  tags?: string[];
}

/** 缓冲区消息 — 用于 Kafka 不可用时的降级累积 */
export interface BufferedMessage {
  /** 目标 topic */
  topic: string;
  /** 分区键 */
  key: string;
  /** 已序列化的完整 JSON 消息 (包含 ctx, version, trace|traces) */
  value: string;
}

// ============================================================================
// §8 类型守卫 (Consumer 端运行时判定的辅助函数)
// ============================================================================

/** 判断是否为 V2 消息 (携带 processedTraceData) */
export function isV2Trace(trace: QueuedApiTrace): trace is QueuedApiTraceV2 {
  return trace.processedTraceData !== undefined || trace.analysisType !== undefined;
}

/** 判断消息是否为 FULL 类型 (单条 trace) */
export function isFullMessage(raw: unknown): raw is FullTraceMessage {
  return (
    typeof raw === "object" &&
    raw !== null &&
    "trace" in raw
  );
}

/** 判断消息是否为 PARTIAL 类型 (批量 traces) */
export function isPartialMessage(raw: unknown): raw is PartialTraceBatchMessage {
  return (
    typeof raw === "object" &&
    raw !== null &&
    "traces" in raw
  );
}

// ============================================================================
// §9 Kafka 序列化/反序列化
// ============================================================================

/**
 * 将 FULL 消息序列化为 Kafka message value
 * 注意: Date 类型自动转为 ISO 8601 字符串
 */
export function serializeFullMessage(
  trace: QueuedApiTrace,
  version: 1 | 2 = 2,
  ctx: MetloContext = {},
): string {
  return JSON.stringify({ ctx, version, trace });
}

/**
 * 将 PARTIAL 批量消息序列化为 Kafka message value
 */
export function serializePartialBatch(
  traces: QueuedApiTraceV2[],
  ctx: MetloContext = {},
): string {
  return JSON.stringify({ ctx, version: 2, traces });
}

/**
 * 从 Kafka message value 反序列化
 * 返回类型守卫友好的联合类型
 */
export function deserializeMessage(value: string): FullTraceMessage | PartialTraceBatchMessage {
  const raw = JSON.parse(value) as FullTraceMessage | PartialTraceBatchMessage;
  // Date 字段还原: 所有 trace.createdAt 从 ISO 8601 字符串 → Date
  if ("trace" in raw) {
    raw.trace.createdAt = new Date(raw.trace.createdAt);
  } else if ("traces" in raw) {
    for (const t of raw.traces) {
      t.createdAt = new Date(t.createdAt);
    }
  }
  return raw;
}
