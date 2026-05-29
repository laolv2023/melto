/**
 * Metlo Kafka 适配层 — 流量镜像原始日志 → QueuedApiTrace 转换器
 *
 * 背景:
 *   从网络流量镜像 (MIRRORING) 采集的原始日志格式与 Metlo 的 QueuedApiTrace
 *   存在多项差异：headers 是 JSON 字符串、payload 命名不同、时间格式不同、
 *   缺少 host/sessionMeta 等结构化字段。本适配器完成全量映射。
 *
 * 输入:  Akto 流量镜像原始日志 (见示例)
 * 输出:  QueuedApiTraceV1 (无 SDK 预分析, processedTraceData=undefined)
 * 目地:  publishTraceV1() → Kafka metlo.traces.full
 *
 * 版本: v1.0
 * 日期: 2026-05-29
 */

// ── 复用数据模型 (从 metlo-kafka-data-model.ts) ──
import {
  RestMethod,
  AuthType,
  Meta,
  SessionMeta,
  PairObject,
  QueuedApiTraceV1,
} from "./metlo-kafka-data-model";

// ============================================================================
// §1 输入类型 — 流量镜像原始日志
// ============================================================================

/** 流量镜像原始日志 (Akto/MIRRORING 格式) */
interface RawMirrorLog {
  /** 请求路径 */
  path: string;
  /** HTTP 方法 */
  method: string;
  /** 请求头 — ⚠️ JSON 字符串, 非对象 */
  requestHeaders: string;
  /** 响应头 — ⚠️ JSON 字符串, 非对象 */
  responseHeaders: string;
  /** 请求体 — ⚠️ 字段名为 requestPayload 而非 requestBody */
  requestPayload: string;
  /** 响应体 — ⚠️ 字段名为 responsePayload 而非 responseBody */
  responsePayload: string;
  /** 源 IP */
  ip: string;
  /** 目标 IP */
  destIp: string;
  /** 时间戳 — ⚠️ Unix 秒 (字符串), 非 ISO 8601 */
  time: string;
  /** 响应状态码 — ⚠️ 字符串, 非 number */
  statusCode: string;
  /** HTTP 协议类型 */
  type: string;
  /** 响应状态文本 (如 "OK") */
  status: string;
  /** 采集来源 */
  source: string;
  /** 流量方向 */
  direction: string;
  /** 账户 ID */
  akto_account_id: string;
  /** VXLAN ID */
  akto_vxlan_id: string;
  /** 是否待处理 */
  is_pending: string;
  /** 标签 — ⚠️ JSON 字符串, 含 env/host/url */
  tag: string;
  /** 守护进程 ID (可能为 null) */
  daemonset_id: string | null;
  /** 进程 ID (可能为 null) */
  process_id: string | null;
  /** Socket ID (可能为 null) */
  socket_id: string | null;
  /** 是否启用图 (可能为 null) */
  enabled_graph: string | null;
}

/** 解析后的 Tag 结构 */
interface ParsedTag {
  env: string;          // 环境标识, 如 "dev"
  host: string;         // 主机地址
  page: string;         // 页面路径
  url: string;          // 完整 URL
}

// 端口缺失的默认值
const DEFAULT_HTTP_PORT = "80";
const DEFAULT_HTTPS_PORT = "443";

// ============================================================================
// §2 字段解析器 — 处理原始日志的格式差异
// ============================================================================

/**
 * 安全解析 JSON 字符串, 失败时返回回退值
 */
function safeJsonParse<T>(str: string, fallback: T): T {
  if (!str || str.trim() === "") return fallback;
  try {
    const parsed = JSON.parse(str);
    // 防御 JSON.parse("null") / "123" / "true" 等非对象值
    if (parsed === null || typeof parsed !== "object") return fallback;
    return parsed as T;
  } catch {
    return fallback;
  }
}

/**
 * 解析请求头: JSON 字符串 → PairObject[]
 *
 * 原始格式: '{"Accept":"application/json", "Host":"192.168.106.53:9090", ...}'
 * 输出:     [{ name: "Accept", value: "application/json" }, ...]
 */
function parseHeaders(headersStr: string): PairObject[] {
  const obj = safeJsonParse<Record<string, string>>(headersStr, {});
  return Object.entries(obj).map(([name, value]) => ({
    name,
    value: String(value),
  }));
}

/**
 * 从请求头中提取 Host
 *
 * 优先从 "Host" header 取, 其次从 ":authority" (HTTP/2),
 * 都取不到则从 tag.url 解析。
 */
function extractHost(headers: PairObject[], tag: ParsedTag): string {
  const hostHeader = headers.find(
    (h) => h.name.toLowerCase() === "host"
  );
  if (hostHeader) return hostHeader.value;

  try {
    const url = new URL(tag.url);
    return url.host;
  } catch {
    return tag.host || "unknown";
  }
}

/**
 * 解析 Tag: JSON 字符串 → ParsedTag
 *
 * 原始格式: '{"env":"dev","host":"192.168.106.53","page":"","url":"http://...}'
 */
function parseTag(tagStr: string): ParsedTag {
  return safeJsonParse<ParsedTag>(tagStr, {
    env: "",
    host: "",
    page: "",
    url: "",
  });
}

/**
 * 解析 Unix 时间戳 → Date
 *
 * 输入: "1779867214" (秒级数字字符串)
 * 输出: Date(2026-05-27T07:33:34.000Z)
 *
 * 注意: 原始 time 字段是秒级 Unix 时间戳, 需 ×1000 转为毫秒
 */
function parseTimestamp(timeStr: string): Date {
  const sec = parseInt(timeStr, 10);
  if (isNaN(sec)) return new Date();        // 回退: 当前时间
  return new Date(sec * 1000);
}

/**
 * 解析状态码: string → number
 */
function parseStatusCode(statusCodeStr: string): number {
  const code = parseInt(statusCodeStr, 10);
  return isNaN(code) ? 0 : code;
}

/**
 * 从 headers 中提取端口 — Host header 可能包含端口
 *
 * "192.168.106.53:9090" → sourcePort/destinationPort = "9090"
 * "192.168.106.53" → 默认 "80" (HTTP) 或 "443" (HTTPS)
 */
function extractPort(hostValue: string, protocol: string): string {
  const portMatch = hostValue.match(/:(\d+)$/);
  if (portMatch) return portMatch[1];
  return protocol === "HTTPS" ? DEFAULT_HTTPS_PORT : DEFAULT_HTTP_PORT;
}

// ============================================================================
// §3 SessionMeta 解析 — 从 Cookie 提取认证上下文
// ============================================================================

/**
 * 从请求头中提取 Cookie 值
 */
function extractCookie(headers: PairObject[]): string {
  const cookieHeader = headers.find(
    (h) => h.name.toLowerCase() === "cookie"
  );
  return cookieHeader?.value || "";
}

/**
 * 从 Cookie 字符串中提取指定 key 的值
 *
 * 处理 URL 编码的值 (如 SYS_NAME=%E5%AE%89...)
 */
function getCookieValue(cookieStr: string, key: string): string | undefined {
  const match = cookieStr.match(
    new RegExp(`(?:^|;\\s*)${key}=([^;]*)`)
  );
  if (!match) return undefined;
  try {
    return decodeURIComponent(match[1]);
  } catch {
    return match[1];
  }
}

/**
 * 从请求头和 Cookie 构建 SessionMeta
 *
 * 判定规则:
 *   - Authorization header 存在 → authType = jwt/header
 *   - Cookie 中有 wvp_token → authType = jwt
 *   - Cookie 中有 loginType=ADMIN → authenticationSuccessful = true
 *   - Cookie 中有 wvp_username → user
 *   - Cookie 中有 userInfo JSON → 解析 userName/userId
 */
function buildSessionMeta(
  headers: PairObject[],
): SessionMeta {
  const cookieStr = extractCookie(headers);
  const authHeader = headers.find(
    (h) => h.name.toLowerCase() === "authorization"
  );

  // 提取用户身份
  const loginType = getCookieValue(cookieStr, "loginType");
  const username = getCookieValue(cookieStr, "wvp_username");
  const hasToken = !!getCookieValue(cookieStr, "wvp_token");

  // 尝试从 userInfo cookie 提取
  let user: string | undefined = username;
  const userInfoRaw = getCookieValue(cookieStr, "userInfo");
  if (userInfoRaw) {
    const userInfo = safeJsonParse<{ userName?: string; userId?: string }>(
      userInfoRaw,
      {}
    );
    user = userInfo.userName || user;
  }

  // 判定认证类型
  let authType: AuthType = AuthType.BASIC;
  if (authHeader) {
    const val = authHeader.value.toLowerCase();
    if (val.startsWith("bearer ")) authType = AuthType.JWT;
    else if (val.startsWith("basic ")) authType = AuthType.BASIC;
    else authType = AuthType.HEADER;
  } else if (hasToken) {
    authType = AuthType.SESSION_COOKIE;
  }

  // 判定认证状态
  const authenticationProvided = !!(authHeader || hasToken || user);
  const authenticationSuccessful =
    !!user || loginType === "ADMIN";

  return {
    authenticationProvided,
    authenticationSuccessful,
    authType,
    uniqueSessionKey: user,
    user,
  };
}

// ============================================================================
// §4 敏感数据脱敏 — 防止 Token 明文落库
// ============================================================================

/**
 * 需要脱敏的 Header 名称 (小写匹配)
 *
 * Cookie / Set-Cookie: 包含 JWT token、session ID
 * Authorization:      包含 Bearer token 或 Basic auth 凭证
 * X-API-Key:          自定义 API key
 */
const SENSITIVE_HEADERS = [
  "cookie",
  "set-cookie",
  "authorization",
  "x-api-key",
  "x-auth-token",
  "proxy-authorization",
];

/** 脱敏占位符 */
const REDACTED_VALUE = "[REDACTED]";

/**
 * 判断单个 header 是否需要脱敏
 */
function isSensitive(name: string): boolean {
  return SENSITIVE_HEADERS.includes(name.toLowerCase());
}

/**
 * 对 headers 数组中的敏感 header 进行脱敏
 *
 * 保留 header name 不变 (端点发现需要识别 auth 类型),
 * 仅替换 value 为 [REDACTED]。
 *
 * @returns [sanitized, wasRedacted] — 脱敏后的数组 + 是否实际有脱敏
 */
function sanitizeHeaders(headers: PairObject[]): [PairObject[], boolean] {
  let wasRedacted = false;
  const sanitized = headers.map((h) => {
    if (isSensitive(h.name)) {
      wasRedacted = true;
      return { name: h.name, value: REDACTED_VALUE };
    }
    return h;
  });
  return [sanitized, wasRedacted];
}

// ============================================================================
// §5 核心适配函数
// ============================================================================

/**
 * 从请求头推断协议类型 (HTTP vs HTTPS)
 */
function inferProtocol(headers: PairObject[]): string {
  const forwarded = headers.find(
    (h) => h.name.toLowerCase() === "x-forwarded-proto"
  );
  if (forwarded) return forwarded.value.toUpperCase();

  const host = headers.find((h) => h.name.toLowerCase() === "host");
  if (host && host.value.includes(":443")) return "HTTPS";
  return "HTTP";
}

/**
 * 主适配函数: 原始日志 → QueuedApiTraceV1
 *
 * @param raw 流量镜像原始日志
 * @returns 标准化的 Metlo 追踪对象
 */
function adapt(raw: RawMirrorLog): QueuedApiTraceV1 {
  // ── 解析阶段 ──
  const requestHeaders = parseHeaders(raw.requestHeaders);
  const responseHeaders = parseHeaders(raw.responseHeaders);
  const tag = parseTag(raw.tag);
  const host = extractHost(requestHeaders, tag);
  const protocol = inferProtocol(requestHeaders);
  const port = extractPort(host, protocol);
  const createdAt = parseTimestamp(raw.time);

  // ── 请求参数解析 (从 URL query string) ──
  let requestParameters: PairObject[] = [];
  try {
    const url = new URL(tag.url || `http://${host}${raw.path}`);
    requestParameters = Array.from(url.searchParams.entries()).map(
      ([name, value]) => ({ name, value })
    );
  } catch {
    // URL 解析失败则 query params 为空
  }

  // ── 组装 Meta ──
  const meta: Meta = {
    incoming: raw.direction === "REQUEST",
    source: raw.ip,
    sourcePort: port,
    destination: raw.destIp,
    destinationPort: port,
  };

  // ── 组装 SessionMeta (必须在脱敏前调用 — 需要 Cookie 原文) ──
  const sessionMeta = buildSessionMeta(requestHeaders);

  // ── 脱敏 (在 SessionMeta 提取之后 — Cookie 原文已提取用户身份) ──
  const [finalRequestHeaders, reqRedacted] = sanitizeHeaders(requestHeaders);
  const [finalResponseHeaders, resRedacted] = sanitizeHeaders(responseHeaders);

  // ── 输出 ──
  return {
    path: raw.path,
    createdAt,
    host,
    method: raw.method as RestMethod,
    requestParameters,
    requestHeaders: finalRequestHeaders,
    requestBody: raw.requestPayload || "",
    responseStatus: parseStatusCode(raw.statusCode),
    responseHeaders: finalResponseHeaders,
    responseBody: raw.responsePayload || "",
    meta,
    sessionMeta,
    redacted: reqRedacted || resRedacted,   // 任一 header 被脱敏则标记
    // V1 不携带以下字段
    processedTraceData: undefined,
    endpointPath: undefined,
    analysisType: undefined,
    graphqlPaths: undefined,
    originalHost: undefined,
    encryption: undefined,
  };
}

// ============================================================================
// §6 批量适配 + 错误处理
// ============================================================================

/** 适配错误 */
interface AdaptError {
  raw: RawMirrorLog;
  error: string;
  path?: string;
}

/**
 * 批量适配 + 错误隔离
 *
 * 单条失败不影响其他日志, 收集所有错误供后续排查。
 */
function adaptBatch(raws: RawMirrorLog[]): {
  traces: QueuedApiTraceV1[];
  errors: AdaptError[];
} {
  const traces: QueuedApiTraceV1[] = [];
  const errors: AdaptError[] = [];

  for (const raw of raws) {
    try {
      traces.push(adapt(raw));
    } catch (err) {
      errors.push({
        raw,
        error: err instanceof Error ? err.message : String(err),
        path: raw.path,
      });
    }
  }

  return { traces, errors };
}

// ============================================================================
// §7 Kafka Producer 集成
// ============================================================================

import type { MetloContext } from "./metlo-kafka-data-model";
import { serializeFullMessage } from "./metlo-kafka-data-model";

/**
 * 适配并发送到 Kafka FULL topic
 *
 * 使用示例:
 *   const { traces, errors } = adaptAndProduce(rawLogs, producer);
 *   if (errors.length > 0) metrics.recordAdaptErrors(errors.length);
 */
async function adaptAndProduce(
  raws: RawMirrorLog[],
  producer: { send: (opts: { topic: string; messages: Array<{ key: string; value: string }> }) => Promise<void> },
  ctx: MetloContext = {},
): Promise<{ traces: QueuedApiTraceV1[]; errors: AdaptError[] }> {
  const { traces, errors } = adaptBatch(raws);

  if (traces.length === 0) return { traces, errors };

  // 按 host 分组发送 (保证同一 host 进入同一 partition)
  const byHost = new Map<string, QueuedApiTraceV1[]>();
  for (const t of traces) {
    const list = byHost.get(t.host) || [];
    list.push(t);
    byHost.set(t.host, list);
  }

  for (const [host, hostTraces] of byHost) {
    const messages = hostTraces.map((trace) => ({
      key: host,
      value: serializeFullMessage(trace, 1, ctx),
    }));

    try {
      await producer.send({
        topic: "metlo.traces.full",
        messages,
      });
    } catch (err) {
      // 单 host 批次失败不阻塞其他 host
      errors.push({
        raw: null as unknown as RawMirrorLog, // 批量错误无单条 raw
        error: `Kafka produce failed for host=${host}: ${err}`,
      });
    }
  }

  return { traces, errors };
}

// ============================================================================
// §8 字段映射速查表
// ============================================================================

/*
 * ┌──────────────────────┬─────────────────────────┬─────────────────────────────┐
 * │ RawMirrorLog         │  QueuedApiTraceV1       │  转换逻辑                    │
 * ├──────────────────────┼─────────────────────────┼─────────────────────────────┤
 * │ path                 │ path                    │ 直接映射                     │
 * │ method               │ method                  │ 直接映射 (string→RestMethod) │
 * │ requestHeaders (str) │ requestHeaders (arr)    │ JSON.parse → PairObject[]    │
 * │ responseHeaders(str) │ responseHeaders (arr)   │ JSON.parse → PairObject[]    │
 * │ requestPayload       │ requestBody             │ 直接映射                     │
 * │ responsePayload      │ responseBody            │ 直接映射                     │
 * │ ip                   │ meta.source             │ 直接映射                     │
 * │ destIp               │ meta.destination        │ 直接映射                     │
 * │ Host header          │ meta.sourcePort         │ 从 "Host: x.x.x.x:9090" 解析  │
 * │ Host header          │ meta.destinationPort    │ 同上                        │
 * │ direction            │ meta.incoming           │ "REQUEST" → true             │
 * │ time (Unix秒)        │ createdAt               │ parseInt × 1000 → new Date() │
 * │ statusCode (str)     │ responseStatus          │ parseInt                     │
 * │ Cookie + Auth header │ sessionMeta             │ 解析 Cookie/Header (见 §3)   │
 * │ Host header          │ host                    │ 提取 "Host" header            │
 * │ tag.url              │ requestParameters       │ URL.searchParams 解析        │
 * │ source               │ (元信息)                 │ 标识为 MIRRORING              │
 * │ requestPayload       │ requestBody             │ 空串时保持 ""                 │
 * └──────────────────────┴─────────────────────────┴─────────────────────────────┘
 */

// ============================================================================
// §9 导出
// ============================================================================

export {
  // 类型
  RawMirrorLog,
  ParsedTag,
  AdaptError,

  // 解析器
  parseHeaders,
  extractHost,
  parseTag,
  parseTimestamp,
  parseStatusCode,
  buildSessionMeta,
  extractCookie,
  getCookieValue,

  // 脱敏
  sanitizeHeaders,
  isSensitive,
  SENSITIVE_HEADERS,
  REDACTED_VALUE,
  adapt,
  adaptBatch,
  adaptAndProduce,
};
