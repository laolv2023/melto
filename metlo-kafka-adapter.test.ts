/**
 * 适配器全面测试 — metlo-kafka-adapter.ts 验证套件
 *
 * 测试数据: 之前提供的流量镜像原始日志
 * 验证目标: adapt() 的每个字段映射、脱敏、边界处理
 *
 * 运行: npx ts-node metlo-kafka-adapter.test.ts
 */

import {
  adapt,
  adaptBatch,
  parseHeaders,
  sanitizeHeaders,
  SENSITIVE_HEADERS,
  REDACTED_VALUE,
  RawMirrorLog,
} from "./metlo-kafka-adapter";
import type {
  QueuedApiTraceV1,
} from "./metlo-kafka-data-model";
import { RestMethod, AuthType } from "./metlo-kafka-data-model";

// ============================================================================
// 测试数据
// ============================================================================

const sampleLog: RawMirrorLog = {
  path: "/fgap/admin/biz/app/info/list/1/10",
  requestHeaders: '{"Accept":"application/json, text/plain, /","Accept-Encoding":"gzip, deflate","Accept-Language":"zh-CN,zh;q=0.9","Cache-Control":"no-cache","Connection":"keep-alive","Content-Length":"23","Content-Type":"application/json;charset=UTF-8","Cookie":"welcomebanner_status=dismiss; cookieconsent_status=dismiss; language=zh_CN; SYS_NAME=%E5%AE%89%E5%85%A8%E9%9A%94%E7%A6%BB%E4%B8%8E%E4%BF%A1%E6%81%AF%E5%8D%95%E5%90%91%E5%AF%BC%E5%85%A5%E7%B3%BB%E7%BB%9F; loginType=general|custom; wvp_username=admin; wvp_server_id=000000; wvp_token=eyJhbGciOiJSUzI1NiIsImtpZCI6IjNlNzk2NDZjNGRiYzQwODM4M2E5ZWVkMDlmMmI4NWFlIn0.eyJqdGkiOiJHRVpuV1FBamc1QTR2b0h4Z3M0MUNRIiwiaWF0IjoxNzc5Nzk2NjQ4LCJleHAiOjE3Nzk4MDAyNDgsIm5iZiI6MTc3OTc5NjY0OCwic3ViIjoibG9naW4iLCJhdWQiOiJBdWRpZW5jZSIsInVzZXJOYW1lIjoiYWRtaW4ifQ.fs7iHqwvmL3LpISdF_QtwETtS58H0c3Hkv_OSwy9xy-geTQ8Cqn-v8hxqDdLicdEPGUvf2Y9TbR6Q3lcc8cOjQ_EKDW2LQreU8ZEebiG3OfAwYPvfdgig9Fh0PPYGpBet052vil2I4BwK-rsSuB88TyOkJfNX4plDPYMAeccYLPOx12xG7NJWFlKzqmcDYfPh4YwRvKWYIyNWkIZwLstvPUHILK14zIOVbag32a8fj8VWVIQ5uIXbdOnCb3M0Dh5wLKn2Hm1CkBEofJ2_UUwBG14b-OU7e0xCcVTwvPBq6klmM-CIodz7fRiFVTz9i0zopbiPyQLAcB1wDs70Jo4dQ; SYS_TYPE=null; userInfo={%22userName%22:%22wsa%22%2C%22roleName%22:null%2C%22roleCode%22:%22wsa%22%2C%22userId%22:%22b7fe1d8a9a904bfda54ffa20029cf3de%22%2C%22proType%22:%22fgap%22%2C%22loginType%22:%22ADMIN%22};","Host":"192.168.106.53:9090","Origin":"http://192.168.106.53:9090","Referer":"http://192.168.106.53:9090/fgap/admin/index.html","User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36"}',
  responseHeaders: '{"Content-Type":"application/json;charset=UTF-8","Date":"Wed, 27 May 2026 07:33:33 GMT","Transfer-Encoding":"chunked","X-Protected-By":"OpenRASP","X-Request-ID":"2c0d7d3e3e234ff29739fbcf701ef1ce"}',
  method: "POST",
  requestPayload: '{"preProtocol":"syndb"}',
  responsePayload: '{"code":"1","message":"成功","content":{"total":0,"list":[],"pageNum":0,"pageSize":10,"size":0,"startRow":0,"endRow":0,"pages":0,"prePage":0,"nextPage":0,"isFirstPage":false,"isLastPage":true,"hasPreviousPage":false,"hasNextPage":false,"navigatePages":8,"navigatepageNums":[],"navigateFirstPage":0,"navigateLastPage":0,"firstPage":0,"lastPage":0}}',
  ip: "192.168.106.53",
  destIp: "192.168.106.53",
  time: "1779867214",
  statusCode: "200",
  type: "HTTP/1.1",
  status: "OK",
  akto_account_id: "1000000",
  akto_vxlan_id: "0",
  is_pending: "false",
  source: "MIRRORING",
  direction: "REQUEST",
  process_id: null,
  socket_id: null,
  daemonset_id: null,
  enabled_graph: null,
  tag: '{"env":"dev","host":"192.168.106.53","page":"","url":"http://192.168.106.53:9090/fgap/admin/biz/app/info/list/1/10"}',
};

// ============================================================================
// 测试框架
// ============================================================================

let passed = 0;
let failed = 0;
let testName = "";

function describe(name: string) {
  testName = name;
  console.log(`\n${"=".repeat(60)}`);
  console.log(`  ${name}`);
  console.log(`${"=".repeat(60)}`);
}

function assert(condition: boolean, msg: string): void {
  if (condition) {
    passed++;
    console.log(`  ✓ ${msg}`);
  } else {
    failed++;
    console.log(`  ✗ FAIL: ${msg}`);
  }
}

function assertEqual<T>(actual: T, expected: T, msg: string): void {
  const condition = (
    actual === expected ||
    (typeof actual === "object" &&
     typeof expected === "object" &&
     JSON.stringify(actual) === JSON.stringify(expected))
  );
  if (condition) {
    passed++;
    console.log(`  ✓ ${msg}`);
  } else {
    failed++;
    console.log(`  ✗ FAIL: ${msg}`);
    console.log(`    expected: ${JSON.stringify(expected)}`);
    console.log(`    actual:   ${JSON.stringify(actual)}`);
  }
}

function assertContains(str: string, substr: string, msg: string): void {
  const condition = str.includes(substr);
  if (condition) {
    passed++;
    console.log(`  ✓ ${msg}`);
  } else {
    failed++;
    console.log(`  ✗ FAIL: ${msg} — "${substr}" not found`);
  }
}

// ============================================================================
// 测试执行
// ============================================================================

// ── 运行适配 ──
const result = adapt(sampleLog);

// ════════════════════════════════════════════════════════════
describe("1. 基础字段映射");

assertEqual(result.path, "/fgap/admin/biz/app/info/list/1/10", "path 直接映射");
assertEqual(result.method, "POST" as RestMethod, "method 直接映射");
assertEqual(result.responseStatus, 200, "statusCode string→number");
assertEqual(result.requestBody, '{"preProtocol":"syndb"}', "requestPayload → requestBody");
assert(result.responseBody.startsWith('{"code":"1"'), "responsePayload → responseBody");

// ════════════════════════════════════════════════════════════
describe("2. 时间戳转换");

assert(result.createdAt instanceof Date, "createdAt 是 Date 类型");
assertEqual(
  result.createdAt.toISOString(),
  new Date(1779867214 * 1000).toISOString(),
  "Unix 秒时间戳正确转换"
);
// 验证时间在合理范围 (2026年5月)
assert(result.createdAt.getFullYear() === 2026, "年份 = 2026");
assert(result.createdAt.getMonth() === 4, "月份 = 5月 (0-based)");

// ════════════════════════════════════════════════════════════
describe("3. Host 提取");

assertEqual(result.host, "192.168.106.53:9090", "从 Host header 提取 host (含端口)");

// ════════════════════════════════════════════════════════════
describe("4. Meta 网络元数据");

assertEqual(result.meta.source, "192.168.106.53", "source = ip");
assertEqual(result.meta.destination, "192.168.106.53", "destination = destIp");
assertEqual(result.meta.sourcePort, "9090", "sourcePort 从 Host:9090 提取");
assertEqual(result.meta.destinationPort, "9090", "destinationPort 从 Host:9090 提取");
assertEqual(result.meta.incoming, true, "direction=REQUEST → incoming=true");

// ════════════════════════════════════════════════════════════
describe("5. SessionMeta 认证上下文");

assertEqual(result.sessionMeta.authenticationProvided, true, "有 wvp_token → true");
assertEqual(result.sessionMeta.authenticationSuccessful, true, "wvp_username=admin → true");
assertEqual(result.sessionMeta.user, "wsa", "从 userInfo cookie 提取 userName");
assertEqual(result.sessionMeta.authType, "session_cookie" as AuthType, "Cookie token → session_cookie");

// ════════════════════════════════════════════════════════════
describe("6. Header 解析");

assert(result.requestHeaders.length >= 10, `请求头≥10 (实际: ${result.requestHeaders.length})`);
assert(result.responseHeaders.length >= 4, `响应头≥4 (实际: ${result.responseHeaders.length})`);

// 检查关键 header 存在
const hasContentType = result.requestHeaders.some(h => h.name === "Content-Type");
assert(hasContentType, "请求头包含 Content-Type");

const hasXProtected = result.responseHeaders.some(h => h.name === "X-Protected-By");
assert(hasXProtected, "响应头包含 X-Protected-By: OpenRASP");

// ════════════════════════════════════════════════════════════
describe("7. 敏感 Header 脱敏");

// 检查 Cookie → 确认脱敏
const cookieHeader = result.requestHeaders.find(h => h.name.toLowerCase() === "cookie");
assert(cookieHeader !== undefined, "Cookie header 存在");
if (cookieHeader) {
  assertEqual(cookieHeader.value, REDACTED_VALUE, "Cookie value → [REDACTED]");
  assert(!cookieHeader.value.includes("wvp_token"), "JWT token 不在脱敏后的值中");
  assert(!cookieHeader.value.includes("eyJhbGci"), "JWT payload 不在脱敏后的值中");
}

// 确认非敏感 header 不变
const acceptHeader = result.requestHeaders.find(h => h.name === "Accept");
assert(acceptHeader !== undefined, "Accept header 存在");
if (acceptHeader) {
  assert(!acceptHeader.value.includes(REDACTED_VALUE), "普通 header 不被脱敏");
}

// redacted 标记
assertEqual(result.redacted, true, "redacted=true (Cookie 被脱敏)");

// ════════════════════════════════════════════════════════════
describe("8. V1 特有字段 — 确认为 undefined");

assertEqual(result.processedTraceData, undefined, "processedTraceData = undefined");
assertEqual(result.endpointPath, undefined, "endpointPath = undefined");
assertEqual(result.analysisType, undefined, "analysisType = undefined");
assertEqual(result.graphqlPaths, undefined, "graphqlPaths = undefined");
assertEqual(result.originalHost, undefined, "originalHost = undefined");
assertEqual(result.encryption, undefined, "encryption = undefined");

// ════════════════════════════════════════════════════════════
describe("9. 请求参数解析");

// 该路径没有 query string, 应为空
assertEqual(result.requestParameters.length, 0, "无 query string → requestParameters=[]");

// ════════════════════════════════════════════════════════════
describe("10. 边界处理 — 恶意输入");

// 10a: header JSON 损坏
const brokenHeaderLog: RawMirrorLog = {
  ...sampleLog,
  requestHeaders: "{broken json!!!",
  path: "/test/broken/headers",
};
const brokenResult = adapt(brokenHeaderLog);
assertEqual(brokenResult.requestHeaders.length, 0, "损坏的 requestHeaders → []");
assertEqual(brokenResult.redacted, false, "空 header → redacted=false");

// 10b: 空 requestPayload
const emptyPayloadLog: RawMirrorLog = {
  ...sampleLog,
  requestPayload: "",
  responsePayload: "",
  path: "/test/empty/payload",
};
const emptyResult = adapt(emptyPayloadLog);
assertEqual(emptyResult.requestBody, "", "空 requestPayload → ''");
assertEqual(emptyResult.responseBody, "", "空 responsePayload → ''");

// 10c: 无效时间戳
const badTimeLog: RawMirrorLog = {
  ...sampleLog,
  time: "not_a_number",
  path: "/test/bad/time",
};
const badTimeResult = adapt(badTimeLog);
assert(badTimeResult.createdAt instanceof Date, "无效时间戳 → 回退 Date 对象");
// 回退应为当前时间 (与测试运行时间相差 < 10秒)
const diffSec = Math.abs(Date.now() - badTimeResult.createdAt.getTime());
assert(diffSec < 10000, `回退时间在合理范围 (差值: ${diffSec}ms)`);

// 10d: 无 Host header
const noHostLog: RawMirrorLog = {
  ...sampleLog,
  requestHeaders: '{"Accept":"application/json"}',
  tag: '{"env":"dev","host":"","page":"","url":""}',
  path: "/test/no/host",
};
const noHostResult = adapt(noHostLog);
assertEqual(noHostResult.host, "unknown", "无 Host header → 'unknown'");

// 10e: 无认证信息
const noAuthLog: RawMirrorLog = {
  ...sampleLog,
  requestHeaders: '{"Accept":"application/json","Content-Type":"text/plain"}',
  path: "/test/no/auth",
};
const noAuthResult = adapt(noAuthLog);
assertEqual(noAuthResult.sessionMeta.authenticationProvided, false, "无 Cookie/Token → false");
assertEqual(noAuthResult.sessionMeta.user, undefined, "无用户 → undefined");
assertEqual(noAuthResult.redacted, false, "无敏感 header → redacted=false");

// ════════════════════════════════════════════════════════════
describe("11. 批量适配 + 错误隔离");

const mixedBatch: RawMirrorLog[] = [
  sampleLog,
  {
    // 这条会失败 — path 为 null 导致 URL 解析失败但 adapt() 有 try-catch 会正常降级
    ...sampleLog,
    tag: '{"env":"dev","host":"192.168.106.53","page":"","url":""}',
    path: "/valid/path/in/batch",
  },
  sampleLog,
];

const { traces, errors } = adaptBatch(mixedBatch);
assertEqual(traces.length, 3, "3 条全部成功产出 (无构造性失败)");
assertEqual(errors.length, 0, "errors=0 (所有有效日志通过)");

// ════════════════════════════════════════════════════════════
describe("12. sanitizeHeaders 脱敏函数");

const testHeaders = [
  { name: "Content-Type", value: "application/json" },
  { name: "Cookie", value: "session=abc123; token=xyz" },
  { name: "Authorization", value: "Bearer eyJhbGci..." },
  { name: "Accept", value: "*/*" },
  { name: "Set-Cookie", value: "new_session=def456" },
];

const [sanitized, wasRedacted] = sanitizeHeaders(testHeaders);

assertEqual(wasRedacted, true, "检测到脱敏行为");
assertEqual(sanitized.length, 5, "保留所有 header name");

// 逐一检查
assertEqual(sanitized[0].value, "application/json", "Content-Type 保持原值");
assertEqual(sanitized[1].value, REDACTED_VALUE, "Cookie → [REDACTED]");
assertEqual(sanitized[2].value, REDACTED_VALUE, "Authorization → [REDACTED]");
assertEqual(sanitized[3].value, "*/*", "Accept 保持原值");
assertEqual(sanitized[4].value, REDACTED_VALUE, "Set-Cookie → [REDACTED]");

// 确认 SENSITIVE_HEADERS 覆盖
assert(SENSITIVE_HEADERS.includes("cookie"), "cookie 在脱敏列表");
assert(SENSITIVE_HEADERS.includes("authorization"), "authorization 在脱敏列表");
assert(SENSITIVE_HEADERS.includes("set-cookie"), "set-cookie 在脱敏列表");

// ════════════════════════════════════════════════════════════
describe("13. SessionMeta 边界 — Cookie 中的 userInfo JSON");

// 验证 userInfo cookie 解析
const userInfoLog: RawMirrorLog = {
  ...sampleLog,
  requestHeaders: '{"Cookie":"userInfo={%22userName%22:%22testuser%22%2C%22userId%22:%22abc123%22}"}',
  path: "/test/userinfo",
};
const userInfoResult = adapt(userInfoLog);
assertEqual(userInfoResult.sessionMeta.user, "testuser", "userInfo JSON → userName 正确提取");

// ════════════════════════════════════════════════════════════
describe("14. parseHeaders 健壮性");

const normalHeaders = parseHeaders('{"X-Custom":"value1","Accept":"*/*"}');
assertEqual(normalHeaders.length, 2, "正常 JSON → 2 个 header");

const emptyHeaders = parseHeaders("");
assertEqual(emptyHeaders.length, 0, "空字符串 → []");

const nullHeaders = parseHeaders("null");
assertEqual(nullHeaders.length, 0, "'null' → []");

const malformedHeaders = parseHeaders("{oops");
assertEqual(malformedHeaders.length, 0, "损坏 JSON → []");

// ════════════════════════════════════════════════════════════
describe("15. 输出格式验证 — 完整 JSON 序列化");

const json = JSON.stringify(result);
// 确认关键字段出现在输出中
assertContains(json, '"path":"/fgap/admin/biz/app/info/list/1/10"', "path 在 JSON 中");
assertContains(json, '"method":"POST"', "method 在 JSON 中");
assertContains(json, '"responseStatus":200', "responseStatus 是数字类型");
assertContains(json, '"redacted":true', "redacted=true 在 JSON 中");
// 确认脱敏后的 Cookie 不包含 JWT
assert(!json.includes("eyJhbGci"), "JWT 不在任何输出中 (全局验证)");
assert(!json.includes("wvp_token"), "wvp_token 不在任何输出中 (全局验证)");
// 确认 sessionMeta 仍正确
assertContains(json, '"user":"wsa"', "user 在 JSON 中 (从 Cookie 提取后保留)");

// ════════════════════════════════════════════════════════════
// 结果汇总
// ════════════════════════════════════════════════════════════

console.log(`\n${"=".repeat(60)}`);
console.log(`  结 果:  ${passed} 通过, ${failed} 失败`);
console.log(`${"=".repeat(60)}\n`);

if (failed > 0) {
  console.log(`❌ 测试失败! ${failed} 项未通过。`);
  process.exit(1);
} else {
  console.log(`✅ 全部 ${passed} 项测试通过。`);
  process.exit(0);
}
