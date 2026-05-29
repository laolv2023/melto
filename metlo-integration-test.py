#!/usr/bin/env python3
"""
Metlo Kafka 适配层集成测试

流程:
  1. 加载 Kafka dump 日志 (69 条)
  2. JSON → RawMirrorLog 解析
  3. 模拟 TypeScript 适配器逻辑 (Python 复刻)
  4. 序列化为 Kafka message value
  5. 模拟 Consumer 反序列化 + 分流
  6. 模拟 Analyzer 核心 Pipeline:
     - 端点发现 (路径参数化)
     - 敏感数据扫描 (regex matching)
     - SessionMeta 分析
     - 告警生成判断
  7. 输出完整报告
"""

import json
import re
import os
import time as time_module
from datetime import datetime, timezone
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from typing import Optional

# ============================================================================
# 数据加载
# ============================================================================

def load_kafka_dump(path: str) -> list[dict]:
    """从 Kafka dump 文件中提取所有 value 对象"""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    text = text.replace("\t", "")
    blocks = re.findall(r'"value":\s*(\{.*?\n\})', text, re.DOTALL)
    return [json.loads(b) for b in blocks]

# ============================================================================
# Python 复刻: metlo-kafka-adapter.ts 核心逻辑
# ============================================================================

SENSITIVE_HEADERS = {"cookie", "set-cookie", "authorization", "x-api-key", "x-auth-token", "proxy-authorization"}
REDACTED_VALUE = "[REDACTED]"

def parse_headers(headers_str: str) -> list[dict]:
    if not headers_str or headers_str.strip() == "":
        return []
    try:
        obj = json.loads(headers_str)
        if obj is None or not isinstance(obj, dict):
            return []
        return [{"name": k, "value": str(v)} for k, v in obj.items()]
    except (json.JSONDecodeError, TypeError):
        return []

def extract_host(headers: list[dict], tag: dict) -> str:
    for h in headers:
        if h["name"].lower() == "host":
            return h["value"]
    url = tag.get("url", "")
    if url:
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            if parsed.netloc:
                return parsed.netloc
        except Exception:
            pass
    return tag.get("host", "unknown")

def parse_tag(tag_str: str) -> dict:
    try:
        return json.loads(tag_str) or {}
    except (json.JSONDecodeError, TypeError):
        return {}

def parse_timestamp(time_str: str) -> datetime:
    try:
        sec = int(time_str)
        return datetime.fromtimestamp(sec, tz=timezone.utc)
    except (ValueError, TypeError):
        return datetime.now(timezone.utc)

def parse_status_code(sc_str: str) -> int:
    try:
        return int(sc_str)
    except (ValueError, TypeError):
        return 0

def extract_port(host_val: str, protocol: str) -> str:
    m = re.search(r":(\d+)$", host_val)
    if m:
        return m[1]
    return "443" if protocol.upper() == "HTTPS" else "80"

def sanitize_headers(headers: list[dict]) -> tuple[list[dict], bool]:
    was_redacted = False
    result = []
    for h in headers:
        if h["name"].lower() in SENSITIVE_HEADERS:
            was_redacted = True
            result.append({"name": h["name"], "value": REDACTED_VALUE})
        else:
            result.append(h)
    return result, was_redacted

def extract_cookie(headers: list[dict]) -> str:
    for h in headers:
        if h["name"].lower() == "cookie":
            return h["value"]
    return ""

def get_cookie_value(cookie_str: str, key: str) -> Optional[str]:
    m = re.search(rf"(?:^|;\s*){re.escape(key)}=([^;]*)", cookie_str)
    if not m:
        return None
    try:
        from urllib.parse import unquote
        return unquote(m.group(1))
    except Exception:
        return m.group(1)

def build_session_meta(headers: list[dict]) -> dict:
    cookie_str = extract_cookie(headers)
    auth_header = next((h for h in headers if h["name"].lower() == "authorization"), None)
    login_type = get_cookie_value(cookie_str, "loginType")
    username = get_cookie_value(cookie_str, "wvp_username")
    has_token = bool(get_cookie_value(cookie_str, "wvp_token"))

    user = username
    user_info_raw = get_cookie_value(cookie_str, "userInfo")
    if user_info_raw:
        try:
            user_info = json.loads(user_info_raw)
            user = user_info.get("userName", user)
        except json.JSONDecodeError:
            pass

    auth_type = None
    if auth_header:
        val = auth_header["value"].lower()
        if val.startswith("bearer "):
            auth_type = "jwt"
        elif val.startswith("basic "):
            auth_type = "basic"
        else:
            auth_type = "header"
    elif has_token:
        auth_type = "session_cookie"

    auth_provided = bool(auth_header or has_token or user)
    return {
        "authenticationProvided": auth_provided,
        "authenticationSuccessful": auth_provided and bool(user or login_type == "ADMIN"),
        "authType": auth_type or "none",
        "uniqueSessionKey": user,
        "user": user,
    }

def adapt(raw: dict) -> dict:
    """Python 复刻 TypeScript adapt() 函数"""
    request_headers = parse_headers(raw.get("requestHeaders", ""))
    response_headers = parse_headers(raw.get("responseHeaders", ""))
    tag = parse_tag(raw.get("tag", "{}"))
    host = extract_host(request_headers, tag)

    # 推断协议
    protocol = "HTTP"
    for h in request_headers:
        if h["name"].lower() == "x-forwarded-proto":
            protocol = h["value"].upper()
            break
    if not any(h["name"].lower() == "x-forwarded-proto" for h in request_headers):
        m = re.search(r":(\d+)$", host)
        if m and m.group(1) == "443":
            protocol = "HTTPS"

    port = extract_port(host, protocol)

    # SessionMeta (脱敏前)
    session_meta = build_session_meta(request_headers)

    # 脱敏
    final_req_headers, req_redacted = sanitize_headers(request_headers)
    final_res_headers, res_redacted = sanitize_headers(response_headers)

    # 请求参数
    request_params = []
    url_str = tag.get("url", "")
    if url_str:
        try:
            from urllib.parse import urlparse, parse_qs
            parsed = urlparse(url_str)
            for k, v in parse_qs(parsed.query).items():
                request_params.append({"name": k, "value": v[0] if v else ""})
        except Exception:
            pass

    rp = raw.get("requestPayload")
    resp_payload = raw.get("responsePayload")

    return {
        "path": raw.get("path", ""),
        "createdAt": parse_timestamp(raw.get("time", "0")).isoformat(),
        "host": host,
        "method": raw.get("method", "GET"),
        "requestParameters": request_params,
        "requestHeaders": final_req_headers,
        "requestBody": rp if rp else "",
        "responseStatus": parse_status_code(raw.get("statusCode", "200")),
        "responseHeaders": final_res_headers,
        "responseBody": resp_payload if resp_payload else "",
        "meta": {
            "incoming": raw.get("direction", "REQUEST") == "REQUEST",
            "source": raw.get("ip", ""),
            "sourcePort": "0",
            "destination": raw.get("destIp", ""),
            "destinationPort": port,
        },
        "sessionMeta": session_meta,
        "redacted": req_redacted or res_redacted,
    }

# ============================================================================
# 模拟测: Analyzer 核心 Pipeline
# ============================================================================

def parameterize_path(path: str) -> str:
    """模拟 Metlo 端点路径参数化: /users/123 → /users/{param1}"""
    segments = path.strip("/").split("/")
    result = []
    param_idx = 1
    for seg in segments:
        if seg == "":
            continue
        if re.match(r"^\d+$", seg):
            result.append(f"{{param{param_idx}}}")
            param_idx += 1
        elif re.match(r"^[0-9a-f]{8,}$", seg):
            result.append(f"{{param{param_idx}}}")
            param_idx += 1
        else:
            result.append(seg)
    return "/" + "/".join(result)

# 精简版扫描器 — 复刻 scan.ts 的敏感数据检测
SENSITIVE_PATTERNS = {
    "Email": re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"),
    "Credit Card Number": re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
    "IP Address": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "Phone Number": re.compile(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b"),
    "Social Security Number": re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
}

def scan_body(body_str: str) -> dict[str, list[str]]:
    """递归扫描 Body 中的敏感数据"""
    findings = defaultdict(list)
    if not body_str:
        return dict(findings)

    def _scan(value, path_prefix=""):
        if isinstance(value, str):
            for class_name, pattern in SENSITIVE_PATTERNS.items():
                if pattern.search(value):
                    findings[path_prefix or "(root)"].append(class_name)
        elif isinstance(value, dict):
            for k, v in value.items():
                _scan(v, f"{path_prefix}.{k}" if path_prefix else k)
        elif isinstance(value, list):
            for i, item in enumerate(value):
                _scan(item, f"{path_prefix}[{i}]")

    try:
        parsed = json.loads(body_str)
        _scan(parsed)
    except (json.JSONDecodeError, TypeError):
        for class_name, pattern in SENSITIVE_PATTERNS.items():
            if pattern.search(body_str):
                findings["(root)"].append(class_name)

    return dict(findings)

def generate_alerts(trace: dict, endpoint: str) -> list[dict]:
    """模拟 Analyzer 告警生成逻辑
    注意: endpoint 已包含 method (如 \"POST /api/xxx\"), 不再拼接 trace['method']
    """
    alerts = []

    # 新端点告警
    alerts.append({
        "type": "New Endpoint Detected",
        "riskScore": "low",
        "description": f"New endpoint discovered: {endpoint}",
    })

    # 扫描敏感数据
    req_findings = scan_body(trace["requestBody"])
    res_findings = scan_body(trace["responseBody"])

    for location, classes in {**req_findings, **res_findings}.items():
        for cls in classes:
            alerts.append({
                "type": "PII Data Detected",
                "riskScore": "medium",
                "description": f"Sensitive data ({cls}) detected in {location} of {endpoint}",
            })

    # 认证相关告警
    sm = trace["sessionMeta"]
    if not sm["authenticationProvided"] and (
        req_findings or res_findings
    ):
        alerts.append({
            "type": "Unauthenticated Endpoint returning Sensitive Data",
            "riskScore": "high",
            "description": f"Unauthenticated endpoint {endpoint}{' returning sensitive data.' if (req_findings or res_findings) else '.'}",
        })

    if sm["authType"] == "basic" and sm["authenticationProvided"]:
        alerts.append({
            "type": "Basic Authentication Detected",
            "riskScore": "medium",
            "description": f"Basic Auth detected on {endpoint}",
        })

    # OpenAPI 差分 (401)
    if trace["responseStatus"] == 401 and "/api/" in trace["path"]:
        alerts.append({
            "type": "Open API Spec Diff",
            "riskScore": "low",
            "description": f"HTTP 401 on documented endpoint: {endpoint}",
        })

    return alerts

# ============================================================================
# 主测试流程
# ============================================================================

def main():
    print("=" * 70)
    print("  Metlo 集成测试: Kafka Dump → Adapter → Analyzer Pipeline")
    print("=" * 70)

    # ── 1. 加载数据 ──
    print("\n[1] 加载数据...")
    # 默认输入文件 (可替换为 sys.argv[1])
    input_file = "/sandbox/workspace/uploads/log.txt"
    if not os.path.exists(input_file):
        input_file = os.path.join(os.path.dirname(__file__), "uploads", "log.txt")
    raw_logs = load_kafka_dump(input_file)
    print(f"    从 Kafka dump 提取: {len(raw_logs)} 条日志")

    # ── 2. 适配 ──
    print("\n[2] 适配: RawMirrorLog → QueuedApiTraceV1...")
    start = time_module.time()
    traces = []
    adapt_errors = 0
    for i, raw in enumerate(raw_logs):
        try:
            traces.append(adapt(raw))
        except Exception as e:
            adapt_errors += 1
            print(f"    ✗ 日志 #{i} 适配失败: {e}")
    adapt_time = time_module.time() - start
    print(f"    成功: {len(traces)} / 失败: {adapt_errors} / 耗时: {adapt_time:.3f}s")

    # ── 3. 数据统计 ──
    print("\n[3] 数据统计分析...")
    methods = defaultdict(int)
    statuses = defaultdict(int)
    hosts = defaultdict(int)
    auth_types = defaultdict(int)
    redacted_count = 0
    null_response_payload = 0

    for t, raw in zip(traces, raw_logs):
        methods[t["method"]] += 1
        statuses[t["responseStatus"]] += 1
        hosts[t["host"]] += 1
        auth_types[t["sessionMeta"]["authType"]] += 1
        if t["redacted"]:
            redacted_count += 1
        if raw.get("responsePayload") is None:
            null_response_payload += 1

    print(f"    请求方法分布: {dict(methods)}")
    print(f"    状态码分布:   {dict(statuses)}")
    print(f"    主机分布:     {dict(hosts)}")
    print(f"    认证类型分布: {dict(auth_types)}")
    print(f"    脱敏标记(redacted): {redacted_count}/{len(traces)} ({100*redacted_count/max(len(traces),1):.1f}%)")
    print(f"    空响应体(null→\"): {null_response_payload}/{len(traces)}")

    # ── 4. 端点发现 ──
    print("\n[4] 端点发现 + 路径参数化...")
    endpoint_map = defaultdict(list)
    for t in traces:
        param_path = parameterize_path(t["path"])
        key = f"{t['method']} {param_path}"
        endpoint_map[key].append(t)

    print(f"    发现唯一端点: {len(endpoint_map)}")
    for ep, t_list in sorted(endpoint_map.items(), key=lambda x: -len(x[1])):
        print(f"    {ep:60s}  {len(t_list):3d} 条")
        if len(t_list) < 3:
            # 只展示高流量端点前几位
            break

    # ── 5. 敏感数据扫描 ──
    print("\n[5] 敏感数据扫描 (Scanner 模拟)...")
    total_findings = 0
    traces_with_pii = 0
    for t in traces:
        all_findings = {}
        all_findings.update(scan_body(t["requestBody"]))
        all_findings.update(scan_body(t["responseBody"]))
        if all_findings:
            traces_with_pii += 1
            total_findings += sum(len(v) for v in all_findings.values())
    print(f"    PII 命中: {traces_with_pii}/{len(traces)} 条")
    print(f"    总命中数: {total_findings}")

    # ── 6. 告警生成 ──
    print("\n[6] 告警生成 (Alert Pipeline 模拟)...")
    all_alerts = []
    seen_endpoints = set()
    for t in traces:
        param_path = parameterize_path(t["path"])
        ep = f"{t['method']} {param_path}"
        alerts = generate_alerts(t, ep)
        # New Endpoint 去重: 每个端点仅生成一次
        if ep not in seen_endpoints:
            seen_endpoints.add(ep)
        else:
            alerts = [a for a in alerts if a["type"] != "New Endpoint Detected"]
        all_alerts.extend(alerts)

    alert_by_type = defaultdict(int)
    alert_by_risk = defaultdict(int)
    for a in all_alerts:
        alert_by_type[a["type"]] += 1
        alert_by_risk[a["riskScore"]] += 1

    print(f"    总告警数: {len(all_alerts)}")
    print(f"    按类型:")
    for atype, count in sorted(alert_by_type.items(), key=lambda x: -x[1]):
        print(f"      {atype:55s} {count:3d}")
    print(f"    按风险等级:")
    for risk, count in sorted(alert_by_risk.items(), key=lambda x: -x[1]):
        print(f"      {risk:10s} {count:3d}")

    # ── 7. 端到端示例 ──
    print("\n[7] 端到端示例 (取第 1 条)...")
    sample = traces[0]
    print(f"    原始路径:  {raw_logs[0]['path']}")
    print(f"    参数化:    {parameterize_path(raw_logs[0]['path'])}")
    print(f"    Host:      {sample['host']}")
    print(f"    Status:    {sample['responseStatus']}")
    print(f"    Auth:      {sample['sessionMeta']['authType']} (user={sample['sessionMeta'].get('user')})")
    print(f"    Redacted:  {sample['redacted']}")
    print(f"    ReqHeaders (first 3):")
    for h in sample["requestHeaders"][:3]:
        print(f"      {h['name']}: {h['value'][:60]}")
    print(f"    ResBody:   {(sample['responseBody'] or '(null→empty)')[:80]}")

    # ── 8. Authorization 验证 ──
    print("\n[8] Authorization Header 脱敏验证...")
    auth_redacted = 0
    for t in traces:
        for h in t["requestHeaders"]:
            if h["name"].lower() == "authorization":
                if h["value"] == REDACTED_VALUE:
                    auth_redacted += 1
    print(f"    Authorization headers 总数: {sum(1 for t in traces for h in t['requestHeaders'] if h['name'].lower()=='authorization')}")
    print(f"    其中已脱敏: {auth_redacted}")
    if not (auth_redacted > 0):
        raise AssertionError("Authorization 未被脱敏!")

    # ── 9. 结果 ──
    print("\n" + "=" * 70)
    print(f"  集成测试完成: {len(traces)} 条日志 → {len(endpoint_map)} 个端点 → {len(all_alerts)} 个告警")
    print(f"  适配耗时: {adapt_time:.3f}s ({len(traces)/adapt_time:.0f} logs/s)")
    print("=" * 70)

if __name__ == "__main__":
    main()
