# Metlo 集成测试 — 告警产出报告

> 生成时间: 2026-05-29 02:47:19 UTC

> 输入数据: 69 条 Akto Kafka dump 日志

> 输出告警: 139 条

> 覆盖端点: 30 个


## 一、告警概览

| 告警类型 | 风险等级 | 数量 |
|----------|----------|------|
| New Endpoint Detected | `low` | 69 |
| Open API Spec Diff | `low` | 37 |
| PII Data Detected | `medium` | 27 |
| Unauthenticated Endpoint returning Sensitive Data | `high` | 6 |

### 按风险等级分布

| 等级 | 数量 |
|------|------|
| `high` | 6 |
| `medium` | 27 |
| `low` | 106 |

## 二、端点 × 告警矩阵

| 端点 | 请求数 | New Endpoint | PII | OpenAPI Diff | Unauth SenData | Basic Auth | 合计 |
|------|--------|-------------|-----|-------------|---------------|------------|------|
| `POST /api/threat_detection/save_api_distribution_data` | 37 | 37 | 0 | 37 | 0 | 0 | 74 |
| `GET /fgap/admin/sys/config/get-sysid` | 3 | 3 | 0 | 0 | 0 | 0 | 3 |
| `POST /fgap/admin/biz/app/info/list/{{param1}}/{{param2}}` | 2 | 2 | 0 | 0 | 0 | 0 | 2 |
| `GET /resolv` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /api/dashboard/get_threat_configuration` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/sys/logo/select` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/sys/config/get-value/MENU_MODE` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/node/select/fgap/wsf` | 1 | 1 | 2 | 0 | 1 | 0 | 4 |
| `POST /fgap/admin/sys/config/list` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/biz/menu/display/select/type` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/enum/map/DataBaseType` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/biz/datasource/db/list/{{param1}}/{{param2}}` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/biz/datasource/ftp-server/list/{{param1}}/{{param2}}` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/reconciliation/receive/list/{{param1}}/{{param2}}` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/biz/component/service/list/{{param1}}/{{param2}}` | 1 | 1 | 1 | 0 | 1 | 0 | 3 |
| `GET /fgap/admin/sys/dict/select/dict-children/certificate_library_encryption_type` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/sys/dict/select/dict-children/public_or_private_key` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/sys/dict/select/dict-children/signature_algorith_type` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/sys/dict/select/dict-children/the_secret_key_algorithm` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/biz/user/keystore/list/{{param1}}/{{param2}}` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/data/sign/list/{{param1}}/{{param2}}` | 1 | 1 | 1 | 0 | 1 | 0 | 3 |
| `POST /fgap/admin/probe/log/list/{{param1}}/{{param2}}` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/collectMonitor/listPage/network/{{param1}}/{{param2}}` | 1 | 1 | 2 | 0 | 1 | 0 | 4 |
| `GET /fgap/admin/enumerate/getEnum/AlarmCheckType` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/enumerate/getEnum/AlarmLevelType` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/log/warning/list/{{param1}}/{{param2}}` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}` | 1 | 1 | 10 | 0 | 1 | 0 | 12 |
| `GET /fgap/admin/sys/system/handle/version` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |
| `GET /fgap/admin/basis/network/card/list/name` | 1 | 1 | 11 | 0 | 1 | 0 | 13 |
| `POST /fgap/admin/biz/log/aggregate/chart` | 1 | 1 | 0 | 0 | 0 | 0 | 1 |

## 三、HIGH 风险告警详情

### 1. Unauthenticated Endpoint returning Sensitive Data

- **端点**: `GET /fgap/admin/node/select/fgap/wsf`
- **主机**: `192.168.106.53`
- **描述**: Unauthenticated endpoint GET GET /fgap/admin/node/select/fgap/wsf on 192.168.106.53 is returning sensitive data.
- **请求方法**: `GET`
- **响应状态**: `200`
- **认证状态**: provided=False, successful=False
- **来源 IP**: 192.168.106.53
- **时间**: 2026-05-27T08:30:25+00:00
- **响应体敏感数据**: {"content.nodeIp": ["IP Address"], "content.nodeDataExchangeIp": ["IP Address"]}

### 2. Unauthenticated Endpoint returning Sensitive Data

- **端点**: `POST /fgap/admin/biz/component/service/list/{{param1}}/{{param2}}`
- **主机**: `192.168.106.53`
- **描述**: Unauthenticated endpoint POST POST /fgap/admin/biz/component/service/list/{{param1}}/{{param2}} on 192.168.106.53 is returning sensitive data.
- **请求方法**: `POST`
- **响应状态**: `200`
- **认证状态**: provided=False, successful=False
- **来源 IP**: 192.168.106.53
- **时间**: 2026-05-27T08:30:32+00:00
- **响应体敏感数据**: {"content.list[6].context": ["IP Address"]}

### 3. Unauthenticated Endpoint returning Sensitive Data

- **端点**: `POST /fgap/admin/data/sign/list/{{param1}}/{{param2}}`
- **主机**: `192.168.106.53`
- **描述**: Unauthenticated endpoint POST POST /fgap/admin/data/sign/list/{{param1}}/{{param2}} on 192.168.106.53 is returning sensitive data.
- **请求方法**: `POST`
- **响应状态**: `200`
- **认证状态**: provided=False, successful=False
- **来源 IP**: 192.168.106.53
- **时间**: 2026-05-27T08:30:36+00:00
- **响应体敏感数据**: {"content.list[0].ip": ["IP Address"]}

### 4. Unauthenticated Endpoint returning Sensitive Data

- **端点**: `POST /fgap/admin/collectMonitor/listPage/network/{{param1}}/{{param2}}`
- **主机**: `192.168.106.53`
- **描述**: Unauthenticated endpoint POST POST /fgap/admin/collectMonitor/listPage/network/{{param1}}/{{param2}} on 192.168.106.53 is returning sensitive data.
- **请求方法**: `POST`
- **响应状态**: `200`
- **认证状态**: provided=False, successful=False
- **来源 IP**: 192.168.106.53
- **时间**: 2026-05-27T08:30:39+00:00
- **响应体敏感数据**: {"content.network.list[0].txBytes": ["Phone Number"], "content.network.list[1].txBytes": ["Phone Number"]}

### 5. Unauthenticated Endpoint returning Sensitive Data

- **端点**: `POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}`
- **主机**: `192.168.106.53`
- **描述**: Unauthenticated endpoint POST POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}} on 192.168.106.53 is returning sensitive data.
- **请求方法**: `POST`
- **响应状态**: `200`
- **认证状态**: provided=False, successful=False
- **来源 IP**: 192.168.106.53
- **时间**: 2026-05-27T08:30:44+00:00
- **响应体敏感数据**: {"content.list[0].ip": ["IP Address"], "content.list[1].ip": ["IP Address"], "content.list[2].ip": ["IP Address"], "content.list[3].ip": ["IP Address"], "content.list[4].ip": ["IP Address"], "content.list[5].ip": ["IP Address"], "content.list[6].ip": ["IP Address"], "content.list[7].ip": ["IP Address"], "content.list[8].ip": ["IP Address"], "content.list[9].ip": ["IP Address"]}

### 6. Unauthenticated Endpoint returning Sensitive Data

- **端点**: `GET /fgap/admin/basis/network/card/list/name`
- **主机**: `192.168.106.53`
- **描述**: Unauthenticated endpoint GET GET /fgap/admin/basis/network/card/list/name on 192.168.106.53 is returning sensitive data.
- **请求方法**: `GET`
- **响应状态**: `200`
- **认证状态**: provided=False, successful=False
- **来源 IP**: 192.168.106.53
- **时间**: 2026-05-27T08:30:44+00:00
- **响应体敏感数据**: {"content[0].Ip": ["IP Address"], "content[1].Ip": ["IP Address"], "content[2].Ip": ["IP Address"], "content[3].Ip": ["IP Address"], "content[4].Ip": ["IP Address"], "content[5].Ip": ["IP Address"], "content[7].Ip": ["IP Address"], "content[8].Ip": ["IP Address"], "content[9].Ip": ["IP Address"], "content[10].Ip": ["IP Address"], "content[11].Ip": ["IP Address"]}

## 四、MEDIUM 风险告警详情 (前 10 条)

- **[PII Data Detected]** `GET /fgap/admin/node/select/fgap/wsf` — Sensitive data of type 'IP Address' detected in field 'content.nodeIp' of GET GET /fgap/admin/node/select/fgap/wsf
- **[PII Data Detected]** `GET /fgap/admin/node/select/fgap/wsf` — Sensitive data of type 'IP Address' detected in field 'content.nodeDataExchangeIp' of GET GET /fgap/admin/node/select/fgap/wsf
- **[PII Data Detected]** `POST /fgap/admin/biz/component/service/list/{{param1}}/{{param2}}` — Sensitive data of type 'IP Address' detected in field 'content.list[6].context' of POST POST /fgap/admin/biz/component/service/list/{{param1}}/{{param2}}
- **[PII Data Detected]** `POST /fgap/admin/data/sign/list/{{param1}}/{{param2}}` — Sensitive data of type 'IP Address' detected in field 'content.list[0].ip' of POST POST /fgap/admin/data/sign/list/{{param1}}/{{param2}}
- **[PII Data Detected]** `POST /fgap/admin/collectMonitor/listPage/network/{{param1}}/{{param2}}` — Sensitive data of type 'Phone Number' detected in field 'content.network.list[0].txBytes' of POST POST /fgap/admin/collectMonitor/listPage/network/{{param1}}/{{param2}}
- **[PII Data Detected]** `POST /fgap/admin/collectMonitor/listPage/network/{{param1}}/{{param2}}` — Sensitive data of type 'Phone Number' detected in field 'content.network.list[1].txBytes' of POST POST /fgap/admin/collectMonitor/listPage/network/{{param1}}/{{param2}}
- **[PII Data Detected]** `POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}` — Sensitive data of type 'IP Address' detected in field 'content.list[0].ip' of POST POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}
- **[PII Data Detected]** `POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}` — Sensitive data of type 'IP Address' detected in field 'content.list[1].ip' of POST POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}
- **[PII Data Detected]** `POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}` — Sensitive data of type 'IP Address' detected in field 'content.list[2].ip' of POST POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}
- **[PII Data Detected]** `POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}` — Sensitive data of type 'IP Address' detected in field 'content.list[3].ip' of POST POST /fgap/admin/basis/arp/info/list/{{param1}}/{{param2}}

> 共 27 条 MEDIUM 告警，以上为前 10 条。

## 五、LOW 风险告警汇总

- **New Endpoint Detected**: 69 条
- **Open API Spec Diff**: 37 条

> 共 106 条 LOW 告警。

## 六、数据脱敏统计

| 指标 | 值 |
|------|-----|
| 总日志数 | 69 |
| 触发脱敏 (redacted=true) | 38 (55.1%) |
| Authorization header 数 | 38 |
| Authorization 已脱敏 | 38/38 |
| 脱敏 Header 列表 | proxy-authorization, x-api-key, authorization, set-cookie, x-auth-token, cookie |

## 七、数据质量

| 指标 | 值 |
|------|-----|
| responsePayload=null (→ 空串) | 38/69 |
| statusCode 缺失 (→ 0) | 1 |
| 适配失败 | 0 |
| 主机数 | 3 |
| 请求方法数 | 2 |

## 八、结论

- 适配层成功将 69 条 Akto 格式原始日志转换为 Metlo V1 标准格式，**0 条失败**。
- 敏感 Header 脱敏机制正确运行，38 条 Authorization header 全部脱敏。
- `responsePayload: null` 已正确处理，38 条转为空字符串。
- 产生 **6 条 HIGH** 风险告警，涉及未认证端点返回敏感数据。
- 产生 **27 条 MEDIUM** 风险告警，主要为 PII 检测和 Basic Auth。
- 产生 **106 条 LOW** 风险告警，为新端点发现和 OpenAPI 规格差分。