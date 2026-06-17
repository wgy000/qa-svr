# QA服务接口文档

---

## 认证说明

除 `/health` 外，所有接口都需要通过 API Key 认证。

**请求头:**
```
X-API-Key: <QA_API_KEY>
```

**示例:**
```bash
curl -H "X-API-Key: <QA_API_KEY>" http://<QA_HOST>:40000/api/quality-check ...
```

---

## 1. 质检接口（统一入口）

**接口地址:** `POST /api/quality-check`

**特点:** 统一入口，支持两种方式：
1. 上传音频文件质检（单条）
2. 批量质检（单条或多条通话记录都支持）

---

### 方式一：上传音频文件（multipart/form-data）

**Content-Type:** `multipart/form-data`

#### 参数说明

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| audio_file | File | ✅ 必填 | 语音文件 |
| script_standards | String | ❌ 选填 | 话术规范，用 `\n` 分隔多行 |
| general_rules | String | ❌ 选填 | 通用规则，自由格式文本 |
| start | String | ❌ 选填 | 通话开始时间，UTC 1970-01-01至今的毫秒数（上传远程录音时必填） |
| stop | String | ❌ 选填 | 通话结束时间，UTC 1970-01-01至今的毫秒数（上传远程录音时选填） |
| flowNo | String | ❌ 选填 | 通话流水号（上传远程录音时必填） |
| callback_url | String | ❌ 选填 | 质检完成后接收结果的回调地址，详见「7. Webhook 回调」 |
| callback_events | String | ❌ 选填 | 回调事件，`job`（默认，仅终态）或 `item,job`（每条完成也回调） |
| 其他任意字段 | String | ❌ 选填 | 任意自定义字段，主要是放通话记录信息 |

#### 请求示例

```bash
curl -X POST http://<QA_HOST>:40000/api/quality-check \
  -H "X-API-Key: <QA_API_KEY>" \
  -F "audio_file=@/path/to/call.wav" \
  -F "callid=CALL_001" \
  -F "agentno=1001" \
  -F "script_standards=坐席必须报工号\n坐席需礼貌问候用户" \
  -F "general_rules=禁止出现以下内容：1. 态度恶劣 2. 推诿客户"
```

#### 返回示例

```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "completed",
  "total": 1,
  "completed": 1,
  "failed": 0,
  "results": [
    {
      "index": 0,
      "success": true,
      "callid": "CALL_001",
      "result": {
        "is_compliant": true,
        "score": 85,
        "summary": "通话整体符合规范",
        "violations": [],
        "roles": [{"speaker_id": "0", "role": "坐席"}, {"speaker_id": "1", "role": "用户"}],
        "conversations": [
          {"speaker_id": "0", "speaker_role": "坐席", "text": "您好，工号1234为您服务", "start": 0, "end": 2000}
        ],
        "metadata": {"extra": {"callid": "CALL_001", "agentno": "1001"}},
        "asr_result": {...},
        "analysis_time": "2026-04-01 10:00:00"
      }
    }
  ]
}
```

#### 返回字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| job_id | String | 任务唯一标识 |
| status | String | 任务状态：submitted/processing/cancelling/cancelled/completed/failed |
| total | Integer | 总任务数 |
| completed | Integer | 已完成数 |
| failed | Integer | 失败数 |
| results | Array | 质检结果数组 |
| results[].index | Integer | 任务索引 |
| results[].success | Boolean | 是否成功 |
| results[].callid | String | 通话ID（如果有） |
| results[].result | Object | 质检结果详情 |
| results[].result.is_compliant | Boolean | 是否符合规范 |
| results[].result.score | Integer | 评分 (0-100) |
| results[].result.summary | String | 质检总结 |
| results[].result.violations | Array | 违规列表 |
| results[].result.roles | Array | 角色识别结果 |
| results[].result.conversations | Array | 对话内容 |
| results[].result.metadata | Object | 元信息，extra包含自定义参数 |
| results[].result.asr_result | Object | ASR转写结果 |
| results[].result.analysis_time | String | 分析时间 |

---

### 方式二：批量质检（JSON格式）

当需要一次性提交多条通话记录进行质检时，使用 `records` 参数。

**Content-Type:** `application/json`

#### 参数说明

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| records | Array | ✅ 必填 | 通话记录数组，每条记录包含 start、flowNo 等 |
| script_standards | String | ❌ 选填 | 话术规范，用 `\n` 分隔多行 |
| general_rules | String | ❌ 选填 | 通用规则，自由格式文本 |
| callback_url | String | ❌ 选填 | 质检完成后接收结果的回调地址，详见「7. Webhook 回调」 |
| callback_events | String | ❌ 选填 | 回调事件，`job`（默认，仅终态）或 `item,job`（每条完成也回调） |

#### records 数组每项参数

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| start | String | ✅ 必填 | 通话开始时间，UTC+8时区1970-01-01至今的毫秒数 |
| flowNo | String | ✅ 必填 | 通话流水号 |
| callid | String | ❌ 选填 | 通话ID |
| 其他字段 | String | ❌ 选填 | 任意自定义字段 |

#### 请求示例

```bash
curl -X POST http://<QA_HOST>:40000/api/quality-check \
  -H "Content-Type: application/json" \
  -H "X-API-Key: <QA_API_KEY>" \
  -d '{
    "records": [
      {"start": "1737955200000", "flowNo": "call_001"},
      {"start": "1737955300000", "flowNo": "call_002"}
    ]
  }'
```

#### 返回示例

```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "submitted",
  "total": 2,
  "file_urls": [
    {"flowNo": "call_001", "url": "http://<QA_HOST>:40000/audio/call_001.wav"},
    {"flowNo": "call_002", "url": "http://<QA_HOST>:40000/audio/call_002.wav"}
  ]
}
```

#### 返回字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| job_id | String | 任务唯一标识 |
| status | String | 任务状态：submitted（已提交，等待处理） |
| total | Integer | 总任务数 |
| file_urls | Array | 录音文件URL数组 |
| file_urls[].flowNo | String | 通话流水号 |
| file_urls[].url | String | 录音文件HTTP地址，可直接播放 |

---

## 2. 录音地址查询接口（不触发质检）

**接口地址:** `POST /api/quality-search`

**背景（USI-229）:** 播放/下载录音只需要一个可播放地址，之前复用「质检接口」的
`records` 批量分支来拿地址，副作用是顺带创建了一个真实的质检任务、跟真正的质检任务
抢并发，长录音时容易在 Nginx 探测（单节点每次同步请求超时 8s）排队卡住，导致播放请求
一起被拖慢/超时。这个接口只做录音地址定位 + 下载转码（Nginx 探测到的是内网地址，且
原始 `.rec` 格式浏览器无法播放，必须转成 `.wav` 并挂到 qa-svr 自己的 `/audio/` 路径下
才能真正播放），不跑 ASR/Ollama 推理、不创建质检任务，下载转码用独立信号量，不占用
批量质检提交那条路径的并发；`/api/quality-check` 本身未改动。

**Content-Type:** `application/json`

#### 参数说明

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| records | Array | ✅ 必填 | 通话记录数组，形状与「质检接口」的 `records` 一致 |
| records[].start | String | ✅ 必填 | 通话开始时间，UTC 1970-01-01至今的毫秒数 |
| records[].flowNo | String | ✅ 必填 | 通话流水号 |

#### 请求示例

```bash
curl -X POST http://<QA_HOST>:40000/api/quality-search \
  -H "X-API-Key: <QA_API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{"records": [{"start": "1710000000000", "flowNo": "FLOW_001"}]}'
```

#### 响应示例

```json
{
  "file_urls": [
    { "flowNo": "FLOW_001", "url": "http://<QA_HOST>:40000/audio/20260311/FLOW_001.wav" }
  ]
}
```

未找到录音文件、下载或转码失败时对应条目 `url` 为空字符串，不会抛错中断其他条目的查询。

---

## 3. 查询任务状态

**接口地址:** `GET /api/task/{job_id}`

#### 参数说明

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| job_id | String | ✅ 必填 | 任务ID，质检接口返回的job_id |

#### 请求示例

```bash
curl -X GET http://<QA_HOST>:40000/api/task/a1b2c3d4-e5f6-7890-abcd-ef1234567890 \
  -H "X-API-Key: <QA_API_KEY>"
```

#### 返回示例

```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "completed",
  "total": 1,
  "completed": 1,
  "failed": 0,
  "results": [
    {
      "index": 0,
      "success": true,
      "callid": "CALL_001",
      "result": {
        "is_compliant": true,
        "score": 85,
        "summary": "通话整体符合规范",
        "violations": [],
        "roles": [{"speaker_id": "0", "role": "坐席"}],
        "conversations": [...],
        "metadata": {"extra": {"callid": "CALL_001"}},
        "asr_result": {...},
        "analysis_time": "2026-02-26 10:00:00"
      }
    }
  ],
  "created_at": "2026-02-26T10:00:00",
  "updated_at": "2026-02-26T10:03:00"
}
```

#### 返回字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| job_id | String | 任务唯一标识 |
| status | String | 任务状态：submitted/processing/cancelling/cancelled/completed/failed |
| total | Integer | 总任务数 |
| completed | Integer | 已完成数 |
| failed | Integer | 失败数 |
| results | Array | 质检结果数组 |
| results[].index | Integer | 任务索引 |
| results[].success | Boolean | 是否成功 |
| results[].callid | String | 通话ID（如果有） |
| results[].result | Object | 质检结果详情 |
| created_at | String | 任务创建时间 |
| updated_at | String | 任务更新时间 |

#### 任务状态说明

| status | 说明 |
|--------|------|
| submitted | 任务已提交，等待处理 |
| processing | 正在处理中 |
| cancelling | 正在取消中（子任务将在当前步骤完成后停止） |
| cancelled | 任务已取消（结果被拦截，不返回前端） |
| completed | 全部处理完成 |
| failed | 处理失败 |

---

## 4. 查询所有任务

**接口地址:** `GET /api/task`

#### 参数说明

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| limit | Integer | ❌ 选填 | 返回任务数量，默认10 |

#### 请求示例

```bash
curl -X GET "http://<QA_HOST>:40000/api/task?limit=5" \
  -H "X-API-Key: <QA_API_KEY>"
```

#### 返回示例

```json
{
  "tasks": [
    {
      "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "status": "completed",
      "total": 1,
      "completed": 1,
      "failed": 0,
      "created_at": "2026-02-26T10:00:00",
      "updated_at": "2026-02-26T10:03:00"
    },
    {
      "job_id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
      "status": "processing",
      "total": 5,
      "completed": 3,
      "failed": 0,
      "created_at": "2026-02-26T10:05:00",
      "updated_at": "2026-02-26T10:06:00"
    }
  ]
}
```

#### 返回字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| tasks | Array | 任务列表数组 |
| tasks[].job_id | String | 任务唯一标识 |
| tasks[].status | String | 任务状态 |
| tasks[].total | Integer | 总任务数 |
| tasks[].completed | Integer | 已完成数 |
| tasks[].failed | Integer | 失败数 |
| tasks[].created_at | String | 任务创建时间 |
| tasks[].updated_at | String | 任务更新时间 |

---

## 5. 取消质检任务

**接口地址:** `POST /api/job/{job_id}/cancel`

取消指定的质检任务。已在进行中的子任务会在当前步骤（下载/ASR/质检）完成后停止，后续结果不再写入和返回。

#### 参数说明

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| job_id | String | ✅ 必填 | 路径参数，任务ID |

#### 请求示例

```bash
curl -X POST http://<QA_HOST>:40000/api/job/a1b2c3d4-e5f6-7890-abcd-ef1234567890/cancel \
  -H "X-API-Key: <QA_API_KEY>"
```

#### 返回示例

取消进行中的任务：
```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "cancelling",
  "msg": "正在取消"
}
```

取消已提交但未开始的任务：
```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "cancelled",
  "msg": "任务已取消"
}
```

重复取消：
```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "cancelled",
  "msg": "任务已处于取消状态"
}
```

取消已完成的任务：
```json
{
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "completed",
  "msg": "任务已完成，无法取消"
}
```

#### 返回字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| job_id | String | 任务唯一标识 |
| status | String | 当前任务状态 |
| msg | String | 操作结果说明 |

#### 取消行为说明

| 场景 | 行为 |
|------|------|
| 任务处于 `submitted` | 立即标记为 `cancelled`，不开始处理 |
| 任务处于 `processing` | 标记为 `cancelling`，子任务在下一个检查点停止，不存储后续结果 |
| 任务处于 `cancelling` | 返回当前状态，无需重复操作 |
| 任务处于 `completed` | 返回提示，无法取消 |
| 任务处于 `cancelled` | 返回提示，无需重复操作 |
| 任务不存在 | 返回 404 |

> **注意：** 取消标记本身仍是内存级的，服务重启后会丢失；但重启时正在跑的任务会被统一标记为 `failed`（见下节），不会再停留在 `cancelling`。

---

## 6. 任务持久化

任务状态与质检结果会写入 PostgreSQL，因此：

- **服务重启后仍可查询历史任务**：`GET /api/task/{job_id}` 内存未命中时自动回落数据库，不再返回 404。
- **重启会中断进行中的任务**：进程一退出，正在跑的 asyncio 任务就消失了，没有任何人会继续推进它们。服务下次启动时会把上次残留在 `submitted`/`processing`/`cancelling` 的任务统一标记为 `failed`，并补发一次 `job.failed` 回调。**调用方需要把这类任务当作失败重新提交**，而不是继续等它变成 `completed`。
- **结果保留期默认 30 天**（`JOB_RETENTION_DAYS`），过期记录由定时任务清理。
- `GET /api/task?limit=N` 改为按 `created_at` 倒序从数据库返回，不再只是内存里的最后 N 条。

数据库不可用时服务不会中断：连接失败只记录日志，质检照常进行，仅退化为「重启后查不到历史任务」的老行为。

---

## 7. 健康检查

**接口地址:** `GET /health`

> ⚠️ 此接口无需认证

#### 请求示例

```bash
curl -X GET http://<QA_HOST>:40000/health
```

#### 返回示例

```json
{
  "status": "ok"
}
```

#### 返回字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| status | String | 服务状态，ok 表示正常 |

---

## 8. Webhook 回调

提交质检时带上 `callback_url`，质检完成后 qa-svr 会主动把结果 POST 过去，调用方不必再长时间轮询 `/api/task/{job_id}`。

### 启用条件

服务端 `config.py` 必须同时配置 `CALLBACK_SECRET` 和 `CALLBACK_ALLOWED_HOSTS`，两者任一为空则回调功能整体关闭，带 `callback_url` 的提交会直接返回 400。

`CALLBACK_ALLOWED_HOSTS` 是**必填的主机白名单**：qa-svr 与 Ollama、FunASR、Nginx 录音服务同处内网，允许任意回调地址等于把它变成内网 SSRF 跳板。除白名单外，解析到内网/回环地址的主机也会被拒绝。

### 事件类型

| event | 触发时机 |
|-------|----------|
| job.completed | 任务全部处理完成 |
| job.cancelled | 任务被取消 |
| job.failed | 任务因服务重启等原因中断 |
| item.completed | 批量任务中单条记录出结果（需 `callback_events` 含 `item`） |

### 请求头

| 头 | 说明 |
|----|------|
| X-QA-Event | 事件类型 |
| X-QA-Job-Id | 任务 ID |
| X-QA-Delivery-Id | 投递 ID，**同一事件的多次重试保持不变**，接收方据此幂等去重 |
| X-QA-Timestamp | 签名时间戳（Unix 秒） |
| X-QA-Signature | `sha256=<hex>`，见下方验签 |

### 载荷示例

```json
{
  "delivery_id": "6f1c...",
  "event": "job.completed",
  "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "completed",
  "total": 2,
  "completed": 2,
  "failed": 0,
  "results": [
    {
      "index": 0,
      "success": true,
      "callid": "CALL_001",
      "flowNo": "call_001",
      "result": {
        "is_compliant": true,
        "score": 85,
        "summary": "通话整体符合规范",
        "violations": [],
        "roles": [...],
        "conversations": [...],
        "metadata": {"extra": {"callid": "CALL_001"}},
        "analysis_time": "2026-08-04 10:00:00"
      }
    }
  ],
  "created_at": "2026-08-04T10:00:00",
  "updated_at": "2026-08-04T10:03:00",
  "sent_at": "2026-08-04T10:03:01"
}
```

> **注意：** 回调载荷中的 `result` **不含 `asr_result`**。它是 FunASR 的完整原始转写，单条就可能上 MB，批量场景会把回调体撑到几十 MB。需要原始转写时请用 `GET /api/task/{job_id}` 取完整结果。
>
> `item.completed` 事件的 `results` 只含对应的那一条记录。

### 验签

签名对 `{timestamp}.{原始请求体字节}` 做 HMAC-SHA256：

```python
import hmac, hashlib
expected = "sha256=" + hmac.new(
    CALLBACK_SECRET.encode(),
    f"{request.headers['X-QA-Timestamp']}.".encode() + raw_body,
    hashlib.sha256,
).hexdigest()
assert hmac.compare_digest(expected, request.headers["X-QA-Signature"])
```

必须对**未经解析的原始字节**验签，先反序列化再重新序列化会因键顺序、空格差异导致签名对不上。建议同时校验 `X-QA-Timestamp` 与当前时间的偏差（如 5 分钟内），防止重放。

### 重试与幂等

- 接收方返回 2xx 视为投递成功；其余状态码或超时都会重试。
- 退避序列 5s / 10s / 20s / 40s / 80s / 160s，最多 6 次（`CALLBACK_MAX_ATTEMPTS`）。
- 投递记录落在 `qa_callback_deliveries` 表，**服务重启后会继续重试**。
- 同一 `(job_id, event, item_index)` 只会登记一条投递记录，重复触发不会产生重复投递。
- 接收方仍需按 `X-QA-Delivery-Id` 做幂等：重试意味着同一个 delivery_id 可能被送达多次。

---

## 9. 响应码说明

| 状态码 | 说明 |
|--------|------|
| 200 | 请求成功 |
| 400 | 请求参数错误（含 callback_url 未通过白名单校验） |
| 404 | 任务不存在 |
| 422 | 参数验证失败 |
| 500 | 服务器内部错误 |