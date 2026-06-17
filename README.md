# qa-svr — 呼叫中心话术质检服务

把「一通电话」变成结构化质检结果的 HTTP 服务：录音定位 → 下载转码 → FunASR 转写 + 说话人分离 → Ollama 按话术规范打分 → 结果落库 / 回调。

## 架构

```
调用方
  │  POST /api/quality-check
  ▼
qa-svr (FastAPI, :40000)
  ├── Nginx 录音节点探测（多节点并发，.rec / .wav）
  ├── audioconvert 转码 → WAV_FILE_DIR/YYYYMMDD/<flowNo>.wav
  ├── FunASR   /asr     语音转写 + 角色分离（独立服务，默认 :50000）
  └── Ollama   /api/generate  按话术规范输出 JSON 质检结果
        │
        ├── 内存 task_storage（热数据）
        ├── PostgreSQL（旁路持久化，可关闭）
        └── Webhook 回调队列（可关闭）
```

- 提交即返回 `job_id`，质检在后台 asyncio 任务里跑，调用方轮询 `/api/task/{job_id}` 或等回调。
- 数据库是旁路：连不上只记日志，质检照常完成，仅退化为「重启后查不到历史任务」。
- 回调也是旁路：不配 `CALLBACK_SECRET` / `CALLBACK_ALLOWED_HOSTS` 就整体关闭。

## 目录结构

| 文件 | 作用 |
|------|------|
| `main.py` | FastAPI 应用：接口、任务调度、ASR/Ollama 调用、录音下载与转码 |
| `db.py` | PostgreSQL 持久化层（任务快照、历史查询、回调投递队列） |
| `callback.py` | Webhook 投递、URL 白名单校验、HMAC 签名、指数退避重试 |
| `config.py` | 配置（**已被 .gitignore 忽略**，从 `config.py.example` 复制） |
| `schema.sql` | 建表脚本，可重复执行 |
| `funasr/server.py` | 独立的 FunASR 转写服务（Paraformer + VAD + 标点 + CAM++ 说话人） |
| `qa-svr.service` / `ollama.service` / `funasr/asr.service` | systemd 单元 |
| `docs/` | 接口文档、持久化与回调部署说明、Python 依赖清单 |

## 快速开始

要求 Python 3.9+，另需可访问的 FunASR、Ollama 与录音文件 HTTP 服务（Nginx）。

```bash
# 1. 依赖
pip install -r docs/requirements.txt

# 2. 配置（config.py 不入库，必须手动创建）
cp config.py.example config.py
#   至少改：API_KEY、OLLAMA_BASE_URL/MODEL、FUNASR_URL、NGINX_SERVERS、
#          AUDIO_HTTP_HOST、LOG_DIR、WAV_FILE_DIR、AUDIO_CONVERT_BIN

# 3. 建表（不用持久化可跳过，并把 DB_ENABLED 设为 False）
psql -h <host> -U <user> -d <db> -f schema.sql

# 4. 启动
python3 main.py
```

服务默认监听 `0.0.0.0:40000`，自检：

```bash
curl http://127.0.0.1:40000/health          # {"status":"ok"}
curl http://127.0.0.1:40000/health-check    # 附带 Ollama / FunASR 连通性
```

### FunASR 服务

`funasr/server.py` 单独部署（`uvicorn server:app --host 0.0.0.0 --port 50000`），通过环境变量调优：

| 变量 | 默认 | 说明 |
|------|------|------|
| `MAX_CONCURRENT` | 5 | 同时推理路数，满额返回 429「服务繁忙」 |
| `PHYS_CORES` | 6 | 物理核数，用于切分每路线程数（`max(2, PHYS_CORES // MAX_CONCURRENT)`） |
| `ASR_THREADS_PER_TASK` | 0（自动） | 覆盖自动计算的每路线程数 |
| `MAX_FILE_MB` | 100 | 上传大小上限，超出 413 |
| `MEM_AVAIL_MIN_PERCENT` | 15 | 系统可用内存低于该比例时拒绝新请求（503），防 OOM |
| `ASR_BATCH_SIZE_S` | 300 | 单次推理 batch 时长，影响峰值内存 |
| `MALLOC_ARENA_MAX` | — | **必须写在 systemd 的 `Environment=` 里**，进程内设置无效 |

> 线程数相关的环境变量在 import torch 之前设置才生效，所以都写死在 server.py 顶部。
> `GET /health` 会回显实际生效的线程/内存配置，部署后可直接 `curl` 确认。

## 接口

除 `/health`、`/health-check` 外均需请求头 `X-API-Key: <API_KEY>`（`API_KEY` 为空则不校验）。

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/quality-check` | 统一质检入口，三种用法见下 |
| POST | `/api/quality-search` | 只定位录音并返回可播放 URL，不建任务、不跑推理 |
| GET | `/api/task/{job_id}` | 查询任务（内存未命中自动回落数据库） |
| GET | `/api/task?limit=N` | 按创建时间倒序列出最近 N 个任务 |
| POST | `/api/job/{job_id}/cancel` | 取消任务 |
| GET | `/audio/{path}` | 播放/下载已转码的 WAV（已防路径穿越） |
| GET | `/health` | 存活探针 |
| GET | `/health-check` | 依赖连通性探针 |

`/api/quality-check` 的三种提交方式：

1. **上传音频** — `multipart/form-data`，`audio_file` 必填，其余表单字段会成为结果的 `metadata.extra`。
2. **批量** — `application/json`，`records: [{start, flowNo, ...}]`；提交阶段会并发预下载录音并把 `file_urls` 一并返回。
3. **单条远程** — 表单只带 `start`(毫秒时间戳) + `flowNo`，由服务自己去 Nginx 找录音。

`callback_url` / `callback_events` 在三种方式下均可选（`callback_events` 默认 `job`，可设 `item,job`）。

完整字段、返回示例与响应码见 [docs/qa-inspection-api-v8.md](docs/qa-inspection-api-v8.md)。

## 任务状态

`submitted` → `processing` → `completed` / `failed` / `cancelled`（取消时经过 `cancelling`）。

- 取消是协作式的：每个检查点（下载前、ASR 前/后、Ollama 后、存储前）确认标记，已算出的中间结果直接丢弃。
- 取消标记只存在内存；重启时残留的非终态任务统一标记为 `failed` 并补发 `job.failed` 回调——**调用方应把这类任务当作失败重新提交**。

## 并发与去重

| 配置 | 作用 |
|------|------|
| `BATCH_CONCURRENCY` | 批量任务内同时处理（ASR + Ollama 整段）的记录数 |
| `FUNASR_CONCURRENCY` | 本地 FunASR 并发槽，需与上游实际能力对齐（上游满额返回 429 时会自动重试 5 次、间隔 3s） |
| `OLLAMA_CONCURRENCY` | 本地 Ollama 并发槽 |

另外两条避免重复消耗算力的机制：

- **同一 flowNo 去重**：正在处理中的 flowNo 会被记入 `flowno_inflight`，重复提交直接跳过并记为失败项；任务结束后再进入 60s 冷却期（`FLOWNO_DEDUP_COOLDOWN_SECONDS`），覆盖「算完分 → 回调投递完成」这段窗口。
- **Ollama JSON 重试**：模型偶发输出被截断/带 markdown 围栏的 JSON 时，同一次调用内最多重试 3 次，不产生新 job。

## 持久化

`qa_jobs`（任务主体）、`qa_job_results`（每条结果）、`qa_callback_deliveries`（回调队列）。

- 内存优先，`GET /api/task/{job_id}` 未命中时回落数据库。
- `periodic_flush` 每 `FLUSH_INTERVAL_SECONDS`(3s) 把变更过的任务写库；**只有写成功才允许内存淘汰**。
- 终态任务在内存保留 `MEMORY_RETENTION_SECONDS`(1800s) 后淘汰；库里保留 `JOB_RETENTION_DAYS`(30 天)，由定时任务清理。
- 音频文件按天分目录存放，保留 30 天后由 `cleanup_old_files` 删除。

## Webhook 回调

同时配置 `CALLBACK_SECRET` 与 `CALLBACK_ALLOWED_HOSTS` 才启用，否则带 `callback_url` 的提交直接 400。

- 事件：`job.completed` / `job.failed` / `job.cancelled` / `item.completed`。
- 请求头：`X-QA-Event`、`X-QA-Job-Id`、`X-QA-Delivery-Id`、`X-QA-Timestamp`、`X-QA-Signature`。
- 签名：对 `{timestamp}.{原始请求体字节}` 做 HMAC-SHA256，`sha256=<hex>`；务必用未解析的原始字节验签。
- 重试：2xx 视为成功，否则 5s/10s/20s/40s/80s/160s，最多 6 次（`CALLBACK_MAX_ATTEMPTS`）；记录落库，重启后继续投递。
- 幂等：`(job_id, event, item_index)` 唯一，同一事件只登记一条；接收方仍需按 `X-QA-Delivery-Id` 去重。
- 白名单主机若解析到内网/回环/链路本地地址会被拒绝（防内网 SSRF）；回调载荷会剔除 `asr_result`，需要原始转写请查 `GET /api/task/{job_id}`。

部署细节见 [docs/persistence-and-callback-deploy.md](docs/persistence-and-callback-deploy.md)。

## 部署（systemd）

```bash
cp qa-svr.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now qa-svr
journalctl -u qa-svr -f
```

单元文件假定代码在 `/opt/qa-svr`、用 `python3` 启动；Ollama 侧的参数（`OLLAMA_NUM_PARALLEL`、`OLLAMA_NUM_THREAD`、`OLLAMA_KEEP_ALIVE` 等）见 `ollama.service`。

## 日志与监控

- 应用日志：`LOG_DIR/qa-svr.log`，按天轮转、保留 90 天；另有按任务打的 `[job_id][flowNo] ASR|Ollama|质检完成 | 耗时 | 得分 | 内存` 性能行。
- GlitchTip（Sentry 协议）可选：设 `GLITCHTIP_DSN` 即启用，其余 `GLITCHTIP_*` 均为环境变量；未安装 `sentry-sdk` 会跳过并告警。
- 进程内存超过 `MAX_MEMORY_MB`(2048) 时强制 `gc.collect()`，每小时打印一次内存水位。

## 录音获取机制

1. 用 `start`（毫秒时间戳，UTC+8）算出 `YYYYMMDD/HH` 目录。
2. 并发请求 `NGINX_SERVERS` 各节点的目录列表，匹配文件名中含 `flowNo` 的 `.rec` / `.wav`（单节点每次同步请求超时 8s）。
3. 命中后下载：`.rec` 用 `AUDIO_CONVERT_BIN -t wav -b 64` 转码，`.wav` 直接落盘；当天已存在同名 wav 则复用，不再重复下载。
4. 返回/回放地址统一把内网 `host:port` 替换成 `AUDIO_HTTP_HOST:AUDIO_HTTP_PORT`（外网可访问地址）。

## 运维命令

```bash
systemctl status qa-svr        # 状态
systemctl restart qa-svr       # 重启
systemctl enable --now qa-svr  # 开机自启
journalctl -u qa-svr -f        # 实时日志
tail -f /opt/qa-svr/log/qa-svr.log
```

## 常见问题

| 现象 | 排查 |
|------|------|
| 服务起不来 | `journalctl -u qa-svr -n 100`；确认 `config.py` 存在、端口 40000 未被占用 |
| `/health-check` 显示 ollama=error | `curl $OLLAMA_BASE_URL/api/tags`，检查模型是否已 `ollama pull` |
| `/health-check` 显示 funasr=error | `curl $FUNASR_URL/health`，确认 FunASR 服务已启动、端口 50000 可达 |
| 质检项报「duplicate flowNo ... skipped」 | 同一 flowNo 有任务在跑或刚结束 60s 内，属预期去重 |
| 找不到录音 | 依次确认 Nginx 节点可达、`YYYYMMDD/HH` 目录存在、文件名含 flowNo；`.rec` 还需检查 `AUDIO_CONVERT_BIN` 可执行 |
| ASR 返回 429「服务繁忙」 | 上游 FunASR 并发已满，服务会自动重试；长期出现应下调 `FUNASR_CONCURRENCY` |

## 已知限制

- `/audio/` 目前只做路径穿越校验，**未启用 URL 签名**（`AUDIO_SIGN_KEY` 已预留但未接）。
- 任务取消标记、flowNo 去重表均在进程内存，仅适用于单实例部署；多实例需要外部共享状态。
