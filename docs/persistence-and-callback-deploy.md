# 任务持久化与 Webhook 回调 —— 部署说明

本次改动给 qa-svr 加了两件事：任务状态落 PostgreSQL、质检完成后主动回调。
两者都是**可关闭的旁路能力**，不配置就退回改动前的纯内存行为，因此可以分步上线。

相关文件：

| 文件 | 作用 |
|------|------|
| `db.py` | PostgreSQL 持久化层（新增） |
| `callback.py` | Webhook 投递与签名（新增） |
| `schema.sql` | 建表脚本（新增，需手动执行） |
| `config.py` | 新增数据库与回调配置项 |
| `main.py` | 接入落库、回调触发、重启回收 |
| `docs/requirements.txt` | 新增 `asyncpg` |

---

## 步骤 1：安装依赖

```bash
pip install asyncpg>=0.29.0
```

---

## 步骤 2：建库表

在服务器的 PostgreSQL 上执行（脚本可重复执行，不依赖任何扩展，普通业务账号即可）：

```bash
psql -h <host> -U <user> -d <db> -f schema.sql
```

建出三张表：

- `qa_jobs` —— 任务主体（状态、进度、回调配置）
- `qa_job_results` —— 每条通话的质检结果
- `qa_callback_deliveries` —— 回调投递队列（含重试状态）

---

## 步骤 3：填 config.py

数据库部分（**必填，仓库里是占位值**）：

```python
DB_ENABLED = True
DB_HOST = "127.0.0.1"
DB_PORT = 5432
DB_NAME = "qa_svr"
DB_USER = "qa_svr"
DB_PASSWORD = "<真实密码>"
```

回调部分（先留空即可，等接收端就绪再开）：

```python
CALLBACK_SECRET = "<随机长字符串，与接收端共享>"
CALLBACK_ALLOWED_HOSTS = ["<接收端主机名>"]
```

> `CALLBACK_SECRET` 和 `CALLBACK_ALLOWED_HOSTS` 任一为空，回调功能整体关闭，投递 worker 不启动，带 `callback_url` 的提交会返回 400。
>
> 白名单不是可选项：qa-svr 与 Ollama、FunASR、Nginx 录音服务同处内网，放开任意回调地址等于开放内网 SSRF 跳板。

生成 secret：

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(48))"
```

---

## 步骤 4：重启并验证

```bash
systemctl restart qa-svr
```

日志里应看到：

```
PostgreSQL 连接池就绪 | qa_svr@127.0.0.1:5432/qa_svr pool=1-8
回调投递 worker 已启动 | 间隔 2s | 单轮上限 10
```

若数据库连不上，只会打印一条 error 然后继续以纯内存模式运行，**质检功能不受影响**：

```
PostgreSQL 连接失败，持久化关闭，服务继续以纯内存模式运行: ...
```

验证落库：

```bash
# 提交一次质检后
psql -d qa_svr -c "SELECT job_id, status, total, completed, failed FROM qa_jobs ORDER BY created_at DESC LIMIT 5;"
```

验证重启后仍可查询（这是本次改动最主要的收益）：

```bash
# 记下某个已完成任务的 job_id，重启服务后再查，应当仍返回 200 而不是 404
systemctl restart qa-svr
curl -H "X-API-Key: <QA_API_KEY>" http://127.0.0.1:40000/api/task/<job_id>
```

---

## 步骤 5：接收端就绪后开启回调（本仓库 / Supabase 侧）

回调接收端 `ai-quality-inspection-callback` 已实现（USI-217 阶段2），部署好之后按下面顺序打开：

1. 生成的 `CALLBACK_SECRET`（步骤 3）同步设置为 Supabase 密钥，两边必须**逐字符一致**：
   ```bash
   supabase secrets set QA_CALLBACK_SECRET=<与 config.py 里的 CALLBACK_SECRET 相同> --project-ref <project-ref>
   ```
2. 部署接收端函数：
   ```bash
   supabase functions deploy ai-quality-inspection-callback --no-verify-jwt --project-ref <project-ref>
   ```
3. `CALLBACK_ALLOWED_HOSTS` 里加上 Supabase 函数域名 `<project-ref>.supabase.co`，否则 qa-svr 会以「不在白名单」拒绝提交。
4. 确认接收端可达后，再设置 `QA_CALLBACK_URL` 并重新部署 `ai-quality-inspection`（提交请求才会开始带 `callback_url`）：
   ```bash
   supabase secrets set QA_CALLBACK_URL=https://<project-ref>.supabase.co/functions/v1/ai-quality-inspection-callback --project-ref <project-ref>
   supabase functions deploy ai-quality-inspection --no-verify-jwt --project-ref <project-ref>
   ```

`QA_CALLBACK_URL` 未设置时，`ai-quality-inspection` 提交请求不带 `callback_url`，行为和改动前完全一致（纯轮询）——这一步是唯一真正打开回调流量的开关，其余步骤都是为它做准备，建议先做好 1-3 步、观察一段时间持久化是否稳定，再做第 4 步。

---

## 排查

| 现象 | 排查方向 |
|------|----------|
| 任务查不到 / 一直 404 | 看日志有没有「PostgreSQL 连接失败」；`DB_ENABLED` 是否为 True |
| 重启后任务变 failed | 预期行为。进行中的任务随进程消失，会被统一标记 failed 并补发 `job.failed` 回调，调用方需重新提交 |
| 回调收不到 | `SELECT status, attempts, last_status_code, last_error FROM qa_callback_deliveries ORDER BY created_at DESC LIMIT 10;` |
| 提交返回 400 且提示白名单 | `CALLBACK_ALLOWED_HOSTS` 没配，或回调域名不在其中 |
| 接收端验签失败 | 必须对未解析的原始字节验签，且签名内容是 `{timestamp}.{body}`，见 API 文档「7. Webhook 回调」 |

---

## 回滚

改动是向后兼容的，回滚有两档：

1. **只关回调**：`CALLBACK_SECRET = ""`，重启。任务照常落库。
2. **全关**：`DB_ENABLED = False` 且 `CALLBACK_SECRET = ""`，重启。行为与改动前完全一致，建好的表留着不影响。

代码本身无需回退。
