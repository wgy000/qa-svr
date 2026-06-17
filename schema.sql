-- qa-svr 任务持久化与回调投递表结构
-- 执行方式：psql -h <host> -U <user> -d <db> -f schema.sql
-- 可重复执行（全部 IF NOT EXISTS）。
-- 不依赖任何扩展，普通业务账号即可执行；delivery_id 由应用侧生成。

-- ===================== 质检任务 =====================
CREATE TABLE IF NOT EXISTS qa_jobs (
    job_id          UUID        PRIMARY KEY,
    -- submitted / processing / cancelling / cancelled / completed / failed
    status          TEXT        NOT NULL,
    -- upload（上传音频） / remote（单条远程录音） / batch（批量）
    mode            TEXT        NOT NULL DEFAULT 'batch',
    total           INTEGER     NOT NULL DEFAULT 0,
    completed       INTEGER     NOT NULL DEFAULT 0,
    failed          INTEGER     NOT NULL DEFAULT 0,
    callback_url    TEXT,
    callback_events TEXT        NOT NULL DEFAULT 'job',
    file_urls       JSONB       NOT NULL DEFAULT '[]'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_qa_jobs_created_at ON qa_jobs (created_at DESC);
-- 重启回收只关心非终态任务，用部分索引避免扫全表
CREATE INDEX IF NOT EXISTS idx_qa_jobs_active ON qa_jobs (status)
    WHERE status IN ('submitted', 'processing', 'cancelling');

-- ===================== 单条质检结果 =====================
CREATE TABLE IF NOT EXISTS qa_job_results (
    job_id     UUID        NOT NULL REFERENCES qa_jobs (job_id) ON DELETE CASCADE,
    idx        INTEGER     NOT NULL,
    success    BOOLEAN     NOT NULL DEFAULT FALSE,
    callid     TEXT,
    flowno     TEXT,
    score      INTEGER,
    -- 完整质检结果（含 asr_result / conversations / violations 等）
    result     JSONB,
    error      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, idx)
);

CREATE INDEX IF NOT EXISTS idx_qa_job_results_flowno ON qa_job_results (flowno)
    WHERE flowno IS NOT NULL;

-- ===================== 回调投递队列 =====================
CREATE TABLE IF NOT EXISTS qa_callback_deliveries (
    delivery_id      UUID        PRIMARY KEY,
    job_id           UUID        NOT NULL REFERENCES qa_jobs (job_id) ON DELETE CASCADE,
    -- job.completed / job.failed / job.cancelled / item.completed
    event            TEXT        NOT NULL,
    -- job 级事件为 -1；item 级事件为该条记录在 results 中的下标
    item_index       INTEGER     NOT NULL DEFAULT -1,
    callback_url     TEXT        NOT NULL,
    -- pending / sending / delivered / failed
    status           TEXT        NOT NULL DEFAULT 'pending',
    attempts         INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code INTEGER,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 同一任务的同一事件只登记一次，保证重复触发不会重复投递
CREATE UNIQUE INDEX IF NOT EXISTS uq_qa_callback_deliveries_event
    ON qa_callback_deliveries (job_id, event, item_index);
CREATE INDEX IF NOT EXISTS idx_qa_callback_deliveries_due
    ON qa_callback_deliveries (next_attempt_at)
    WHERE status = 'pending';
