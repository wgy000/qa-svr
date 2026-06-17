"""PostgreSQL 持久化层。

设计原则：数据库是「旁路」，不是质检主链路的依赖。
任何一次 DB 调用失败都只记录日志、不抛给调用方，质检流程照常跑完；
DB_ENABLED=False 时所有函数变成 no-op，行为与未接入数据库时完全一致。
"""

import json
import logging
import uuid as uuid_lib
from datetime import datetime, timedelta, timezone
from typing import Optional

from config import (
    DB_ENABLED,
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
    DB_POOL_MIN,
    DB_POOL_MAX,
    DB_CONNECT_TIMEOUT,
    JOB_RETENTION_DAYS,
)

logger = logging.getLogger("qa.db")

_pool = None


def is_enabled() -> bool:
    return _pool is not None


def _uuid(value: str) -> uuid_lib.UUID:
    """UUID 列显式传 uuid 对象，避免依赖驱动对字符串的隐式转换。"""
    return value if isinstance(value, uuid_lib.UUID) else uuid_lib.UUID(str(value))


def _parse_iso(value) -> Optional[datetime]:
    """把 task_storage 里的 isoformat 字符串转成 aware datetime（服务器本地时区 UTC+8）。"""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone(timedelta(hours=8)))
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone(timedelta(hours=8)))


async def init_pool():
    """建立连接池。失败不阻断服务启动，只是退回纯内存模式。"""
    global _pool
    if not DB_ENABLED:
        logger.info("DB_ENABLED=False，跳过 PostgreSQL 初始化（纯内存模式）")
        return

    try:
        import asyncpg
    except ImportError:
        logger.error("asyncpg 未安装，持久化关闭（pip install asyncpg）")
        return

    try:
        _pool = await asyncpg.create_pool(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            min_size=DB_POOL_MIN,
            max_size=DB_POOL_MAX,
            timeout=DB_CONNECT_TIMEOUT,
            command_timeout=DB_CONNECT_TIMEOUT,
        )
        logger.info(f"PostgreSQL 连接池就绪 | {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME} pool={DB_POOL_MIN}-{DB_POOL_MAX}")
    except Exception as e:
        _pool = None
        logger.error(f"PostgreSQL 连接失败，持久化关闭，服务继续以纯内存模式运行: {e}")


async def close_pool():
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL 连接池已关闭")


async def save_job(job_id: str, task: dict, callback_url: str = "", callback_events: str = "", mode: str = "") -> bool:
    """整体落库一个 job（含全部 results）。幂等，可反复调用。

    返回是否真正写入成功——调用方要靠它判断内存数据能不能安全丢弃。
    """
    if _pool is None:
        return False
    try:
        created_at = _parse_iso(task.get("created_at")) or datetime.now(timezone(timedelta(hours=8)))
        updated_at = _parse_iso(task.get("updated_at")) or created_at
        results = [item for item in task.get("results", []) if item]
        job_uuid = _uuid(job_id)

        async with _pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO qa_jobs (job_id, status, mode, total, completed, failed,
                                         callback_url, callback_events, file_urls, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                    ON CONFLICT (job_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        total = EXCLUDED.total,
                        completed = EXCLUDED.completed,
                        failed = EXCLUDED.failed,
                        updated_at = EXCLUDED.updated_at
                    """,
                    job_uuid,
                    task.get("status", "submitted"),
                    mode or task.get("mode", "batch"),
                    int(task.get("total", 0)),
                    int(task.get("completed", 0)),
                    int(task.get("failed", 0)),
                    callback_url or task.get("callback_url") or None,
                    callback_events or task.get("callback_events") or "job",
                    json.dumps(task.get("file_urls", []), ensure_ascii=False),
                    created_at,
                    updated_at,
                )

                for item in results:
                    result_obj = item.get("result")
                    await conn.execute(
                        """
                        INSERT INTO qa_job_results (job_id, idx, success, callid, flowno, score, result, error)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        ON CONFLICT (job_id, idx) DO UPDATE SET
                            success = EXCLUDED.success,
                            callid = EXCLUDED.callid,
                            flowno = EXCLUDED.flowno,
                            score = EXCLUDED.score,
                            result = EXCLUDED.result,
                            error = EXCLUDED.error
                        """,
                        job_uuid,
                        int(item.get("index", 0)),
                        bool(item.get("success", False)),
                        item.get("callid"),
                        item.get("flowNo") or None,
                        int(result_obj.get("score", 0)) if isinstance(result_obj, dict) else None,
                        json.dumps(result_obj, ensure_ascii=False) if result_obj is not None else None,
                        item.get("error"),
                    )
        return True
    except Exception as e:
        logger.error(f"[{job_id}] 落库失败: {e}")
        return False


async def load_job(job_id: str) -> Optional[dict]:
    """按 job_id 读回任务，形状与 task_storage 中的条目一致。服务重启后靠它兜底。"""
    if _pool is None:
        return None
    try:
        job_uuid = _uuid(job_id)
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT status, total, completed, failed, callback_url, callback_events, "
                "file_urls, created_at, updated_at FROM qa_jobs WHERE job_id = $1",
                job_uuid,
            )
            if row is None:
                return None

            result_rows = await conn.fetch(
                "SELECT idx, success, callid, flowno, result, error "
                "FROM qa_job_results WHERE job_id = $1 ORDER BY idx",
                job_uuid,
            )

        results = []
        for item in result_rows:
            entry = {
                "index": item["idx"],
                "success": item["success"],
                "callid": item["callid"],
                "flowNo": item["flowno"] or "",
            }
            if item["result"] is not None:
                entry["result"] = json.loads(item["result"])
            if item["error"]:
                entry["error"] = item["error"]
            results.append(entry)

        return {
            "status": row["status"],
            "total": row["total"],
            "completed": row["completed"],
            "failed": row["failed"],
            "callback_url": row["callback_url"] or "",
            "callback_events": row["callback_events"] or "job",
            "file_urls": json.loads(row["file_urls"]) if row["file_urls"] else [],
            "results": results,
            "created_at": row["created_at"].isoformat(),
            "updated_at": row["updated_at"].isoformat(),
        }
    except Exception as e:
        logger.error(f"[{job_id}] 读库失败: {e}")
        return None


async def list_jobs(limit: int = 10) -> Optional[list]:
    if _pool is None:
        return None
    try:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT job_id, status, total, completed, failed, created_at, updated_at "
                "FROM qa_jobs ORDER BY created_at DESC LIMIT $1",
                limit,
            )
        return [
            {
                "job_id": str(row["job_id"]),
                "status": row["status"],
                "total": row["total"],
                "completed": row["completed"],
                "failed": row["failed"],
                "created_at": row["created_at"].isoformat(),
                "updated_at": row["updated_at"].isoformat(),
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"列出任务失败: {e}")
        return None


async def recover_stale_jobs() -> list:
    """把上次进程退出时卡在非终态的任务标记为 failed。

    这些任务的 asyncio task 已随进程消失，不会再有人推进它们；
    不落终态的话前端会永远轮询一个不再变化的 processing 状态。
    返回被回收的 job 列表，交给上层补发回调。
    """
    if _pool is None:
        return []
    try:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(
                """
                UPDATE qa_jobs
                SET status = 'failed', updated_at = now()
                WHERE status IN ('submitted', 'processing', 'cancelling')
                RETURNING job_id, callback_url, callback_events
                """
            )
        if rows:
            logger.warning(f"服务重启回收了 {len(rows)} 个中断任务，已标记为 failed")
        return [
            {
                "job_id": str(row["job_id"]),
                "callback_url": row["callback_url"] or "",
                "callback_events": row["callback_events"] or "job",
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"回收中断任务失败: {e}")
        return []


async def purge_expired_jobs():
    """清理超过保留期的任务，避免表无限增长。"""
    if _pool is None:
        return
    try:
        cutoff = datetime.now(timezone(timedelta(hours=8))) - timedelta(days=JOB_RETENTION_DAYS)
        async with _pool.acquire() as conn:
            deleted = await conn.execute("DELETE FROM qa_jobs WHERE created_at < $1", cutoff)
        logger.info(f"清理过期任务: {deleted}")
    except Exception as e:
        logger.error(f"清理过期任务失败: {e}")


# ===================== 回调投递队列 =====================

async def enqueue_delivery(job_id: str, event: str, callback_url: str, item_index: int = -1) -> Optional[str]:
    """登记一次待投递的回调。同一 (job_id, event, item_index) 只会有一条记录，天然幂等。"""
    if _pool is None or not callback_url:
        return None
    try:
        delivery_id = uuid_lib.uuid4()
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO qa_callback_deliveries (delivery_id, job_id, event, item_index, callback_url)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (job_id, event, item_index) DO NOTHING
                RETURNING delivery_id
                """,
                delivery_id,
                _uuid(job_id),
                event,
                item_index,
                callback_url,
            )
        return str(row["delivery_id"]) if row else None
    except Exception as e:
        logger.error(f"[{job_id}] 登记回调失败: {e}")
        return None


async def claim_due_deliveries(limit: int) -> list:
    """取出到期的待投递记录并置为 sending，避免重复投递。"""
    if _pool is None:
        return []
    try:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(
                """
                UPDATE qa_callback_deliveries SET status = 'sending', updated_at = now()
                WHERE delivery_id IN (
                    SELECT delivery_id FROM qa_callback_deliveries
                    WHERE status = 'pending' AND next_attempt_at <= now()
                    ORDER BY next_attempt_at
                    LIMIT $1
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING delivery_id, job_id, event, item_index, callback_url, attempts
                """,
                limit,
            )
        return [
            {
                "delivery_id": str(row["delivery_id"]),
                "job_id": str(row["job_id"]),
                "event": row["event"],
                "item_index": row["item_index"],
                "callback_url": row["callback_url"],
                "attempts": row["attempts"],
            }
            for row in rows
        ]
    except Exception as e:
        logger.error(f"认领待投递回调失败: {e}")
        return []


async def mark_delivery_result(
    delivery_id: str,
    delivered: bool,
    status_code: Optional[int],
    error: str,
    next_attempt_at: Optional[datetime],
):
    """记录一次投递结果。next_attempt_at 为空表示不再重试。"""
    if _pool is None:
        return
    try:
        if delivered:
            status = "delivered"
        elif next_attempt_at is not None:
            status = "pending"
        else:
            status = "failed"

        async with _pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE qa_callback_deliveries
                SET status = $2, attempts = attempts + 1, last_status_code = $3,
                    last_error = $4, next_attempt_at = COALESCE($5, next_attempt_at), updated_at = now()
                WHERE delivery_id = $1
                """,
                _uuid(delivery_id),
                status,
                status_code,
                (error or "")[:1000] or None,
                next_attempt_at,
            )
    except Exception as e:
        logger.error(f"[{delivery_id}] 更新投递结果失败: {e}")


async def requeue_stale_sending(older_than_seconds: int):
    """把卡在 sending 的记录放回队列（进程在投递途中被杀会留下这种记录）。"""
    if _pool is None:
        return
    try:
        cutoff = datetime.now(timezone(timedelta(hours=8))) - timedelta(seconds=older_than_seconds)
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE qa_callback_deliveries
                SET status = 'pending', updated_at = now()
                WHERE status = 'sending' AND updated_at < $1
                """,
                cutoff,
            )
    except Exception as e:
        logger.error(f"回收卡住的投递记录失败: {e}")
