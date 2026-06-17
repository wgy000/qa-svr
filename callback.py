"""Webhook 回调投递。

质检任务到达终态后主动把结果 POST 给调用方，替代调用方长时间轮询。
投递记录落在 qa_callback_deliveries 表，进程重启后会继续重试。
"""

import asyncio
import hashlib
import hmac
import ipaddress
import json
import logging
import socket
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
from urllib.parse import urlparse

import requests

import db
from config import (
    CALLBACK_ALLOWED_HOSTS,
    CALLBACK_SECRET,
    CALLBACK_TIMEOUT,
    CALLBACK_MAX_ATTEMPTS,
    CALLBACK_BACKOFF_BASE_SECONDS,
    CALLBACK_BACKOFF_MAX_SECONDS,
    CALLBACK_WORKER_INTERVAL,
    CALLBACK_BATCH_SIZE,
)

logger = logging.getLogger("qa.callback")

_CST = timezone(timedelta(hours=8))
# 投递途中进程被杀会留下 sending 记录，超过这个时间就放回队列重试
_SENDING_STALE_SECONDS = 300


def _check_public_address(host: str) -> Optional[str]:
    """解析主机名并确认它不指向内网/回环/链路本地地址。返回错误说明，None 表示通过。

    解析失败一律按不通过处理（fail-closed），宁可拒绝一次提交，也不放行一个查不清去向的地址。
    """
    try:
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror as e:
        return f"callback_url 主机 {host} 无法解析（{e}）"

    for info in infos:
        try:
            addr = ipaddress.ip_address(info[4][0])
        except ValueError:
            return f"callback_url 主机 {host} 解析结果非法"
        if addr.is_private or addr.is_loopback or addr.is_link_local or addr.is_reserved or addr.is_multicast:
            return f"callback_url 主机 {host} 解析到内网地址 {addr}，已拒绝"
    return None


def validate_callback_url(url: str) -> Optional[str]:
    """校验回调地址。返回错误说明，None 表示通过。

    白名单是硬性要求：qa-svr 与 Ollama、FunASR、Nginx 录音服务同处内网，
    放开任意回调地址等于把它变成内网 SSRF 跳板。
    """
    if not url:
        return "callback_url 为空"
    if not CALLBACK_SECRET:
        return "服务端未配置 CALLBACK_SECRET，回调功能不可用"
    if not CALLBACK_ALLOWED_HOSTS:
        return "服务端未配置 CALLBACK_ALLOWED_HOSTS 白名单，回调功能不可用"

    try:
        parsed = urlparse(url)
    except ValueError:
        return "callback_url 格式非法"

    if parsed.scheme not in ("http", "https"):
        return "callback_url 必须是 http 或 https"

    host = (parsed.hostname or "").lower()
    if not host:
        return "callback_url 缺少主机名"
    if host not in [item.lower() for item in CALLBACK_ALLOWED_HOSTS]:
        return f"callback_url 主机 {host} 不在 CALLBACK_ALLOWED_HOSTS 白名单内"

    return _check_public_address(host)


def _strip_heavy_fields(results: list) -> list:
    """回调载荷里去掉 asr_result。

    它是 FunASR 的完整原始转写，单条就可能上 MB，批量场景会把回调体撑到几十 MB，
    而下游只用 conversations/roles/violations/score。需要原始转写时仍可 GET /api/task/{job_id}。
    """
    trimmed = []
    for item in results:
        if not item:
            continue
        if isinstance(item.get("result"), dict):
            result_obj = {k: v for k, v in item["result"].items() if k != "asr_result"}
            trimmed.append({**item, "result": result_obj})
        else:
            trimmed.append(item)
    return trimmed


def build_payload(delivery: dict, task: dict) -> dict:
    results = _strip_heavy_fields(task.get("results", []))
    item_index = delivery.get("item_index", -1)
    if item_index >= 0:
        results = [item for item in results if item.get("index") == item_index]

    return {
        "delivery_id": delivery["delivery_id"],
        "event": delivery["event"],
        "job_id": delivery["job_id"],
        "status": task.get("status", ""),
        "total": task.get("total", 0),
        "completed": task.get("completed", 0),
        "failed": task.get("failed", 0),
        "results": results,
        "created_at": task.get("created_at", ""),
        "updated_at": task.get("updated_at", ""),
        "sent_at": datetime.now(_CST).isoformat(),
    }


def sign_body(body: bytes, timestamp: str) -> str:
    mac = hmac.new(CALLBACK_SECRET.encode(), f"{timestamp}.".encode() + body, hashlib.sha256)
    return f"sha256={mac.hexdigest()}"


def _post_callback(url: str, body: bytes, headers: dict) -> tuple:
    """同步发起投递，返回 (状态码, 错误说明)。"""
    try:
        response = requests.post(url, data=body, headers=headers, timeout=CALLBACK_TIMEOUT)
        return response.status_code, "" if response.ok else response.text[:500]
    except requests.exceptions.RequestException as e:
        return None, str(e)


def _next_attempt_at(attempts_done: int) -> Optional[datetime]:
    """指数退避。已达最大次数返回 None，表示放弃重试。"""
    if attempts_done >= CALLBACK_MAX_ATTEMPTS:
        return None
    delay = min(CALLBACK_BACKOFF_BASE_SECONDS * (2 ** attempts_done), CALLBACK_BACKOFF_MAX_SECONDS)
    return datetime.now(_CST) + timedelta(seconds=delay)


async def _deliver_one(delivery: dict, load_task: Callable, executor):
    job_id = delivery["job_id"]
    delivery_id = delivery["delivery_id"]
    attempts_done = delivery["attempts"]

    task = await load_task(job_id)
    if task is None:
        await db.mark_delivery_result(delivery_id, False, None, "任务数据已不存在，放弃投递", None)
        logger.error(f"[{job_id}] 回调放弃：任务数据已不存在")
        return

    payload = build_payload(delivery, task)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-QA-Event": delivery["event"],
        "X-QA-Job-Id": job_id,
        "X-QA-Delivery-Id": delivery_id,
        "X-QA-Timestamp": timestamp,
        "X-QA-Signature": sign_body(body, timestamp),
    }

    loop = asyncio.get_event_loop()
    status_code, error = await loop.run_in_executor(
        executor, _post_callback, delivery["callback_url"], body, headers
    )

    delivered = status_code is not None and 200 <= status_code < 300
    if delivered:
        await db.mark_delivery_result(delivery_id, True, status_code, "", None)
        logger.info(f"[{job_id}] 回调投递成功 | {delivery['event']} | {len(body)} bytes | 第{attempts_done + 1}次")
        return

    next_at = _next_attempt_at(attempts_done + 1)
    await db.mark_delivery_result(delivery_id, False, status_code, error, next_at)
    if next_at is None:
        logger.error(f"[{job_id}] 回调投递失败且已达最大重试次数 | code={status_code} | {error}")
    else:
        logger.warning(f"[{job_id}] 回调投递失败，将重试 | code={status_code} | {error}")


async def worker_loop(load_task: Callable, executor):
    """轮询投递队列。与质检主链路完全解耦，投递失败不影响质检结果。"""
    if not CALLBACK_SECRET or not CALLBACK_ALLOWED_HOSTS:
        logger.warning("CALLBACK_SECRET 或 CALLBACK_ALLOWED_HOSTS 未配置，回调投递worker 不启动")
        return

    logger.info(f"回调投递 worker 已启动 | 间隔 {CALLBACK_WORKER_INTERVAL}s | 单轮上限 {CALLBACK_BATCH_SIZE}")
    while True:
        try:
            await db.requeue_stale_sending(_SENDING_STALE_SECONDS)
            deliveries = await db.claim_due_deliveries(CALLBACK_BATCH_SIZE)
            if deliveries:
                await asyncio.gather(
                    *[_deliver_one(item, load_task, executor) for item in deliveries]
                )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"回调 worker 异常: {e}")
        await asyncio.sleep(CALLBACK_WORKER_INTERVAL)
