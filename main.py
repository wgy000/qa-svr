import sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, File, UploadFile, Form, Request, Depends
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel
from typing import Dict, List, Optional
from datetime import datetime, timedelta, timezone
import requests
import json
import logging
import logging.handlers
import os
import shutil
import asyncio
import time
import gc
import uuid
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import psutil

from config import OLLAMA_BASE_URL, OLLAMA_MODEL, API_PORT, API_HOST, FUNASR_URL, FUNASR_CONCURRENCY, OLLAMA_CONCURRENCY, BATCH_CONCURRENCY, API_KEY, AUDIO_HTTP_HOST, AUDIO_HTTP_PORT, NGINX_AUDIO_HOST, NGINX_AUDIO_PORT, NGINX_SERVERS, LOG_DIR, WAV_FILE_DIR, AUDIO_CONVERT_BIN, GLITCHTIP_DSN, GLITCHTIP_ENVIRONMENT, GLITCHTIP_RELEASE, GLITCHTIP_TRACES_SAMPLE_RATE, GLITCHTIP_AUTO_SESSION_TRACKING, GLITCHTIP_ENABLE_LOGS, GLITCHTIP_SEND_DEFAULT_PII, MEMORY_RETENTION_SECONDS, FLUSH_INTERVAL_SECONDS

import db
import callback

MAX_MEMORY_MB = 2048
WORKER_PROCESSES = 4


def setup_logging():
    """初始化日志配置"""
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        log_file = os.path.join(LOG_DIR, "qa-svr.log")
        
        file_handler = logging.handlers.TimedRotatingFileHandler(
            log_file,
            when='midnight',
            interval=1,
            backupCount=90,
            encoding='utf-8'
        )
        file_handler.suffix = "%Y-%m-%d"
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(file_handler)
        
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.INFO)
        stderr_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root_logger.addHandler(stderr_handler)
        
        return logging.getLogger("qa.content"), logging.getLogger("qa.perf")
    except Exception as e:
        print(f"日志初始化失败: {e}", file=sys.stderr)
        logging.basicConfig(level=logging.DEBUG)
        return logging.getLogger("qa.content"), logging.getLogger("qa.perf")
    finally:
        # 关闭 requests 库的 HTTP 连接日志
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)


logger, perf_logger = setup_logging()


def setup_sentry():
    """初始化 GlitchTip（Sentry 协议）SDK，把 logger 错误事件统一上报"""
    if not GLITCHTIP_DSN:
        logger.info("GlitchTip DSN 未配置，跳过 SDK 初始化")
        return
    try:
        import sentry_sdk
        from sentry_sdk.integrations.logging import LoggingIntegration
    except ImportError:
        logger.warning("sentry-sdk 未安装，跳过 GlitchTip 集成（pip install 'sentry-sdk[fastapi]'）")
        return

    try:
        sentry_sdk.init(
            dsn=GLITCHTIP_DSN,
            environment=GLITCHTIP_ENVIRONMENT,
            release=GLITCHTIP_RELEASE,
            traces_sample_rate=GLITCHTIP_TRACES_SAMPLE_RATE,
            auto_session_tracking=GLITCHTIP_AUTO_SESSION_TRACKING,
            send_default_pii=GLITCHTIP_SEND_DEFAULT_PII,
            enable_logs=GLITCHTIP_ENABLE_LOGS,
            integrations=[
                LoggingIntegration(
                    level=logging.INFO,              # INFO 及以上写入 breadcrumb
                    event_level=logging.ERROR,       # ERROR 级别触发 Sentry 事件（issue）
                    sentry_logs_level=logging.INFO,  # INFO 及以上作为 Sentry Logs 发送
                ),
            ],
            # 默认已自动集成 fastapi + starlette + stdlib
        )
        logger.info(f"GlitchTip 初始化完成 | env={GLITCHTIP_ENVIRONMENT} release={GLITCHTIP_RELEASE} traces={GLITCHTIP_TRACES_SAMPLE_RATE} logs={GLITCHTIP_ENABLE_LOGS}")
    except Exception as e:
        logger.error(f"GlitchTip 初始化失败: {e}")


setup_sentry()

process_pool = ProcessPoolExecutor(max_workers=WORKER_PROCESSES)
thread_pool = ThreadPoolExecutor(max_workers=WORKER_PROCESSES * 4)
# /api/quality-search 的下载+转码使用独立信号量，不与批量质检提交共享
quality_search_semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)

task_storage = {}
cancelled_jobs: set = set()  # 已取消的job_id集合
job_meta: dict = {}          # job_id -> {callback_url, callback_events, mode}
_flushed_at: dict = {}       # job_id -> 上次落库时的 updated_at，用于识别脏数据
_terminal_since: dict = {}   # job_id -> 进入终态的时间戳，用于内存淘汰
# 正在处理中的 flowNo -> job_id，用于拦截同一录音的重复提交
flowno_inflight: Dict[str, str] = {}
# 任务结束后的冷却期，覆盖结果落库与回调投递的时间窗口
FLOWNO_DEDUP_COOLDOWN_SECONDS = 60
flowno_cooldown: Dict[str, tuple] = {}  # flowNo -> (job_id, cooldown_until_epoch)


def release_flowno(flowno: str, job_id: str):
    """任务对某个 flowNo 的处理结束（成功/失败/取消）：转入冷却期而不是直接放行。"""
    flowno_inflight.pop(flowno, None)
    flowno_cooldown[flowno] = (job_id, time.time() + FLOWNO_DEDUP_COOLDOWN_SECONDS)

TERMINAL_STATUSES = ("completed", "failed", "cancelled")


class TaskStatus(BaseModel):
    job_id: str
    status: str
    total: int
    completed: int
    failed: int
    results: List[dict]
    created_at: str
    updated_at: str


async def load_task(job_id: str) -> Optional[dict]:
    """读取任务：内存优先，未命中时回落数据库（服务重启后靠它把历史任务查回来）。"""
    task = task_storage.get(job_id)
    if task is not None:
        return task
    return await db.load_job(job_id)


def register_job(job_id: str, mode: str, callback_url: Optional[str], callback_events: Optional[str]):
    job_meta[job_id] = {
        "mode": mode,
        "callback_url": (callback_url or "").strip(),
        "callback_events": (callback_events or "job").strip(),
    }


async def flush_job(job_id: str):
    """把某个任务的当前内存快照写入数据库。

    只有确认写入成功才记 _flushed_at，否则一次静默失败的写会让内存副本被误判成
    「已持久化」而被淘汰，数据就凭空消失了。
    """
    task = task_storage.get(job_id)
    if task is None:
        return
    meta = job_meta.get(job_id, {})
    saved = await db.save_job(
        job_id,
        task,
        meta.get("callback_url", ""),
        meta.get("callback_events", "job"),
        meta.get("mode", ""),
    )
    if saved:
        _flushed_at[job_id] = task.get("updated_at", "")


async def on_job_finished(job_id: str):
    """任务到达终态：立即落库并登记 job 级回调。"""
    await flush_job(job_id)
    task = task_storage.get(job_id)
    meta = job_meta.get(job_id, {})
    callback_url = meta.get("callback_url", "")
    if task is None or not callback_url:
        return
    await db.enqueue_delivery(job_id, f"job.{task['status']}", callback_url)


async def on_item_finished(job_id: str, idx: int):
    """批量任务中单条记录出结果：按需登记 item 级回调，让调用方能看到实时进度。"""
    meta = job_meta.get(job_id, {})
    callback_url = meta.get("callback_url", "")
    if not callback_url or "item" not in meta.get("callback_events", "job"):
        return
    await db.enqueue_delivery(job_id, "item.completed", callback_url, idx)


async def run_job(coro, job_id: str):
    """统一包住质检任务的执行。

    process_* 内部有多个取消分支会提前 return，用 finally 兜住所有出口，
    保证任务无论怎么结束都会落库并触发一次终态回调。
    """
    try:
        await coro
    except Exception as e:
        logger.error(f"[{job_id}] 任务执行异常: {e}")
    finally:
        await on_job_finished(job_id)


async def periodic_flush():
    """定时把内存中变动过的任务写库，并淘汰已落库的老任务，防止内存无限增长。"""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_SECONDS)
        try:
            now = time.time()
            for job_id, task in list(task_storage.items()):
                updated_at = task.get("updated_at", "")
                if _flushed_at.get(job_id) != updated_at:
                    await flush_job(job_id)

                if task.get("status") not in TERMINAL_STATUSES:
                    continue
                _terminal_since.setdefault(job_id, now)
                # 只有确认已落库才敢从内存丢弃，否则读取会失去兜底
                if not db.is_enabled() or _flushed_at.get(job_id) != updated_at:
                    continue
                if now - _terminal_since[job_id] < MEMORY_RETENTION_SECONDS:
                    continue
                task_storage.pop(job_id, None)
                job_meta.pop(job_id, None)
                _flushed_at.pop(job_id, None)
                _terminal_since.pop(job_id, None)
                cancelled_jobs.discard(job_id)
        except Exception as e:
            logger.error(f"定时落库异常: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(WAV_FILE_DIR, exist_ok=True)
    await db.init_pool()

    # 上次进程退出时卡在非终态的任务已经没人推进了，标记失败并补发回调，
    # 否则调用方会永远轮询一个不再变化的 processing 状态
    for stale in await db.recover_stale_jobs():
        if stale["callback_url"]:
            await db.enqueue_delivery(stale["job_id"], "job.failed", stale["callback_url"])

    asyncio.create_task(periodic_cleanup())
    asyncio.create_task(periodic_flush())
    asyncio.create_task(callback.worker_loop(load_task, thread_pool))
    logger.info(f"音频存储目录: {WAV_FILE_DIR}")
    logger.info(f"工作进程数: {WORKER_PROCESSES}, 内存限制: {MAX_MEMORY_MB}MB")
    yield
    logger.info("正在关闭服务，清理资源...")
    process_pool.shutdown(wait=True)
    thread_pool.shutdown(wait=True)
    await db.close_pool()
    logger.info("资源已清理")


api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Depends(api_key_header)):
    if not API_KEY:
        return True
    if api_key and api_key == API_KEY:
        return True
    raise HTTPException(status_code=401, detail="Invalid or missing API Key")


app = FastAPI(
    title="呼叫中心话术质检系统",
    description="集成FunASR和Ollama的一站式质检服务",
    version="1.0.0",
    lifespan=lifespan
)


def is_cancelled(job_id: str) -> bool:
    """检查任务是否已被取消"""
    return job_id in cancelled_jobs


@app.post("/api/job/{job_id}/cancel", summary="取消质检任务")
async def cancel_job(job_id: str, _: bool = Depends(verify_api_key)):
    """取消指定的质检任务，已在进行中的子任务完成当前步骤后停止，不再存储后续结果"""
    if job_id not in task_storage:
        # 内存里没有说明任务已结束并被淘汰，或进程重启过——查库给出准确状态，而不是笼统的 404
        persisted = await db.load_job(job_id)
        if persisted is None:
            raise HTTPException(status_code=404, detail="任务不存在")
        return {"job_id": job_id, "status": persisted["status"], "msg": "任务已结束，无法取消"}

    status = task_storage[job_id]["status"]
    if status == "cancelled":
        return {"job_id": job_id, "status": "cancelled", "msg": "任务已处于取消状态"}
    if status == "completed":
        return {"job_id": job_id, "status": "completed", "msg": "任务已完成，无法取消"}
    if status == "submitted":
        task_storage[job_id]["status"] = "cancelled"
        task_storage[job_id]["updated_at"] = datetime.now().isoformat()
        cancelled_jobs.add(job_id)
        logger.info(f"[{job_id}] 任务已取消（submitted阶段）")
        return {"job_id": job_id, "status": "cancelled", "msg": "任务已取消"}

    # processing / cancelling 状态：标记取消，子任务在下一个检查点退出
    cancelled_jobs.add(job_id)
    task_storage[job_id]["status"] = "cancelling"
    task_storage[job_id]["updated_at"] = datetime.now().isoformat()
    logger.info(f"[{job_id}] 收到取消请求，进行中的子任务将在当前步骤完成后停止")
    return {"job_id": job_id, "status": "cancelling", "msg": "正在取消"}


os.makedirs(WAV_FILE_DIR, exist_ok=True)

OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"

funasr_semaphore = asyncio.Semaphore(FUNASR_CONCURRENCY)
ollama_semaphore = asyncio.Semaphore(OLLAMA_CONCURRENCY)


def convert_to_http_url(local_path: str) -> str:
    """将本地文件路径转换为HTTP URL"""
    if not local_path or not os.path.exists(local_path):
        return ""
    # 提取相对于WAV_FILE_DIR的路径部分
    if local_path.startswith(WAV_FILE_DIR):
        relative_path = local_path[len(WAV_FILE_DIR):].lstrip('/')
    else:
        relative_path = os.path.basename(local_path)
    return f"http://{AUDIO_HTTP_HOST}:{AUDIO_HTTP_PORT}/audio/{relative_path}"


def get_memory_usage_mb() -> float:
    process = psutil.Process()
    return process.memory_info().rss / 1024 / 1024


def check_memory_and_cleanup():
    memory_mb = get_memory_usage_mb()
    if memory_mb > MAX_MEMORY_MB:
        perf_logger.warning(f"内存使用过高: {memory_mb:.1f}MB > {MAX_MEMORY_MB}MB，强制GC")
        gc.collect()
        memory_after = get_memory_usage_mb()
        perf_logger.info(f"GC后内存: {memory_after:.1f}MB")
    return memory_mb


def save_audio_file(audio_data: bytes, filename: str) -> str:
    try:
        os.makedirs(WAV_FILE_DIR, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        save_dir = os.path.join(WAV_FILE_DIR, date_str)
        os.makedirs(save_dir, exist_ok=True)
        
        ext = os.path.splitext(filename)[1] if '.' in filename else '.wav'
        name_without_ext = os.path.splitext(filename)[0] if '.' in filename else filename
        save_filename = f"{name_without_ext}_{int(time.time() * 1000)}{ext}"
        save_path = os.path.join(save_dir, save_filename)
        
        with open(save_path, 'wb') as f:
            f.write(audio_data)
        return save_path
    except Exception as e:
        logger.error(f"保存音频文件失败: {e}")
        return ""


def cleanup_old_files():
    try:
        if not os.path.exists(WAV_FILE_DIR):
            return
        
        cutoff_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
        
        for item in os.listdir(WAV_FILE_DIR):
            item_path = os.path.join(WAV_FILE_DIR, item)
            if os.path.isdir(item_path) and item < cutoff_date:
                shutil.rmtree(item_path)
                logger.info(f"已清理 {item} 的音频文件")
    except Exception as e:
        logger.error(f"清理音频文件失败: {e}")


async def periodic_cleanup():
    while True:
        cleanup_old_files()
        await db.purge_expired_jobs()
        # 清掉已过期的 flowNo 去重冷却记录，避免常年运行后 flowno_cooldown 无限增长
        now = time.time()
        expired = [flowno for flowno, (_, until) in flowno_cooldown.items() if now >= until]
        for flowno in expired:
            flowno_cooldown.pop(flowno, None)
        memory_mb = check_memory_and_cleanup()
        perf_logger.info(f"定时检查 - 内存使用: {memory_mb:.1f}MB")
        await asyncio.sleep(3600)

def _probe_one_nginx(base_url: str, date_str: str, hour_str: str, flowNo: str, job_id: str = "") -> str:
    """在单个 Nginx 节点上查找录音文件，返回文件完整 URL，未找到返回空串"""
    import urllib.parse
    import re

    dir_url = f"{base_url}/{date_str}/{hour_str}/"
    idx_tag = f"[{job_id}][{flowNo}]"

    try:
        response = requests.get(dir_url, timeout=8)
        if response.status_code == 200:
            files = re.findall(r'href="([^"]+\.rec)"', response.text)
            files += re.findall(r'<a[^>]*>([^<]+\.rec)</a>', response.text)
            files += re.findall(r'([\w\-\.]+\.rec)', response.text)
            files += re.findall(r'href="([^"]+\.wav)"', response.text)
            files += re.findall(r'<a[^>]*>([^<]+\.wav)</a>', response.text)
            files += re.findall(r'([\w\-\.]+\.wav)', response.text)

            for filename in files:
                if flowNo in filename and (filename.endswith('.rec') or filename.endswith('.wav')):
                    file_url = f"{dir_url}{urllib.parse.quote(filename)}"
                    logger.debug(f"{idx_tag} 在 {base_url} 找到录音文件: {file_url}")
                    return file_url
    except requests.exceptions.RequestException as e:
        logger.debug(f"{idx_tag} 节点 {base_url} 请求失败: {e}")
    except Exception as e:
        logger.debug(f"{idx_tag} 节点 {base_url} 异常: {e}")

    return ""


def find_remote_audio_file(start_ms: int, flowNo: str, job_id: str = "") -> str:
    """从多个 Nginx 节点并发查找录音文件，返回第一个命中的 URL"""
    tz = timezone(timedelta(hours=8))
    dt = datetime.fromtimestamp(start_ms / 1000, tz=tz)

    date_str = dt.strftime("%Y%m%d")
    hour_str = dt.strftime("%H")
    idx_tag = f"[{job_id}][{flowNo}]"

    if not NGINX_SERVERS:
        logger.warning(f"{idx_tag} 未配置 Nginx 服务器")
        return ""

    # 单节点直接查，无需并发
    if len(NGINX_SERVERS) == 1:
        srv = NGINX_SERVERS[0]
        base_url = f"http://{srv['host']}:{srv['port']}"
        result = _probe_one_nginx(base_url, date_str, hour_str, flowNo, job_id)
        if not result:
            logger.warning(f"{idx_tag} Nginx未找到录音文件")
        return result

    # 多节点：用 ThreadPoolExecutor 并发探测
    logger.debug(f"{idx_tag} 多节点并发查找录音，共 {len(NGINX_SERVERS)} 个 Nginx")

    futures = []
    with ThreadPoolExecutor(max_workers=len(NGINX_SERVERS)) as executor:
        for srv in NGINX_SERVERS:
            base_url = f"http://{srv['host']}:{srv['port']}"
            future = executor.submit(_probe_one_nginx, base_url, date_str, hour_str, flowNo, job_id)
            futures.append((srv, future))

    # 按提交顺序收集结果，第一个命中的直接返回
    for srv, future in futures:
        try:
            result = future.result(timeout=10)
            if result:
                logger.info(f"{idx_tag} 在节点 {srv['host']}:{srv['port']} 找到录音文件")
                return result
        except Exception as e:
            logger.debug(f"{idx_tag} 节点 {srv['host']}:{srv['port']} 结果异常: {e}")

    logger.warning(f"{idx_tag} 所有 Nginx 节点均未找到录音文件")
    return ""


def convert_to_external_url(internal_url: str) -> str:
    """将内网URL转换为外网URL（用于返回给调用方），支持多 Nginx 节点"""
    if not internal_url:
        return ""
    external_host = f"{AUDIO_HTTP_HOST}:{AUDIO_HTTP_PORT}"
    # 遍历所有 Nginx 节点，匹配到哪个就替换哪个
    for srv in NGINX_SERVERS:
        internal_host = f"{srv['host']}:{srv['port']}"
        if internal_host in internal_url:
            return internal_url.replace(internal_host, external_host)
    # 兜底：用旧配置兼容
    internal_host = f"{NGINX_AUDIO_HOST}:{NGINX_AUDIO_PORT}"
    return internal_url.replace(internal_host, external_host)


def get_recording_url(start_ms: int, flowNo: str, job_id: str = "") -> str:
    """通过Nginx获取录音文件URL"""
    return find_remote_audio_file(start_ms, flowNo, job_id)


def download_audio_from_url(audio_url: str, flowNo: str, job_id: str = "") -> str:
    """从URL下载音频文件并转换为wav格式"""
    if not audio_url:
        raise Exception("录音URL为空")
    
    date_str = datetime.now().strftime("%Y%m%d")
    save_dir = os.path.join(WAV_FILE_DIR, date_str)
    os.makedirs(save_dir, exist_ok=True)
    
    is_rec = audio_url.endswith('.rec')
    
    if is_rec:
        rec_filename = f"{flowNo}.rec"
        wav_filename = f"{flowNo}.wav"
        rec_path = os.path.join(save_dir, rec_filename)
        wav_path = os.path.join(save_dir, wav_filename)
    else:
        wav_filename = f"{flowNo}.wav"
        wav_path = os.path.join(save_dir, wav_filename)
        rec_path = ""
    
    idx_tag = f"[{job_id}][{flowNo}]"

    # 同一 flowNo 当天已下载转码过则直接复用，避免重复下载 + 转码
    if os.path.exists(wav_path):
        logger.debug(f"{idx_tag} 命中本地缓存，跳过下载转码: {wav_path}")
        return wav_path

    logger.debug(f"{idx_tag} 下载录音: {audio_url}")

    try:
        response = requests.get(audio_url, timeout=60)
        if response.status_code != 200:
            raise Exception(f"下载录音失败, HTTP: {response.status_code}")
        
        if is_rec:
            with open(rec_path, 'wb') as f:
                f.write(response.content)
            logger.debug(f"{idx_tag} rec文件已保存: {rec_path}")
            
            import subprocess
            convert_cmd = f"{AUDIO_CONVERT_BIN} -t wav -b 64 -i {rec_path} -o {wav_path}"
            result = subprocess.run(convert_cmd, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"音频转换失败: {result.stderr}")
            
            if rec_path and os.path.exists(rec_path):
                os.remove(rec_path)
            return wav_path
        else:
            with open(wav_path, 'wb') as f:
                f.write(response.content)
            return wav_path
            
    except requests.exceptions.RequestException as e:
        raise Exception(f"下载录音文件失败: {e}")


class CallMetadata(BaseModel):
    extra: dict = {}


class ASRSegment(BaseModel):
    text: str
    spk: str
    start: int
    end: int


class ViolationItem(BaseModel):
    description: str
    segment: Optional[str] = None


class RoleInfo(BaseModel):
    speaker_id: str
    role: str


class ConversationItem(BaseModel):
    speaker_id: str
    speaker_role: str
    text: str
    start: int
    end: int


class QAResponse(BaseModel):
    is_compliant: bool
    score: int
    summary: str
    violations: List[ViolationItem]
    roles: List[RoleInfo]
    conversations: List[ConversationItem]
    metadata: Optional[CallMetadata]
    asr_result: Optional[dict]
    analysis_time: str


class ASRResponse(BaseModel):
    code: int
    result: List[ASRSegment]


def call_funasr(audio_data: bytes, filename: str = "audio.wav", job_id: str = "", flowNo: str = "") -> dict:
    files = {"file": (filename, audio_data, "audio/wav")}
    start_time = time.time()
    
    idx_tag = f"[{job_id}][{flowNo}]" if flowNo else f"[{job_id}]"
    try:
        response = requests.post(f"{FUNASR_URL}/asr", files=files, timeout=600)
        elapsed = time.time() - start_time
        perf_logger.info(f"{idx_tag} ASR | {elapsed:.2f}s | {filename}")
        
        if response.status_code != 200:
            raise Exception(f"FunASR调用失败: {response.text}")
        return response.json()
    except requests.exceptions.Timeout:
        logger.error(f"{idx_tag} ASR超时: {filename}")
        raise Exception("FunASR调用超时")
    except Exception as e:
        logger.error(f"{idx_tag} ASR异常: {e}")
        raise Exception(f"FunASR调用异常: {str(e)}")


FUNASR_BUSY_RETRY_ATTEMPTS = 5  # 首次 + 4 次重试
FUNASR_BUSY_RETRY_DELAY_SECONDS = 3  # 每次重试前固定等待，足够覆盖一次 ASR 调用的典型耗时


def _is_funasr_busy_error(exc: Exception) -> bool:
    msg = str(exc)
    return "并发已达上限" in msg or "服务繁忙" in msg


async def call_funasr_async(audio_data: bytes, filename: str = "audio.wav", job_id: str = "", flowNo: str = "") -> dict:
    idx_tag = f"[{job_id}][{flowNo}]" if flowNo else f"[{job_id}]"
    loop = asyncio.get_event_loop()
    last_exc: Optional[Exception] = None
    for attempt in range(1, FUNASR_BUSY_RETRY_ATTEMPTS + 1):
        try:
            async with funasr_semaphore:
                result = await loop.run_in_executor(thread_pool, call_funasr, audio_data, filename, job_id, flowNo)
            check_memory_and_cleanup()
            return result
        except Exception as e:
            last_exc = e
            if not _is_funasr_busy_error(e) or attempt == FUNASR_BUSY_RETRY_ATTEMPTS:
                raise
            # 先退出 async with 释放本地并发槽位再等待，不占着槽位空等，让其他排队的记录
            # 有机会先跑；上游真正的槽位由 FunASR 自己按其硬上限调度。
            logger.warning(
                f"{idx_tag} FunASR 并发繁忙（第{attempt}/{FUNASR_BUSY_RETRY_ATTEMPTS}次），"
                f"{FUNASR_BUSY_RETRY_DELAY_SECONDS}s 后重试"
            )
            await asyncio.sleep(FUNASR_BUSY_RETRY_DELAY_SECONDS)
    raise last_exc


def build_prompt(current_time: str, asr_texts: str, standards: List[str], general_rules: Optional[str] = None) -> str:
    asr_texts_escaped = asr_texts.replace('{', '{{').replace('}', '}}')
    general_rules_escaped = general_rules.replace('{', '{{').replace('}', '}}') if general_rules else ""
    standards_text = '\n'.join([f"- {s}" for s in standards])
    
    prompt_parts = [
        "你是一个呼叫中心质检专家。",
        f"当前时间：{current_time}",
        "",
        "【任务说明】",
        "请仔细阅读对话内容，识别出每个说话人的角色：",
        "1. 坐席（提供服务的客服人员，通常先开口问候，主动询问需求，结束时会感谢来电）",
        "2. 用户（拨打热线咨询的客户，会描述问题或需求，询问解决方案）",
        "",
        "【角色识别关键线索】",
        "- 坐席开场通常说\"您好，请问有什么可以帮您\"",
        "- 坐席结束通常说\"感谢您的来电\"、\"祝您生活愉快\"",
        "- 用户会描述问题、重复坐席的话、或询问\"请问...\"",
        "- 坐席的句子通常比用户更长，包含更多服务用语",
        "",
        "【话术规范标准】",
        standards_text
    ]
    
    if general_rules:
        prompt_parts.extend([
            "",
            "【通用规则】",
            general_rules_escaped
        ])
    
    prompt_parts.extend([
        "",
        "【实际通话转译文本】",
        asr_texts_escaped,
        "",
        "请进行对比分析：",
        "1. 根据上述线索，准确识别每个spk的角色（坐席或用户）",
        "2. 只对坐席的对话进行质检",
        "3. 判断通话内容是否符合话术规范",
        "4. 列出违规内容",
        "5. 只对坐席的对话内容给出1-100分",
        "",
        "{\"is_compliant\": true或false,\n  \"violations\": [{\"description\": \"违规描述\", \"segment\": \"坐席说的具体原话\"}],\n  \"score\": 85,\n  \"summary\": \"简要总结\",\n  \"roles\": [{\"speaker_id\": \"0\", \"role\": \"坐席\"}, {\"speaker_id\": \"1\", \"role\": \"用户\"}]}",
        "",
        "重要提醒：",
        "- 必须返回纯JSON字符串，不要用```包裹",
        "- violations中每个违规必须包含description（违规描述）和segment（坐席说的具体原话）",
        "- role字段必须填写\"坐席\"或\"用户\""
    ])
    
    return '\n'.join(prompt_parts)


OLLAMA_JSON_RETRY_ATTEMPTS = 3  # 首次 + 2 次重试；LLM 采样具有随机性，JSON 格式错误多为偶发


def _parse_ollama_json(response_text: str) -> dict:
    """解析 Ollama 返回文本为 JSON；失败时尝试剥离 markdown 代码块围栏后重新提取 {...} 再解析一次。"""
    try:
        return json.loads(response_text)
    except json.JSONDecodeError as json_err:
        try:
            import re
            cleaned = re.sub(r'```json\s*\n?', '', response_text)
            cleaned = re.sub(r'```python\s*\n?', '', cleaned)
            cleaned = re.sub(r'```\s*\n?', '', cleaned)
            cleaned = cleaned.strip()

            start = cleaned.find('{')
            end = cleaned.rfind('}') + 1
            if start >= 0 and end > 0:
                cleaned = cleaned[start:end]
                return json.loads(cleaned)
        except Exception:
            pass
        raise json_err


def call_ollama_sync(prompt: str, job_id: str = "", flowNo: str = "") -> dict:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "num_predict": 2048,   # 最大生成 token 数，防止 JSON 被截断（默认 128 太小）
            "num_ctx": 4096,       # 上下文窗口，兼容长对话 prompt
            # 不传 num_thread：per-request num_thread 在 OLLAMA_NUM_PARALLEL>1 时会被
            # 解释成共享 runner 线程池上限（而非每路），5 路请求共用 2 线程 = 串行化 → 全超时。
            # 线程数由 ollama.service 的 OLLAMA_NUM_THREAD 环境变量统一管（=物理核）。
        }
    }

    idx_tag = f"[{job_id}][{flowNo}]" if flowNo else f"[{job_id}]"
    last_json_err: Optional[json.JSONDecodeError] = None
    for attempt in range(1, OLLAMA_JSON_RETRY_ATTEMPTS + 1):
        try:
            response = requests.post(OLLAMA_URL, json=payload, timeout=600)

            if response.status_code != 200:
                raise Exception(f"Ollama调用失败: {response.text}")

            result = response.json()
            response_text = result.get("response", "").strip()

            if not response_text:
                raise Exception("Ollama返回空响应")

            try:
                return _parse_ollama_json(response_text)
            except json.JSONDecodeError as json_err:
                last_json_err = json_err
                # Ollama 偶发输出非法 JSON，同一次调用内重试，不产生新 job
                logger.warning(
                    f"{idx_tag} JSON解析失败（第{attempt}/{OLLAMA_JSON_RETRY_ATTEMPTS}次），"
                    f"原始前100字符: {response_text[:100]}"
                )
                if attempt < OLLAMA_JSON_RETRY_ATTEMPTS:
                    continue
                raise json_err
        except requests.exceptions.Timeout:
            logger.error(f"{idx_tag} Ollama调用超时")
            raise Exception("Ollama调用超时")
        except json.JSONDecodeError:
            raise Exception(f"Ollama调用异常: {last_json_err}")
        except Exception as e:
            logger.error(f"{idx_tag} Ollama调用异常: {e}")
            raise Exception(f"Ollama调用异常: {str(e)}")

    # 理论不可达：循环要么 return，要么 raise
    raise Exception(f"Ollama调用异常: {last_json_err}")


async def call_ollama(prompt: str, job_id: str = "", flowNo: str = "") -> dict:
    async with ollama_semaphore:
        loop = asyncio.get_event_loop()
        ollama_start = time.time()
        result = await loop.run_in_executor(thread_pool, call_ollama_sync, prompt, job_id, flowNo)
        ollama_elapsed = time.time() - ollama_start
        idx_tag = f"[{job_id}][{flowNo}]" if flowNo else f"[{job_id}]"
        perf_logger.info(f"{idx_tag} Ollama | {ollama_elapsed:.2f}s")
        check_memory_and_cleanup()
        return result


async def do_quality_check(asr_result: dict, metadata: Optional[CallMetadata] = None, script_standards: Optional[str] = None, general_rules: Optional[str] = None, job_id: str = "", flowNo: str = "") -> QAResponse:
    start_time = time.time()
    
    if asr_result.get("code") != 200:
        raise HTTPException(status_code=400, detail="ASR转译失败")
    
    result_list = asr_result.get("result", [])
    if not result_list:
        raise HTTPException(status_code=400, detail="无对话内容")
    
    metadata_obj = metadata if metadata else CallMetadata()
    idx_tag = f"[{job_id}][{flowNo}]" if flowNo else f"[{job_id}]"
    logger.debug(f"{idx_tag} 质检分析 | 对话条数: {len(result_list)}")
    
    asr_texts = []
    for item in result_list:
        text = item.get("text", "").strip()
        spk = item.get("spk", "")
        if text:
            asr_texts.append(f"spk{spk}: {text}")
    
    combined_text = "\n".join(asr_texts)
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if script_standards:
        standards = [s.strip() for s in script_standards.split('\n') if s.strip()]
    else:
        standards = [
            "坐席必须报工号",
            "坐席需礼貌问候用户",
            "坐席需明确询问用户需求",
            "坐席需提供解决方案",
            "坐席需礼貌结束通话"
        ]
    
    prompt = build_prompt(current_time, combined_text, standards, general_rules)
    
    analysis_result = await call_ollama(prompt, job_id, flowNo)
    
    violations_raw = analysis_result.get("violations", [])
    violations = []
    for v in violations_raw:
        if isinstance(v, str):
            violations.append(ViolationItem(description=v))
        elif isinstance(v, dict):
            violations.append(ViolationItem(
                description=v.get("description", ""),
                segment=v.get("segment")
            ))
    
    roles_raw = analysis_result.get("roles", [])
    roles = []
    for r in roles_raw:
        if isinstance(r, str):
            parts = r.split(":")
            if len(parts) >= 2:
                roles.append(RoleInfo(speaker_id=parts[0], role=parts[1]))
        elif isinstance(r, dict):
            # speaker_id 可能是数字，统一转字符串，避免 pydantic 校验失败
            roles.append(RoleInfo(
                speaker_id=str(r.get("speaker_id", "")),
                role=r.get("role", "")
            ))
    
    role_map = {r.speaker_id: r.role for r in roles}
    
    conversations = []
    for item in result_list:
        spk = str(item.get("spk", ""))
        speaker_role = role_map.get(spk, "未知")
        conversations.append(ConversationItem(
            speaker_id=spk,
            speaker_role=speaker_role,
            text=item.get("text", "").strip(),
            start=item.get("start", 0),
            end=item.get("end", 0)
        ))
    
    total_elapsed = time.time() - start_time
    memory_mb = get_memory_usage_mb()
    perf_logger.info(f"{idx_tag} 质检完成 | {total_elapsed:.2f}s | 得分: {analysis_result.get('score')} | 内存: {memory_mb:.1f}MB")
    
    return QAResponse(
        is_compliant=analysis_result.get("is_compliant", False),
        score=analysis_result.get("score", 0),
        summary=analysis_result.get("summary", ""),
        violations=violations,
        roles=roles,
        conversations=conversations,
        metadata=metadata_obj,
        asr_result=asr_result,
        analysis_time=current_time
    )


async def process_single_task(job_id: str, audio_data: bytes, filename: str, extra: dict, script_standards: Optional[str], general_rules: Optional[str] = None):
    task_storage[job_id] = {
        "status": "processing",
        "total": 1,
        "completed": 0,
        "failed": 0,
        "results": [],
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }

    try:
        # --- 取消检查：处理前 ---
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 单条任务已取消")
            return

        saved_file_path = save_audio_file(audio_data, filename)
        asr_result = await call_funasr_async(audio_data, filename, job_id, "")

        # --- 取消检查：ASR后、Ollama前（拦截ASR结果） ---
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 单条任务已取消（ASR完成，拦截结果）")
            return

        metadata = CallMetadata(extra=extra)
        qa_result = await do_quality_check(asr_result, metadata, script_standards, general_rules, job_id, "")

        # --- 取消检查：质检完成后、存储前（拦截质检结果） ---
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 单条任务已取消（质检完成，拦截结果）")
            return

        task_storage[job_id]["results"].append({
            "index": 0,
            "success": True,
            "callid": extra.get("callid"),
            "flowNo": extra.get("flowNo", ""),
            "result": qa_result.model_dump()
        })
        task_storage[job_id]["completed"] = 1
        logger.info(f"[{job_id}] 单条质检完成，得分: {qa_result.score}")
    except Exception as e:
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 单条任务已取消（异常中）")
            return
        logger.error(f"[{job_id}] 单条质检失败: {e}")
        task_storage[job_id]["results"].append({
            "index": 0,
            "success": False,
            "callid": extra.get("callid"),
            "flowNo": extra.get("flowNo", ""),
            "error": str(e)
        })
        task_storage[job_id]["failed"] = 1

    if task_storage[job_id]["status"] != "cancelled":
        task_storage[job_id]["status"] = "completed"
        task_storage[job_id]["updated_at"] = datetime.now().isoformat()


async def process_remote_task(job_id: str, extra: dict, script_standards: Optional[str], general_rules: Optional[str] = None, file_url: str = ""):
    loop = asyncio.get_event_loop()
    
    task_storage[job_id] = {
        "status": "processing",
        "total": 1,
        "completed": 0,
        "failed": 0,
        "results": [],
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    try:
        # --- 取消检查：处理前 ---
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 远程任务已取消")
            return

        flowNo = extra.get('flowNo', '')

        saved_file_path = await loop.run_in_executor(thread_pool, download_audio_from_url, file_url, flowNo, job_id)

        # --- 取消检查：下载后、ASR前 ---
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 远程任务已取消（下载完成，拦截）")
            return

        with open(saved_file_path, 'rb') as f:
            audio_data = f.read()

        filename = os.path.basename(saved_file_path)

        asr_result = await call_funasr_async(audio_data, filename, job_id, flowNo)

        # --- 取消检查：ASR后、Ollama前（拦截ASR结果） ---
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 远程任务已取消（ASR完成，拦截结果）")
            return

        metadata = CallMetadata(extra=extra)
        qa_result = await do_quality_check(asr_result, metadata, script_standards, general_rules, job_id, flowNo)

        # --- 取消检查：质检完成后、存储前（拦截质检结果） ---
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 远程任务已取消（质检完成，拦截结果）")
            return

        task_storage[job_id]["results"].append({
            "index": 0,
            "success": True,
            "callid": extra.get("callid"),
            "flowNo": extra.get("flowNo", ""),
            "result": qa_result.model_dump()
        })
        task_storage[job_id]["completed"] = 1
        logger.info(f"[{job_id}][{flowNo}] 远程质检完成，得分: {qa_result.score}")
    except Exception as e:
        if is_cancelled(job_id):
            task_storage[job_id]["status"] = "cancelled"
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()
            logger.info(f"[{job_id}] 远程任务已取消（异常中）")
            return
        logger.error(f"[{job_id}][{flowNo}] 远程质检失败: {e}")
        task_storage[job_id]["results"].append({
            "index": 0,
            "success": False,
            "callid": extra.get("callid"),
            "flowNo": extra.get("flowNo", ""),
            "error": str(e)
        })
        task_storage[job_id]["failed"] = 1

    if task_storage[job_id]["status"] != "cancelled":
        task_storage[job_id]["status"] = "completed"
        task_storage[job_id]["updated_at"] = datetime.now().isoformat()


async def process_batch_task(job_id: str, records: List[dict], script_standards: Optional[str], general_rules: Optional[str] = None, file_urls: Optional[List[dict]] = None):
    """处理批量质检任务（内部并行）"""
    if file_urls is None:
        file_urls = []
    
    total = len(records)
    
    task_storage[job_id] = {
        "status": "processing",
        "total": total,
        "completed": 0,
        "failed": 0,
        "results": [None] * total,
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    batch_semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
    # 任务级信号量覆盖 ASR + Ollama 整段，避免阶段级信号量叠加导致峰值并发翻倍
    task_semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)
    storage_lock = asyncio.Lock()
    
    async def process_one_record(idx: int, record: dict):
        rec_start = record.get("start", "")
        rec_flowNo = record.get("flowNo", "")

        extra = {k: v for k, v in record.items() if k not in ["start", "flowNo"]}
        if rec_start:
            extra["start"] = rec_start
        if rec_flowNo:
            extra["flowNo"] = rec_flowNo

        # --- 取消检查：下载前 ---
        if is_cancelled(job_id):
            return

        # 同一 flowNo 已被其他 job 处理中则跳过；字典赋值做 check-and-set，避免竞态
        registered_flowno = ""
        if rec_flowNo:
            existing_job = flowno_inflight.get(rec_flowNo)
            cooldown_entry = flowno_cooldown.get(rec_flowNo)
            in_cooldown = (
                cooldown_entry is not None
                and cooldown_entry[0] != job_id
                and time.time() < cooldown_entry[1]
            )
            if (existing_job and existing_job != job_id) or in_cooldown:
                blocking_job = existing_job if existing_job else cooldown_entry[0]
                logger.warning(
                    f"[{job_id}][{rec_flowNo}] 该 flowNo 已有 job {blocking_job} 正在质检中或刚质检完成（冷却期内），跳过本次重复提交"
                )
                item_result = {
                    "index": idx,
                    "success": False,
                    "callid": extra.get("callid"),
                    "flowNo": rec_flowNo,
                    "error": f"duplicate flowNo already in-flight/cooling down as job {blocking_job}, skipped",
                }
                async with storage_lock:
                    task_storage[job_id]["results"][idx] = item_result
                    task_storage[job_id]["failed"] += 1
                    task_storage[job_id]["updated_at"] = datetime.now().isoformat()
                await on_item_finished(job_id, idx)
                return
            flowno_inflight[rec_flowNo] = job_id
            registered_flowno = rec_flowNo

        try:
            loop = asyncio.get_event_loop()

            # 优先使用入口阶段已下载好的本地文件
            local_path = file_urls[idx].get("local_path", "") if idx < len(file_urls) else ""

            if local_path and os.path.exists(local_path):
                saved_file_path = local_path
                logger.debug(f"[{job_id}][{rec_flowNo}] 复用本地文件: {saved_file_path}")
            else:
                # 文件下载阶段用 semaphore 限制并发（避免过多下载请求）
                async with batch_semaphore:
                    if rec_start and rec_flowNo:
                        download_url = get_recording_url(int(rec_start), rec_flowNo, job_id)
                    else:
                        download_url = ""
                    saved_file_path = await loop.run_in_executor(
                        thread_pool, download_audio_from_url, download_url, rec_flowNo, job_id
                    )

            # --- 取消检查：ASR前 ---
            if is_cancelled(job_id):
                if registered_flowno:
                    flowno_inflight.pop(registered_flowno, None)
                return

            filename = os.path.basename(saved_file_path)

            async with task_semaphore:
                # --- 取消检查：拿到名额后重新确认（排队期间可能已被取消） ---
                if is_cancelled(job_id):
                    if registered_flowno:
                        flowno_inflight.pop(registered_flowno, None)
                    return

                # 排队期间不占内存：拿到名额后才把 wav 读进来（单条 10~19MB，
                # 10 条一起排队时能省下一百多 MB 常驻）
                with open(saved_file_path, 'rb') as f:
                    audio_data = f.read()

                asr_result = await call_funasr_async(audio_data, filename, job_id, rec_flowNo)
                del audio_data

                # --- 取消检查：ASR后、Ollama前（拦截ASR结果） ---
                if is_cancelled(job_id):
                    if registered_flowno:
                        flowno_inflight.pop(registered_flowno, None)
                    return

                metadata = CallMetadata(extra=extra)
                qa_result = await do_quality_check(asr_result, metadata, script_standards, general_rules, job_id, rec_flowNo)

            # --- 取消检查：质检完成后、存储前（拦截质检结果） ---
            if is_cancelled(job_id):
                if registered_flowno:
                    flowno_inflight.pop(registered_flowno, None)
                return

            item_result = {
                "index": idx,
                "success": True,
                "callid": extra.get("callid"),
                "flowNo": rec_flowNo,
                "result": qa_result.model_dump()
            }
        except Exception as e:
            # 取消状态下的异常直接丢弃
            if is_cancelled(job_id):
                if registered_flowno:
                    flowno_inflight.pop(registered_flowno, None)
                return
            item_result = {
                "index": idx,
                "success": False,
                "callid": extra.get("callid"),
                "flowNo": rec_flowNo,
                "error": str(e)
            }
            logger.debug(f"[{job_id}] {rec_flowNo} 异常: {e}")

        if registered_flowno:
            release_flowno(registered_flowno, job_id)

        async with storage_lock:
            task_storage[job_id]["results"][idx] = item_result
            if item_result["success"]:
                task_storage[job_id]["completed"] += 1
            else:
                task_storage[job_id]["failed"] += 1
            task_storage[job_id]["updated_at"] = datetime.now().isoformat()

        await on_item_finished(job_id, idx)

    logger.info(f"[{job_id}] 批量任务开始，共{total}条，并发数: {BATCH_CONCURRENCY}")
    await asyncio.gather(*[process_one_record(idx, record) for idx, record in enumerate(records)])

    if is_cancelled(job_id):
        task_storage[job_id]["status"] = "cancelled"
        logger.info(f"[{job_id}] 批量任务已取消，成功: {task_storage[job_id]['completed']}，失败: {task_storage[job_id]['failed']}")
    else:
        task_storage[job_id]["status"] = "completed"
        logger.info(f"[{job_id}] 批量任务完成，成功: {task_storage[job_id]['completed']}，失败: {task_storage[job_id]['failed']}")
    task_storage[job_id]["updated_at"] = datetime.now().isoformat()


@app.post("/api/quality-check")
async def quality_check(
    request: Request,
    script_standards: Optional[str] = Form(None),
    general_rules: Optional[str] = Form(None),
    audio_file: Optional[UploadFile] = File(None),
    start: Optional[str] = Form(None),
    stop: Optional[str] = Form(None),
    flowNo: Optional[str] = Form(None),
    callback_url: Optional[str] = Form(None),
    callback_events: Optional[str] = Form(None),
    _: bool = Depends(verify_api_key)
):
    job_id = str(uuid.uuid4())

    content_type = request.headers.get("content-type", "")

    # 解析 records 参数（批量模式）
    records = None
    if request.headers.get("content-type", "").find("application/json") >= 0:
        try:
            body = await request.json()
            records = body.get("records", [])
            callback_url = body.get("callback_url") or callback_url
            callback_events = body.get("callback_events") or callback_events
        except:
            pass

    if callback_url:
        url_error = callback.validate_callback_url(callback_url)
        if url_error:
            raise HTTPException(status_code=400, detail=url_error)

    if audio_file:
        extra = {}
        try:
            form = await request.form()
            for key, value in form.items():
                if key not in ['audio_file', 'script_standards', 'general_rules', 'start', 'stop', 'flowNo',
                               'callback_url', 'callback_events'] and value:
                    extra[key] = value
        except:
            pass

        if start:
            extra['start'] = start
        if stop:
            extra['stop'] = stop
        if flowNo:
            extra['flowNo'] = flowNo

        logger.info(f"[{job_id}] 收到单条质检请求")
        audio_data = await audio_file.read()
        filename = audio_file.filename or "audio.wav"

        register_job(job_id, "upload", callback_url, callback_events)
        asyncio.create_task(run_job(
            process_single_task(job_id, audio_data, filename, extra, script_standards, general_rules),
            job_id,
        ))

        return {"job_id": job_id, "status": "submitted", "total": 1}
    
    # 批量模式：records 参数
    if records and isinstance(records, list) and len(records) > 0:
        logger.info(f"[{job_id}] 收到批量质检请求，共{len(records)}条")
        
        # 并发下载所有录音文件，完成后同步返回 file_urls 给前端
        loop = asyncio.get_event_loop()
        # 入口阶段的预下载同样要限流：不限并发时 N 条记录会同时打 Nginx 探测 + 下载，
        # 记录数一多就把提交请求拖到调用方超时
        prefetch_semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)

        async def fetch_one(record: dict) -> dict:
            rec_start = record.get("start", "")
            rec_flowNo = record.get("flowNo", "")
            local_path = ""
            external_url = ""
            if rec_start and rec_flowNo:
                try:
                    async with prefetch_semaphore:
                        download_url = get_recording_url(int(rec_start), rec_flowNo, job_id)
                        saved_file_path = await loop.run_in_executor(
                            thread_pool, download_audio_from_url, download_url, rec_flowNo, job_id
                        )
                    local_path = saved_file_path
                    external_url = convert_to_http_url(saved_file_path)
                except Exception as e:
                    logger.error(f"[{job_id}][{rec_flowNo}] 获取录音URL失败: {e}")
            return {"flowNo": rec_flowNo, "url": external_url, "local_path": local_path}

        file_urls = await asyncio.gather(*[fetch_one(r) for r in records])
        file_urls = list(file_urls)

        register_job(job_id, "batch", callback_url, callback_events)
        asyncio.create_task(run_job(
            process_batch_task(job_id, records, script_standards, general_rules, file_urls),
            job_id,
        ))

        # 返回给前端时去掉 local_path（内部字段）
        response_file_urls = [{"flowNo": f["flowNo"], "url": f["url"]} for f in file_urls]
        return {"job_id": job_id, "status": "submitted", "total": len(records), "file_urls": response_file_urls}
    
    # 单条远程模式
    if not start or not flowNo:
        raise HTTPException(status_code=400, detail="未上传音频文件时，start和flowNo或records为必填参数")
    
    extra = {}
    if start:
        extra['start'] = start
    if stop:
        extra['stop'] = stop
    if flowNo:
        extra['flowNo'] = flowNo
    
    logger.info(f"[{job_id}][{flowNo}] 收到远程质检请求")
    
    # 立即获取录音文件URL
    file_url = ""
    start_ms = int(start)
    file_url = get_recording_url(start_ms, flowNo, job_id)
    
    register_job(job_id, "remote", callback_url, callback_events)
    asyncio.create_task(run_job(
        process_remote_task(job_id, extra, script_standards, general_rules, file_url),
        job_id,
    ))

    return {"job_id": job_id, "status": "submitted", "total": 1, "file_url": file_url}


@app.post("/api/quality-search")
async def quality_search(request: Request, _: bool = Depends(verify_api_key)):
    """只定位并返回可播放的录音地址，不跑 ASR/Ollama 推理、不创建质检 job。

    Nginx 探测到的是内网地址（如 172.x.x.x:8100），浏览器既连不通也放不出 .rec 原始
    格式，必须像 /api/quality-check 一样下载并转码成 .wav、再返回 qa-svr 自己对外提供
    的 /audio/ 地址才能真正播放/下载——这一步不是「质检」（ASR/Ollama 推理），跳过它
    只会让接口返回一个打不开的内网链接。真正要避免跟批量质检抢的是并发：下载+转码用
    独立的 quality_search_semaphore，不复用批量质检提交那条路径的 prefetch_semaphore，
    也不 register_job/run_job，所以不会创建质检任务、不占用 ASR/Ollama 并发。

    请求体沿用 /api/quality-check 的 records 数组形状，前端 get_recording_url 动作
    改调这个新接口即可，payload 不用变。
    """
    body: dict = {}
    if request.headers.get("content-type", "").find("application/json") >= 0:
        try:
            body = await request.json()
        except Exception:
            body = {}

    records = body.get("records") if isinstance(body.get("records"), list) else None
    if not records:
        flowNo = body.get("flowNo") or body.get("flowno")
        start = body.get("start")
        if flowNo and start:
            records = [{"flowNo": flowNo, "start": start}]

    if not records:
        raise HTTPException(status_code=400, detail="records（或 flowNo+start）为必填参数")

    search_id = str(uuid.uuid4())
    loop = asyncio.get_event_loop()

    async def resolve_one(record: dict) -> dict:
        flowNo = record.get("flowNo") or record.get("flowno") or ""
        start = record.get("start", "")
        if not flowNo or not start:
            return {"flowNo": flowNo, "url": ""}
        try:
            start_ms = int(start)
        except (TypeError, ValueError):
            return {"flowNo": flowNo, "url": ""}
        url = ""
        try:
            async with quality_search_semaphore:
                internal_url = await loop.run_in_executor(
                    thread_pool, get_recording_url, start_ms, flowNo, search_id
                )
                if internal_url:
                    saved_path = await loop.run_in_executor(
                        thread_pool, download_audio_from_url, internal_url, flowNo, search_id
                    )
                    url = convert_to_http_url(saved_path)
        except Exception as e:
            logger.error(f"[{search_id}][{flowNo}] 定位录音URL失败: {e}")
            url = ""
        return {"flowNo": flowNo, "url": url}

    file_urls = await asyncio.gather(*[resolve_one(r) for r in records])
    return {"file_urls": list(file_urls)}


@app.get("/api/task/{job_id}")
async def get_task_status(job_id: str, _: bool = Depends(verify_api_key)):
    task = await load_task(job_id)
    if task is None:
        raise HTTPException(status_code=404, detail="任务不存在")

    return {
        "job_id": job_id,
        "status": task["status"],
        "total": task["total"],
        "completed": task["completed"],
        "failed": task["failed"],
        "results": [item for item in task["results"] if item],
        "created_at": task["created_at"],
        "updated_at": task["updated_at"]
    }


@app.get("/api/task")
async def list_tasks(limit: int = 10, _: bool = Depends(verify_api_key)):
    persisted = await db.list_jobs(limit)
    if persisted is not None:
        return {"tasks": persisted}

    tasks = []
    for job_id, task in list(task_storage.items())[-limit:]:
        tasks.append({
            "job_id": job_id,
            "status": task["status"],
            "total": task["total"],
            "completed": task["completed"],
            "failed": task["failed"],
            "created_at": task["created_at"]
        })
    return {"tasks": tasks}


@app.get("/health")
async def health_check():
    return {"status": "ok"}


@app.get("/audio/{file_path:path}")
async def get_audio(file_path: str):
    """音频文件访问"""
    # 防止路径遍历
    file_path = os.path.normpath(file_path)
    if ".." in file_path or file_path.startswith("/"):
        raise HTTPException(status_code=403, detail="Invalid path")
    
    full_path = os.path.join(WAV_FILE_DIR, file_path)
    if not os.path.exists(full_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    return FileResponse(full_path, media_type="audio/wav")


@app.get("/health-check")
async def health_check_extended():
    try:
        requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
        ollama_status = "ok"
    except:
        ollama_status = "error"
    
    try:
        requests.get(f"{FUNASR_URL}/health", timeout=5)
        funasr_status = "ok"
    except:
        funasr_status = "error"
    
    return {
        "status": "ok",
        "qa_service": "running",
        "ollama": ollama_status,
        "funasr": funasr_status
    }


if __name__ == "__main__":
    import uvicorn
    
    logging.getLogger("uvicorn.access").handlers.clear()
    logging.getLogger("uvicorn.error").handlers.clear()
    logging.getLogger("uvicorn").handlers.clear()
    
    uvicorn.run(app, host=API_HOST, port=API_PORT, access_log=False)
