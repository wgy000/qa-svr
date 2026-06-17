import os

# ======================================================================
# 线程数必须在 import torch / funasr 之前设置：OpenMP / MKL 的线程池在 torch
# 首次 import 时按环境变量固化，之后再改不生效。
#
# 按物理核（非 vCPU）切分，超线程对 PyTorch 收益接近 0；下限 2 线程。
# 换机器时只需改 PHYS_CORES 环境变量，不用改代码。
# ======================================================================
MAX_CONCURRENT = int(os.environ.get("MAX_CONCURRENT", "5"))

_phys_cores = int(os.environ.get("PHYS_CORES", "6"))
_asr_threads = int(
    os.environ.get("ASR_THREADS_PER_TASK", "0")
) or max(2, _phys_cores // MAX_CONCURRENT)

os.environ["OMP_NUM_THREADS"] = str(_asr_threads)
os.environ["MKL_NUM_THREADS"] = str(_asr_threads)
os.environ["OPENBLAS_NUM_THREADS"] = str(_asr_threads)
os.environ["NUMEXPR_NUM_THREADS"] = str(_asr_threads)

# MALLOC_ARENA_MAX 不能在这里设置：glibc 的 arena 在进程首次 malloc 时初始化，
# 运行期改环境变量无效，必须写在 systemd 的 Environment= 里。

import asyncio
import ctypes
import ctypes.util
import logging
import logging.handlers
import tempfile
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

import numpy as np
import soundfile as sf
import ffmpeg
import psutil
import torch
from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.responses import JSONResponse
from sklearn.cluster import AgglomerativeClustering

from funasr import AutoModel

# torch 侧再补一刀：环境变量只管到 OMP/MKL，torch 自身的 intra/inter-op 线程池
# 还需要显式设置。interop 固定为 1——ASR 是单条串行的算子链，跨算子并行只会
# 增加调度开销。
torch.set_num_threads(_asr_threads)
try:
    torch.set_num_interop_threads(1)
except RuntimeError:
    # interop 线程池一旦被使用过就不允许再改，import 阶段正常不会走到这里
    pass

# ======================
# 日志配置
# ======================
# 不用 basicConfig，因为 uvicorn 启动时会覆盖 root logger 的配置。
# 改为给我们的 logger 单独挂 handler，不受 uvicorn 干扰。
_LOG_FMT = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 日志目录：默认与 server.py 同级的 logs/，可通过环境变量覆盖
_LOG_DIR = os.environ.get("LOG_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs"))
os.makedirs(_LOG_DIR, exist_ok=True)

# 按天轮转文件 handler：每天零点新建一个文件，保留 30 天
# 文件名格式：funasr-server.log.YYYY-MM-DD
_file_handler = logging.handlers.TimedRotatingFileHandler(
    filename=os.path.join(_LOG_DIR, "funasr-server.log"),
    when="midnight",       # 每天零点轮转
    interval=1,
    backupCount=30,        # 最多保留 30 个历史文件
    encoding="utf-8",
)
_file_handler.suffix = "%Y-%m-%d"   # 轮转后文件名结尾：funasr-server.log.2026-06-01
_file_handler.setFormatter(_LOG_FMT)

# 控制台 handler（保留，方便 journalctl / systemd 查看）
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_LOG_FMT)

logger = logging.getLogger("funasr-server")
logger.setLevel(logging.INFO)
logger.handlers.clear()         # 防止重复添加（reload 场景）
logger.addHandler(_file_handler)
logger.addHandler(_console_handler)
logger.propagate = False        # 不向上传播到 root，避免被 uvicorn 二次格式化

# ======================
# 资源优化配置
# ======================
# 线程限制已在文件顶部（所有 torch/funasr import 之前）完成，见开头注释。
logger.info(
    f"物理核: {_phys_cores}，并发上限: {MAX_CONCURRENT}，"
    f"每路 ASR 线程数: {_asr_threads}，MALLOC_ARENA_MAX: {os.environ.get('MALLOC_ARENA_MAX')}"
)

# 并发推理信号量：同时最多 MAX_CONCURRENT 个请求进入推理阶段
_infer_semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# 上传文件大小上限（字节），默认 100 MB
MAX_FILE_BYTES = int(os.environ.get("MAX_FILE_MB", "100")) * 1024 * 1024

# 推理线程池：隔离 CPU 密集型推理，不阻塞事件循环
_executor = ThreadPoolExecutor(max_workers=MAX_CONCURRENT, thread_name_prefix="asr-infer")

# 系统可用内存低于该百分比时拒绝新任务，避免被 OOM Killer 杀掉
MEM_AVAIL_MIN_PERCENT = float(os.environ.get("MEM_AVAIL_MIN_PERCENT", "15"))

# 单次推理的 batch 时长上限（秒），决定激活张量的峰值内存占用
ASR_BATCH_SIZE_S = int(os.environ.get("ASR_BATCH_SIZE_S", "300"))

# 每次推理结束后把 glibc 空闲堆归还给 OS。PyTorch 推理会产生大量短生命周期的大块
# 内存，free 之后默认留在进程 arena 里不还给系统，表现为 RSS 单调上涨。
# 置 "0" 可关闭。
ASR_MALLOC_TRIM = os.environ.get("ASR_MALLOC_TRIM", "1") != "0"
_libc = None
if ASR_MALLOC_TRIM:
    try:
        _libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6")
        _libc.malloc_trim.argtypes = [ctypes.c_size_t]
        _libc.malloc_trim.restype = ctypes.c_int
    except Exception as _e:  # 非 glibc 平台（musl/macOS）没有 malloc_trim，静默降级
        _libc = None
        logger.warning(f"malloc_trim 不可用，跳过内存回收: {_e}")


def _release_free_heap():
    """把 glibc 的空闲堆归还给 OS；不可用时是 no-op。"""
    if _libc is None:
        return
    try:
        _libc.malloc_trim(0)
    except Exception:
        pass

# ======================
# 全局：启动时加载三个模型（仅一次）
# ======================
logger.info("开始加载模型，请稍候...")
_model_load_start = time.time()
asr_pipeline = AutoModel(
    model="iic/speech_seaco_paraformer_large_asr_nat-zh-cn-16k-common-vocab8404-pytorch",
    vad_model="iic/speech_fsmn_vad_zh-cn-16k-common-pytorch",
    punc_model="iic/punc_ct-transformer_cn-en-common-vocab471067-large",
    spk_model="iic/speech_campplus_sv_zh-cn_16k-common",
    device="cpu",
    disable_update=True
)

vad_model = asr_pipeline.vad_model
asr_model = asr_pipeline.model
punc_model = asr_pipeline.punc_model
spk_model = asr_pipeline.spk_model

logger.info(f"所有模型加载完成，耗时 {time.time() - _model_load_start:.1f}s")

app = FastAPI(title="FunASR 说话人日志服务", version="1.0")


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """请求/响应日志中间件：记录每次请求的方法、路径、耗时和状态码"""
    start = time.time()
    logger.info(f"→ {request.method} {request.url.path}  client={request.client.host}")
    response = await call_next(request)
    elapsed = time.time() - start
    logger.info(
        f"← {request.method} {request.url.path}  "
        f"status={response.status_code}  elapsed={elapsed:.3f}s"
    )
    return response


def _is_already_target_wav(path: str) -> bool:
    """判断文件是否已经是 16k 单声道 PCM WAV，是则跳过 ffmpeg 重编码。"""
    try:
        info = ffmpeg.probe(path)
    except Exception:
        return False
    streams = [st for st in info.get("streams", []) if st.get("codec_type") == "audio"]
    if len(streams) != 1:
        return False
    st = streams[0]
    return (
        st.get("codec_name") == "pcm_s16le"
        and int(st.get("channels", 0)) == 1
        and int(st.get("sample_rate", 0)) == 16000
    )


def convert_to_wav(input_path: str, output_path: str):
    """将任意音频转为 16k 单通道 PCM WAV；已经是目标格式则直接复制，跳过重编。"""
    if _is_already_target_wav(input_path):
        shutil.copyfile(input_path, output_path)
        return
    try:
        (
            ffmpeg
            .input(input_path)
            .output(
                output_path,
                acodec='pcm_s16le',
                ac=1,
                ar='16000',
                f='wav'
            )
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"音频转换失败: {str(e)}")


def load_audio(wav_path: str) -> np.ndarray:
    """读取 WAV 为 float32 numpy 数组"""
    try:
        audio, sr = sf.read(wav_path)
        if sr != 16000:
            raise HTTPException(status_code=400, detail="采样率必须为 16kHz")
        if len(audio.shape) > 1:
            audio = audio[:, 0]
        return audio.astype(np.float32)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"音频读取失败: {str(e)}")


def _run_inference(audio: np.ndarray) -> list:
    """同步推理函数，在线程池中执行，避免阻塞事件循环。

    通过 disable_pbar=True 直接关闭 FunASR 内部 tqdm 进度条，
    避免并发时多线程竞争全局 sys.stderr 导致 I/O on closed file 错误。
    """
    try:
        res = asr_pipeline.generate(
            input=audio,
            batch_size_s=ASR_BATCH_SIZE_S,
            return_spk_res=True,
            disable_pbar=True,
        )
        result = []
        for sent in res[0].get("sentence_info", []):
            result.append({
                "text": sent.get("text", ""),
                "spk": sent.get("spk", 0),
                "start": sent.get("start", 0.0),
                "end": sent.get("end", 0.0)
            })
        return result
    finally:
        # 推理产生的激活张量已释放，但默认不会还给 OS，这里显式归还
        _release_free_heap()


@app.post("/asr", summary="上传音频进行带角色分离的识别")
async def transcribe_audio(file: UploadFile = File(...)):
    filename = file.filename or "unknown"

    # --- 文件大小限制 ---
    file_bytes = await file.read()
    file_size_kb = len(file_bytes) / 1024
    if len(file_bytes) > MAX_FILE_BYTES:
        logger.warning(
            f"[REJECT] 文件过大: {filename}  size={file_size_kb:.1f}KB  "
            f"limit={MAX_FILE_BYTES // 1024}KB"
        )
        raise HTTPException(
            status_code=413,
            detail=f"文件过大（{len(file_bytes) // 1024 // 1024} MB），上限 {MAX_FILE_BYTES // 1024 // 1024} MB"
        )

    # --- 系统内存检查：可用内存低于总内存 30% 时拒绝 ---
    vm = psutil.virtual_memory()
    avail_pct = vm.available / vm.total * 100
    if avail_pct < MEM_AVAIL_MIN_PERCENT:
        logger.warning(
            f"[MEM] 可用内存不足，拒绝请求: {filename}  "
            f"available={vm.available // 1024 // 1024}MB "
            f"({avail_pct:.1f}%)  "
            f"threshold={MEM_AVAIL_MIN_PERCENT}%"
        )
        raise HTTPException(
            status_code=503,
            detail=f"系统可用内存不足（{avail_pct:.0f}%），请稍后重试"
        )

    logger.info(f"[RECV] 文件: {filename}  size={file_size_kb:.1f}KB")

    with tempfile.TemporaryDirectory() as tmp_dir:
        input_path = os.path.join(tmp_dir, filename)
        wav_path = os.path.join(tmp_dir, "audio.wav")

        with open(input_path, "wb") as f:
            f.write(file_bytes)

        convert_to_wav(input_path, wav_path)
        audio = load_audio(wav_path)
        audio_duration = len(audio) / 16000  # 音频时长（秒）
        logger.info(f"[PREP] 音频转换完成: {filename}  duration={audio_duration:.1f}s")

        # --- 并发控制 + 线程池推理 ---
        if _infer_semaphore.locked() and _infer_semaphore._value == 0:
            logger.warning(
                f"[BUSY] 并发已满，拒绝请求: {filename}  "
                f"max_concurrent={MAX_CONCURRENT}"
            )
            raise HTTPException(
                status_code=429,
                detail=f"服务繁忙，当前并发已达上限（{MAX_CONCURRENT}），请稍后重试"
            )

        async with _infer_semaphore:
            infer_start = time.time()
            logger.info(
                f"[INFER] 开始推理: {filename}  "
                f"slots_in_use={MAX_CONCURRENT - _infer_semaphore._value}"
            )
            try:
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(_executor, _run_inference, audio)
            except Exception as e:
                logger.error(f"[ERROR] 推理失败: {filename}  error={str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"处理失败: {str(e)}")

            infer_elapsed = time.time() - infer_start
            logger.info(
                f"[DONE] 推理完成: {filename}  "
                f"elapsed={infer_elapsed:.2f}s  sentences={len(result)}"
            )

    return JSONResponse({"code": 200, "result": result})


@app.get("/health")
async def health_check():
    """健康检查，包含当前资源占用情况"""
    proc = psutil.Process(os.getpid())
    mem_info = proc.memory_info()
    cpu_percent = psutil.cpu_percent(interval=0.1)
    vm = psutil.virtual_memory()
    avail_pct = vm.available / vm.total * 100
    memory_ok = avail_pct >= MEM_AVAIL_MIN_PERCENT

    return {
        "status": "ok",
        "models_loaded": True,
        "memory_ok": memory_ok,
        "concurrency": {
            "max": MAX_CONCURRENT,
            "available": _infer_semaphore._value,
            "in_use": MAX_CONCURRENT - _infer_semaphore._value
        },
        "process": {
            "rss_mb": round(mem_info.rss / 1024 / 1024, 1),
            "vms_mb": round(mem_info.vms / 1024 / 1024, 1),
            "num_threads": proc.num_threads(),
        },
        # 暴露线程配置，便于部署后确认限制生效
        "tuning": {
            "phys_cores": _phys_cores,
            "asr_threads_per_task": _asr_threads,
            "torch_num_threads": torch.get_num_threads(),
            "omp_num_threads": os.environ.get("OMP_NUM_THREADS"),
            "malloc_arena_max": os.environ.get("MALLOC_ARENA_MAX"),
            "malloc_trim": _libc is not None,
            "batch_size_s": ASR_BATCH_SIZE_S,
            "mem_avail_min_percent": MEM_AVAIL_MIN_PERCENT,
        },
        "system": {
            "cpu_percent": cpu_percent,
            "mem_total_mb": round(vm.total / 1024 / 1024, 1),
            "mem_used_mb": round(vm.used / 1024 / 1024, 1),
            "mem_percent": vm.percent,
            "mem_available_mb": round(vm.available / 1024 / 1024, 1),
            "mem_available_pct": round(avail_pct, 1),
            "mem_accept_new": memory_ok
        }
    }
