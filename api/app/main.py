"""水质数仓单实例 PITR 恢复 API。

职责：物理基准备份 + WAL 持续归档之上，登记命名核对点，
把核对点恢复到独立只读实例并开放样品查询。Swagger 见 /docs。
"""
import os
import secrets
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from . import backuper, config, recoverer, source, state
from .walutil import lsn_to_int

_stop = threading.Event()

DESCRIPTION = """
面向水质数仓值班场景的单实例时间点恢复（PITR）服务。

* 基准备份：`pg_basebackup` 在线物理拷贝，源库全程可写；
* 核对点：`pg_create_restore_point` 命名恢复点 + WAL 归档齐备检查；
  同名登记原子占位，并发最多确认一个，已确认名称继续拒绝重复登记；
  每次登记使用内部唯一标记名，中断后同名重试得到全新 LSN，绝不复用旧标记；
* 恢复：基准备份 + 归档 WAL 重放到**登记时那个内部标记**（pause → 校验停点
  与登记 LSN 精确一致 → promote），产出**独立只读**实例；跨点后才提交的事务
  不会出现；停点不一致或未完成一律 `failed`，绝不报就绪；
* 查询：只打恢复实例，绝不用当前源库冒充恢复结果；
* 一次只恢复一项；失败或重启后可按原核对点重试（任务绑定登记时的身份快照）。
"""


# ------------------------------------------------------------------ 模型

class CheckpointIn(BaseModel):
    # PostgreSQL 的恢复点名称上限为 63 字节，校验不通过会在源库报错
    name: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$",
                      examples=["cp1"], description="核对点名称（唯一，≤63 字符）")


class RecoveryIn(BaseModel):
    checkpoint: str = Field(examples=["cp1"], description="要恢复到的核对点名称")


class QueryIn(BaseModel):
    sql: str = Field(examples=["SELECT * FROM samples ORDER BY id"],
                     description="单条只读 SELECT 语句")


# ------------------------------------------------------------------ 生命周期

def _refresher_loop() -> None:
    while not _stop.is_set():
        try:
            refresh_checkpoints()
        except Exception:
            pass
        _stop.wait(config.POLL_INTERVAL)


def refresh_checkpoints() -> None:
    """waiting_wal → ready（WAL 已归档）；新基准备份使更早核对点 expired。"""
    for cp in state.all_rows("SELECT * FROM checkpoints WHERE status='waiting_wal'"):
        if cp["wal_file"] and os.path.exists(
                os.path.join(config.ARCHIVE_DIR, cp["wal_file"])):
            state.execute(
                "UPDATE checkpoints SET status='ready', ready_at=? WHERE id=?",
                (state.now(), cp["id"]),
            )
    backup = backuper.latest_done()
    if backup:
        for cp in state.all_rows(
            "SELECT * FROM checkpoints WHERE status IN ('waiting_wal','ready')"
        ):
            if (cp["restore_lsn"]
                    and lsn_to_int(cp["restore_lsn"]) < lsn_to_int(backup["start_lsn"])):
                state.execute(
                    "UPDATE checkpoints SET status='expired' WHERE id=?", (cp["id"],)
                )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    _stop.clear()  # 同一进程内可重复启动（测试/热重载），复位关停信号
    for d in (config.BACKUP_DIR, config.RECOVERY_DIR,
              os.path.dirname(config.STATE_DB) or "."):
        os.makedirs(d, exist_ok=True)
    state.init_db()
    recoverer.resume_after_restart()
    threading.Thread(target=_refresher_loop, name="refresher", daemon=True).start()
    threading.Thread(target=recoverer.dispatch_loop, args=(_stop,),
                     name="dispatcher", daemon=True).start()
    # 首次启动自动制作一份基准备份
    if not state.one_row(
        "SELECT id FROM backups WHERE status IN ('running','done') LIMIT 1"
    ):
        try:
            backuper.start_backup()
        except Exception:
            pass
    yield
    _stop.set()
    for j in state.all_rows(
        "SELECT * FROM jobs WHERE status IN"
        " ('preparing','recovering','verifying','ready')"
    ):
        if j.get("data_dir"):
            recoverer.stop_instance(j["data_dir"])


app = FastAPI(title="水质数仓 PITR 恢复服务", version="1.0.0",
              description=DESCRIPTION, lifespan=lifespan)


# ------------------------------------------------------------------ 工具

_INTERNAL_CP_FIELDS = ("source_name", "boot_id")


def _public_cp(cp: dict | None) -> dict | None:
    """对外隐藏内部标记名与进程标识，保持 API 形状稳定。"""
    if cp is None:
        return None
    return {k: v for k, v in cp.items() if k not in _INTERNAL_CP_FIELDS}


def _job_or_404(job_id: int) -> dict:
    job = state.one_row("SELECT * FROM jobs WHERE id=?", (job_id,))
    if not job:
        raise HTTPException(404, f"恢复任务 #{job_id} 不存在")
    return job


def _cp_or_404(name: str) -> dict:
    cp = state.one_row("SELECT * FROM checkpoints WHERE name=?", (name,))
    if not cp:
        raise HTTPException(404, f"核对点 {name} 不存在")
    return cp


def _confirmed_cp_or_409(name: str) -> dict:
    """恢复只能使用已确认的核对点；pending/aborted 从未绑定可信 LSN。"""
    cp = _cp_or_404(name)
    if cp["status"] == "pending":
        raise HTTPException(409, f"核对点 {name} 正在登记中（pending），LSN 尚未确认")
    if cp["status"] == "aborted":
        raise HTTPException(
            409, f"核对点 {name} 登记已中断/失败（aborted），请用同名重新登记")
    return cp


def _job_cp_or_409(job: dict) -> dict:
    """任务与核对点的身份校验：任务创建时绑定的 id/LSN 必须与当前记录一致。

    同名核对点被作废后重新登记会得到新的 id 与 LSN；旧任务绝不能拿新标记
    冒充登记结果，也不能把旧标记用在新任务上。
    """
    cp = state.one_row(
        "SELECT * FROM checkpoints WHERE name=?", (job["checkpoint_name"],)
    )
    err = state.checkpoint_identity_error(job, cp)
    if err:
        raise HTTPException(409, err)
    return cp


def _ensure_no_active_job() -> None:
    active = ",".join(f"'{s}'" for s in state.ACTIVE_JOB_STATES)
    j = state.one_row(f"SELECT id, status FROM jobs WHERE status IN ({active})"
                      " ORDER BY id DESC LIMIT 1")
    if j:
        raise HTTPException(
            409, f"已有进行中的恢复任务 #{j['id']}（{j['status']}），一次只恢复一项"
        )


# ------------------------------------------------------------------ 基础

@app.get("/", summary="服务信息")
def root():
    return {"service": "水质数仓 PITR 恢复服务", "docs": "/docs",
            "api": ["/api/health", "/api/basebackup", "/api/checkpoints",
                    "/api/recoveries"]}


@app.get("/api/health", summary="健康检查")
def health():
    src = source.ping()
    archived = len([f for f in os.listdir(config.ARCHIVE_DIR)
                    if not f.startswith(".")]) if os.path.isdir(config.ARCHIVE_DIR) else 0
    return {
        "status": "ok" if src else "degraded",
        "source_reachable": src,
        "archive_dir": config.ARCHIVE_DIR,
        "archived_wal_files": archived,
    }


# ------------------------------------------------------------------ 基准备份

@app.get("/api/basebackup", tags=["backup"], summary="基准备份状态")
def backup_status():
    return {
        "latest_done": backuper.latest_done(),
        "running": state.one_row(
            "SELECT * FROM backups WHERE status='running'"
            " ORDER BY id DESC LIMIT 1"),
        "history": state.all_rows(
            "SELECT * FROM backups ORDER BY id DESC LIMIT 10"),
    }


@app.post("/api/basebackup", tags=["backup"], status_code=202,
          summary="制作物理基准备份（pg_basebackup，源库不停写）")
def make_backup(force: bool = Query(
        False, description="已存在备份时是否强制重做（更早核对点将过期）")):
    try:
        bid = backuper.start_backup(force=force)
    except backuper.BusyError as e:
        raise HTTPException(409, str(e))
    return {"id": bid, "status": "running"}


# ------------------------------------------------------------------ 核对点

@app.post("/api/checkpoints", tags=["checkpoint"], status_code=201,
          summary="登记命名核对点（pg_create_restore_point）")
def create_checkpoint(body: CheckpointIn):
    """并发同名请求最多一个成功；已确认名称继续拒绝重复登记。

    流程：先原子占位（pending，不携带任何旧 LSN）→ 用**本次新生成的唯一内部名**
    在源库写入恢复点 → 把本次调用返回的 LSN 绑定到该占位。源库已写标记但 API
    未登记就退出时，占位留在 pending；重启后统一作废（aborted），同名可重新
    登记。每次登记的内部名都不同，恢复端绝不会被历史同名标记误导。
    """
    source_name = f"pitr_{secrets.token_hex(8)}"[:63]
    try:
        cid = state.reserve_checkpoint(body.name, source_name)
    except state.CheckpointExists as e:
        raise HTTPException(409, str(e))
    try:
        lsn, wal_file = source.create_restore_point(source_name)
        cp = state.confirm_checkpoint(cid, body.name, lsn, wal_file)
    except state.CheckpointNotPending as e:
        # 并发同名请求已占位成功 / 重启清理，本次源库标记不可绑定
        state.abort_checkpoint(cid, str(e))
        raise HTTPException(409, str(e))
    except Exception as e:  # noqa: BLE001
        state.abort_checkpoint(cid, f"在源库创建命名恢复点失败: {e}")
        raise HTTPException(502, f"在源库创建命名恢复点失败: {e}")
    refresh_checkpoints()  # 立即检查一次，通常几秒内即 ready
    return _public_cp(state.one_row("SELECT * FROM checkpoints WHERE id=?", (cid,)))


@app.get("/api/checkpoints", tags=["checkpoint"], summary="核对点列表")
def list_checkpoints():
    return [_public_cp(c) for c in
            state.all_rows("SELECT * FROM checkpoints ORDER BY id")]


@app.get("/api/checkpoints/{name}", tags=["checkpoint"], summary="核对点详情")
def get_checkpoint(name: str):
    return _public_cp(_cp_or_404(name))


# ------------------------------------------------------------------ 恢复任务

@app.post("/api/recoveries", tags=["recovery"], status_code=202,
          summary="按核对点发起恢复（一次只恢复一项）")
def start_recovery(body: RecoveryIn):
    cp = _confirmed_cp_or_409(body.checkpoint)
    if cp["status"] == "expired":
        raise HTTPException(409, "核对点已过期（存在更新的基准备份），无法恢复")
    _ensure_no_active_job()
    jid = state.execute(
        "INSERT INTO jobs (checkpoint_name, checkpoint_id, checkpoint_lsn,"
        " status, created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (body.checkpoint, cp["id"], cp["restore_lsn"],
         "queued", state.now(), state.now()),
    )
    return state.one_row("SELECT * FROM jobs WHERE id=?", (jid,))


@app.get("/api/recoveries", tags=["recovery"], summary="恢复任务列表")
def list_recoveries():
    return state.all_rows("SELECT * FROM jobs ORDER BY id DESC LIMIT 50")


@app.get("/api/recoveries/{job_id}", tags=["recovery"], summary="恢复任务状态")
def get_recovery(job_id: int):
    return _job_or_404(job_id)


@app.post("/api/recoveries/{job_id}/retry", tags=["recovery"], status_code=202,
          summary="按原核对点重试（失败/取消/被取代后）")
def retry_recovery(job_id: int):
    job = _job_or_404(job_id)
    if job["status"] not in state.RETRYABLE_JOB_STATES:
        raise HTTPException(409, f"任务状态 {job['status']} 不可重试")
    # 重试只认任务创建时绑定的核对点身份；迁移期未补全的旧任务直接拒绝，
    # 绝不按名称去猜，避免用到后来同名重新登记的标记。
    if job.get("checkpoint_id") is None or not job.get("checkpoint_lsn"):
        raise HTTPException(
            409, f"任务 #{job_id} 缺少核对点身份快照，无法安全重试；"
                 "请对当前核对点重新发起恢复")
    cp = _job_cp_or_409(job)
    if cp["status"] == "expired":
        raise HTTPException(409, "核对点已过期（存在更新的基准备份），无法恢复")
    _ensure_no_active_job()
    # 重试仍绑定原任务登记时的 id/LSN，绝不改用后来同名登记的标记
    jid = state.execute(
        "INSERT INTO jobs (checkpoint_name, checkpoint_id, checkpoint_lsn,"
        " status, created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (job["checkpoint_name"], job["checkpoint_id"], job["checkpoint_lsn"],
         "queued", state.now(), state.now()),
    )
    return state.one_row("SELECT * FROM jobs WHERE id=?", (jid,))


@app.delete("/api/recoveries/{job_id}", tags=["recovery"],
            summary="取消进行中任务 / 停止已就绪实例")
def cancel_recovery(job_id: int):
    job = _job_or_404(job_id)
    if job["status"] in state.ACTIVE_JOB_STATES:
        state.update_job(job_id, cancel_requested=1, detail="取消请求已受理")
        return {"id": job_id, "status": job["status"], "cancel_requested": True}
    if job["status"] == "ready":
        if job.get("data_dir"):
            recoverer.stop_instance(job["data_dir"])
        state.update_job(job_id, status="stopped", detail="实例已停止")
        return state.one_row("SELECT * FROM jobs WHERE id=?", (job_id,))
    return job


# ------------------------------------------------------------------ 目标查询

def _ready_job_or_409(job_id: int) -> dict:
    job = _job_or_404(job_id)
    if job["status"] != "ready":
        raise HTTPException(
            409, f"恢复任务未就绪（当前状态 {job['status']}），目标查询未开放")
    return job


def _query_meta(job: dict) -> dict:
    cp = state.one_row("SELECT * FROM checkpoints WHERE name=?",
                       (job["checkpoint_name"],))
    return {
        "job_id": job["id"],
        "checkpoint": job["checkpoint_name"],
        # 报告任务绑定并校验过的 LSN，而不是名字当前可能已变化的记录
        "restore_lsn": job.get("checkpoint_lsn")
                       or (cp["restore_lsn"] if cp else None),
        "checkpoint_id": job.get("checkpoint_id"),
        "queried_instance": f"recovered-instance:{config.RECOVERY_PORT}"
                            f"（独立只读库，非生产源库）",
        "read_only": True,
    }


@app.get("/api/recoveries/{job_id}/samples", tags=["query"],
         summary="查询恢复实例中的样品（仅 ready 后开放）")
def query_samples(job_id: int,
                  station_no: str | None = Query(None, description="按站号过滤"),
                  sample_no: str | None = Query(None, description="按样品号过滤"),
                  limit: int = Query(100, ge=1, le=1000)):
    job = _ready_job_or_409(job_id)
    try:
        rows = recoverer.fetch_samples(station_no, sample_no, limit)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"查询恢复实例失败: {e}")
    return {**_query_meta(job), "row_count": len(rows), "rows": rows}


@app.post("/api/recoveries/{job_id}/query", tags=["query"],
          summary="只读 SQL 查询（演示用，限单条 SELECT）")
def adhoc_query(job_id: int, body: QueryIn):
    job = _ready_job_or_409(job_id)
    try:
        rows = recoverer.run_adhoc_query(body.sql)
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:  # noqa: BLE001
        raise HTTPException(502, f"查询恢复实例失败: {e}")
    return {**_query_meta(job), "row_count": len(rows), "rows": rows}
