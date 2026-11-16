"""SQLite 持久化：基准备份 / 核对点 / 恢复任务的状态全部落盘。

API 重启后据此续跑或按原点重试，不会丢状态、不会把半成品当成功。
"""
import secrets
import sqlite3
import threading
from datetime import datetime, timezone

from . import config

_LOCK = threading.RLock()
# 本进程启动标识：启动清理只作废上一个进程遗留的 pending，不误伤本进程在途登记
_BOOT_ID = secrets.token_hex(8)


class CheckpointExists(RuntimeError):
    """同名核对点已被确认，拒绝重复登记。"""


class CheckpointNotPending(RuntimeError):
    """登记占位已被并发请求或重启清理取代，不能把本次源库标记绑上去。"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS backups (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    path        TEXT NOT NULL,
    status      TEXT NOT NULL,          -- running | done | failed
    start_lsn   TEXT,                   -- 备份起始 WAL 位点（恢复可用性的下界）
    timeline    INTEGER,
    size_bytes  INTEGER,
    error       TEXT,
    created_at  TEXT NOT NULL,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS checkpoints (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,   -- 用户可见核对点名称（唯一）
    source_name TEXT NOT NULL,          -- 源库恢复点内部唯一名（每次登记新生成）
    restore_lsn TEXT NOT NULL,          -- pg_create_restore_point 本次调用返回的 LSN
    wal_file    TEXT NOT NULL,          -- 该 LSN 所在的 WAL 段文件名
    status      TEXT NOT NULL,          -- pending | waiting_wal | ready | expired | aborted
    error       TEXT,                   -- 登记失败/中断的原因
    boot_id     TEXT NOT NULL DEFAULT '', -- 占位进程标识；重启后据此作废旧 pending
    created_at  TEXT NOT NULL,
    ready_at    TEXT
);
CREATE TABLE IF NOT EXISTS jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    checkpoint_name  TEXT NOT NULL,
    checkpoint_id    INTEGER,           -- 与 checkpoints.id 绑定，防止同名错配
    checkpoint_lsn   TEXT,              -- 发起任务时登记的 LSN 快照
    status           TEXT NOT NULL,     -- queued | waiting_wal | preparing | recovering
                                        -- | verifying | ready | failed
                                        -- | superseded | stopped | cancelled
    detail           TEXT,
    error            TEXT,
    port             INTEGER,
    data_dir         TEXT,
    log_tail         TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    ready_at         TEXT
);
"""

# 进行中（未终结）的任务状态：一次只允许一个
ACTIVE_JOB_STATES = ("queued", "waiting_wal", "preparing", "recovering", "verifying")
# 允许发起重试的终结状态
RETRYABLE_JOB_STATES = ("failed", "stopped", "cancelled", "superseded")
# 已经确认、绑定到具体源库标记的核对点状态（pending/aborted 不算确认）
CONFIRMED_CHECKPOINT_STATES = ("waiting_wal", "ready", "expired")


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect() -> sqlite3.Connection:
    # isolation_level=None：自动提交模式，显式 BEGIN IMMEDIATE 才可控（占名乐观锁）
    con = sqlite3.connect(config.STATE_DB, timeout=30, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_db() -> None:
    with _LOCK:
        con = _connect()
        try:
            con.executescript(SCHEMA)
            _migrate(con)
            con.commit()
        finally:
            con.close()


def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}


def _migrate(con: sqlite3.Connection) -> None:
    """旧版本状态库就地补列，保证已有核对点/任务仍可查询与重试。"""
    cp_cols = _columns(con, "checkpoints")
    if "error" not in cp_cols:
        con.execute("ALTER TABLE checkpoints ADD COLUMN error TEXT")
    if "boot_id" not in cp_cols:
        con.execute("ALTER TABLE checkpoints ADD COLUMN boot_id TEXT NOT NULL"
                    " DEFAULT ''")
    if "source_name" not in cp_cols:
        con.execute("ALTER TABLE checkpoints ADD COLUMN source_name TEXT NOT NULL"
                    " DEFAULT ''")
        # 旧记录当初就是用可见名称在源库写的标记，按原语义续用
        con.execute("UPDATE checkpoints SET source_name=name WHERE source_name=''")
    job_cols = _columns(con, "jobs")
    if "checkpoint_id" not in job_cols:
        con.execute("ALTER TABLE jobs ADD COLUMN checkpoint_id INTEGER")
    if "checkpoint_lsn" not in job_cols:
        con.execute("ALTER TABLE jobs ADD COLUMN checkpoint_lsn TEXT")
    # 旧任务补上核对点身份快照。只做一次（meta 标记）：否则每次启动都会把
    # 无法安全绑定的旧任务重新挂到同名新身份上，绕过身份校验。
    migrated = con.execute(
        "SELECT value FROM meta WHERE key='job_identity_backfilled'"
    ).fetchone()
    if not migrated:
        con.execute(
            "UPDATE jobs SET checkpoint_id=("
            " SELECT id FROM checkpoints WHERE checkpoints.name=jobs.checkpoint_name"
            "), checkpoint_lsn=("
            " SELECT restore_lsn FROM checkpoints WHERE checkpoints.name=jobs.checkpoint_name"
            ") WHERE checkpoint_id IS NULL"
        )
        con.execute(
            "INSERT INTO meta (key, value) VALUES ('job_identity_backfilled', ?)",
            (now(),),
        )
    # 上一个进程遗留的 pending 说明登记流程在源库写标记之后、API 落库之前中断；
    # 作废它（不保留 LSN），同名可重新登记。本进程在途占位不受影响。
    con.execute(
        "UPDATE checkpoints SET status='aborted', restore_lsn='', wal_file='',"
        " error='API 重启时登记未完成，已作废；可用同名重新登记'"
        " WHERE status='pending' AND boot_id<>?",
        (_BOOT_ID,),
    )


def execute(sql: str, params: tuple = ()) -> int:
    with _LOCK:
        con = _connect()
        try:
            cur = con.execute(sql, params)
            con.commit()
            return cur.lastrowid
        finally:
            con.close()


def all_rows(sql: str, params: tuple = ()) -> list[dict]:
    with _LOCK:
        con = _connect()
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()


def one_row(sql: str, params: tuple = ()) -> dict | None:
    rows = all_rows(sql, params)
    return rows[0] if rows else None


def update_job(job_id: int, **fields) -> None:
    if not fields:
        return
    fields["updated_at"] = now()
    cols = ", ".join(f"{k}=?" for k in fields)
    execute(f"UPDATE jobs SET {cols} WHERE id=?", (*fields.values(), job_id))


# ---------------------------------------------------------------- 核对点登记

def reserve_checkpoint(name: str, source_name: str) -> int:
    """原子占位：同名核对点只允许一个请求进入登记流程。

    - 已确认（waiting_wal/ready/expired）→ CheckpointExists；
    - 仍有 pending 占位（并发同名请求正在进行）→ CheckpointExists；
    - 只有 aborted（上次登记中断且未确认）才允许同名重新登记。
    source_name 是写入源库的内部唯一标记名。占位阶段不写 restore_lsn，
    绝不让旧标记被误认。返回 checkpoints.id。
    """
    with _LOCK:
        con = _connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "SELECT id, status FROM checkpoints WHERE name=?", (name,)
            ).fetchone()
            if cur and cur["status"] != "aborted":
                con.rollback()
                raise CheckpointExists(f"核对点 {name} 已存在")
            if cur:
                # 上次登记中断（aborted）：删除旧身份，本次重新登记得到新 id，
                # 任何引用旧占位的任务都会因身份不一致被拒绝。
                con.execute("DELETE FROM checkpoints WHERE id=?", (cur["id"],))
            try:
                cur = con.execute(
                    "INSERT INTO checkpoints"
                    " (name, source_name, restore_lsn, wal_file, status, boot_id,"
                    "  created_at)"
                    " VALUES (?,?, '', '', 'pending', ?, ?)",
                    (name, source_name, _BOOT_ID, now()),
                )
            except sqlite3.IntegrityError:
                # 多进程并发占名：唯一约束保证只有一个插入成功
                con.rollback()
                raise CheckpointExists(f"核对点 {name} 已存在")
            cid = cur.lastrowid
            con.commit()
            return cid
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


def confirm_checkpoint(cid: int, name: str, lsn: str, wal_file: str) -> dict:
    """把本次源库标记的 LSN 绑定到占位记录上。

    只有仍处于 pending 且名称一致的占位才可确认；若占位已被并发请求或
    重启清理取代，抛 CheckpointNotPending——本次源库标记绝不复用到新登记上。
    """
    with _LOCK:
        con = _connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "SELECT name, status FROM checkpoints WHERE id=?", (cid,)
            ).fetchone()
            if (not cur or cur["name"] != name or cur["status"] != "pending"):
                con.rollback()
                raise CheckpointNotPending(
                    f"核对点 {name} 的登记占位已失效（并发登记或重启清理）"
                )
            con.execute(
                "UPDATE checkpoints SET restore_lsn=?, wal_file=?, status='waiting_wal',"
                " error=NULL, created_at=? WHERE id=?",
                (lsn, wal_file, now(), cid),
            )
            con.commit()
            return dict(con.execute(
                "SELECT * FROM checkpoints WHERE id=?", (cid,)
            ).fetchone())
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


def abort_checkpoint(cid: int, reason: str) -> None:
    """登记失败：作废占位，同名可重试；绝不留下可被恢复误用的半成品。"""
    with _LOCK:
        execute(
            "UPDATE checkpoints SET status='aborted', error=?, restore_lsn='',"
            " wal_file='' WHERE id=? AND status='pending'",
            (reason[-2000:], cid),
        )


def checkpoint_identity_error(job: dict, cp: dict | None) -> str | None:
    """任务绑定的核对点身份是否仍成立；返回错误描述，成立时返回 None。

    同名核对点被作废后重新登记会得到新的 id/LSN/源库标记名。任务只能使用
    创建时快照的原身份，绝不能把后来同名登记的标记当成自己的。
    """
    if not cp:
        return f"任务 #{job['id']} 的核对点 {job['checkpoint_name']} 不存在"
    if cp["status"] in ("pending", "aborted"):
        return (f"核对点 {cp['name']} 当前为 {cp['status']}，未绑定可恢复的 LSN")
    if job.get("checkpoint_id") is not None and job["checkpoint_id"] != cp["id"]:
        return (f"核对点 {cp['name']} 已被重新登记（身份变化），"
                f"任务 #{job['id']} 绑定的旧标记已作废")
    if job.get("checkpoint_lsn") and job["checkpoint_lsn"] != cp["restore_lsn"]:
        return (f"核对点 {cp['name']} 的 LSN 与任务 #{job['id']} 登记时不一致，"
                "拒绝用错配的标记恢复")
    if not cp.get("source_name"):
        return f"核对点 {cp['name']} 缺少源库标记身份，无法恢复"
    return None
