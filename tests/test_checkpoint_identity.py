"""核对点登记身份衔接的并发 / 中断用例。

不依赖真实 PostgreSQL：source.create_restore_point 用可控假实现替代，
重点验证「同名并发最多确认一个」「中断后同名重试不复用旧标记」
「恢复只认已确认身份」这些 API 与状态层语义。
"""
import threading

import pytest
from fastapi.testclient import TestClient

from app import config, main, source, state


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "api.db"))
    monkeypatch.setattr(config, "ARCHIVE_DIR", str(tmp_path / "archive"))
    monkeypatch.setattr(config, "BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.setattr(config, "RECOVERY_DIR", str(tmp_path / "recoveries"))
    monkeypatch.setattr(config, "POLL_INTERVAL", 0.05)
    for p in (config.ARCHIVE_DIR, config.BACKUP_DIR, config.RECOVERY_DIR):
        (tmp_path / "archive").mkdir(exist_ok=True)
    # 预置一份“已完成”的备份，避免 lifespan 触发真实 pg_basebackup
    state.init_db()
    state.execute(
        "INSERT INTO backups (path, status, start_lsn, timeline, created_at)"
        " VALUES (?,?,?,?,?)",
        (config.BACKUP_DIR + "/base", "done", "0/1000", 1, state.now()),
    )
    (tmp_path / "recoveries").mkdir(exist_ok=True)
    return tmp_path


@pytest.fixture()
def client(env):
    # 不使用 with：避免 lifespan 退出时 set 全局 _stop，影响同进程其他用例
    c = TestClient(main.app)
    c.__enter__()
    yield c
    c.__exit__(None, None, None)


class FakeSource:
    """线程安全的自增 LSN 生成器，记录每次被调用的内部标记名。"""

    def __init__(self):
        self.calls: list[str] = []
        self._n = 0
        self._lock = threading.Lock()
        self.gate: threading.Event | None = None

    def create_restore_point(self, name: str) -> tuple[str, str]:
        with self._lock:
            self.calls.append(name)
            self._n += 1
            n = self._n
        if self.gate is not None:
            self.gate.wait(5)
        return f"0/{0x1000 * n:X}", f"WAL{n:024X}"


@pytest.fixture()
def fake_source(monkeypatch):
    fs = FakeSource()
    monkeypatch.setattr(source, "create_restore_point", fs.create_restore_point)
    return fs


def test_concurrent_same_name_only_one_confirmed(env, fake_source):
    # 用 gate 把“已占名、尚未写源库”的窗口拉长，制造真正的并发重叠
    fake_source.gate = threading.Event()
    barrier = threading.Barrier(8)
    results: list[tuple[int, dict]] = []

    def submit():
        barrier.wait()
        with TestClient(main.app) as c:
            r = c.post("/api/checkpoints", json={"name": "cp-race"})
            results.append((r.status_code, r.json()))

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for t in threads:
        t.start()
    # 等其他 7 个请求都撞上 409 后，再放行唯一占位者
    import time as _time
    for _ in range(100):
        if sum(1 for code, _ in results if code == 409) >= 7:
            break
        _time.sleep(0.05)
    fake_source.gate.set()
    for t in threads:
        t.join(10)

    codes = sorted(code for code, _ in results)
    assert codes == [201] + [409] * 7, results
    # 只有占位成功的那一个请求真正写入了源库恢复点
    assert len(fake_source.calls) == 1

    cps = state.all_rows("SELECT * FROM checkpoints WHERE name='cp-race'")
    assert len(cps) == 1
    assert cps[0]["status"] in state.CONFIRMED_CHECKPOINT_STATES
    assert cps[0]["restore_lsn"] == "0/1000"


def test_confirmed_name_rejects_duplicate(env, client, fake_source):
    r1 = client.post("/api/checkpoints", json={"name": "cp1"})
    assert r1.status_code == 201
    r2 = client.post("/api/checkpoints", json={"name": "cp1"})
    assert r2.status_code == 409
    # 拒绝的请求没有再去源库写第二个同名标记
    assert len(fake_source.calls) == 1


def test_interrupted_registration_aborts_on_restart_and_retry_gets_new_identity(
        env, client, fake_source):
    # 1) 模拟：源库已写标记，但 API 在确认前退出 → 状态库里只剩 pending 占位
    old_source_name = "pitr_old"
    cid_old = state.reserve_checkpoint("cp-crash", old_source_name)
    pending = state.one_row("SELECT * FROM checkpoints WHERE id=?", (cid_old,))
    assert pending["status"] == "pending" and pending["restore_lsn"] == ""

    # 2) 模拟进程崩溃后重启：在本进程视角下，把该占位标成上一个进程留下的，
    #    再 init_db（启动清理）——应作废它，而不是留着冒充已登记
    state.execute("UPDATE checkpoints SET boot_id='previous-boot' WHERE id=?",
                  (cid_old,))
    state.init_db()
    aborted = state.one_row("SELECT * FROM checkpoints WHERE id=?", (cid_old,))
    assert aborted["status"] == "aborted"
    assert aborted["restore_lsn"] == ""

    # 3) 同名重试：成功，且拿到全新的内部标记名与 LSN（没有复用旧标记）
    r = client.post("/api/checkpoints", json={"name": "cp-crash"})
    assert r.status_code == 201, r.text
    new = r.json()
    assert new["status"] in state.CONFIRMED_CHECKPOINT_STATES
    # 内部标记名不对外暴露，从状态库核对：本次是全新身份
    assert "source_name" not in new
    stored = state.one_row("SELECT * FROM checkpoints WHERE id=?", (new["id"],))
    assert stored["source_name"] != old_source_name
    assert stored["source_name"] in fake_source.calls
    assert new["restore_lsn"]

    # 4) 旧占位的确认请求（代表被中断的旧请求迟到返回）必须被拒绝
    with pytest.raises(state.CheckpointNotPending):
        state.confirm_checkpoint(cid_old, "cp-crash", "0/DEAD", "WALDEAD")


def test_pending_or_aborted_checkpoint_cannot_start_recovery(env, client, fake_source):
    cid = state.reserve_checkpoint("cp-pending", "pitr_pending")
    r = client.post("/api/recoveries", json={"checkpoint": "cp-pending"})
    assert r.status_code == 409
    assert "pending" in r.json()["detail"]

    state.abort_checkpoint(cid, "模拟中断")
    r = client.post("/api/recoveries", json={"checkpoint": "cp-pending"})
    assert r.status_code == 409
    assert "aborted" in r.json()["detail"]
    assert state.all_rows("SELECT * FROM jobs") == []


def test_job_bound_to_old_identity_rejected_after_reregistration(
        env, client, fake_source):
    # 先建立并确认一个核对点及其失败任务
    r = client.post("/api/checkpoints", json={"name": "cp-id"})
    assert r.status_code == 201
    old_cp = r.json()
    r = client.post("/api/recoveries", json={"checkpoint": "cp-id"})
    assert r.status_code == 202
    job = r.json()
    assert job["checkpoint_id"] == old_cp["id"]
    assert job["checkpoint_lsn"] == old_cp["restore_lsn"]
    state.update_job(job["id"], status="failed", error="模拟失败")

    # 该核对点被作废后同名重新登记，身份（id / LSN / 内部标记名）全部变化
    state.execute("UPDATE checkpoints SET status='aborted' WHERE id=?",
                  (old_cp["id"],))
    r = client.post("/api/checkpoints", json={"name": "cp-id"})
    assert r.status_code == 201
    new_cp = r.json()
    assert new_cp["id"] != old_cp["id"]
    assert new_cp["restore_lsn"] != old_cp["restore_lsn"]

    # 旧任务的 retry 不得使用新登记的标记
    r = client.post(f"/api/recoveries/{job['id']}/retry")
    assert r.status_code == 409
    assert "旧标记" in r.json()["detail"] or "不一致" in r.json()["detail"]
    assert not state.one_row(
        "SELECT id FROM jobs WHERE id>? AND status='queued'", (job["id"],))


def test_retry_of_original_checkpoint_still_works(env, client, fake_source):
    r = client.post("/api/checkpoints", json={"name": "cp-retry"})
    assert r.status_code == 201
    cp = r.json()
    r = client.post("/api/recoveries", json={"checkpoint": "cp-retry"})
    job = r.json()
    state.update_job(job["id"], status="failed", error="模拟失败")

    r = client.post(f"/api/recoveries/{job['id']}/retry")
    assert r.status_code == 202, r.text
    retried = r.json()
    assert retried["id"] != job["id"]
    assert retried["checkpoint_id"] == cp["id"]
    assert retried["checkpoint_lsn"] == cp["restore_lsn"]
    assert retried["status"] == "queued"
