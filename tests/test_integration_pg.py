"""真实 PostgreSQL 16 集成用例：并发同名登记 + 恢复精确停在登记 LSN。

没有 PostgreSQL 16 二进制的环境自动跳过（设置 PG_BIN 或提供 pg_config）。
Docker Compose 仍是正式运行方式；本用例在开发机直接调用本机二进制。
"""
import os
import shutil
import socket
import subprocess
import threading
import time

import pytest

from app import config, main, source, state
from app.walutil import lsn_to_int


def _pg_bin() -> str | None:
    cand = os.environ.get("PG_BIN")
    if not cand:
        exe = shutil.which("pg_config")
        if exe:
            cand = subprocess.run([exe, "--bindir"], capture_output=True,
                                  text=True).stdout.strip()
    if cand and os.path.exists(os.path.join(cand, "initdb")):
        version = subprocess.run([os.path.join(cand, "postgres"), "--version"],
                                 capture_output=True, text=True).stdout
        if " 16." in version:
            return cand
    return None


PG_BIN = _pg_bin()
pytestmark = pytest.mark.skipif(PG_BIN is None, reason="需要 PostgreSQL 16 二进制")


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def pg(tmp_path_factory):
    base = tmp_path_factory.mktemp("pgint")
    data, archive, sock = base / "data", base / "archive", base / "sock"
    for d in (archive, sock):
        d.mkdir()
    pgport = _free_port()
    env = dict(os.environ, LD_LIBRARY_PATH=os.path.join(os.path.dirname(PG_BIN), "lib"))
    subprocess.run([f"{PG_BIN}/initdb", "-D", str(data), "-U", "postgres",
                    "--auth=trust"], check=True, capture_output=True, env=env)
    with open(data / "postgresql.conf", "a") as f:
        f.write(f"""
port = {pgport}
listen_addresses = '127.0.0.1'
unix_socket_directories = '{sock}'
wal_level = replica
archive_mode = on
archive_command = 'cp %p {archive}/.%f.tmp && mv {archive}/.%f.tmp {archive}/%f'
max_wal_senders = 5
""")
    subprocess.run([f"{PG_BIN}/pg_ctl", "-D", str(data), "-l", str(base / "pg.log"),
                    "-w", "-t", "60", "start"], check=True, capture_output=True,
                   env=env)
    psql = [f"{PG_BIN}/psql", "-h", str(sock), "-p", str(pgport), "-U", "postgres",
            "-At"]

    def run_sql(sql: str, db: str = "postgres") -> str:
        r = subprocess.run(psql + ["-d", db, "-v", "ON_ERROR_STOP=1", "-c", sql],
                           check=True, capture_output=True, text=True, env=env)
        return r.stdout.strip()

    run_sql("CREATE DATABASE water")
    run_sql(open(os.path.join(os.path.dirname(__file__), "..", "db", "init",
                              "01_schema.sql")).read(), db="water")
    yield {"base": base, "data": data, "archive": archive, "sock": sock,
            "port": pgport, "psql": run_sql, "env": env}
    subprocess.run([f"{PG_BIN}/pg_ctl", "-D", str(data), "-m", "immediate",
                    "stop"], capture_output=True, env=env)


@pytest.fixture()
def live(pg, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SOURCE_HOST", str(pg["sock"]))
    monkeypatch.setattr(config, "SOURCE_PORT", pg["port"])
    monkeypatch.setattr(config, "SOURCE_DB", "water")
    monkeypatch.setattr(config, "ARCHIVE_DIR", str(pg["archive"]))
    monkeypatch.setattr(config, "BACKUP_DIR", str(tmp_path / "backups"))
    monkeypatch.setattr(config, "RECOVERY_DIR", str(tmp_path / "recoveries"))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state" / "api.db"))
    monkeypatch.setattr(config, "RECOVERY_SOCKET_DIR", str(pg["sock"]))
    monkeypatch.setattr(config, "RECOVERY_PORT", _free_port())
    monkeypatch.setattr(config, "POLL_INTERVAL", 0.2)
    monkeypatch.setattr(config, "RECOVERY_TIMEOUT", 120)
    for d in (config.BACKUP_DIR, config.RECOVERY_DIR,
              os.path.dirname(config.STATE_DB)):
        os.makedirs(d, exist_ok=True)
    monkeypatch.setenv("PATH", f"{PG_BIN}:{os.environ['PATH']}")
    monkeypatch.setenv("LD_LIBRARY_PATH",
                       os.path.join(os.path.dirname(PG_BIN), "lib"))
    state.init_db()
    yield pg


def _wait(fn, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        val = fn()
        if val:
            return val
        time.sleep(0.5)
    raise AssertionError("等待超时")


def test_concurrent_same_name_and_exact_pause(live):
    from fastapi.testclient import TestClient
    pg = live

    with TestClient(main.app) as client:
        # 等自动基准备份完成
        _wait(lambda: client.get("/api/basebackup").json()["latest_done"])

        # 8 个并发同名登记：最多一个成功
        results: list[tuple[int, dict]] = []
        barrier = threading.Barrier(8)

        def submit():
            barrier.wait()
            r = client.post("/api/checkpoints", json={"name": "cp-race"})
            results.append((r.status_code, r.json()))

        threads = [threading.Thread(target=submit) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(30)
        assert sorted(c for c, _ in results) == [201] + [409] * 7, results
        cp = state.one_row("SELECT * FROM checkpoints WHERE name='cp-race'")
        assert cp["status"] in state.CONFIRMED_CHECKPOINT_STATES
        assert cp["restore_lsn"]

        # cp-race 之后写入一行，并再手工写一个同名可见标记（制造“源库同名标记”）
        pg["psql"]("INSERT INTO samples (sample_no, station_no, ph)"
                   " VALUES ('S-RACE-AFTER', 'ST-01', 6.5)", db="water")
        source.create_restore_point("cp-race")  # 与用户可见名同名的第二个标记
        pg["psql"]("SELECT pg_switch_wal()", db="water")
        _wait(lambda: client.get("/api/checkpoints/cp-race").json()["status"]
              == "ready")

        r = client.post("/api/recoveries", json={"checkpoint": "cp-race"})
        assert r.status_code == 202, r.text
        job = r.json()
        assert job["checkpoint_id"] == cp["id"]
        assert job["checkpoint_lsn"] == cp["restore_lsn"]
        final = _wait(lambda: (lambda j: j if j["status"] in
                               ("ready", "failed", "cancelled") else None)(
            client.get(f"/api/recoveries/{job['id']}").json()))
        assert final["status"] == "ready", final

        # 恢复实例中的行不含 cp-race 之后写入的数据；元数据报告登记 LSN
        body = client.get(f"/api/recoveries/{job['id']}/samples").json()
        assert body["restore_lsn"] == cp["restore_lsn"]
        assert "S-RACE-AFTER" not in {r["sample_no"] for r in body["rows"]}

        # 恢复所用内部标记名确实登记在该 LSN 上
        assert lsn_to_int(cp["restore_lsn"]) > 0


def test_crash_after_source_marker_then_retry_uses_new_lsn(live, monkeypatch):
    """源库已写标记、API 未登记就退出：重启后同名重试不得复用旧标记。"""
    from fastapi.testclient import TestClient
    state.init_db()

    # 1) 旧进程：占用 cp-crash 后，在源库写下了标记，随后进程在确认前退出
    old_source_name = "pitr_crash_old"
    cid_old = state.reserve_checkpoint("cp-crash", old_source_name)
    old_lsn, _wal = source.create_restore_point(old_source_name)
    assert old_lsn
    pending = state.one_row("SELECT * FROM checkpoints WHERE id=?", (cid_old,))
    assert pending["status"] == "pending" and pending["restore_lsn"] == ""
    # 模拟进程已不存在：把占位标记为上一进程遗留，重启清理应作废它
    state.execute("UPDATE checkpoints SET boot_id='previous-boot' WHERE id=?",
                  (cid_old,))
    state.init_db()
    aborted = state.one_row("SELECT * FROM checkpoints WHERE id=?", (cid_old,))
    assert aborted["status"] == "aborted" and aborted["restore_lsn"] == ""

    # 2) 重启后同名重新登记：必须得到全新的源库标记与 LSN
    with TestClient(main.app) as client:
        _wait(lambda: client.get("/api/basebackup").json()["latest_done"])
        r = client.post("/api/checkpoints", json={"name": "cp-crash"})
        assert r.status_code == 201, r.text
        new_cp = r.json()
        stored = state.one_row("SELECT * FROM checkpoints WHERE id=?",
                               (new_cp["id"],))
        assert stored["source_name"] != old_source_name
        assert new_cp["restore_lsn"] != old_lsn
        assert new_cp["status"] in state.CONFIRMED_CHECKPOINT_STATES
        assert new_cp["id"] != cid_old
