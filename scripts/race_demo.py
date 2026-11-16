#!/usr/bin/env python3
"""核对点身份衔接演示：并发同名登记 + 中断后同名重试。

用法（Compose 环境）：
    docker compose up -d --build
    python3 scripts/race_demo.py

环境变量：
    API_URL    默认 http://localhost:8000
    DEMO_PSQL  在源库执行 SQL 的命令前缀
               默认 "docker compose exec -T db psql -U postgres -d water"

流程：
  1. 8 个线程同时登记同名核对点 race -> 恰好 1 个成功，其余 409；
  2. 已确认名称再次登记 -> 409（名称未被重复确认）；
  3. 模拟“源库已写标记、API 未登记就退出”：直接向源库写一个同名可见恢复点，
     证明恢复不会因此停到那个“孤儿”同名标记；
  4. 该核对点之后写入一行，恢复必须看不到它。
"""
import concurrent.futures
import json
import os
import shlex
import subprocess
import sys
import time
import urllib.error
import urllib.request

API = os.environ.get("API_URL", "http://localhost:8000")
DEMO_PSQL = os.environ.get(
    "DEMO_PSQL", "docker compose exec -T db psql -U postgres -d water")

FAILED: list[str] = []


def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        API + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            return resp.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            return e.code, {"raw": raw}


def psql(sql: str) -> None:
    subprocess.run(shlex.split(DEMO_PSQL) + ["-v", "ON_ERROR_STOP=1", "-c", sql],
                   check=True, capture_output=True, text=True)


def psql_scalar(sql: str) -> str:
    r = subprocess.run(shlex.split(DEMO_PSQL)
                       + ["-tA", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       check=True, capture_output=True, text=True)
    return r.stdout.strip()


def wait_for(desc: str, fn, timeout: int = 300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = fn()
        if r:
            return r
        time.sleep(2)
    raise SystemExit(f"超时：{desc}")


def check(desc: str, cond: bool) -> None:
    print(f"  [{'PASS' if cond else 'FAIL'}] {desc}")
    if not cond:
        FAILED.append(desc)


def main() -> int:
    print("== 0. 等待 API 与基准备份 ==")
    wait_for("API 健康", lambda: api("GET", "/api/health")[1].get("status") == "ok")
    wait_for("基准备份完成",
             lambda: (api("GET", "/api/basebackup")[1].get("latest_done") or {}).get(
                 "status") == "done")

    print("== 1. 8 个并发同名登记 race ==")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(
            lambda _: api("POST", "/api/checkpoints", {"name": "race"}), range(8)))
    codes = sorted(c for c, _ in results)
    check("恰好 1 个登记成功（201）", codes.count(201) == 1)
    check("其余 7 个被拒绝（409）", codes.count(409) == 7)

    cp = api("GET", "/api/checkpoints/race")[1]
    wait_for("核对点 race 归档就绪",
             lambda: api("GET", "/api/checkpoints/race")[1].get("status") == "ready")
    print(f"  race 就绪：LSN={cp['restore_lsn']} WAL={cp['wal_file']}")

    print("== 2. 已确认名称再次登记 ==")
    code, body = api("POST", "/api/checkpoints", {"name": "race"})
    check("重复登记返回 409", code == 409)

    print("== 3. 源库写入同名“孤儿”恢复点（模拟中断） ==")
    # 与用户可见名称完全相同，若按名称恢复会停在这里而到不了后续登记位置
    psql("SELECT pg_create_restore_point('race')")
    psql("SELECT pg_switch_wal()")
    psql("INSERT INTO samples (sample_no, station_no, ph)"
         " VALUES ('S-RACE-AFTER', 'ST-09', 6.11)")
    print("  已写入同名标记与 S-RACE-AFTER（在 race 登记之后）")

    print("== 4. 恢复 race ==")
    code, job = api("POST", "/api/recoveries", {"checkpoint": "race"})
    assert code == 202, f"发起恢复失败: {code} {job}"
    jid = job["id"]
    final = wait_for("任务完成",
                     lambda: api("GET", f"/api/recoveries/{jid}")[1].get("status")
                     in ("ready", "failed", "cancelled"))
    job = api("GET", f"/api/recoveries/{jid}")[1]
    check("恢复就绪（未把错位恢复报成功）", job["status"] == "ready")

    rows = api("GET", f"/api/recoveries/{jid}/samples?limit=1000")[1]
    check("恢复报告 as-of LSN 等于登记 LSN", rows["restore_lsn"] == cp["restore_lsn"])
    names = {r["sample_no"] for r in rows["rows"]}
    check("不包含登记之后/同名孤儿标记之后的 S-RACE-AFTER",
          "S-RACE-AFTER" not in names)
    check("包含登记前已有数据 S-2026-0001", "S-2026-0001" in names)

    print()
    if FAILED:
        print(f"共 {len(FAILED)} 项校验失败：")
        for f in FAILED:
            print(f"  - {f}")
        return 1
    print("全部校验通过 ✅")
    print(f"Swagger 文档：{API}/docs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
