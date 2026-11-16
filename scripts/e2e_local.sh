#!/usr/bin/env bash
# 无 Docker 环境下的本地端到端冒烟：用本机 PostgreSQL 16 二进制 + uvicorn 直跑
# （Docker Compose 仍是正式启动方式；本脚本用于开发机自测）。
#
# 需要：PG16 bin（PG_BIN 或 pg_config --bindir）、python 依赖已安装。
# 用法：bash scripts/e2e_local.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PG_BIN="${PG_BIN:-$(pg_config --bindir 2>/dev/null || true)}"
if [[ ! -x "$PG_BIN/initdb" ]]; then
  echo "找不到 PostgreSQL 16 二进制，请设置 PG_BIN" >&2
  exit 1
fi
PG_LIB="$(dirname "$PG_BIN")/lib"
export LD_LIBRARY_PATH="$PG_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

TMP="$(mktemp -d /tmp/pitr-e2e.XXXXXX)"
PGDATA="$TMP/pgdata"
ARCHIVE="$TMP/archive"
BACKUPS="$TMP/backups"
RECOVERIES="$TMP/recoveries"
STATE="$TMP/state"
SOCK="$TMP/sock"
PGPORT=55440
APIPORT="${APIPORT:-8800}"
API_PID=""
cleanup() {
  [[ -n "$API_PID" ]] && kill "$API_PID" 2>/dev/null || true
  "$PG_BIN/pg_ctl" -D "$PGDATA" -m immediate stop >/dev/null 2>&1 || true
  # 恢复实例可能还活着
  pkill -f "$RECOVERIES" >/dev/null 2>&1 || true
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then rm -rf "$TMP"; else echo "保留临时目录：$TMP"; fi
}
trap cleanup EXIT
mkdir -p "$ARCHIVE" "$BACKUPS" "$RECOVERIES" "$STATE" "$SOCK"

echo "== init source cluster =="
"$PG_BIN/initdb" -D "$PGDATA" -U postgres --auth=trust >/dev/null
cat >> "$PGDATA/postgresql.conf" <<EOF
port = $PGPORT
listen_addresses = '127.0.0.1'
unix_socket_directories = '$SOCK'
wal_level = replica
archive_mode = on
archive_command = 'cp %p $ARCHIVE/.%f.tmp && mv $ARCHIVE/.%f.tmp $ARCHIVE/%f'
max_wal_senders = 5
EOF
"$PG_BIN/pg_ctl" -D "$PGDATA" -l "$TMP/pg.log" -w -t 60 start >/dev/null
PSQL=("$PG_BIN/psql" -h "$SOCK" -p "$PGPORT" -U postgres -At)
"${PSQL[@]}" -c "CREATE DATABASE water" >/dev/null
"${PSQL[@]}" -d water -c "$(cat "$ROOT/db/init/01_schema.sql")" >/dev/null

echo "== start api =="
cd "$ROOT/api"
SOURCE_HOST="$SOCK" SOURCE_PORT="$PGPORT" SOURCE_DB=water \
SOURCE_USER=postgres SOURCE_PASSWORD=postgres \
ARCHIVE_DIR="$ARCHIVE" BACKUP_DIR="$BACKUPS" RECOVERY_DIR="$RECOVERIES" \
STATE_DB="$STATE/api.db" RECOVERY_HOST=127.0.0.1 RECOVERY_PORT=55441 \
RECOVERY_SOCKET_DIR="$SOCK" POLL_INTERVAL=0.5 RECOVERY_TIMEOUT=120 \
PATH="$PG_BIN:$PATH" \
  python3 -m uvicorn app.main:app --host 127.0.0.1 --port "$APIPORT" \
  >"$TMP/api.log" 2>&1 &
API_PID=$!

for _ in $(seq 1 60); do
  if curl -fsS -m 2 "http://127.0.0.1:$APIPORT/api/health" >/dev/null 2>&1; then break; fi
  if ! kill -0 "$API_PID" 2>/dev/null; then
    echo "API 启动失败：" >&2; tail -40 "$TMP/api.log" >&2; exit 1
  fi
  sleep 1
done

API_URL="http://127.0.0.1:$APIPORT" \
DEMO_PSQL="$PG_BIN/psql -h $SOCK -p $PGPORT -U postgres -d water" \
  python3 "$ROOT/scripts/demo.py"

API_URL="http://127.0.0.1:$APIPORT" \
DEMO_PSQL="$PG_BIN/psql -h $SOCK -p $PGPORT -U postgres -d water" \
  python3 "$ROOT/scripts/race_demo.py"
