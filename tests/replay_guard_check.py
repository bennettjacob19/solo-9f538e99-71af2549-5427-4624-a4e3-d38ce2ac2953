#!/usr/bin/env python3
"""自包含端到端：真实 walsender 重放已固化清理的旧事务时，存储层必须拦截。

为什么需要这个测试
------------------
正常运行下复制槽 confirmed_flush_lsn 始终 >= 保留水位，PostgreSQL 还会回收
旧 WAL，因此"已清理事务被重新投递给存储层"在生产里属于需要纵深防御的场景。
本测试用一套独立的小型 PostgreSQL + 独立 SQLite 库确定性地构造该场景：

  t1、t2 提交并被捕获 → 在 t2 固化基线（水位=t2，t1/t2 的 txns 记录被删）
  → t3 提交被捕获 → 停应用 → 用真实复制协议 start_lsn=0 重放该槽全部历史：
     * t1、t2（LSN <= 水位）必须被 HistoryStore 判为 before_horizon，不落盘；
     * 重放前后事务数/行数/水位零变化，已清理事务不复活；
     * 重启应用后 t3 之后的新事务仍正常捕获（流不被拦截卡死）。

环境变量：PGBIN（PostgreSQL bin 目录）、PGPORT（默认 55444）、
SQLDIR（默认 /tmp/replay-guard-sqlite）、PGDATA（默认 /tmp/replay-guard-pgdata）。
用法：python3 tests/replay_guard_check.py
"""
import json
import os
import signal
import socket
import subprocess
import time
import urllib.request

import psycopg2
import psycopg2.extras

from app.decoder import parse_message
from app.storage import HistoryStore, lsn_to_num

PGBIN = os.environ.get("PGBIN", "/tmp/pgdeb/extracted/usr/lib/postgresql/15/bin")
PGPORT = int(os.environ.get("PGPORT", "55444"))
PGDATA = os.environ.get("PGDATA", "/tmp/replay-guard-pgdata")
PGSOCK = os.environ.get("PGSOCK", "/tmp/replay-guard-pgsock")
SQLDIR = os.environ.get("SQLDIR", "/tmp/replay-guard-sqlite")
SQLITE = os.path.join(SQLDIR, "history.db")
APPPORT = int(os.environ.get("APPPORT", "8022"))
BASE = f"http://127.0.0.1:{APPPORT}"
DSN = f"host={PGSOCK} port={PGPORT} dbname=postgres user=postgres"
SLOT = "artifacts_history_slot"
SHA = "a" * 64

failures: list[str] = []


def check(name, cond, detail=""):
    print(("PASS" if cond else "FAIL") + f": {name} {detail}", flush=True)
    if not cond:
        failures.append(name)


def run(cmd, check=True, **kw):
    return subprocess.run(cmd, capture_output=True, text=True,
                          check=check, **kw)


def psql(sql):
    return run([f"{PGBIN}/psql", "-h", PGSOCK, "-p", str(PGPORT),
                "-U", "postgres", "-tAc", sql]).stdout.strip()


def api(path, method="GET", payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def wait_api(path, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(BASE + path, timeout=2) as r:
                return json.loads(r.read())
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f"app not ready: {path}")


def app_pid():
    out = subprocess.run(["ps", "-eo", "pid,args"],
                         capture_output=True, text=True).stdout
    for line in out.splitlines():
        if f"uvicorn app.main:app" in line and f"--port {APPPORT}" in line:
            return int(line.split()[0])
    return None


def start_app():
    os.makedirs(SQLDIR, exist_ok=True)
    log = open("/tmp/replay-guard-app.log", "ab")
    env = dict(os.environ, PG_DSN=DSN, SQLITE_PATH=SQLITE,
               REPLICATION_SLOT=SLOT)
    subprocess.Popen(
        ["python3", "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(APPPORT)],
        env=env, stdout=log, stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            if wait_api("/api/progress", timeout=1).get("phase") == "STREAMING":
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("app did not reach STREAMING")


def stop_app():
    pid = app_pid()
    if pid:
        os.kill(pid, signal.SIGTERM)
        for _ in range(30):
            if app_pid() is None:
                break
            time.sleep(0.5)
    time.sleep(1)


def main():
    # 0) 全新独立 PG（logical）+ 独立 SQLite；REUSE_PG=1 时复用已在运行的库
    run(["rm", "-rf", SQLDIR])
    os.makedirs(PGSOCK, exist_ok=True)
    reuse_pg = os.environ.get("REUSE_PG") == "1"
    if not reuse_pg:
        run(["rm", "-rf", PGDATA, PGSOCK])
        os.makedirs(PGSOCK, exist_ok=True)
        run([f"{PGBIN}/initdb", "-D", PGDATA, "-U", "postgres",
             "--auth=trust", "-E", "UTF8", "--no-sync"])
        with open(f"{PGDATA}/postgresql.conf", "a") as fh:
            fh.write(
                f"\nport={PGPORT}\nunix_socket_directories='{PGSOCK}'\n"
                "listen_addresses=''\nwal_level=logical\n"
                "max_wal_senders=10\nmax_replication_slots=10\n"
                # 测试期间保留足够 WAL，保证临时槽能重放全部测试事务
                "wal_keep_size=256MB\n"
            )
        run([f"{PGBIN}/pg_ctl", "-D", PGDATA, "-l",
             "/tmp/replay-guard-pg.log", "start", "-w"])
        started_here = True
    else:
        started_here = False
    try:
        if reuse_pg:
            # 复用模式：清掉上轮残留，保证从干净状态开始
            psql("SELECT pg_drop_replication_slot(slot_name) "
                 "FROM pg_replication_slots WHERE slot_name=%s" %
                 ("'" + SLOT + "'"))
            psql("DROP TABLE IF EXISTS artifacts")
        psql("CREATE TABLE artifacts (artifact_no INTEGER PRIMARY KEY,"
             " service_name TEXT NOT NULL, version TEXT NOT NULL,"
             " sha256 TEXT NOT NULL)")
        psql("INSERT INTO artifacts VALUES (1,'s','1',%s)" % ("'" + SHA + "'"))

        # 1) 先建临时重放槽（起点必须早于应用首基线与全部测试事务），
        #    再启动应用完成首次基线。
        replay_slot = SLOT + "_replay"
        psql(f"SELECT pg_drop_replication_slot('{replay_slot}') "
             "WHERE EXISTS (SELECT 1 FROM pg_replication_slots "
             f"WHERE slot_name='{replay_slot}')")
        psql(f"SELECT pg_create_logical_replication_slot('{replay_slot}',"
             " 'test_decoding')")

        start_app()
        check("应用进入 STREAMING", wait_api("/api/progress")["phase"]
              == "STREAMING")

        def write(no, version):
            code, _ = api("/api/source/operations", "POST", {
                "operations": [{
                    "op": "insert", "artifact_no": no, "service_name": "s",
                    "version": version, "sha256": SHA}]})
            assert code == 200, code

        def wait_txn_count(target, timeout=20):
            deadline = time.time() + timeout
            while time.time() < deadline:
                if wait_api("/api/progress")["captured_transactions"] >= target:
                    return
                time.sleep(0.5)
            raise RuntimeError("txn count not reached")

        # t1、t2、t3 三个源事务
        write(2, "t1"); wait_txn_count(1)
        write(3, "t2"); wait_txn_count(2)
        write(4, "t3"); wait_txn_count(3)

        points = wait_api("/api/points")["points"]
        txn_points = [p for p in points if p["kind"] == "TXN"]
        t2_lsn = txn_points[1]["lsn"]   # 固化点
        check("捕获到三个源事务", len(txn_points) == 3, str(len(txn_points)))

        # 2) 在 t2 固化：水位=t2，t1/t2 事务记录被清理，t3 保留
        code, fr = api("/api/retention/freeze", "POST", {"point": t2_lsn})
        check("固化成功", code == 200, str(fr))
        check("固化清除了 t1/t2 两个事务", fr.get("removed_txns") == 2,
              str(fr.get("removed_txns")))

        store = HistoryStore(SQLITE)
        horizon = store.get_progress()["retention_horizon_lsn"]
        horizon_num = lsn_to_num(horizon)
        txns_before = store.get_progress()["captured_transactions"]
        rows_before = store.get_progress()["history_rows"]
        check("保留水位等于固化点", horizon == t2_lsn, horizon)

        # 3) 停应用，用预建的临时槽（起点早于 t1）重放完整历史逻辑流
        stop_app()
        conn = psycopg2.connect(
            DSN, connection_factory=psycopg2.extras.LogicalReplicationConnection)
        cur = conn.cursor()
        cur.start_replication(
            slot_name=replay_slot, start_lsn=0,
            options={"include-xids": "1", "include-timestamp": "1"})

        replayed: list[tuple[int, str, list]] = []
        state = {"xid": None, "changes": []}

        def collect(msg):
            if msg.data_start == 0:
                return
            parsed = parse_message(msg.payload.decode("utf-8"), "artifacts")
            if parsed is None:
                return
            if parsed["kind"] == "BEGIN":
                state["xid"] = parsed["xid"]
                state["changes"] = []
            elif parsed["kind"] == "CHANGE":
                state["changes"].append(parsed)
            elif parsed["kind"] == "COMMIT":
                lsn_int = msg.wal_end
                text = f"{lsn_int >> 32:X}/{lsn_int & 0xFFFFFFFF:08X}"
                replayed.append((state["xid"], text, list(state["changes"])))
                state["xid"], state["changes"] = None, []

        # 非阻塞轮询读取：读到静默（连续 quiet 秒无消息）即认为历史流已投递完。
        # 不依赖固定事务条数，避免时序竞态导致的偶发失败。
        import select
        deadline = time.time() + 25
        quiet_deadline = None
        while time.time() < deadline:
            ready, _, _ = select.select([conn.fileno()], [], [], 1.0)
            if not ready:
                # 连续 3 秒没有任何新消息 → 流已排空
                if quiet_deadline is None:
                    quiet_deadline = time.time() + 3
                elif time.time() > quiet_deadline and replayed:
                    break
                continue
            quiet_deadline = None
            conn.poll()
            msg = cur.read_message()
            if msg is None or msg.data_start == 0:
                continue  # 空事件 / keepalive
            collect(msg)
            text = msg.payload.decode("utf-8")
            if text.startswith("COMMIT"):
                lsn_int = msg.wal_end
                lsn_text = f"{lsn_int >> 32:X}/{lsn_int & 0xFFFFFFFF:08X}"
                print(f"  replayed COMMIT #{len(replayed)} at {lsn_text}",
                      flush=True)
        conn.close()
        check("walsender 重放出了历史事务",
              len(replayed) >= 1, f"count={len(replayed)}")

        # 4) 把重放事务逐条喂给存储层：<= 水位的必须拦截，其余正常
        blocked = 0
        for xid, lsn_text, changes in replayed:
            outcome = store.persist_transaction(xid, lsn_text, None, changes)
            if lsn_to_num(lsn_text) <= horizon_num:
                if outcome == "before_horizon":
                    blocked += 1
                else:
                    print(f"  未拦截: xid={xid} lsn={lsn_text} {outcome}")
        expect_blocked = sum(
            1 for _, t, _ in replayed if lsn_to_num(t) <= horizon_num)
        check("至少重放出一个水位及更早的已清理事务",
              expect_blocked >= 1, f"expect={expect_blocked}")
        check("水位及更早的重放事务全部被拦截（含水位点本身）",
              blocked == expect_blocked and expect_blocked >= 1,
              f"{blocked}/{expect_blocked}")

        p2 = store.get_progress()
        check("重放后已清理事务未复活（事务数不变）",
              p2["captured_transactions"] == txns_before,
              f"{txns_before}->{p2['captured_transactions']}")
        check("重放后历史行数不变",
              p2["history_rows"] == rows_before,
              f"{rows_before}->{p2['history_rows']}")
        check("重放后保留水位不变",
              p2["retention_horizon_lsn"] == horizon)
        store.close()

        # 5) 重启应用：复制流恢复，水位之后的新事务正常捕获
        start_app()
        code, _ = api("/api/source/operations", "POST", {
            "operations": [{
                "op": "insert", "artifact_no": 5, "service_name": "s",
                "version": "t4", "sha256": SHA}]})
        check("重放防护后新写入受理", code == 200, f"http={code}")
        got = []
        for _ in range(20):
            _, h = api("/api/history/5")
            got = h["history"]
            if got:
                break
            time.sleep(1)
        check("重放防护后复制流仍正常推进、新变更被捕获", len(got) == 1)

    finally:
        stop_app()
        if reuse_pg:
            psql(f"SELECT pg_drop_replication_slot('{replay_slot}') "
                 "WHERE EXISTS (SELECT 1 FROM pg_replication_slots "
                 f"WHERE slot_name='{replay_slot}')")
        if started_here:
            run([f"{PGBIN}/pg_ctl", "-D", PGDATA, "stop", "-m", "fast"],
                check=False)

    print(f"\n==== replay guard: "
          f"{'全部通过' if not failures else '失败 ' + str(failures)} ====")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
