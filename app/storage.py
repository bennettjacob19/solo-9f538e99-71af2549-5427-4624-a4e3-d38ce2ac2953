"""SQLite 历史库：只增不改的变更日志 + 捕获状态 + 基线固化（历史清理）。

关键约束：
- 所有写入在一把进程内锁内串行化，SQLite 开启 WAL；
- 一个源事务的全部行变更与该事务的提交点在同一个 SQLite 事务里落盘，
  保证多行事务整体可见、不会出现半条事务；
- 复制槽的推进位置（last_commit_lsn）与对应数据在同一事务更新，
  崩溃重启后靠 xid 去重，绝不重复、也不丢数据。

基线固化（retention/freeze）
---------------------------
值班工程师在网页选择一个已捕获提交点 P，把 P 时刻的整张目录固化为新基线，
清除 P（含）之前逐笔累积的变更与事务记录：
- 快照、删除、新基线插入、水位更新在**同一个 SQLite 事务**内完成，
  任何时刻强杀都只会看到"旧基线"或"新基线"，不存在半份基线；
- retention_horizon 记录保留起点：早于它的提交点明确不可查（API 410），
  复制流若重放出更早的事务一律拒绝落盘（但仍向源端确认槽位，避免卡流）；
- 事务提交后执行 VACUUM 回收页，后续写入可复用腾出的文件空间。
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

log = logging.getLogger("storage")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS capture_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS txns (
    xid            INTEGER PRIMARY KEY,
    commit_lsn     TEXT NOT NULL,
    commit_lsn_num INTEGER NOT NULL,
    commit_ts      TEXT
);

CREATE TABLE IF NOT EXISTS artifact_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_no  INTEGER NOT NULL,
    change_type  TEXT NOT NULL,              -- BASELINE / INSERT / UPDATE / DELETE
    service_name TEXT,
    version      TEXT,
    sha256       TEXT,
    xid          INTEGER,
    commit_lsn   TEXT,
    commit_lsn_num INTEGER,
    captured_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_hist_artifact ON artifact_history(artifact_no, id);
CREATE INDEX IF NOT EXISTS idx_hist_lsn ON artifact_history(commit_lsn_num);

-- 每次基线固化（历史清理）的审计记录，与清理事务一同原子落盘
CREATE TABLE IF NOT EXISTS retention_events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    point_lsn           TEXT NOT NULL,
    point_lsn_num       INTEGER NOT NULL,
    removed_txns        INTEGER NOT NULL,
    removed_history_rows INTEGER NOT NULL,
    new_baseline_rows   INTEGER NOT NULL,
    db_pages_before     INTEGER,
    db_pages_after      INTEGER,
    created_at          TEXT NOT NULL
);
"""

PHASE_INITIALIZING = "INITIALIZING"
PHASE_STREAMING = "STREAMING"
PHASE_ERROR = "ERROR"


class RetentionError(Exception):
    """基线固化相关的可预期错误。"""


class NotStreamingError(RetentionError):
    """捕获未进入 STREAMING，不允许固化。"""


class PointNotCapturedError(RetentionError):
    """给定 LSN 不是已捕获的提交点。"""


class PointRetainedAwayError(RetentionError):
    """给定 LSN 早于保留水位，对应历史已被清理，明确不可查。"""


def lsn_to_num(lsn: str) -> int:
    """'0/16B3748' -> 整数，便于比较先后。"""
    high, low = lsn.split("/", 1)
    return (int(high, 16) << 32) | int(low, 16)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


class HistoryStore:
    def __init__(self, path: str) -> None:
        import os

        directory = os.path.dirname(path) or "."
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=FULL;")
        self._conn.execute("PRAGMA foreign_keys=ON;")
        self._conn.executescript(SCHEMA_SQL)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ state
    def _get_state(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM capture_state WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def get_state(self) -> dict[str, str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT key, value FROM capture_state"
            ).fetchall()
            return {r["key"]: r["value"] for r in rows}

    def get_phase(self) -> str:
        return self._get_state("phase") or PHASE_INITIALIZING

    def is_ready(self) -> bool:
        return self.get_phase() == PHASE_STREAMING

    def _set_many(self, items: dict[str, Any]) -> None:
        self._conn.executemany(
            """
            INSERT INTO capture_state(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            [(k, str(v)) for k, v in items.items()],
        )

    def mark_initializing(self, slot_name: str) -> None:
        with self._lock:
            self._set_many(
                {
                    "phase": PHASE_INITIALIZING,
                    "slot_name": slot_name,
                    "initialized_at": utc_now_iso(),
                    "last_error": "",
                }
            )

    def mark_error(self, message: str) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_ERROR, "last_error": message})

    def clear_error(self) -> None:
        with self._lock:
            self._set_many({"last_error": ""})

    def update_last_message_lsn(self, lsn: str) -> None:
        with self._lock:
            self._set_many({"last_message_lsn": lsn})

    # ----------------------------------------------------------------- writes
    def save_baseline(
        self, rows: Iterable[sqlite3.Row | tuple[int, str, str, str]], lsn: str
    ) -> None:
        """整批基线与基线提交点在一个 SQLite 事务里落盘。"""
        now = utc_now_iso()
        lsn_num = lsn_to_num(lsn)
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._set_many(
                    {
                        "phase": PHASE_INITIALIZING,
                        "baseline_lsn": lsn,
                        "baseline_lsn_num": lsn_num,
                        "baseline_captured_at": now,
                    }
                )
                payload = [
                    (
                        r[0],
                        "BASELINE",
                        r[1],
                        r[2],
                        r[3],
                        None,
                        lsn,
                        lsn_num,
                        now,
                    )
                    for r in rows
                ]
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_streaming(self) -> None:
        with self._lock:
            self._set_many({"phase": PHASE_STREAMING, "streaming_at": utc_now_iso()})

    def persist_transaction(
        self,
        xid: int,
        commit_lsn: str,
        commit_ts: str | None,
        changes: list[dict[str, Any]],
    ) -> str:
        """落盘一个已提交源事务。

        返回值：
          "written"  新写入；
          "duplicate" 重复事务（重启重放，已跳过）；
          "before_horizon" 该事务 LSN 早于保留水位（历史已清理），拒绝落盘，
                           避免已清理的事务重现；调用方仍应确认复制槽位。
        """
        lsn_num = lsn_to_num(commit_lsn)
        now = utc_now_iso()
        with self._lock:
            # 保留水位防护：水位及更早的事务都已由新基线代表，
            # 即使复制流重放出来也绝不允许重新落盘（含水位点本身——
            # 它的 txns 记录已在固化时删除，只放行会导致该事务复活）。
            horizon = self._get_state("retention_horizon_lsn_num")
            if horizon is not None and lsn_num <= int(horizon):
                log.warning(
                    "skip txn %s at %s: at or earlier than retention horizon %s",
                    xid, commit_lsn, horizon,
                )
                return "before_horizon"
            existed = self._conn.execute(
                "SELECT 1 FROM txns WHERE xid=?", (xid,)
            ).fetchone()
            if existed:
                return "duplicate"
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "INSERT INTO txns(xid, commit_lsn, commit_lsn_num, commit_ts)"
                    " VALUES (?, ?, ?, ?)",
                    (xid, commit_lsn, lsn_num, commit_ts),
                )
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            c["artifact_no"],
                            c["change_type"],
                            c.get("service_name"),
                            c.get("version"),
                            c.get("sha256"),
                            xid,
                            commit_lsn,
                            lsn_num,
                            now,
                        )
                        for c in changes
                    ],
                )
                # 槽位推进位置与数据同事务落盘
                self._set_many(
                    {
                        "last_commit_lsn": commit_lsn,
                        "last_commit_lsn_num": lsn_num,
                        "last_xid": xid,
                    }
                )
                self._conn.execute("COMMIT")
                return "written"
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------------ reads
    def get_points(self) -> list[dict[str, Any]]:
        with self._lock:
            points: list[dict[str, Any]] = []
            baseline_lsn = self._get_state("baseline_lsn")
            if baseline_lsn:
                count = self._conn.execute(
                    "SELECT COUNT(*) AS c FROM artifact_history"
                    " WHERE change_type='BASELINE'"
                ).fetchone()["c"]
                points.append(
                    {
                        "kind": "BASELINE",
                        "lsn": baseline_lsn,
                        "lsn_num": lsn_to_num(baseline_lsn),
                        "xid": None,
                        "commit_ts": self._get_state("baseline_captured_at"),
                        "change_count": count,
                    }
                )
            for row in self._conn.execute(
                """
                SELECT t.xid, t.commit_lsn, t.commit_lsn_num, t.commit_ts,
                       (SELECT COUNT(*) FROM artifact_history h
                         WHERE h.xid = t.xid) AS change_count
                FROM txns t ORDER BY t.commit_lsn_num, t.xid
                """
            ).fetchall():
                points.append(
                    {
                        "kind": "TXN",
                        "lsn": row["commit_lsn"],
                        "lsn_num": row["commit_lsn_num"],
                        "xid": row["xid"],
                        "commit_ts": row["commit_ts"],
                        "change_count": row["change_count"],
                    }
                )
            return points

    def get_directory_at(self, lsn_num: int) -> list[dict[str, Any]]:
        """查看某个提交点（含）为止的整目录：每个编号取最后一条变更。

        排序键为 (commit_lsn_num DESC, id DESC) 而不是仅 id：基线固化后
        新基线行的自增 id 大于保留下来的后续变更，必须先按 LSN 定位时序。
        """
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, service_name, version, sha256, change_type
                FROM (
                    SELECT artifact_no, service_name, version, sha256,
                           change_type,
                           ROW_NUMBER() OVER (
                               PARTITION BY artifact_no
                               ORDER BY commit_lsn_num DESC, id DESC
                           ) AS rn
                    FROM artifact_history
                    WHERE commit_lsn_num <= ?
                )
                WHERE rn = 1 AND change_type != 'DELETE'
                ORDER BY artifact_no
                """,
                (lsn_num,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_history_of(self, artifact_no: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT artifact_no, change_type, service_name, version, sha256,
                       xid, commit_lsn, commit_lsn_num, captured_at
                FROM artifact_history WHERE artifact_no=?
                ORDER BY commit_lsn_num, id
                """,
                (artifact_no,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_progress(self) -> dict[str, Any]:
        with self._lock:
            state = self.get_state()
            txn_count = self._conn.execute(
                "SELECT COUNT(*) AS c FROM txns"
            ).fetchone()["c"]
            history_rows = self._conn.execute(
                "SELECT COUNT(*) AS c FROM artifact_history"
            ).fetchone()["c"]
            page_count = self._conn.execute(
                "PRAGMA page_count"
            ).fetchone()[0]
            return {
                "phase": state.get("phase", PHASE_INITIALIZING),
                "slot_name": state.get("slot_name"),
                "baseline_lsn": state.get("baseline_lsn"),
                "retention_horizon_lsn": state.get("retention_horizon_lsn"),
                "retention_count": self._conn.execute(
                    "SELECT COUNT(*) AS c FROM retention_events"
                ).fetchone()["c"],
                "last_commit_lsn": state.get("last_commit_lsn"),
                "last_message_lsn": state.get("last_message_lsn"),
                "last_xid": int(state["last_xid"]) if state.get("last_xid") else None,
                "captured_transactions": txn_count,
                "history_rows": history_rows,
                "db_pages": page_count,
                "last_error": state.get("last_error") or None,
                "initialized_at": state.get("initialized_at"),
            }

    # ------------------------------------------------- 基线固化（历史清理）
    def is_point_queryable(self, lsn_num: int) -> str:
        """判定提交点可查性。

        返回 "ok" / "gone"（早于保留水位，已清理）/ "unknown"（从未捕获）。
        """
        with self._lock:
            horizon = self._get_state("retention_horizon_lsn_num")
            if horizon is not None and lsn_num < int(horizon):
                return "gone"
            row = self._conn.execute(
                "SELECT 1 FROM artifact_history WHERE commit_lsn_num=? LIMIT 1",
                (lsn_num,),
            ).fetchone()
            return "ok" if row else "unknown"

    def get_retention_events(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT id, point_lsn, point_lsn_num, removed_txns,
                       removed_history_rows, new_baseline_rows,
                       db_pages_before, db_pages_after, created_at
                FROM retention_events ORDER BY id
                """
            ).fetchall()
            return [dict(r) for r in rows]

    def freeze_baseline(
        self,
        point_lsn: str,
        crash_before_commit: bool = False,
    ) -> dict[str, Any]:
        """把 point_lsn 时刻的整张目录固化为新基线，清除更早的逐笔变更。

        全程在单个 SQLite 事务内完成（快照→删除→新基线→水位→审计），
        crash_before_commit=True 时在 COMMIT 前强杀进程，用于崩溃测试：
        由于一切都在未提交事务里，重启后库内状态与调用前完全一致。
        """
        lsn_num = lsn_to_num(point_lsn)
        now = utc_now_iso()
        with self._lock:
            if self.get_phase() != PHASE_STREAMING:
                raise NotStreamingError("捕获未进入 STREAMING，不能固化基线")
            # 提交点必须存在且尚未被清理
            status = self.is_point_queryable(lsn_num)
            if status == "gone":
                raise PointRetainedAwayError(
                    f"提交点 {point_lsn} 早于保留水位，历史已被清理，不可作为保留起点"
                )
            if status == "unknown":
                raise PointNotCapturedError(
                    f"{point_lsn} 不是已捕获的提交点，请先在提交点列表中选择"
                )

            # 1) 先在同一事务视图内快照该点整张目录（DELETE 的编号不进基线）
            snapshot = self._conn.execute(
                """
                SELECT artifact_no, service_name, version, sha256 FROM (
                    SELECT artifact_no, service_name, version, sha256,
                           change_type,
                           ROW_NUMBER() OVER (
                               PARTITION BY artifact_no
                               ORDER BY commit_lsn_num DESC, id DESC
                           ) AS rn
                    FROM artifact_history
                    WHERE commit_lsn_num <= ?
                ) WHERE rn = 1 AND change_type != 'DELETE'
                ORDER BY artifact_no
                """,
                (lsn_num,),
            ).fetchall()

            pages_before = self._conn.execute("PRAGMA page_count").fetchone()[0]
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                # 2) 清除保留起点（含）之前的全部逐笔变更与事务记录。
                #    起点本身的事务被新基线取代；起点之后的记录原样保留。
                removed_history = self._conn.execute(
                    "DELETE FROM artifact_history WHERE commit_lsn_num <= ?",
                    (lsn_num,),
                ).rowcount
                removed_txns = self._conn.execute(
                    "DELETE FROM txns WHERE commit_lsn_num <= ?",
                    (lsn_num,),
                ).rowcount

                # 3) 插入新基线行：起点时刻整张目录，LSN 即保留起点
                self._conn.executemany(
                    """
                    INSERT INTO artifact_history(
                        artifact_no, change_type, service_name, version, sha256,
                        xid, commit_lsn, commit_lsn_num, captured_at)
                    VALUES (?, 'BASELINE', ?, ?, ?, NULL, ?, ?, ?)
                    """,
                    [
                        (r["artifact_no"], r["service_name"], r["version"],
                         r["sha256"], point_lsn, lsn_num, now)
                        for r in snapshot
                    ],
                )

                # 4) 水位与基线指针同事务更新；last_commit_lsn 仅在被清掉时
                #    回落到保留起点，保证进度指针永远指向库里真实存在的点
                state_items = {
                    "baseline_lsn": point_lsn,
                    "baseline_lsn_num": lsn_num,
                    "baseline_captured_at": now,
                    "retention_horizon_lsn": point_lsn,
                    "retention_horizon_lsn_num": lsn_num,
                    "last_retention_at": now,
                }
                last_commit_num = self._get_state("last_commit_lsn_num")
                if last_commit_num is not None and int(last_commit_num) <= lsn_num:
                    state_items["last_commit_lsn"] = point_lsn
                    state_items["last_commit_lsn_num"] = lsn_num
                    state_items["last_xid"] = ""
                self._set_many(state_items)

                # 5) 审计记录先占位，页数在 VACUUM 后回填（见事务外）
                cur = self._conn.execute(
                    """
                    INSERT INTO retention_events(
                        point_lsn, point_lsn_num, removed_txns,
                        removed_history_rows, new_baseline_rows,
                        db_pages_before, db_pages_after, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
                    """,
                    (point_lsn, lsn_num, removed_txns, removed_history,
                     len(snapshot), pages_before, now),
                )
                event_id = cur.lastrowid

                if crash_before_commit:
                    # 测试钩子：模拟清理中途强杀。未提交事务随进程死亡
                    # 整体回滚，重启后不得出现半份基线。
                    log.warning(
                        "TEST hook: crashing before freeze COMMIT at %s", point_lsn
                    )
                    os._exit(88)

                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        # 事务已持久化。VACUUM 回收空闲页，让后续写入复用腾出的文件空间。
        # VACUUM 不能在事务内执行；失败不影响固化正确性，仅记录并上报。
        # WAL 模式下 VACUUM 只重排主库页，必须再做 TRUNCATE checkpoint
        # 才能把 WAL 合并回主库并把空闲页还给文件系统。
        vacuum_error = None
        pages_after = None
        try:
            with self._lock:
                self._conn.execute("VACUUM")
                self._conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                pages_after = self._conn.execute(
                    "PRAGMA page_count"
                ).fetchone()[0]
                self._conn.execute(
                    "UPDATE retention_events SET db_pages_after=? WHERE id=?",
                    (pages_after, event_id),
                )
        except Exception as exc:  # VACUUM 失败不回滚已完成的固化
            vacuum_error = str(exc)
            log.warning("VACUUM after freeze failed: %s", exc)

        return {
            "point_lsn": point_lsn,
            "point_lsn_num": lsn_num,
            "removed_txns": removed_txns,
            "removed_history_rows": removed_history,
            "new_baseline_rows": len(snapshot),
            "db_pages_before": pages_before,
            "db_pages_after": pages_after,
            "vacuum_error": vacuum_error,
            "event_id": event_id,
            "created_at": now,
        }
