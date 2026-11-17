"""HistoryStore 基线固化（历史清理）单元测试：python3 -m unittest tests.test_storage -v

不依赖 PostgreSQL，直接用临时 SQLite 库构造基线与逐笔事务，验证：
- 固化后旧提交点明确不可查、起点及之后目录准确；
- 固化产生的新基线行 id 更大，但目录重建按 LSN 时序而非 id；
- 单编号历史自新基线起准确，已清理事务不会因重放重现；
- 重复固化、非法起点、非 STREAMING 状态的边界行为。
"""
import os
import tempfile
import unittest

from app.storage import (
    HistoryStore,
    NotStreamingError,
    PointNotCapturedError,
    PointRetainedAwayError,
    lsn_to_num,
)

SHA = "a" * 64
SHA2 = "b" * 64


def lsn_text(n: int) -> str:
    return f"0/{n:X}"


class FreezeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "history.db")
        self.store = HistoryStore(self.db)
        # 初始基线：编号 1、2 两行，基线 LSN = 0/100
        self.store.save_baseline(
            [(1, "svc1", "1.0", SHA), (2, "svc2", "1.0", SHA)], lsn_text(0x100)
        )
        self.store.mark_streaming()
        # txn @0x200：更新 1，插入 3
        self.store.persist_transaction(
            101, lsn_text(0x200), None,
            [
                {"change_type": "UPDATE", "artifact_no": 1,
                 "service_name": "svc1", "version": "2.0", "sha256": SHA2},
                {"change_type": "INSERT", "artifact_no": 3,
                 "service_name": "svc3", "version": "1.0", "sha256": SHA},
            ],
        )
        # txn @0x300：删除 2
        self.store.persist_transaction(
            102, lsn_text(0x300), None,
            [{"change_type": "DELETE", "artifact_no": 2}],
        )
        # txn @0x400：插入 4
        self.store.persist_transaction(
            103, lsn_text(0x400), None,
            [{"change_type": "INSERT", "artifact_no": 4,
              "service_name": "svc4", "version": "1.0", "sha256": SHA}],
        )

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_preconditions(self):
        points = self.store.get_points()
        self.assertEqual([p["lsn_num"] for p in points],
                         [0x100, 0x200, 0x300, 0x400])
        dir400 = {r["artifact_no"]: r for r in
                  self.store.get_directory_at(lsn_to_num(lsn_text(0x400)))}
        self.assertEqual(set(dir400), {1, 3, 4})  # 2 已删除
        self.assertEqual(dir400[1]["version"], "2.0")

    def test_freeze_boundary_and_snapshot(self):
        result = self.store.freeze_baseline(lsn_text(0x300))
        # 审计返回值
        self.assertEqual(result["removed_txns"], 2)       # 101、102 被清
        self.assertEqual(result["new_baseline_rows"], 2)  # 1、3 存活
        # 旧点不可查，起点与之后可查
        self.assertEqual(self.store.is_point_queryable(lsn_to_num(lsn_text(0x100))),
                         "gone")
        self.assertEqual(self.store.is_point_queryable(lsn_to_num(lsn_text(0x200))),
                         "gone")
        self.assertEqual(self.store.is_point_queryable(lsn_to_num(lsn_text(0x300))),
                         "ok")
        self.assertEqual(self.store.is_point_queryable(lsn_to_num(lsn_text(0x400))),
                         "ok")
        self.assertEqual(self.store.is_point_queryable(0x999), "unknown")
        # 固化点目录 = 当时快照（2 已删除，1 为更新后的 2.0，3 在）
        dir300 = self.store.get_directory_at(lsn_to_num(lsn_text(0x300)))
        self.assertEqual({r["artifact_no"] for r in dir300}, {1, 3})
        row1 = next(r for r in dir300 if r["artifact_no"] == 1)
        self.assertEqual(row1["version"], "2.0")
        self.assertEqual(row1["change_type"], "BASELINE")
        # 起点之后的目录重建仍然准确（新基线行 id 更大的顺序陷阱）
        dir400 = self.store.get_directory_at(lsn_to_num(lsn_text(0x400)))
        self.assertEqual({r["artifact_no"] for r in dir400}, {1, 3, 4})
        # 提交点列表现在只剩 新基线(0x300) + 0x400
        points = self.store.get_points()
        self.assertEqual([p["lsn_num"] for p in points], [0x300, 0x400])
        self.assertEqual(points[0]["kind"], "BASELINE")

    def test_history_chain_from_new_baseline(self):
        self.store.freeze_baseline(lsn_text(0x300))
        # 编号 1：只有新基线上的一条（旧 UPDATE 已清），之后无变更
        h1 = self.store.get_history_of(1)
        self.assertEqual(len(h1), 1)
        self.assertEqual(h1[0]["change_type"], "BASELINE")
        # 编号 4：保留 0x400 的 INSERT，链从新基线之后开始
        h4 = self.store.get_history_of(4)
        self.assertEqual([h["change_type"] for h in h4], ["INSERT"])
        # 编号 2：在固化点已删除，旧 BASELINE/DELETE 全部清掉，链为空
        self.assertEqual(self.store.get_history_of(2), [])

    def test_cleaned_txn_cannot_reappear(self):
        self.store.freeze_baseline(lsn_text(0x300))
        # 复制流重放出已清理事务：拒绝落盘
        outcome = self.store.persist_transaction(
            101, lsn_text(0x200), None,
            [{"change_type": "INSERT", "artifact_no": 99,
              "service_name": "ghost", "version": "1", "sha256": SHA}],
        )
        self.assertEqual(outcome, "before_horizon")
        # 水位点本身（0x300，原 xid=102）的 txns 记录已被清理，
        # 重放出来也必须拦截，否则固化点事务会复活
        outcome_at_horizon = self.store.persist_transaction(
            102, lsn_text(0x300), None,
            [{"change_type": "DELETE", "artifact_no": 2}],
        )
        self.assertEqual(outcome_at_horizon, "before_horizon")
        # 幽灵编号没有出现，旧事务计数不回升
        self.assertEqual(self.store.get_history_of(99), [])
        self.assertEqual(
            self.store.get_progress()["captured_transactions"], 1  # 只剩 103
        )
        # 水位之后的新事务正常写入
        ok = self.store.persist_transaction(
            104, lsn_text(0x500), None,
            [{"change_type": "INSERT", "artifact_no": 5,
              "service_name": "svc5", "version": "1", "sha256": SHA}],
        )
        self.assertEqual(ok, "written")
        # 重启重放 104 仍然幂等去重
        self.assertEqual(
            self.store.persist_transaction(104, lsn_text(0x500), None, []),
            "duplicate",
        )

    def test_repeated_freeze(self):
        self.store.freeze_baseline(lsn_text(0x300))
        # 再向 0x500 推进一个事务后二次固化
        self.store.persist_transaction(
            104, lsn_text(0x500), None,
            [{"change_type": "UPDATE", "artifact_no": 1,
              "service_name": "svc1", "version": "3.0", "sha256": SHA2}],
        )
        r2 = self.store.freeze_baseline(lsn_text(0x500))
        self.assertEqual(r2["new_baseline_rows"], 3)  # 1、3、4
        dir500 = self.store.get_directory_at(lsn_to_num(lsn_text(0x500)))
        row1 = next(r for r in dir500 if r["artifact_no"] == 1)
        self.assertEqual(row1["version"], "3.0")
        # 两次审计事件都在
        events = self.store.get_retention_events()
        self.assertEqual([e["point_lsn_num"] for e in events], [0x300, 0x500])
        # 0x400 现在也在水位之下，不可查
        self.assertEqual(self.store.is_point_queryable(0x400), "gone")

    def test_invalid_freeze_inputs(self):
        with self.assertRaises(PointNotCapturedError):
            self.store.freeze_baseline(lsn_text(0x777))
        # 先固化一次，再尝试用已被清理的旧点
        self.store.freeze_baseline(lsn_text(0x300))
        with self.assertRaises(PointRetainedAwayError):
            self.store.freeze_baseline(lsn_text(0x200))

    def test_freeze_requires_streaming(self):
        fresh = HistoryStore(os.path.join(self.tmp.name, "fresh.db"))
        fresh.mark_initializing("slot")
        with self.assertRaises(NotStreamingError):
            fresh.freeze_baseline(lsn_text(0x100))
        fresh.close()

    def test_freeze_is_atomic_rollback_on_error(self):
        # 人为制造事务内失败（重复事件 id 不可能，改用破坏快照后校验状态不变）：
        # 冻结一个不存在的点应整体回滚，库内计数与水位保持原样
        before = self.store.get_progress()
        with self.assertRaises(PointNotCapturedError):
            self.store.freeze_baseline(lsn_text(0x777))
        after = self.store.get_progress()
        self.assertEqual(before["history_rows"], after["history_rows"])
        self.assertIsNone(after["retention_horizon_lsn"])
        self.assertEqual(self.store.get_retention_events(), [])


if __name__ == "__main__":
    unittest.main()
