"""采样配置迁移、五分钟窗口代表、幂等结算和调查恢复验收。

直接运行：``python tests/test_sample_strategy_migration.py``
也可被 pytest 收集。测试不依赖机器时区，所有时间证据均显式给出。
"""
from __future__ import annotations

import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.core.dedup_sample import DedupSamplePipeline  # noqa: E402
from app.core.models import EventRecord, EventTypeDef, SampleConfig  # noqa: E402
from app.core.sample_strategy_migration import (  # noqa: E402
    MergeStrategyVersion,
    MigrationOrchestrator,
    SamplingCoordinator,
    StrategyNotConfirmed,
    StrategyRegistry,
    representative_ids,
    select_representatives,
    window_for_time,
)


def rows_r1_r2() -> list[dict[str, str]]:
    return [
        {"id": "R1", "customer_id": "C1",
         "event_time": "2026-09-01T09:01:00+08:00"},
        {"id": "R2", "customer_id": "C1",
         "event_time": "2026-09-01T09:08:00+08:00"},
    ]


def test_offset_times_share_timeline_and_boundary_is_half_open() -> None:
    plus_eight = window_for_time(datetime.fromisoformat("2026-09-01T09:01:00+08:00"))
    zulu = window_for_time(datetime.fromisoformat("2026-09-01T01:01:00+00:00"))
    assert plus_eight.window_id == zulu.window_id

    before_boundary = window_for_time(
        datetime.fromisoformat("2026-09-01T09:04:59+08:00"))
    on_boundary = window_for_time(
        datetime.fromisoformat("2026-09-01T09:05:00+08:00"))
    after_boundary = window_for_time(
        datetime.fromisoformat("2026-09-01T09:05:01+08:00"))
    assert before_boundary.window_id != on_boundary.window_id
    assert on_boundary.window_id == after_boundary.window_id

    strategy = MergeStrategyVersion(
        plan_id="TZ", version=2, scope="tumbling_window_latest",
        window_size_minutes=5, merge_key="customer_id", status="confirmed",
        timezone_status="offset_confirmed", timezone_verified=True)
    same_window = select_representatives([
        {"id": "OFFSET", "customer_id": "C1",
         "event_time": "2026-09-01T09:01:00+08:00"},
        {"id": "ZULU", "customer_id": "C1",
         "event_time": "2026-09-01T01:01:00+00:00"},
    ], strategy)
    assert len(same_window) == 1
    assert same_window[0].window_id == plus_eight.window_id


def test_legacy_whole_segment_keeps_r2_but_new_windows_keep_r1_and_r2() -> None:
    events = rows_r1_r2()
    legacy = MergeStrategyVersion(
        plan_id="P", version=1, scope="legacy_whole_segment_latest",
        merge_key="customer_id", status="confirmed")
    windowed = MergeStrategyVersion(
        plan_id="P", version=2, scope="tumbling_window_latest",
        window_size_minutes=5, merge_key="customer_id", status="confirmed",
        timezone_status="offset_confirmed", timezone_verified=True)

    assert representative_ids(select_representatives(events, legacy)) == ["R2"]
    reps = select_representatives(events, windowed)
    assert representative_ids(reps) == ["R1", "R2"]
    assert [rep.window_id for rep in reps] == [
        "W5m@20260901T010000Z", "W5m@20260901T010500Z"]
    assert all(rep.strategy_version == "v2" for rep in reps)
    assert all(rep.row_checksum for rep in reps)


def test_missing_timezone_evidence_stays_pending() -> None:
    pending = MergeStrategyVersion(
        plan_id="BAD", version=2, scope="tumbling_window_latest",
        window_size_minutes=5, merge_key="customer_id",
        status="pending_verification")
    naive_rows = [{"id": "B", "customer_id": "C", "event_time": "2026-09-01 09:01:00"}]
    try:
        select_representatives(naive_rows, pending)
    except StrategyNotConfirmed:
        pass
    else:
        raise AssertionError("缺证新版策略不应被用于采样")

    confirmed_tz = MergeStrategyVersion(
        plan_id="OK", version=2, scope="tumbling_window_latest",
        window_size_minutes=5, merge_key="customer_id",
        source_timezone="Asia/Shanghai", timezone_verified=True, status="confirmed")
    reps = select_representatives(naive_rows, confirmed_tz)
    assert reps[0].id == "B"
    assert reps[0].window_id == "W5m@20260901T010000Z"


def test_claimed_old_version_conflicts_with_migrated_new_version() -> None:
    orch = MigrationOrchestrator()
    migration = orch.migrate(
        {"plan_id": "CONFLICT", "merge_key": "customer_id", "sample_ratio": 1.0},
        {"window_size_minutes": 5}, events=rows_r1_r2())
    orch.registry.confirm_window(migration.new.strategy_id)

    orch.coordinator.claim_round(
        "ROUND-OLD", plan_id="CONFLICT",
        strategy_id=migration.legacy.strategy_id, events=rows_r1_r2())
    old_result = orch.coordinator.confirm_round("ROUND-OLD")
    assert old_result.sample_ids == ["R2"]
    assert old_result.resolution == "original_claim"

    orch.coordinator.claim_round(
        "ROUND-NEW", plan_id="CONFLICT",
        strategy_id=migration.legacy.strategy_id, events=rows_r1_r2())
    new_result = orch.coordinator.confirm_round(
        "ROUND-NEW", confirm_new_version=True)
    assert new_result.sample_ids == ["R1", "R2"]
    assert new_result.resolution == "confirmed_new_version"


def test_concurrent_confirmation_settles_once() -> None:
    registry = StrategyRegistry()
    strategy = registry.register(
        plan_id="CONC", version=1, scope="tumbling_window_latest",
        window_size_minutes=5, merge_key="customer_id", status="confirmed",
        timezone_status="offset_confirmed", timezone_verified=True)
    coordinator = SamplingCoordinator(registry=registry)
    coordinator.claim_round("ROUND-CONC", strategy=strategy, events=rows_r1_r2())

    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = [
            pool.submit(coordinator.confirm_round, "ROUND-CONC")
            for _ in range(32)
        ]
        results = [future.result() for future in as_completed(futures)]

    assert len({result.settlement_id for result in results}) == 1
    assert coordinator.ledger.count() == 1
    assert all(result.sample_ids == ["R1", "R2"] for result in results)


def test_investigation_recovery_keeps_old_r2_and_only_delivers_r1() -> None:
    orch = MigrationOrchestrator()
    migration = orch.migrate(
        {"plan_id": "INV", "merge_key": "customer_id"},
        {"window_size_minutes": 5}, events=rows_r1_r2())
    orch.registry.confirm_window(migration.new.strategy_id)
    window_samples = select_representatives(rows_r1_r2(), migration.new)

    orch.desk.create_case("INV-1", quota=2)
    receipt = orch.desk.receive_delivery(
        "INV-1", row_id="R2", investigator="detective",
        strategy_id=migration.legacy.strategy_id,
        strategy_version=migration.legacy.strategy_version,
        register_strategy_side=False)
    assert receipt.strategy_side_registered is False

    recovery = orch.recover_investigation("INV-1", window_samples)
    assert recovery.pending_sample_ids == ["R1"]
    r2_manifest = next(sample for sample in recovery.samples if sample.id == "R2")
    assert r2_manifest.delivery_status == "already_delivered"
    assert r2_manifest.actually_delivered_strategy_version == "v1"
    assert recovery.quota.used == 1

    delivered_r1 = orch.desk.claim_next_undelivered(
        "INV-1", recovery.samples, investigator="detective")
    assert delivered_r1.row_id == "R1"
    assert orch.desk.quotas.case("INV-1").used == 2
    assert orch.desk.claim_next_undelivered("INV-1", recovery.samples) is None

    verification = orch.verify_delivery("INV-1", receipt.delivery_id)
    assert verification.ok is True
    assert verification.row_id == "R2"
    assert verification.strategy_version == "v1"


def test_original_sample_entry_and_historical_replay_keep_r2() -> None:
    event_type = EventTypeDef(type_id="T", type_name="t", category_tag="c")
    records = [
        EventRecord(
            "R1", event_type, "raw1", {"customer_id": "C1"}, {}, {},
            datetime.fromisoformat("2026-09-01T09:01:00+08:00"), "host"),
        EventRecord(
            "R2", event_type, "raw2", {"customer_id": "C1"}, {}, {},
            datetime.fromisoformat("2026-09-01T09:08:00+08:00"), "host"),
    ]
    config = SampleConfig(
        plan_id="P", event_type_id="T", sample_ratio=1.0,
        merge_key="customer_id")
    result = DedupSamplePipeline().sample(records, config)
    assert [record.record_id for record in result] == ["R2"]

    orch = MigrationOrchestrator()
    legacy = orch.registry.register(
        plan_id="REPLAY", version=1, scope="legacy_whole_segment_latest",
        merge_key="customer_id", status="confirmed")
    orch.coordinator.claim_round(
        "REPLAY-1", strategy=legacy, config=config, events=records)
    settled = orch.coordinator.confirm_round("REPLAY-1")
    replayed = orch.coordinator.replay_round("REPLAY-1", records)
    assert settled.sample_ids == ["R2"]
    assert replayed.sample_ids == ["R2"]
    assert replayed.matches_original_entry is True


TESTS = {
    "offset_timeline": test_offset_times_share_timeline_and_boundary_is_half_open,
    "r1_r2_strategies": test_legacy_whole_segment_keeps_r2_but_new_windows_keep_r1_and_r2,
    "missing_timezone": test_missing_timezone_evidence_stays_pending,
    "claimed_conflict": test_claimed_old_version_conflicts_with_migrated_new_version,
    "concurrent_settlement": test_concurrent_confirmation_settles_once,
    "investigation_recovery": test_investigation_recovery_keeps_old_r2_and_only_delivers_r1,
    "historical_replay": test_original_sample_entry_and_historical_replay_keep_r2,
}


if __name__ == "__main__":
    import traceback

    passed = failed = 0
    for name, test in TESTS.items():
        try:
            test()
            passed += 1
            print(f"  PASS  test_sample_strategy_migration: {name}")
        except Exception:
            failed += 1
            print(f"  FAIL  test_sample_strategy_migration: {name}")
            traceback.print_exc()
    print(f"\ntest_sample_strategy_migration: {passed} passed, {failed} failed, {len(TESTS)} total")
    sys.exit(1 if failed else 0)
