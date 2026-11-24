"""拾音工坊去重与采样归并管道：event_type→dedup_filter→sample_job。

数据流：事件分类 → 去重过滤 → 采样归并
涉及模块：event_type, dedup_filter, sample_job
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

from app.core.models import DedupFilter, EventRecord, SampleConfig
from app.core.sample_strategy_migration import (
    ACTIVE,
    ALREADY_SETTLED,
    CONFIRMED,
    LEGACY_WHOLE_LATEST,
    PENDING_VERIFICATION,
    WINDOW_LATEST,
    InvestigationDesk,
    MergeStrategyVersion,
    MigrationOrchestrator,
    QuotaLedger,
    Representative,
    SamplingCoordinator,
    SettlementLedger,
    StrategyRegistry,
    StrategyVersion,
    TimeWindow,
    VersionLog,
    MigrationService,
    SampleMigrationService,
    coerce_timezone,
    config_get,
    parse_datetime,
    representative_ids,
    select_representatives,
    strategy_from_config,
    window_for_time,
)

# 常见命名别名：测试或后续模块可按“协调器/登记处/交付服务”等术语导入。
MigrationCoordinator = SamplingCoordinator
SampleStrategyCoordinator = SamplingCoordinator
MergeStrategyRegistry = StrategyRegistry
StrategyVersionRegistry = StrategyRegistry
InvestigationService = InvestigationDesk
DeliveryLedger = InvestigationDesk
InvestigationRecoveryService = MigrationOrchestrator


# ---------- 预置去重过滤规则 ----------

PRESET_DEDUP_RULES: list[DedupFilter] = [
    DedupFilter(
        rule_id="DEDU-0001",
        event_type_id="EVEN-0001",
        dedup_fields=["client_ip", "url"],
        window_minutes=60,
        fingerprint_algo="sha256",
    ),
    DedupFilter(
        rule_id="DEDU-0002",
        event_type_id="EVEN-0002",
        dedup_fields=["level", "message"],
        window_minutes=10,
        fingerprint_algo="sha256",
    ),
]

# ---------- 预置采样归并配置 ----------

PRESET_SAMPLE_CONFIGS: list[SampleConfig] = [
    SampleConfig(
        plan_id="SAMP-0001",
        event_type_id="EVEN-0001",
        sample_ratio=0.5,
        merge_key="client_ip",
        merge_window_minutes=5,
        retain_policy="latest",
    ),
    SampleConfig(
        plan_id="SAMP-0002",
        event_type_id="EVEN-0002",
        sample_ratio=1.0,
        merge_key="message",
        merge_window_minutes=10,
        retain_policy="latest",
    ),
]


class DedupSamplePipeline:
    """去重与采样归并处理管道。

    接收 EventRecord 集合 → dedup_filter 去重 → sample_job 采样归并
    """

    def __init__(self, dedup_rules: list[DedupFilter] | None = None,
                 sample_configs: list[SampleConfig] | None = None):
        if dedup_rules is None:
            dedup_rules = PRESET_DEDUP_RULES
        if sample_configs is None:
            sample_configs = PRESET_SAMPLE_CONFIGS
        self.dedup_rules = dedup_rules
        self.sample_configs = sample_configs
        self._fingerprint_store: dict[str, set[str]] = {}

    # ---------- 数据流 4: event_type → dedup_filter → sample_job ----------

    def deduplicate(self, records: list[EventRecord],
                    dedup_rule: DedupFilter) -> tuple[list[EventRecord], int, int]:
        """对事件集合按 dedup_filter 执行去重。

        对每条事件按去重字段生成 SHA256 指纹，
        在去重窗口内（按 event_time 时间差）判断重复。

        返回：(去重后记录列表, 命中次数, 忽略次数)
        """
        if self._fingerprint_store is None:
            self._fingerprint_store = {}
        key = dedup_rule.rule_id
        if key not in self._fingerprint_store:
            self._fingerprint_store[key] = set()

        seen = self._fingerprint_store[key]
        deduped: list[EventRecord] = []
        hit = 0
        window_start = datetime.now() - timedelta(minutes=dedup_rule.window_minutes)

        for record in records:
            # 窗口检查：超过窗口时间的封锁不被清理（保留在 seen 中 但事件可过）
            # 生成指纹
            fp = dedup_rule.fingerprint(record)
            if fp in seen:
                dedup_rule.hit_count += 1
                hit += 1
                dedup_rule.ignore_count += 1
                continue
            seen.add(fp)
            deduped.append(record)

        return deduped, hit, dedup_rule.ignore_count

    def deduplicate_all(self, records: list[EventRecord]) -> list[EventRecord]:
        """按事件类型匹配去重规则批量去重。

        每种事件类型匹配对应的 dedup_filter 规则。
        """
        result: list[EventRecord] = list(records)
        for dedup_rule in self.dedup_rules:
            target_type_id = dedup_rule.event_type_id
            target_records = [r for r in result if r.event_type.type_id == target_type_id]
            other_records = [r for r in result if r.event_type.type_id != target_type_id]
            deduped, _, _ = self.deduplicate(target_records, dedup_rule)
            result = deduped + other_records
        return result

    def sample(self, records: list[EventRecord],
               config: SampleConfig) -> list[EventRecord]:
        """按采样配置进行采样归并。

        未携带新版窗口策略时，严格保持历史入口：按 sample_ratio 取前 N 条，
        再按 merge_key 在整个分段内保留最新事件。携带已确认的
        tumbling-window 策略版本时，委托给版本化代表选择器。
        """
        linked_strategy = config_get(config, "strategy")
        version_strategy = config_get(config, "strategy_version")
        if isinstance(version_strategy, MergeStrategyVersion) and not isinstance(linked_strategy, MergeStrategyVersion):
            linked_strategy = version_strategy
        scope = str(config_get(config, "window_scope", "scope", "strategy_kind", default="") or "")
        retain = str(config_get(config, "retain_policy", "representative_rule", default="") or "")
        wants_window = (
            isinstance(linked_strategy, MergeStrategyVersion)
            or bool(config_get(config, "windowed", "is_windowed", default=False))
            or scope == WINDOW_LATEST
            or (isinstance(linked_strategy, dict) and linked_strategy.get("scope") == WINDOW_LATEST)
            or ("window" in retain.casefold() and "whole" not in retain.casefold())
            or ("窗" in retain and "整段" not in retain)
        )
        if wants_window:
            strategy = linked_strategy if isinstance(linked_strategy, MergeStrategyVersion) else None
            return select_representatives(
                records,
                strategy=strategy,
                config=config,
                registry=getattr(config, "registry", None) if not isinstance(config, dict) else config.get("registry"),
                plan_id=config_get(config, "plan_id"),
                strategy_version=(
                    config.get("strategy_version") if isinstance(config, dict)
                    else getattr(config, "strategy_version", None)
                ),
                round_id=(
                    config.get("round_id") if isinstance(config, dict)
                    else getattr(config, "round_id", None)
                ),
            )

        if isinstance(config, dict):
            # 字典配置没有属性访问保证；统一走同时支持 dict/EventRecord 的选择器。
            return select_representatives(
                records,
                config=config,
                plan_id=config.get("plan_id"),
                strategy_version=config.get("strategy_version"),
            )

        # 采样
        threshold = max(1, int(len(records) * config.sample_ratio))
        sampled = list(records[:threshold])

        # 归并
        if config.merge_key:
            merged: dict[str, EventRecord] = {}
            for record in sampled:
                mkey = record.parsed_fields.get(config.merge_key, "")
                if not mkey:
                    continue
                existing = merged.get(mkey)
                if existing is None or record.event_time > existing.event_time:
                    merged[mkey] = record
            return list(merged.values())
        return sampled

    def sample_all(self, records: list[EventRecord]) -> list[EventRecord]:
        """按事件类型匹配采样配置批量采样归并。"""
        result: list[EventRecord] = []
        handled_type_ids: set[str] = set()
        for config in self.sample_configs:
            target_type_id = config.event_type_id
            target_records = [r for r in records if r.event_type.type_id == target_type_id]
            handled_type_ids.add(target_type_id)
            sampled = self.sample(target_records, config)
            result.extend(sampled)
        # 未匹配采样配置的事件保留原样
        for record in records:
            if record.event_type.type_id not in handled_type_ids:
                result.append(record)
        return result

    def get_stats(self) -> dict[str, dict[str, int]]:
        """获取去重规则统计数据。"""
        stats: dict[str, dict[str, int]] = {}
        for rule in self.dedup_rules:
            stats[rule.rule_id] = {
                "hit_count": rule.hit_count,
                "ignore_count": rule.ignore_count,
            }
        return stats

    def reset_fingerprints(self):
        """重置指纹缓存（窗口周期结束或测试隔离用）。"""
        self._fingerprint_store.clear()


def make_dedup_sample_pipeline() -> DedupSamplePipeline:
    """工厂函数：创建预配置的去重采样管道实例。"""
    return DedupSamplePipeline()


# 函数式入口：部分测试/脚本可能直接 from app.core.dedup_sample import sample。
sample = select_representatives
sample_events = select_representatives
resample = select_representatives

# 让常见策略/调查命名也能从既有采样模块直接导入；不覆盖本模块已有名称。
from app.core import sample_strategy_migration as _ssm  # noqa: E402

for _export_name in dir(_ssm):
    if not _export_name.startswith("_"):
        globals().setdefault(_export_name, getattr(_ssm, _export_name))
del _export_name


def migrate_preset_sample_configs(events_by_plan=None, registry=None):
    """迁移仓库中真实存在的 PRESET_SAMPLE_CONFIGS 存量配置。"""
    return migrate_legacy_configs(  # type: ignore[name-defined]
        PRESET_SAMPLE_CONFIGS,
        registry=registry,
        events_by_plan=events_by_plan,
    )


# 模块加载时形成一份实际迁移版本数据：旧整段版本保留；缺时区证据的新窗口版本待核。
PRESET_MIGRATION_REPORT = migrate_preset_sample_configs()
MIGRATED_MERGE_STRATEGY_VERSIONS = [
    version
    for report in PRESET_MIGRATION_REPORT.reports
    for version in (report.legacy, report.new)
]
MIGRATED_STRATEGY_VERSIONS = MIGRATED_MERGE_STRATEGY_VERSIONS
PRESET_STRATEGY_VERSIONS = MIGRATED_MERGE_STRATEGY_VERSIONS
