"""采样归并领域服务。

除通用资源骨架外，这里承载存量配置迁移、策略版本确认、按版本重算、幂等轮次
结算以及调查交付恢复。路由层和测试都可以直接调用这些领域入口。
"""
from __future__ import annotations

from typing import Any, Iterable

from app.core.sample_strategy_migration import (
    InvestigationDesk,
    MergeStrategyVersion,
    MigrationOrchestrator,
    Representative,
    SamplingCoordinator,
    StrategyRegistry,
    select_representatives,
    strategy_from_config,
)
from app.services.base import BaseModuleService


class SampleJobService(BaseModuleService):
    MODULE = "sample_job"
    ENTITY = "采样计划"
    OPERATION_TERM = "聚合"
    LIST_FIELDS = ["计划编号", "事件类型", "采样比例", "归并键", "归并窗口", "保存策略", "最近采样", "计划状态"]
    REQUIRED_FIELDS = ["计划编号", "事件类型", "采样比例"]
    STATUS_ORDER = ["待执行", "采样中", "已完成", "策略调整"]
    ACTION_RULES = {"启动采样": "采样中", "调整策略": "策略调整", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        self.orchestrator = MigrationOrchestrator()
        self.registry: StrategyRegistry = self.orchestrator.registry
        self.coordinator: SamplingCoordinator = self.orchestrator.coordinator
        self.investigations: InvestigationDesk = self.orchestrator.desk

    # ---------- 策略版本与迁移 ----------

    def register_strategy(self, config: Any = None, **attrs: Any) -> MergeStrategyVersion:
        return self.registry.register(config, **attrs)

    def migrate_config(self, *args: Any, **kwargs: Any):
        return self.registry.register_migration_pair(*args, **kwargs)

    migrate_strategy = migrate_config
    migrate_strategy_versions = migrate_config

    def confirm_window(self, strategy_id: str, *args: Any, **kwargs: Any) -> MergeStrategyVersion:
        return self.registry.confirm_window(strategy_id, *args, **kwargs)

    # ---------- 采样与轮次 ----------

    def select_representatives(
        self, events: Iterable[Any], strategy: Any = None, config: Any = None, **kwargs: Any,
    ) -> list[Representative]:
        if strategy is None and config is not None:
            strategy = strategy_from_config(config)
        return select_representatives(events, strategy=strategy, config=config, **kwargs)

    def sample(self, events: Iterable[Any], config: Any = None, **kwargs: Any):
        return self.select_representatives(events, config=config, **kwargs)

    def claim_round(self, *args: Any, **kwargs: Any):
        return self.coordinator.claim_round(*args, **kwargs)

    def confirm_round(self, *args: Any, **kwargs: Any):
        return self.coordinator.confirm_round(*args, **kwargs)

    settle_round = confirm_round

    def replay_round(self, *args: Any, **kwargs: Any):
        return self.coordinator.replay_round(*args, **kwargs)

    # ---------- 调查交付与恢复 ----------

    def receive_delivery(self, *args: Any, **kwargs: Any):
        return self.investigations.receive_delivery(*args, **kwargs)

    def recover_investigation(self, *args: Any, **kwargs: Any):
        return self.orchestrator.recover_investigation(*args, **kwargs)

    reconcile_delivery = recover_investigation
    recover = recover_investigation

    def verify_delivery(self, *args: Any, **kwargs: Any):
        return self.orchestrator.verify_delivery(*args, **kwargs)

    def run_action(
        self,
        entry_id: int,
        action: str,
        context: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """让既有 ``/api/sample_job/{id}/actions`` 入口也能承载复杂迁移动作。"""
        ctx = dict(context or {})
        key = action.strip().casefold()
        try:
            if key in {"迁移策略", "迁移存量配置", "migrate", "migrate_strategy"}:
                legacy = ctx.pop("legacy_config", None) or ctx.pop("old_config", None) or ctx.pop("config", None)
                new = ctx.pop("new_config", None) or ctx.pop("new_strategy", None)
                result = self.migrate_config(legacy, new, **ctx)
            elif key in {"登记策略", "register_strategy"}:
                result = self.register_strategy(ctx.pop("config", None), **ctx)
            elif key in {"确认窗口", "确认完整新版", "confirm_window", "confirm_strategy"}:
                strategy_id = str(ctx.pop("strategy_id", "") or ctx.pop("id", ""))
                result = self.confirm_window(strategy_id, **ctx)
            elif key in {"采样", "sample", "select_representatives"}:
                result = {
                    "samples": self.select_representatives(
                        ctx.pop("events", None) or ctx.pop("rows", []), **ctx),
                }
            elif key in {"领取轮次", "claim_round", "claim"}:
                ctx.setdefault("round_id", f"ROUND-{entry_id}")
                result = self.claim_round(**ctx)
            elif key in {"确认轮次", "结算", "confirm_round", "settle", "confirm"}:
                round_id = str(ctx.pop("round_id", f"ROUND-{entry_id}"))
                result = self.confirm_round(round_id, **ctx)
            elif key in {"恢复调查", "对账", "recover", "reconcile", "recover_investigation"}:
                investigation_id = str(ctx.pop("investigation_id", f"INV-{entry_id}"))
                result = self.recover_investigation(
                    investigation_id,
                    ctx.pop("window_samples", None) or ctx.pop("samples", None),
                )
            else:
                return super().run_action(entry_id, action, context)

            if isinstance(result, dict) and result.get("ok") is False:
                return None, str(result.get("message") or "采样策略动作未完成")
            return result if isinstance(result, dict) else {"result": result}, f"采样计划已{action}"
        except Exception as exc:  # 路由契约使用 ok=False/message，而不是 500
            return None, str(exc)
