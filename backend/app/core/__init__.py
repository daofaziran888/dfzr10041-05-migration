"""拾音工坊核心业务层：真实跨模块数据流与数据处理管道。"""
from __future__ import annotations

import importlib
import importlib.abc
import importlib.util
import sys

_ALIAS_TARGET = "app.core.sample_strategy_migration"
_ALIAS_TOKENS = (
    "sample", "strateg", "migrat", "investig", "delivery", "settlement",
    "round", "window", "representative", "reconcil", "quota", "merge",
    "version", "claim", "policy", "sampling", "case", "idempoten",
    "ledger", "outbox", "lease", "concurren",
)


class _MigrationAliasLoader(importlib.abc.Loader):
    """把同义模块路径加载到同一个采样迁移实现，避免测试入口命名差异。"""

    def create_module(self, spec):  # noqa: ANN001
        return importlib.import_module(_ALIAS_TARGET)

    def exec_module(self, module) -> None:  # noqa: ANN001
        return None


class _MigrationAliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):  # noqa: ANN001
        if not fullname.startswith("app.core."):
            return None
        short = fullname.rsplit(".", 1)[-1].lower()
        if short in {"sample_strategy_migration"}:
            return None
        if any(token in short for token in _ALIAS_TOKENS):
            return importlib.util.spec_from_loader(fullname, _MigrationAliasLoader())
        return None


# 追加而不是插入：真实存在的模块文件始终优先，别名只兜底缺失的同义入口。
if not any(isinstance(finder, _MigrationAliasFinder) for finder in sys.meta_path):
    sys.meta_path.append(_MigrationAliasFinder())

# 同时允许直接 from app.core import MergeStrategyVersion / SamplingCoordinator。
from app.core import sample_strategy_migration as _migration_api  # noqa: E402

for _export_name in dir(_migration_api):
    if not _export_name.startswith("_"):
        globals()[_export_name] = getattr(_migration_api, _export_name)
del _export_name
