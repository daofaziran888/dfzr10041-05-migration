"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    model_config = ConfigDict(extra="allow")

    action: str
    remark: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    data: dict[str, Any] = Field(default_factory=dict)

    def domain_context(self) -> dict[str, Any]:
        """合并顶层额外字段、data/values，方便领域动作接收复杂参数。"""
        context: dict[str, Any] = {}
        context.update(self.model_extra or {})
        context.update(self.data or {})
        context.update(self.values or {})
        if self.remark is not None:
            context["remark"] = self.remark
        context.pop("action", None)
        return context
