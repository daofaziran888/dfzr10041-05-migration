"""采样策略版本迁移与调查恢复的专用 HTTP 入口。

通用 sample_job 资源路由负责表格 CRUD；这些路径承载有请求体的领域动作。
测试也可以直接调用 SampleJobService / MigrationOrchestrator，不必经过 HTTP。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from app.core.sample_strategy_migration import to_jsonable
from app.services import service_for

router = APIRouter(prefix="/api/sample-strategy", tags=["采样策略迁移"])


def _service():
    service = service_for("sample_job")
    assert service is not None
    return service


def _json(result: Any) -> JSONResponse:
    return JSONResponse(to_jsonable(result))


@router.post("/migrate")
def migrate(payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    service = _service()
    legacy = payload.get("legacy_config") or payload.get("old_config") or payload.get("config")
    new = payload.get("new_config") or payload.get("new_strategy")
    result = service.migrate_config(
        legacy,
        new,
        events=payload.get("events"),
        window_size_minutes=int(payload.get("window_size_minutes", 5)),
        auto_confirm=bool(payload.get("auto_confirm", False)),
    )
    return _json(result)


@router.post("/strategies")
def register_strategy(payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    return _json(_service().register_strategy(payload))


@router.post("/strategies/{strategy_id}/confirm")
def confirm_window(strategy_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    result = _service().confirm_window(
        strategy_id,
        payload.get("windows"),
        expected_version=payload.get("expected_version"),
        source_timezone=payload.get("source_timezone"),
        timezone_verified=payload.get("timezone_verified"),
        verifier=payload.get("verifier"),
    )
    return _json(result)


@router.post("/sample")
def sample(payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    service = _service()
    strategy = payload.get("strategy")
    if isinstance(strategy, dict):
        strategy = service.register_strategy(strategy) if payload.get("register", False) else strategy
    result = service.select_representatives(
        payload.get("events") or payload.get("rows") or [],
        strategy=strategy,
        config=payload.get("config"),
        strategy_version=payload.get("strategy_version"),
        plan_id=payload.get("plan_id"),
    )
    return _json({"samples": result, "sample_ids": [x["id"] for x in result]})


@router.post("/rounds")
def claim_round(payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    result = _service().claim_round(**payload)
    return _json(result)


@router.post("/rounds/{round_id}/claim")
def claim_named_round(round_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    payload = dict(payload)
    payload["round_id"] = round_id
    return _json(_service().claim_round(**payload))


@router.post("/rounds/{round_id}/confirm")
def confirm_round(round_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    payload = dict(payload)
    payload.pop("round_id", None)
    result = _service().confirm_round(round_id, **payload)
    return _json(result)


@router.post("/investigations/{investigation_id}/deliveries")
def receive_delivery(investigation_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    payload = dict(payload)
    payload["investigation_id"] = investigation_id
    return _json(_service().receive_delivery(**payload))


@router.post("/investigations/{investigation_id}/recover")
def recover(investigation_id: str, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    result = _service().recover_investigation(
        investigation_id,
        payload.get("window_samples") or payload.get("samples"),
    )
    return _json(result)


@router.get("/investigations/{investigation_id}/deliveries/{delivery_id}/verify")
def verify(investigation_id: str, delivery_id: str) -> JSONResponse:
    return _json(_service().verify_delivery(investigation_id, delivery_id))
