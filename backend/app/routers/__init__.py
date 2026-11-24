"""业务路由汇总：专用采样迁移入口先于通配资源路由注册。"""
from __future__ import annotations

from app.routers import resource, sample_strategy

ROUTERS = [sample_strategy, resource]
