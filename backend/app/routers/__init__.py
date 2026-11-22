"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

注意顺序：跨模块编排路由（/api/cross/...）必须排在通用资源路由
（/api/{module}/...）之前，否则固定前缀会被动态路径 {module}/{entry_id}
抢先匹配，导致跨模块端点全部 422。
"""
from __future__ import annotations

from app.routers import resource, cross_module

ROUTERS = [cross_module, resource]