"""本地定制：把 Neo4j 同步层接到 Explorer 的 FastAPI 生命周期与路由。

分工：
- ``neo4j_sync.py``  纯数据读写，不依赖 FastAPI。
- 本模块             应用接线：mutation 回调、启动全量加载、/api/graph/reload。

上游同步约定：本文件为本地新增（上游无同名文件），同步时不产生冲突；
``app.py`` 里只保留几行调用。
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .neo4j_sync import load_ontology_state, reload_graph, sync_mutation

logger = logging.getLogger(__name__)


def _read_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def install_neo4j_mutation_sync(app: FastAPI, session: Any, store: Any) -> None:
    """在已有 mutation_callback 链尾追加 Neo4j 落库。

    上游 ``runtime.install_mutation_bridge`` 已负责索引更新与 WebSocket 广播；
    本函数只补持久化，沿用它留下的 ``previous_callback`` 链，无需复制上游实现。
    """
    if store is None:
        return
    previous_callback = getattr(session.graph, "mutation_callback", None)

    def on_mutation(event_type: str, entity_id: str, payload: dict) -> None:
        sync_mutation(store, event_type, entity_id, payload)
        if callable(previous_callback):
            previous_callback(event_type, entity_id, payload)

    session.graph.mutation_callback = on_mutation


async def bootstrap_from_neo4j(app: FastAPI, store: Any, session: Any) -> bool:
    """启动时从 Neo4j 全量加载内存图与本体状态，带重试等待 Neo4j 就绪。

    返回 True 表示加载成功；False 表示重试耗尽（沿用空图继续运行，不致命）。
    ``Neo4jStore`` 构造是 lazy 的，首次查询才连 Bolt——服务启动时 Neo4j 常常
    尚未就绪，不等待会让内存图静默为空。``reload_graph`` 内部先 clear 再加载，
    因此重复重试幂等、不会重复计数。
    """
    max_attempts = max(1, _read_int_env("NEO4J_BOOTSTRAP_MAX_ATTEMPTS", 60))
    backoff = max(0.5, float(os.environ.get("NEO4J_BOOTSTRAP_BACKOFF", "2")))

    # 默认空容器，保证即便加载失败 app.state 也有可用默认值（后续访问不 KeyError）。
    app.state.ontology_drafts = {}
    app.state.ontology_proposals = {}
    app.state.ontology_versions = {}

    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            reload_graph(store, session)
            from .routes.ontology import (
                DraftResponse,
                ProposalResponse,
                VersionEntry,
            )

            onto_state = load_ontology_state(store)
            app.state.ontology_drafts = {
                d["draft_id"]: DraftResponse(**d)
                for d in onto_state["drafts"]
                if d.get("draft_id")
            }
            app.state.ontology_proposals = {
                p["proposal_id"]: ProposalResponse(**p)
                for p in onto_state["proposals"]
                if p.get("proposal_id")
            }
            app.state.ontology_versions = {
                uri: [VersionEntry(**v) for v in vlist]
                for uri, vlist in onto_state["versions"].items()
            }
            logger.info(
                "启动从 Neo4j 加载成功（第 %d 次尝试）：内存图 + %d drafts / "
                "%d proposals / %d versions",
                attempt,
                len(app.state.ontology_drafts),
                len(app.state.ontology_proposals),
                sum(len(v) for v in app.state.ontology_versions.values()),
            )
            return True
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            logger.warning(
                "启动从 Neo4j 加载失败（第 %d/%d 次）：%s", attempt, max_attempts, exc
            )
            if attempt < max_attempts:
                await asyncio.sleep(backoff)
    logger.error(
        "启动从 Neo4j 加载在 %d 次尝试后仍失败，使用空图继续：%s",
        max_attempts,
        last_err,
    )
    return False


def register_neo4j_routes(app: FastAPI) -> None:
    """注册本地定制路由。"""

    @app.post("/api/graph/reload", tags=["local"])
    async def reload_graph_endpoint(request: Request):
        """从 Neo4j 重新全量加载内存图。

        供外部（如 RDB 同步任务）直接写入 Neo4j 后，让 Explorer 感知最新数据。
        """
        store = getattr(request.app.state, "neo4j_store", None)
        session = getattr(request.app.state, "session", None)
        if store is None or session is None:
            return JSONResponse(
                status_code=400,
                content={"detail": "Neo4j 未配置或会话未初始化"},
            )
        try:
            reload_graph(store, session)
            return {
                "status": "ok",
                "nodes": len(session.graph.nodes),
                "edges": len(session.graph.edges),
            }
        except Exception as exc:  # noqa: BLE001
            return JSONResponse(status_code=500, content={"detail": str(exc)})
