"""
Semantica Explorer FastAPI application factory.
"""

import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional, Sequence

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .. import __version__
from ..context.agent_memory import AgentMemory
from ..context.context_graph import ContextGraph
from .dependencies import anonymous_access_allowed, get_expected_api_key, require_auth
from .markdown_resources import MarkdownResourceRegistry
from .neo4j_sync import (
    load_ontology_state,
    make_store,
    reload_graph,
    sync_mutation,
)
from .runtime import explorer_capabilities
from .session import GraphSession
from .ws import ConnectionManager, install_graph_updates_websocket


def _read_int_env(name: str, default: int) -> int:
    raw_value = os.environ.get(name)
    if raw_value is None or raw_value.strip() == "":
        return default
    try:
        return int(raw_value)
    except ValueError:
        return default


def _read_explorer_settings() -> dict:
    if "ALLOWED_ORIGINS" in os.environ:
        raw_origins = os.environ["ALLOWED_ORIGINS"]
    elif "EXPLORER_CORS_ORIGINS" in os.environ:
        raw_origins = os.environ["EXPLORER_CORS_ORIGINS"]
    else:
        raw_origins = (
            "http://localhost:5173,http://127.0.0.1:5173,"
            "http://localhost:8000,http://127.0.0.1:8000"
        )
    return {
        "allowed_origins": [
            origin.strip() for origin in raw_origins.split(",") if origin.strip()
        ],
        # These are read and stored for future use when direct FalkorDB connection
        # support is added to the Explorer. Currently GraphSession uses an in-memory
        # ContextGraph and does not open a network connection to FalkorDB.
        "falkordb_host": os.environ.get("FALKORDB_HOST", "localhost"),
        "falkordb_port": _read_int_env("FALKORDB_PORT", 6379),
        "provenance_storage_path": os.environ.get(
            "SEMANTICA_PROVENANCE_DB",
            os.environ.get("EXPLORER_PROVENANCE_DB"),
        ),
    }


def _install_mutation_bridge(app: FastAPI, session: GraphSession) -> None:
    if getattr(session.graph, "_mutation_bridge_installed", False):
        return
    session.graph._mutation_bridge_installed = True
    previous_callback = getattr(session.graph, "mutation_callback", None)

    def on_mutation(event_type: str, entity_id: str, payload: dict) -> None:
        session.handle_graph_mutation(event_type, entity_id, payload)
        store = getattr(app.state, "neo4j_store", None)
        if store is not None:
            sync_mutation(store, event_type, entity_id, payload)
        if callable(previous_callback):
            previous_callback(event_type, entity_id, payload)
        loop = getattr(app.state, "event_loop", None)
        manager = getattr(app.state, "ws_manager", None)
        if loop is None or manager is None or loop.is_closed():
            return
        message = {
            "event_type": event_type,
            "entity_id": entity_id,
            "payload": payload,
        }
        asyncio.run_coroutine_threadsafe(
            manager.broadcast("graph_mutation", message),
            loop,
        )

    session.graph.mutation_callback = on_mutation


async def _bootstrap_neo4j_state(
    app: FastAPI,
    neo4j_store: Any,
    session: Any,
) -> bool:
    """启动阶段从 Neo4j 全量加载内存图与本体状态，带重试等待 Neo4j 就绪。

    返回 True 表示加载成功；False 表示耗尽重试（仍用空图继续运行，不致命）。

    为什么需要重试：``Neo4jStore`` 构造是 lazy 的（不建立连接），首次
    ``execute_query`` 才会真正连 Bolt。容器启动时 Neo4j 往往还没就绪，若不在
    启动时等待其健康再加载，内存图会静默为空，导致 Registry 重启后计数归零。
    ``reload_graph`` 内部 ``ContextGraph.from_dict`` 会先 ``clear()`` 再加载，
    因此重复重试幂等安全、不会重复计数。
    """
    import asyncio
    import logging as _blog

    logger = _blog.getLogger(__name__)
    max_attempts = max(1, _read_int_env("NEO4J_BOOTSTRAP_MAX_ATTEMPTS", 60))
    backoff = max(0.5, float(os.environ.get("NEO4J_BOOTSTRAP_BACKOFF", "2")))

    # 默认空容器，保证即便加载失败 app.state 也有可用默认值（后续访问不 KeyError）。
    app.state.ontology_drafts = {}
    app.state.ontology_proposals = {}
    app.state.ontology_versions = {}

    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            # 全量刷新内存图（from_dict 先 clear 再加载，幂等安全）。
            reload_graph(neo4j_store, session)
            # 加载本体 drafts / proposals / versions 到 app.state。
            from .routes.ontology import (
                DraftResponse,
                ProposalResponse,
                VersionEntry,
            )
            onto_state = load_ontology_state(neo4j_store)
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
                "启动从 Neo4j 加载失败（第 %d/%d 次）：%s",
                attempt, max_attempts, exc,
            )
            if attempt < max_attempts:
                await asyncio.sleep(backoff)
    logger.error(
        "启动从 Neo4j 加载在 %d 次尝试后仍失败，使用空图继续：%s",
        max_attempts, last_err,
    )
    return False


def create_app(
    session: Optional[GraphSession] = None,
    provenance_storage_path: Optional[str] = None,
    agent_memory: Optional[AgentMemory] = None,
    allowed_origins: Optional[Sequence[str]] = None,
) -> FastAPI:
    """Create an Explorer application over live graph and memory objects.

    Args:
        session: Graph session exposed by the Explorer. A new in-memory graph
            session is created when omitted.
        provenance_storage_path: Optional per-app provenance database path.
        agent_memory: Existing AgentMemory instance to expose in the Memories
            workspace. The workspace is unavailable when omitted.
        allowed_origins: Optional custom sequence of allowed CORS/WebSocket
            origins. When omitted, origins are read from ALLOWED_ORIGINS (or
            EXPLORER_CORS_ORIGINS) or default to standard localhost origins.

    Returns:
        Configured FastAPI application.
    """
    settings = _read_explorer_settings()
    if allowed_origins is not None:
        settings["allowed_origins"] = [
            origin.strip() for origin in allowed_origins if origin.strip()
        ]
    prov_path = provenance_storage_path or settings.get("provenance_storage_path")

    # 穿山甲基座：初始内存图用空图；真正的全量加载放到 lifespan 启动时执行
    # （带重试等待 Neo4j 就绪）。原因：Neo4j 在 import 阶段常常尚未就绪，若在此
    # 同步加载会静默失败 → 重启后内存图为空、Registry 计数归零。lifespan 重试
    # 可消除启动时序竞态。
    neo4j_store = make_store()
    initial_graph = ContextGraph(advanced_analytics=False)

    if session is None:
        active_session = GraphSession(
            initial_graph,
            provenance_storage_path=prov_path,
        )
    else:
        active_session = session
        if prov_path is not None:
            active_session.set_provenance_storage_path(prov_path)
    markdown_resources = MarkdownResourceRegistry(active_session.graph, agent_memory)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        import logging as _lifespan_logging
        _lifespan_logger = _lifespan_logging.getLogger(__name__)
        if anonymous_access_allowed():
            _lifespan_logger.warning(
                "Explorer is running with SEMANTICA_ALLOW_ANONYMOUS=true — "
                "all API routes are unauthenticated. Do not expose this "
                "process beyond localhost."
            )
        elif get_expected_api_key():
            _lifespan_logger.info("Explorer API authentication: enabled (SEMANTICA_API_KEY set).")
        else:
            _lifespan_logger.warning(
                "Explorer API authentication: NOT CONFIGURED. All protected "
                "routes will return 503 until SEMANTICA_API_KEY is set."
            )

        app.state.event_loop = asyncio.get_running_loop()
        app.state.ws_manager = ConnectionManager()
        app.state.session = active_session
        app.state.agent_memory = agent_memory
        app.state.markdown_resources = markdown_resources
        _install_mutation_bridge(app, active_session)

        # Phase 2: 启动从 Neo4j 全量加载内存图 + 本体状态（带重试，等待
        # Neo4j 就绪，消除启动时序竞态导致的 Registry 空计数问题）。
        if neo4j_store is not None:
            await _bootstrap_neo4j_state(app, neo4j_store, active_session)

        yield

    app = FastAPI(
        title="Semantica Knowledge Explorer",
        description="Interactive dashboard API for exploring Semantica knowledge graphs.",
        version=__version__,
        lifespan=lifespan,
    )

    app.state.explorer_settings = settings
    app.state.neo4j_store = neo4j_store

    # allow_credentials lets browsers send cookies/auth headers cross-origin.
    # Credentials aren't needed for the X-API-Key auth scheme below, and
    # enabling them when origins are broadened creates cross-site request
    # risk. Set EXPLORER_CORS_CREDENTIALS=true explicitly to opt in (e.g.
    # for a reverse-proxy setup that injects its own cookie-based auth).
    _allow_credentials = os.environ.get("EXPLORER_CORS_CREDENTIALS", "false").lower() == "true"
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings["allowed_origins"],
        allow_credentials=_allow_credentials,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Content-Type", "Authorization", "X-API-Key"],
        max_age=600,
    )

    import logging as _logging
    _logger = _logging.getLogger(__name__)

    @app.exception_handler(KeyError)
    async def key_error_handler(_request: Request, exc: KeyError):
        _logger.warning("KeyError: %s", exc)
        return JSONResponse(status_code=404, content={"detail": "Resource not found"})

    @app.exception_handler(ValueError)
    async def value_error_handler(_request: Request, exc: ValueError):
        _logger.warning("ValueError: %s", exc)
        return JSONResponse(status_code=422, content={"detail": "Invalid input"})

    @app.exception_handler(Exception)
    async def generic_error_handler(_request: Request, exc: Exception):
        if isinstance(exc, HTTPException):
            raise exc
        _logger.exception("Unhandled exception")
        return JSONResponse(status_code=500, content={"detail": "Internal server error"})

    from .routes.analytics import router as analytics_router
    from .routes.annotations import router as annotations_router
    from .routes.decisions import router as decisions_router
    from .routes.enrich import router as enrich_router
    from .routes.export_import import router as export_import_router
    from .routes.graph import router as graph_router
    from .routes.markdown import router as markdown_router
    from .routes.memories import router as memories_router
    from .routes.ontology import router as ontology_router
    from .routes.provenance import router as provenance_router
    from .routes.sparql import router as sparql_router
    from .routes.temporal import router as temporal_router
    from .routes.vocabulary import router as vocabulary_router

    _auth = [Depends(require_auth)]
    app.include_router(graph_router, dependencies=_auth)
    app.include_router(analytics_router, dependencies=_auth)
    app.include_router(decisions_router, dependencies=_auth)
    app.include_router(temporal_router, dependencies=_auth)
    app.include_router(enrich_router, dependencies=_auth)
    app.include_router(export_import_router, dependencies=_auth)
    app.include_router(markdown_router, dependencies=_auth)
    app.include_router(memories_router, dependencies=_auth)
    app.include_router(annotations_router, dependencies=_auth)
    app.include_router(sparql_router, dependencies=_auth)
    app.include_router(provenance_router, dependencies=_auth)
    app.include_router(vocabulary_router, dependencies=_auth)
    app.include_router(ontology_router, dependencies=_auth)

    install_graph_updates_websocket(app, settings["allowed_origins"])

    @app.post("/api/graph/reload")
    async def reload_graph_endpoint(request: Request):
        """从 Neo4j 重新全量加载内存图。

        供外部（如 RDB 同步任务）直接写入 Neo4j 后，让 Explorer 感知最新数据。
        匿名模式（SEMANTICA_ALLOW_ANONYMOUS）下可直接调用。
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

    @app.get("/", include_in_schema=False)
    async def root():
        index_path = Path(__file__).resolve().parent.parent / "static" / "index.html"
        if index_path.is_file():
            return FileResponse(index_path)
        _logger.warning(
            "Explorer frontend bundle not found — UI unavailable. "
            "Install the package via pip to get the pre-built bundle, "
            "or run `cd explorer && npm ci && npm run build` from the repo root."
        )
        return HTMLResponse(
            '<!doctype html><html lang="en"><head><meta charset="UTF-8">'
            '<title>Semantica Knowledge Explorer</title>'
            '<style>body{font-family:sans-serif;padding:2rem;max-width:600px;margin:auto}'
            'code{background:#f4f4f4;padding:2px 6px;border-radius:3px}</style></head>'
            "<body><h2>Explorer UI not available</h2>"
            "<p>The frontend bundle was not found. This usually means the package was "
            "installed from source without building the frontend first.</p>"
            "<p><strong>To fix:</strong> reinstall via "
            "<code>pip install semantica[explorer]</code>, or build from source with "
            "<code>cd explorer &amp;&amp; npm ci &amp;&amp; npm run build</code> "
            "then restart the server.</p>"
            '<p>The REST API is still fully available at <a href="/docs">/docs</a>.</p>'
            "</body></html>",
            status_code=200,
        )

    @app.get("/api/health")
    async def health():
        return {"status": "ok"}

    @app.get("/api/info")
    async def info():
        return {
            "name": "Semantica Knowledge Explorer",
            "version": __version__,
            "status": "active",
            "capabilities": explorer_capabilities(agent_memory),
        }

    static_dir = Path(__file__).resolve().parent.parent / "static"
    if static_dir.is_dir():
        assets_dir = static_dir / "assets"
        if assets_dir.is_dir():
            app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

        @app.get("/{full_path:path}", include_in_schema=False)
        async def serve_spa(full_path: str):
            if full_path.startswith("api/"):
                raise HTTPException(status_code=404, detail="API route not found")
            index_path = static_dir / "index.html"
            if index_path.is_file():
                return FileResponse(index_path)
            raise HTTPException(status_code=404, detail="Frontend build missing")

    return app


# Module-level app instance used by uvicorn and Docker CMD.
app = create_app()
