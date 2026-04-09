"""
FastAPI application factory.

Creates the FastAPI app with:
- LangGraph agent (compiled graph with SQLite checkpointer — kept for
  LangGraph internal state; there is no built-in Cosmos checkpointer)
- Cosmos DB Database for chat metadata (replaces SQLite chats.db)
- RuntimeStateManager for per-thread AgentState
- CORS middleware
- Static file serving for the React frontend
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver

from .config import get_server_config
from .database import Database
from .runtime_state import RuntimeStateManager

logger = logging.getLogger("server.app")


def _run_azure_connectivity_check():
    """
    Run a lightweight LLM call to verify Azure/OpenAI connectivity and auth.
    Returns a JSON-serializable status dict.
    """
    from agent.config import get_config
    from openai import AzureOpenAI

    cfg = get_config()
    checked_at = datetime.now(timezone.utc).isoformat()

    if not cfg.openai_api_key:
        return {
            "ok": False,
            "checked_at": checked_at,
            "provider": "azure",
            "error_type": "missing_api_key",
            "error": "API key is missing",
        }

    try:
        client = AzureOpenAI(
            api_key=cfg.openai_api_key,
            azure_endpoint=cfg.azure_openai_endpoint,
            api_version=cfg.azure_openai_api_version,
            timeout=15.0,
        )
        provider = "azure"

        response = client.chat.completions.create(
            model=cfg.openai_model,
            messages=[{"role": "user", "content": "ping"}],
            max_completion_tokens=128,
        )
        model_used = getattr(response, "model", cfg.openai_model)
        return {
            "ok": True,
            "checked_at": checked_at,
            "provider": provider,
            "model": model_used,
            "deployment": cfg.openai_model,
            "endpoint": cfg.azure_openai_endpoint or None,
        }
    except Exception as exc:
        status_code = getattr(exc, "status_code", None)
        return {
            "ok": False,
            "checked_at": checked_at,
            "provider": "azure",
            "deployment": cfg.openai_model,
            "endpoint": cfg.azure_openai_endpoint or None,
            "status_code": status_code,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    cfg = get_server_config()

    # -- STARTUP --

    from .cosmos_store import get_cosmos_store, is_cosmos_enabled
    cosmos_enabled = is_cosmos_enabled()
    app.state.cosmos_enabled = cosmos_enabled

    # 1. Initialize data store (Cosmos DB or PostgreSQL)
    store = get_cosmos_store()
    app.state.cosmos_store = store
    if cosmos_enabled:
        if store.available:
            logger.info("CosmosStore initialised — all 8 containers")
        else:
            logger.error("CRITICAL: CosmosStore is NOT available — check COSMOS_* env vars")
    else:
        if store.available:
            logger.info("COSMOS=OFF — PostgresStore initialised (6 tables). RAG/Embeddings DISABLED.")
        else:
            logger.error("CRITICAL: PostgresStore is NOT available — check POSTGRES_* env vars")

    # 1a. Initialize chat metadata database (Cosmos or PostgreSQL via factory)
    db = None
    try:
        db = Database()
        db.initialize()
        app.state.db = db
        logger.info("Chat database initialized (backend=%s)", "cosmos" if cosmos_enabled else "postgres")
    except Exception as e:
        logger.error("Chat database failed to initialize: %s", e)
        app.state.db = None

    # 1b. Initialize Azure Blob Storage
    from .blob_store import get_blob_store, BlobStore
    blob_store = get_blob_store()
    app.state.blob_store = blob_store
    if blob_store.available:
        logger.info("Azure Blob Storage initialized")
    else:
        logger.error(
            "CRITICAL: Azure Blob Storage is NOT available — "
            "file uploads and artifact storage will fail. "
            "Check AZURE_STORAGE_CONNECTION_STRING in .env"
        )

    # 1c. Start blob cache cleanup daemon (TTL-based eviction of /tmp/sox_blob_cache)
    BlobStore.start_cache_cleanup_daemon()

    # 2. Initialize LangGraph checkpointer (async SQLite — internal to LangGraph)
    #    The SQLite checkpointer stores LangGraph graph state (message history).
    #    We keep SQLite here because there is no built-in Cosmos DB checkpointer.
    #    Ensure the data directory exists (belt-and-suspenders — config.__post_init__ also does this)
    os.makedirs(os.path.dirname(cfg.checkpoints_db_path), exist_ok=True)

    async with AsyncSqliteSaver.from_conn_string(cfg.checkpoints_db_path) as checkpointer:
        app.state.checkpointer = checkpointer
        logger.info("LangGraph checkpointer at %s", cfg.checkpoints_db_path)

        # 3. Build the LangGraph agent
        from .graph import create_graph
        graph = create_graph(checkpointer)
        app.state.graph = graph
        logger.info("LangGraph agent compiled")

        # 4. Initialize runtime state manager
        state_manager = RuntimeStateManager()
        app.state.state_manager = state_manager

        # 5. Validate agent config
        from agent.config import get_config
        agent_cfg = get_config()
        if not agent_cfg.openai_api_key or agent_cfg.openai_api_key.startswith("<"):
            logger.warning("OPENAI_API_KEY may not be set correctly")

        # 5b. Startup Azure/OpenAI connectivity probe (non-fatal)
        app.state.azure_health = _run_azure_connectivity_check()
        if app.state.azure_health.get("ok"):
            logger.info(
                "LLM connectivity check passed (%s/%s)",
                app.state.azure_health.get("provider"),
                app.state.azure_health.get("deployment"),
            )
        else:
            logger.warning(
                "LLM connectivity check failed: %s",
                app.state.azure_health.get("error"),
            )

        # 5c. Auto-seed ICOFAR handbook for RAG (background, non-blocking)
        #     Skipped when COSMOS=OFF (RAG requires Cosmos Embeddings container)
        app.state.handbook_project_id = "icofar-handbook"

        if not cosmos_enabled:
            logger.info("COSMOS=OFF — skipping ICOFAR handbook RAG seed (RAG disabled)")
        else:
            handbook_path = os.path.join(
                os.path.dirname(__file__), "..", "..",
                "sox_package 2",
                "handbook-internal-controls-over-financial-reporting.pdf",
            )
            if os.path.exists(handbook_path):
                import threading

                def _seed_handbook():
                    try:
                        from engines.RAGEngine import RAGEngine
                        engine = RAGEngine()
                        index_status = engine.is_indexed(app.state.handbook_project_id)
                        if index_status.get("indexed"):
                            logger.info("ICOFAR handbook already indexed (%d chunks) — skipping seed", index_status.get("chunk_count", 0))
                            return
                        logger.info("Seeding ICOFAR handbook (background)...")
                        result = engine.ingest(file_path=handbook_path, project_id=app.state.handbook_project_id, force_reindex=False)
                        if result.success:
                            logger.info("ICOFAR handbook seeded: %d pages, %d chunks in %.1fs", result.total_pages, result.total_chunks, result.duration_seconds)
                        else:
                            logger.warning("ICOFAR handbook seed failed: %s", result.error)
                    except Exception as exc:
                        logger.warning("ICOFAR handbook seed error (non-fatal): %s", exc)

                threading.Thread(target=_seed_handbook, daemon=True, name="handbook-seed").start()
            else:
                logger.warning("ICOFAR handbook PDF not found at %s — RAG unavailable", handbook_path)

        # 6. Start idle state eviction background task
        async def eviction_loop():
            while True:
                await asyncio.sleep(300)  # every 5 minutes
                state_manager.evict_idle()

        eviction_task = asyncio.create_task(eviction_loop())

        logger.info("Server started — model=%s", agent_cfg.openai_model)

        yield  # App is running

        # -- SHUTDOWN --
        eviction_task.cancel()
        try:
            await eviction_task
        except asyncio.CancelledError:
            pass

        if db is not None:
            db.close()
        logger.info("Server shut down")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="Control Testing Agent",
        version="1.0.0",
        lifespan=lifespan,
    )

    # CORS — allow the Vite dev server and any configured origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            o.strip()
            for o in os.getenv("FLASK_CORS_ORIGINS", "").split(",")
            if o.strip()
        ] or ["http://localhost:5173"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    internal_token = os.getenv("FLASK_INTERNAL_TOKEN", "").strip()

    @app.middleware("http")
    async def internal_api_guard(request: Request, call_next):
        if internal_token and request.url.path.startswith("/api/"):
            open_paths = {"/api/health", "/api/health/azure"}
            open_prefixes = ("/api/auth/",)
            if (
                request.url.path not in open_paths
                and not request.url.path.startswith(open_prefixes[0])
            ):
                provided = request.headers.get("x-internal-token", "")
                if provided != internal_token:
                    return JSONResponse(
                        status_code=401,
                        content={"detail": "Unauthorized internal API access"},
                    )
        return await call_next(request)

    # Register API routes
    from .routes.auth import router as auth_router
    from .routes.chats import router as chats_router
    from .routes.messages import router as messages_router
    from .routes.upload import router as upload_router
    from .routes.users import router as users_router
    from .routes.projects import router as projects_router
    from .routes.documents import router as documents_router
    from .routes.embeddings import router as embeddings_router

    app.include_router(auth_router)
    app.include_router(chats_router)
    app.include_router(messages_router)
    app.include_router(upload_router)
    app.include_router(users_router)
    app.include_router(projects_router)
    app.include_router(documents_router)
    app.include_router(embeddings_router)

    # Health endpoint
    @app.get("/api/health")
    async def health():
        state_mgr = app.state.state_manager
        blob = getattr(app.state, "blob_store", None)
        store = getattr(app.state, "cosmos_store", None)
        _cosmos_on = getattr(app.state, "cosmos_enabled", True)
        if _cosmos_on:
            db_info = {"available": store.available if store else False,
                       "containers": ["Sessions", "Messages", "Agent-memory", "Embeddings",
                                      "Context-cache", "Users", "Projects", "Documents"]}
        else:
            db_info = {"available": store.available if store else False,
                       "tables": ["sessions", "messages", "agent_memory", "users", "projects", "documents"],
                       "disabled": ["Embeddings", "Context-cache", "RAG"]}
        return {
            "status": "ok",
            "active_states": state_mgr.active_count,
            "azure_openai": getattr(app.state, "azure_health", None),
            "db_backend": "cosmos" if _cosmos_on else "postgres",
            "db": db_info,
            "blob_storage": "available" if (blob and blob.available) else "unavailable",
        }

    @app.get("/api/health/azure")
    async def health_azure(refresh: bool = False):
        if refresh:
            app.state.azure_health = _run_azure_connectivity_check()
        return {
            "status": "ok",
            "azure_openai": getattr(app.state, "azure_health", None),
        }

    # Serve React frontend static files (built with `npm run build`)
    static_dir = os.path.join(os.path.dirname(__file__), "..", "web", "dist")
    if os.path.exists(static_dir):
        app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
        logger.info("Serving frontend from %s", static_dir)
    else:
        @app.get("/")
        async def root():
            return {
                "message": "Control Testing Agent API",
                "docs": "/docs",
                "note": "Run 'cd web && npm run build' to serve the frontend",
            }

    return app
