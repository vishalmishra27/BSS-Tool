#!/usr/bin/env python
"""
Entry point for the Control Testing Agent web interface.

Starts the FastAPI server that serves:
  1. REST API for chat management
  2. SSE streaming for real-time agent responses (via LangGraph)
  3. React frontend static files (when built)

Usage:
    python run_server.py
    # Or: uvicorn server.app:create_app --factory --host 0.0.0.0 --port 8000
"""

import os
import sys

# Suppress InsecureRequestWarning from urllib3 (we use connection_verify=False
# for Cosmos DB in corporate/proxy environments)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Load .env FIRST — before any server/agent modules are imported ────────────
# database.py and cosmos_store.py read COSMOS_* as module-level constants,
# so load_dotenv() must run before those modules are imported.
from dotenv import load_dotenv
_flask_api_dir = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_flask_api_dir, ".env"))

# ── Ensure flask-api/ and engines/ are on sys.path ────────────────────────────
sys.path.insert(0, _flask_api_dir)
sys.path.insert(0, os.path.join(_flask_api_dir, "engines"))

# ── Now it is safe to import server modules ───────────────────────────────────
import logging
import uvicorn
from server.config import get_server_config
from server.app import create_app


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-24s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    # Reduce noise from httpx, openai, and azure-cosmos internals
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("azure").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    cfg = get_server_config()
    app = create_app()

    print(f"\n  Control Testing Agent — Web Interface")
    print(f"  Server:   http://localhost:{cfg.port}")
    print(f"  API docs: http://localhost:{cfg.port}/docs")
    print(f"  Press Ctrl+C to stop\n")

    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level="info")


if __name__ == "__main__":
    main()
