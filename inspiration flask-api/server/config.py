"""
Server configuration.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class ServerConfig:
    """Immutable server configuration."""

    host: str = os.getenv("FLASK_HOST", "127.0.0.1")
    port: int = int(os.getenv("FLASK_PORT", "8000"))

    # LangGraph internal SQLite checkpoint path (not exposed to callers)
    checkpoints_db_path: str = os.path.join(
        os.path.dirname(__file__), "data", "checkpoints.db"
    )

    # Session management
    max_active_states: int = 10
    idle_timeout_seconds: int = 1800  # 30 minutes

    # Context windowing
    max_context_messages: int = 40

    # Safety limits
    max_iterations: int = 30        # Max agent→tool loops per invocation
    max_tool_calls: int = 50        # Max total tool calls per invocation

    def __post_init__(self):
        # Ensure the data directory exists for the SQLite checkpoint file
        data_dir = os.path.dirname(self.checkpoints_db_path)
        os.makedirs(data_dir, exist_ok=True)


_server_config = None


def get_server_config() -> ServerConfig:
    global _server_config
    if _server_config is None:
        _server_config = ServerConfig()
    return _server_config
