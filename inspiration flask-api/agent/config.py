"""
Centralised configuration for the autonomous control testing agent.

All Azure OpenAI credentials are sourced from engines/config.py (single
source of truth).  The _apply() call in that module also pushes values
into os.environ, so env-var reads here will pick them up automatically.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# Ensure central config env-vars are applied before we read them.
# This import triggers engines.config._apply() which sets os.environ.
import engines.config as _central  # noqa: F401


@dataclass(frozen=True)
class AgentConfig:
    """Immutable config loaded once at startup."""
    openai_api_key: str
    openai_model: str = "gpt-5.2-chat"
    azure_openai_endpoint: str = ""
    azure_openai_api_version: str = "2024-12-01-preview"
    azure_openai_embedding_deployment: str = "text-embedding-ada-002"
    max_rounds: int = 25          # Max LLM round-trips per user message
    max_retries: int = 2          # Tool-level retry attempts
    retry_delay: float = 2.0      # Base delay in seconds (exponential backoff)
    max_context_messages: int = 40 # Episodic messages kept in LLM context


_config: AgentConfig | None = None


def get_config() -> AgentConfig:
    """Return the singleton config, creating it on first call.

    Reads from engines.config module attributes directly (no hardcoded
    fallbacks here — engines/config.py is the single source of truth).
    """
    global _config
    if _config is None:
        _config = AgentConfig(
            openai_api_key=_central.AZURE_OPENAI_API_KEY,
            openai_model=_central.AZURE_OPENAI_DEPLOYMENT,
            azure_openai_endpoint=_central.AZURE_OPENAI_ENDPOINT,
            azure_openai_api_version=_central.AZURE_OPENAI_API_VERSION,
            azure_openai_embedding_deployment=_central.AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
        )
    return _config
