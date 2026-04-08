"""
Autonomous Control Testing Agent.

Factory function to wire up all components and return a ready-to-use agent.

Usage::

    from agent import create_agent

    loop, state = create_agent()
    loop.process_message("Load the RCM and check for duplicates.", state)
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from .config import get_config
from .llm import LLMClient
from .types import AgentState
from .tools import discover_tools
from .core.memory import Memory
from .core.executor import Executor
from .core.reflector import Reflector
from .core.loop import AgentLoop, AgentCallbacks, DefaultCallbacks

logger = logging.getLogger("agent")


def create_agent(
    callbacks: Optional[AgentCallbacks] = None,
) -> Tuple[AgentLoop, AgentState]:
    """
    Wire all components together and return ``(loop, state)``.

    Parameters
    ----------
    callbacks
        Optional UI callbacks.  Falls back to :class:`DefaultCallbacks`
        (prints to stdout).

    Returns
    -------
    loop : AgentLoop
        The autonomous reasoning loop.
    state : AgentState
        Fresh, mutable session state.
    """
    cfg = get_config()
    llm = LLMClient(
        api_key=cfg.openai_api_key,
        model=cfg.openai_model,
        azure_endpoint=cfg.azure_openai_endpoint,
        azure_api_version=cfg.azure_openai_api_version,
    )
    registry = discover_tools()
    executor = Executor(registry)
    memory = Memory()
    reflector = Reflector()
    state = AgentState()

    loop = AgentLoop(
        llm=llm,
        registry=registry,
        executor=executor,
        memory=memory,
        reflector=reflector,
        callbacks=callbacks or DefaultCallbacks(),
    )

    logger.info(
        "Agent created: %d tools, model=%s",
        len(registry),
        cfg.openai_model,
    )
    return loop, state
