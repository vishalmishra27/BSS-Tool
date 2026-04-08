"""
Tool execution engine with retry logic and error normalisation.

Decoupled from the agent loop so execution policy (retries, timeouts)
can evolve independently.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, Optional

from ..config import get_config
from ..types import AgentState, ToolResult

logger = logging.getLogger("agent.executor")

# ---------------------------------------------------------------------------
# TOD / TOE mismatch detection
# ---------------------------------------------------------------------------
_TOD_PATTERNS = re.compile(
    r"\b(tod\b|test\s+of\s+design)", re.IGNORECASE
)
_TOE_PATTERNS = re.compile(
    r"\b(toe\b|test\s+of\s+(operating\s+)?effectiveness)", re.IGNORECASE
)
_TOE_TOOLS = {"run_test_of_effectiveness", "preview_toe_attributes"}
_TOD_TOOLS = {"run_test_of_design", "preview_tod_attributes"}


def _check_tod_toe_mismatch(
    tool_name: str, user_message: str
) -> Optional[str]:
    """Return an error string if the LLM picked the wrong TOD/TOE tool."""
    if not user_message:
        return None

    user_wants_tod = bool(_TOD_PATTERNS.search(user_message))
    user_wants_toe = bool(_TOE_PATTERNS.search(user_message))

    # Only flag when intent is unambiguous (one but not both)
    if user_wants_tod and not user_wants_toe and tool_name in _TOE_TOOLS:
        return (
            f"The user asked for Test of Design (TOD), but you called {tool_name} "
            f"which is a TOE tool. Use run_test_of_design instead."
        )
    if user_wants_toe and not user_wants_tod and tool_name in _TOD_TOOLS:
        return (
            f"The user asked for Test of Effectiveness (TOE), but you called {tool_name} "
            f"which is a TOD tool. Use preview_toe_attributes / run_test_of_effectiveness instead."
        )
    return None


class Executor:
    """Execute tools by name via the registry, with retries on transient errors."""

    def __init__(self, registry: Dict[str, Any]) -> None:
        self._registry = registry
        self._last_user_message: str = ""

    def set_user_message(self, message: str) -> None:
        """Record the latest user message for intent-matching guards."""
        self._last_user_message = message

    def execute(
        self, tool_name: str, args: Dict[str, Any], state: AgentState
    ) -> ToolResult:
        cfg = get_config()
        tool = self._registry.get(tool_name)

        if tool is None:
            return ToolResult(
                success=False,
                data={},
                error=f"Unknown tool: {tool_name}",
            )

        # TOD / TOE mismatch guard
        mismatch = _check_tod_toe_mismatch(tool_name, self._last_user_message)
        if mismatch:
            logger.warning("TOD/TOE mismatch blocked: %s", mismatch)
            return ToolResult(
                success=False,
                data={"tool_mismatch": True},
                error=mismatch,
            )

        # Precondition check
        precond_error = tool.preconditions(state)
        if precond_error:
            return ToolResult(
                success=False,
                data={},
                error=f"Precondition failed: {precond_error}",
            )

        last_error: Optional[str] = None
        for attempt in range(1, cfg.max_retries + 1):
            try:
                t0 = time.time()
                result = tool.execute(args, state)
                result.duration_seconds = time.time() - t0
                logger.info(
                    "%s completed in %.1fs (attempt %d)",
                    tool_name,
                    result.duration_seconds,
                    attempt,
                )
                state.tool_call_count += 1
                return result

            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "%s attempt %d failed: %s", tool_name, attempt, last_error
                )
                # Don't retry on logic / argument errors
                if isinstance(exc, (ValueError, TypeError, KeyError, FileNotFoundError)):
                    break
                if attempt < cfg.max_retries:
                    delay = cfg.retry_delay * (2 ** (attempt - 1))
                    logger.info("Retrying %s in %.1fs ...", tool_name, delay)
                    time.sleep(delay)

        return ToolResult(
            success=False,
            data={},
            error=f"{tool_name} failed after {cfg.max_retries} attempts: {last_error}",
        )
