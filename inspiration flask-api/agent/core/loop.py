"""
The autonomous reasoning loop.

DESIGN PRINCIPLE: The LLM is the sole decision-maker at every turn.
This loop does NOT iterate through plan steps, does NOT use a state
machine, and does NOT branch on reflection flags.

Each turn:
  1. Build context (goal, memory, state, plan scratchpad, reflector notes)
  2. Call LLM with ALL tools available
  3. LLM outputs text (reasoning / response) + optional tool calls
  4. Execute any tool calls; reflector injects advisory notes
  5. Loop continues — LLM decides what happens next
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Protocol

from ..config import get_config
from ..llm import LLMClient
from ..types import AgentState
from ..utils import sanitize_for_json
from .executor import Executor
from .memory import Memory
from .reflector import Reflector
from .suggestions import get_suggestions
from ..prompts.system import build_system_prompt

logger = logging.getLogger("agent.loop")


def _strip_llm_suggestions(content: str) -> str:
    """Remove LLM-generated suggestion/upload-prompt blocks from content.

    The deterministic ``get_suggestions()`` output is authoritative — any
    similar text the LLM generated on its own must be stripped first to
    avoid duplication.
    """
    # 1. Strip "**What you can do next:**" blocks
    if "**What you can do next:**" in content:
        content = content[:content.index("**What you can do next:**")].rstrip()

    # 2. Strip LLM-generated upload prompts (scoping phases)
    _UPLOAD_MARKER = "Would you like to upload a modified Excel"
    if _UPLOAD_MARKER in content:
        # Find the preamble that introduces the upload prompt
        _PREAMBLES = [
            "---",
            "You can download the exported",
            "You can download the quantitative",
            "You can download the qualitative",
            "You can download the scoping",
        ]
        marker_idx = content.find(_UPLOAD_MARKER)
        for preamble in _PREAMBLES:
            idx = content.rfind(preamble, 0, marker_idx)
            if idx != -1:
                content = content[:idx].rstrip()
                break
        else:
            # No preamble found — cut from the marker sentence start
            # Walk back to find the sentence start
            cut = content.rfind("\n", 0, marker_idx)
            if cut != -1:
                content = content[:cut].rstrip()
            else:
                content = content[:marker_idx].rstrip()

    return content


# ---------------------------------------------------------------------------
# Callback protocol — how the loop communicates with the outer UI
# ---------------------------------------------------------------------------

class AgentCallbacks(Protocol):
    """Events the agent loop emits for the UI layer."""

    def on_speak(self, text: str) -> None:
        """The agent has reasoning / text to display."""
        ...

    def on_act(self, tool_name: str, args: Dict[str, Any]) -> None:
        """The agent is about to execute a tool."""
        ...

    def on_tool_result(self, tool_name: str, result: Dict[str, Any], duration: float) -> None:
        """A tool has finished executing."""
        ...

    def on_error(self, error: str) -> None:
        """An unrecoverable error occurred."""
        ...


@dataclass
class DefaultCallbacks:
    """Print-based callbacks for terminal usage."""

    def on_speak(self, text: str) -> None:
        print(f"\nAgent: {text}")

    def on_act(self, tool_name: str, args: Dict[str, Any]) -> None:
        args_preview = json.dumps(args, default=str)
        if len(args_preview) > 200:
            args_preview = args_preview[:200] + "..."
        print(f"\n  >> Tool: {tool_name}({args_preview})")

    def on_tool_result(self, tool_name: str, result: Dict[str, Any], duration: float) -> None:
        print(f"  << {tool_name} completed in {duration:.1f}s")

    def on_error(self, error: str) -> None:
        print(f"\n  [ERROR] {error}")


# ---------------------------------------------------------------------------
# Agent Loop
# ---------------------------------------------------------------------------

class AgentLoop:
    """
    The autonomous reasoning loop.

    No plan iteration. No step state machine. The LLM decides every turn.
    """

    def __init__(
        self,
        llm: LLMClient,
        registry: Dict[str, Any],
        executor: Executor,
        memory: Memory,
        reflector: Reflector,
        callbacks: Optional[AgentCallbacks] = None,
    ) -> None:
        self._llm = llm
        self._registry = registry
        self._executor = executor
        self._memory = memory
        self._reflector = reflector
        self._callbacks = callbacks or DefaultCallbacks()

    # -- Public API ---------------------------------------------------------

    def process_message(self, user_message: str, state: AgentState) -> None:
        """
        Handle one user message through the autonomous loop.

        The loop runs until the LLM responds without tool calls
        (meaning it's done or waiting for user input), or until
        ``max_rounds`` is reached.
        """
        cfg = get_config()
        self._executor.set_user_message(user_message)
        self._memory.add_user_message(user_message)
        # Reset per-turn reflector state so repeat-call counters don't carry
        # over from previous user messages and produce false loop warnings.
        self._reflector.reset_turn()
        # Clear scoping re-entry guard on new user message.  The guard
        # prevents the LLM from calling the tool multiple times within a
        # SINGLE turn; on a genuinely new user message we want the tool
        # to proceed normally (the user has responded).
        if state.scoping_awaiting_input:
            state.scoping_awaiting_input = False
            logger.debug("scoping_awaiting_input cleared — new user message received")
        last_tool_name: str | None = None

        for round_num in range(1, cfg.max_rounds + 1):
            logger.info("=== Round %d / %d ===", round_num, cfg.max_rounds)

            # 1. BUILD CONTEXT
            system_prompt = build_system_prompt(
                state=state,
                tool_summary=self._capability_summary(),
            )
            messages = self._memory.build_context(
                plan_scratchpad=state.plan_scratchpad
            )
            tool_schemas = [
                tool.to_openai_schema() for tool in self._registry.values()
            ]

            # 2. CALL LLM — it decides everything
            try:
                response = self._llm.chat_with_tools(
                    system=system_prompt,
                    messages=messages,
                    tools=tool_schemas,
                )
            except Exception as exc:
                error_msg = f"LLM API call failed: {exc}"
                logger.error(error_msg)
                self._callbacks.on_error(error_msg)
                self._memory.add_assistant_message(
                    f"I encountered an error calling the AI service: {exc}"
                )
                return

            choice = response.choices[0]
            msg = choice.message

            # 4. No tool calls → agent is done or waiting for user
            if not msg.tool_calls:
                # Replace or append deterministic suggestions.
                # The LLM may write its own suggestions but miss critical
                # workflow steps — deterministic suggestions are authoritative.
                suggestions_block = get_suggestions(state, last_tool_name)
                display_content = msg.content or ""
                if suggestions_block and display_content:
                    # Strip any LLM-generated suggestions so ours take precedence
                    display_content = _strip_llm_suggestions(display_content)
                    # Show cleaned content + deterministic suggestions
                    self._callbacks.on_speak(display_content + suggestions_block)
                    # Update memory with cleaned content + suggestions
                    self._memory.add_assistant_message(display_content, None)
                    self._memory.amend_last_assistant(suggestions_block)
                else:
                    # No suggestions to append — show as-is
                    if display_content:
                        self._callbacks.on_speak(display_content)
                    self._memory.add_assistant_message(msg.content, None)
                logger.info("Agent finished (no tool calls). Round %d.", round_num)
                return

            # 3. SHOW REASONING (text output = agent thinking aloud)
            # Only show short status messages when tool calls are pending —
            # suppress long pre-emptive summaries that hallucinate results
            # the LLM hasn't received yet.
            if msg.content:
                if msg.tool_calls and len(msg.content) > 280:
                    # Likely hallucinating results before tool execution;
                    # trim to just the first sentence/status line.
                    short = msg.content.split("\n")[0][:280]
                    if short != msg.content:
                        short = short.rstrip(". ") + " …"
                        self._callbacks.on_speak(short)
                    else:
                        self._callbacks.on_speak(msg.content)
                else:
                    self._callbacks.on_speak(msg.content)

            # Record in memory (with tool_calls if present)
            self._memory.add_assistant_message(msg.content, msg.tool_calls)

            # 5. EXECUTE TOOLS
            for tc in msg.tool_calls:
                name = tc.function.name
                try:
                    args = json.loads(tc.function.arguments or "{}")
                except json.JSONDecodeError:
                    args = {}

                self._callbacks.on_act(name, args)

                result = self._executor.execute(name, args, state)
                if result.success:
                    result_data = sanitize_for_json(result.data)
                else:
                    # Preserve structured payloads from tools even on failure.
                    # Some tools intentionally return guidance in result.data
                    # (e.g., awaiting_input step-gates) alongside an error.
                    result_data = sanitize_for_json(result.data or {})
                    result_data["error"] = result.error

                # 6. REFLECTOR INJECTS ADVISORY NOTES (not flow control)
                notes = self._reflector.analyze(name, result_data, state)
                if notes:
                    result_data["_agent_notes"] = notes

                self._callbacks.on_tool_result(
                    name, result_data, result.duration_seconds
                )
                self._memory.add_tool_result(tc.id, name, result_data)
                if result.success:
                    last_tool_name = name

            # Loop continues → LLM sees results + notes → decides next action

        # Max rounds exhausted
        logger.warning("Max rounds (%d) exhausted.", cfg.max_rounds)
        self._callbacks.on_speak(
            f"I've reached the maximum number of reasoning rounds ({cfg.max_rounds}). "
            "Please continue with a follow-up message to resume."
        )

    # -- Internal helpers ---------------------------------------------------

    def _capability_summary(self) -> str:
        """Build a grouped summary of available tools for the system prompt."""
        from ..types import ToolCategory

        groups: Dict[str, List[str]] = {}
        for tool in self._registry.values():
            cat = tool.category.value.upper()
            groups.setdefault(cat, []).append(
                f"  - {tool.name}: {tool.description}"
            )

        parts = []
        for cat in sorted(groups):
            parts.append(f"[{cat}]")
            parts.extend(groups[cat])
        return "\n".join(parts)
