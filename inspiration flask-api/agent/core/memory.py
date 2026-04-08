"""
Working memory for the autonomous agent.

Three tiers:
  1. **Episodic** — full conversation messages (windowed for LLM context).
  2. **Semantic** — key facts auto-extracted from tool results that survive
     context-window truncation.
  3. **Plan scratchpad** — free-form text maintained by the LLM via
     ``update_plan`` tool.  Shown in every context window.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional

from ..config import get_config

logger = logging.getLogger("agent.memory")


class Memory:
    """Manages conversation context for the agent loop."""

    def __init__(self) -> None:
        self._episodic: List[Dict[str, Any]] = []
        self._facts: List[str] = []

    # -- Message recording -------------------------------------------------

    def add_user_message(self, content: str) -> None:
        self._episodic.append({"role": "user", "content": content})

    def add_assistant_message(
        self, content: Optional[str], tool_calls: Optional[Any] = None
    ) -> None:
        entry: Dict[str, Any] = {"role": "assistant", "content": content or ""}
        if tool_calls:
            entry["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {
                        "name": tc.function.name,
                        "arguments": tc.function.arguments,
                    },
                }
                for tc in tool_calls
            ]
        self._episodic.append(entry)

    # Maximum characters to keep in a single tool-result message.
    # GPT-5.2 has a 200K+ context window, so we can afford larger results.
    # The old 4000-char limit was truncating scoping tables mid-row, causing
    # the agent to produce empty/broken markdown tables.
    # 32K allows TOD/TOE results for 25+ controls to fit without truncation,
    # preventing the LLM from seeing partial results and stopping mid-way.
    TOOL_RESULT_MAX_CHARS = 32000

    def add_tool_result(
        self, tool_call_id: str, tool_name: str, result: Dict[str, Any]
    ) -> None:
        result_str = json.dumps(result, default=str)
        if len(result_str) > self.TOOL_RESULT_MAX_CHARS:
            result_str = result_str[:self.TOOL_RESULT_MAX_CHARS] + "... [truncated]"
        self._episodic.append(
            {"role": "tool", "tool_call_id": tool_call_id, "content": result_str}
        )
        self._auto_extract_facts(tool_name, result)

    def amend_last_assistant(self, extra_text: str) -> None:
        """Append text to the last assistant message in episodic memory."""
        for msg in reversed(self._episodic):
            if msg.get("role") == "assistant":
                msg["content"] = (msg.get("content") or "") + extra_text
                break

    def replace_last_assistant_content(self, new_content: str) -> None:
        """Replace the content of the last assistant message."""
        for msg in reversed(self._episodic):
            if msg.get("role") == "assistant":
                msg["content"] = new_content
                break

    # -- Semantic fact management ------------------------------------------

    MAX_FACTS = 15  # Prevent unbounded context growth

    def add_fact(self, fact: str) -> None:
        if fact not in self._facts:
            self._facts.append(fact)
            # Evict oldest facts when exceeding cap
            if len(self._facts) > self.MAX_FACTS:
                evicted = self._facts.pop(0)
                logger.debug("Evicted oldest fact (cap=%d): %s", self.MAX_FACTS, evicted)
            logger.debug("Semantic fact: %s", fact)

    def _auto_extract_facts(self, tool_name: str, result: Dict[str, Any]) -> None:
        """Pull key facts from tool results into semantic memory."""
        if result.get("error"):
            return

        if tool_name == "load_rcm" and result.get("success"):
            # Clear stale facts from previous RCM — they're no longer valid
            self._facts = [f for f in self._facts if not f.startswith(("RCM loaded:", "Scoping:"))]
            self.add_fact(
                f"RCM loaded: {result.get('rows', '?')} rows, "
                f"{len(result.get('columns', []))} columns"
            )
        elif tool_name == "run_ai_suggestions" and result.get("success"):
            self.add_fact(
                f"AI suggestions: {result.get('suggestion_count', 0)} generated"
            )
        elif tool_name == "merge_suggestions" and result.get("success"):
            self.add_fact(
                f"Merged {result.get('kept', 0)} suggestions → "
                f"{result.get('new_total', '?')} rows"
            )
        elif tool_name == "run_control_assessment" and result.get("success"):
            self.add_fact(
                f"Control assessment: {result.get('controls_assessed', 0)} controls evaluated"
            )
        elif tool_name == "run_deduplication" and result.get("success"):
            self.add_fact(
                f"Deduplication: {result.get('pair_count', 0)} duplicate pairs found"
            )
        elif tool_name == "preview_tod_attributes" and result.get("controls_count"):
            self.add_fact(
                f"TOD attributes previewed: {result['controls_count']} controls, "
                f"{result.get('total_attributes', 0)} attributes — awaiting approval"
            )
        elif tool_name == "preview_toe_attributes" and result.get("controls_count"):
            self.add_fact(
                f"TOE attributes previewed: {result['controls_count']} controls, "
                f"{result.get('total_attributes', 0)} attributes — awaiting approval"
            )
        elif tool_name == "run_test_of_design" and result.get("success"):
            self.add_fact(
                f"TOD: {result.get('passed', 0)} PASS, {result.get('failed', 0)} FAIL "
                f"out of {result.get('controls_evaluated', 0)}"
            )
        elif tool_name == "remove_failed_tod_controls" and not result.get("error"):
            self.add_fact(
                f"Removed {result.get('removed_count', 0)} failed controls, "
                f"{result.get('remaining_count', '?')} remaining"
            )
        elif tool_name == "run_test_of_effectiveness" and result.get("success"):
            self.add_fact(
                f"TOE: {result.get('effective', 0)} Effective, "
                f"{result.get('effective_with_exceptions', 0)} Exceptions, "
                f"{result.get('not_effective', 0)} Not Effective"
            )
        elif tool_name == "run_sampling_engine" and result.get("success"):
            self.add_fact(
                f"Sampling engine ({result.get('engine_type', 'KPMG')}): "
                f"{result.get('controls_processed', 0)} controls, "
                f"{result.get('total_samples_required', 0)} total samples required"
            )
        elif tool_name == "run_sox_scoping_engine":
            status = result.get("status", "")
            if status == "accounts_fetched":
                self.add_fact(
                    f"Scoping: trial balance ingested, "
                    f"{result.get('account_count', '?')} accounts fetched"
                )
            elif status == "quantitative_done":
                self.add_fact(
                    f"Scoping quantitative done: "
                    f"{result.get('in_scope_count', '?')} in-scope, "
                    f"{result.get('out_of_scope_count', '?')} out-of-scope "
                    f"(benchmark={result.get('benchmark')}, "
                    f"threshold={result.get('materiality_threshold')})"
                )
            elif status == "qualitative_done":
                self.add_fact(
                    f"Scoping qualitative done: "
                    f"{result.get('added_count', 0)} accounts added, "
                    f"{result.get('removed_count', 0)} removed qualitatively"
                )
            elif status == "scoped_done":
                self.add_fact(
                    f"Scoping: {result.get('in_scope_count', '?')} final in-scope accounts determined"
                )
            elif status == "success":
                self.add_fact(
                    f"Scoping complete: RCM workbook exported to {result.get('output_path', '?')}"
                )

    # -- Context builder ---------------------------------------------------

    def build_context(self, plan_scratchpad: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Assemble the ``messages`` array for the LLM call.

        Order:
          1. Semantic facts (compact, always included)
          2. Plan scratchpad (if the agent wrote one)
          3. Recent episodic messages (windowed, with safe boundary)
        """
        messages: List[Dict[str, Any]] = []

        # 1. Semantic facts
        if self._facts:
            facts_block = "SESSION FACTS (persisted):\n" + "\n".join(
                f"  - {f}" for f in self._facts
            )
            messages.append({"role": "user", "content": facts_block})
            messages.append({"role": "assistant", "content": "Acknowledged."})

        # 2. Plan scratchpad
        if plan_scratchpad:
            messages.append(
                {"role": "user", "content": f"YOUR WORKING PLAN:\n{plan_scratchpad}"}
            )
            messages.append({"role": "assistant", "content": "Plan noted."})

        # 3. Recent episodic window — safe boundary
        #    A naive slice can cut in the middle of a tool-call sequence,
        #    leaving orphaned "role: tool" messages without the preceding
        #    "role: assistant" message that had tool_calls.  OpenAI rejects
        #    this with a 400 error.
        #
        #    Fix: after slicing, walk forward from the start of the window
        #    until we find a message that is NOT a "tool" role.  This ensures
        #    we never start the window with orphaned tool results.
        window = self._episodic[-get_config().max_context_messages:]

        # Second pass: estimate total token size and trim large tool results
        # to stay within a safe ~80K token budget (leaves room for system prompt + completion)
        MAX_CONTEXT_CHARS = 200000  # ~50K tokens at 4 chars/token
        total_chars = sum(len(str(m.get("content", ""))) for m in window)
        if total_chars > MAX_CONTEXT_CHARS:
            # Trim tool results from oldest to newest until under budget
            for i, msg in enumerate(window):
                if total_chars <= MAX_CONTEXT_CHARS:
                    break
                if msg.get("role") == "tool" and len(str(msg.get("content", ""))) > 2000:
                    original_len = len(str(msg["content"]))
                    # Truncate to first 1000 chars + summary marker
                    msg["content"] = str(msg["content"])[:1000] + "\n... [truncated to save context]"
                    total_chars -= (original_len - 1000)
                    logger.debug("Trimmed tool result at index %d: %d → 1000 chars", i, original_len)

        # Drop leading orphaned tool messages
        start = 0
        while start < len(window) and window[start].get("role") == "tool":
            start += 1

        messages.extend(window[start:])

        return messages

    def clear(self) -> None:
        """Reset memory for a new session."""
        self._episodic.clear()
        self._facts.clear()
