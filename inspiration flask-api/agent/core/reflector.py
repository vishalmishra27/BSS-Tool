"""
Reflector — injects advisory notes into tool results.

The reflector does **NOT** control flow. It does **NOT** decide whether to
retry or replan.  It returns a list of human-readable notes that are added
to the tool result under ``_agent_notes``.  The LLM sees these notes and
decides what to do with them.

Two levels:
  1. **Rule-based** (fast, no LLM call): catches obvious anomalies like
     100% TOD pass rates or zero dedup pairs in a large RCM.
  2. **LLM reflection**: the system prompt instructs the LLM to reflect
     after every tool result — this happens naturally in the next turn.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from ..types import AgentState

logger = logging.getLogger("agent.reflector")


class Reflector:
    """Analyse a tool result and return advisory notes (may be empty)."""

    def __init__(self):
        self._last_tool_name: str = ""
        self._repeat_count: int = 0

    def reset_turn(self) -> None:
        """Reset per-turn tracking at the start of each new user message.

        Prevents repeat-call counters accumulating across distinct user turns,
        which would wrongly trigger "stuck in a loop" warnings when the user
        legitimately calls the same tool in separate messages.
        """
        self._last_tool_name = ""
        self._repeat_count = 0

    def analyze(
        self,
        tool_name: str,
        result: Dict[str, Any],
        state: AgentState,
    ) -> List[str]:
        """Return a list of advisory notes (possibly empty)."""
        notes: List[str] = []

        # ── Track repeated tool calls ──────────────────────────────────
        if tool_name == self._last_tool_name:
            self._repeat_count += 1
        else:
            self._last_tool_name = tool_name
            self._repeat_count = 1

        if self._repeat_count >= 3:
            notes.append(
                f"WARNING: You've called '{tool_name}' {self._repeat_count} times in a row. "
                "Consider whether you are stuck in a loop. Try a different approach or "
                "ask the user for guidance."
            )

        # ── High tool call count warning ───────────────────────────────
        if state.tool_call_count > 15:
            notes.append(
                f"NOTE: You've made {state.tool_call_count} tool calls this session. "
                "Consider wrapping up or summarizing progress for the user."
            )

        # ── Error handling ────────────────────────────────────────────
        if result.get("error"):
            # Report the error only — do NOT suggest retrying here.
            # The system prompt already instructs the LLM to ask the user
            # before any retry; a second "consider retrying" nudge would
            # conflict with that rule.
            notes.append(f"TOOL ERROR: {result['error']}.")
            return notes

        # ── TOD: suspiciously high pass rate ──────────────────────────
        if tool_name == "run_test_of_design":
            # Phase 1 (document_list) or Phase 2 (evidence_validation) — skip anomaly checks
            if result.get("phase") in ("document_list", "evidence_validation"):
                return notes
            total = result.get("controls_evaluated", 0)
            passed = result.get("passed", 0)
            if total > 10 and passed == total:
                notes.append(
                    f"ANOMALY: 100% pass rate across {total} controls is unusually high. "
                    "Verify evidence quality and completeness before accepting."
                )
            failed = result.get("failed", 0)
            if total > 0 and failed > total * 0.5:
                notes.append(
                    f"WARNING: {failed}/{total} controls FAILED design test. "
                    "This may indicate systemic control design issues."
                )

        # ── TOE: zero deviations ─────────────────────────────────────
        if tool_name == "run_test_of_effectiveness":
            # Phase 1 (document_list) or Phase 2 (evidence_validation) — skip anomaly checks
            if result.get("phase") in ("document_list", "evidence_validation"):
                return notes
            effective = result.get("effective", 0)
            total = result.get("controls_evaluated", 0)
            ne = result.get("not_effective", 0)
            if total > 5 and effective == total:
                notes.append(
                    f"ANOMALY: All {total} controls effective with zero deviations. "
                    "Verify sample selection represents genuine transactions."
                )
            if ne > 0:
                notes.append(
                    f"FINDING: {ne} control(s) rated 'Not Effective'. "
                    "These require remediation per PCAOB standards."
                )

        # ── Dedup: zero pairs in large RCM ───────────────────────────
        if tool_name == "run_deduplication":
            pair_count = result.get("pair_count", 0)
            rcm_rows = len(state.rcm_df) if state.rcm_df is not None else 0
            if pair_count == 0 and rcm_rows > 30:
                notes.append(
                    f"NOTE: Zero duplicates found in {rcm_rows}-row RCM. "
                    "This may be correct, or matching criteria may be too strict."
                )

        # ── AI suggestions: very few or very many ────────────────────
        if tool_name == "run_ai_suggestions":
            count = result.get("suggestion_count", 0)
            if count == 0:
                notes.append(
                    "NOTE: Zero AI suggestions generated. The RCM may already "
                    "be comprehensive, or the analysis may have encountered issues."
                )
            elif count > 25:
                notes.append(
                    f"NOTE: {count} suggestions is a high count. Consider "
                    "prioritising High-priority items first."
                )

        # ── Frequency inference: review anomalies ─────────────────────
        if tool_name == "infer_control_frequency":
            inferred = result.get("controls_inferred", 0)
            defaulted = result.get("defaulted", 0)
            if defaulted > 0:
                notes.append(
                    f"WARNING: {defaulted} control(s) could not be classified by keyword "
                    "or LLM and were defaulted to 'Recurring (multiple times per day)' "
                    "(most conservative). Review these carefully."
                )
            if inferred > 20:
                notes.append(
                    f"NOTE: {inferred} controls had missing frequencies — this is a "
                    "high number. Consider checking if the RCM source has a frequency "
                    "column that wasn't mapped during normalization."
                )
            if result.get("requires_approval"):
                notes.append(
                    "REMINDER: Show the inferred frequencies table to the user and "
                    "mention the downloadable Excel. Ask if they want to modify any "
                    "values before approving. The user can modify via chat "
                    "(modify_control_frequency) or upload a modified Excel "
                    "(upload_frequency_overrides)."
                )

        # ── Sampling engine: unmatched frequencies ───────────────────
        if tool_name == "run_sampling_engine":
            unmatched = result.get("unmatched_frequencies")
            if unmatched and len(unmatched) > 0:
                notes.append(
                    f"WARNING: {len(unmatched)} control(s) had unrecognized frequencies "
                    "and were defaulted to 1 sample. Review the 'Control Frequency' values "
                    "for these controls."
                )
            total_samples = result.get("total_samples_required", 0)
            controls = result.get("controls_processed", 0)
            if controls > 0 and total_samples == controls:
                notes.append(
                    "NOTE: Every control requires exactly 1 sample. This usually means "
                    "all controls are Annual or frequencies couldn't be matched."
                )

        # ── modify_rcm: large-scope changes ────────────────────────────
        if tool_name == "modify_rcm":
            action = result.get("action", "")
            rows_deleted = result.get("rows_deleted", 0)
            if rows_deleted > 10:
                notes.append(
                    f"NOTE: Deleted {rows_deleted} rows. Consider saving a checkpoint "
                    "with save_excel before further modifications."
                )
            rows_updated = result.get("rows_updated", 0)
            if rows_updated > 50:
                notes.append(
                    f"NOTE: Updated {rows_updated} rows. Consider verifying the changes "
                    "with inspect_dataframe and saving a checkpoint."
                )

        # ── Scoping engine: guard against re-calls ─────────────────
        if tool_name == "run_sox_scoping_engine":
            # Note: the upload question is appended deterministically by
            # get_suggestions() — do NOT tell the LLM to include it (avoids
            # duplicate display).
            if result.get("blocked"):
                notes.append(
                    "BLOCKED: You already showed results for this phase. Do NOT call "
                    "run_sox_scoping_engine again. Present a summary of the results "
                    "and wait for the user to reply."
                )

        # ── Control assessment: low match percentage ─────────────────
        if tool_name == "run_control_assessment":
            controls = result.get("results", [])
            low_match = [c for c in controls if _pct_to_float(c.get("match_pct")) < 50]
            if low_match:
                notes.append(
                    f"FINDING: {len(low_match)} control(s) have <50% match with SOPs. "
                    "These may need design remediation."
                )

        if notes:
            logger.info("Reflector notes for %s: %s", tool_name, notes)
        return notes


def _pct_to_float(val: Any) -> float:
    """Convert '57%' or 57 to 57.0."""
    if val is None:
        return 0.0
    s = str(val).replace("%", "").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0
