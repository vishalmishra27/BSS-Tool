"""
LIVE SOX PIPELINE TEST — Real LLM calls through the new autonomous agent.

This script runs the actual agent loop with real OpenAI API calls,
executing the full SOX audit pipeline against the sample RCM data.

It captures ALL output (agent speech, tool calls, tool results, errors)
and saves to a timestamped log file.
"""

from __future__ import annotations

import json
import os
import sys
import time
import logging
from datetime import datetime
from typing import Any, Dict

# ── Setup logging ─────────────────────────────────────────────────────────────

LOG_LINES: list[str] = []


def log(msg: str = "") -> None:
    LOG_LINES.append(msg)
    print(msg)


def section(title: str) -> None:
    log(f"\n{'='*80}")
    log(f"  {title}")
    log(f"{'='*80}\n")


def divider(title: str = "") -> None:
    if title:
        log(f"\n--- {title} {'─'*(72 - len(title))}\n")
    else:
        log(f"\n{'─'*80}\n")


# ── Custom callbacks that capture everything ──────────────────────────────────

from agent.core.loop import AgentCallbacks


class LoggingCallbacks:
    """Capture every agent event into the log."""

    def on_speak(self, text: str) -> None:
        log(f"\nAgent: {text}")

    def on_act(self, tool_name: str, args: Dict[str, Any]) -> None:
        args_str = json.dumps(args, default=str, indent=2)
        if len(args_str) > 500:
            args_str = args_str[:500] + "\n  ... (truncated)"
        log(f"\n  >> Tool: {tool_name}")
        log(f"     Args: {args_str}")

    def on_tool_result(self, tool_name: str, result: Dict[str, Any], duration: float) -> None:
        status = "OK" if not result.get("error") else "ERROR"
        log(f"  << {tool_name} [{status}] {duration:.1f}s")

        # Log key fields from result (not the entire thing)
        result_summary = {}
        for key in result:
            if key == "_agent_notes":
                result_summary["_agent_notes"] = result[key]
            elif key == "error":
                result_summary["error"] = result[key]
            elif key in ("success", "rows", "columns", "suggestion_count",
                         "pair_count", "controls_evaluated", "passed", "failed",
                         "effective", "not_effective", "effective_with_exceptions",
                         "controls_assessed", "kept", "new_total", "removed_count",
                         "action", "path", "plan", "query", "result"):
                result_summary[key] = result[key]
            elif isinstance(result[key], (str, int, float, bool)) and len(str(result[key])) < 200:
                result_summary[key] = result[key]
            elif isinstance(result[key], list):
                result_summary[key] = f"[{len(result[key])} items]"
            elif isinstance(result[key], dict):
                result_summary[key] = f"{{...{len(result[key])} keys}}"

        log(f"     Result: {json.dumps(result_summary, default=str, indent=2)[:1000]}")

    def on_error(self, error: str) -> None:
        log(f"\n  [ERROR] {error}")


# ── Main pipeline test ────────────────────────────────────────────────────────

def main() -> None:
    start = time.time()

    section("AUTONOMOUS SOX AUDIT AGENT — LIVE PIPELINE TEST")
    log(f"Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Python    : {sys.version.split()[0]}")
    log(f"Working dir: {os.getcwd()}")
    log(f"This test makes REAL OpenAI API calls through the new agent architecture.")

    # ── Setup ─────────────────────────────────────────────────────────────

    divider("AGENT INITIALIZATION")

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-20s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    from agent import create_agent
    from agent.config import get_config

    cfg = get_config()
    log(f"Model:    {cfg.openai_model}")
    log(f"API Key:  {cfg.openai_api_key[:12]}...{cfg.openai_api_key[-8:]}")
    log(f"Max rounds per message: {cfg.max_rounds}")

    callbacks = LoggingCallbacks()
    loop, state = create_agent(callbacks=callbacks)
    log(f"Tools registered: {len(loop._registry)}")
    log(f"Tool names: {sorted(loop._registry.keys())}")

    # Track which pipeline stages complete
    stages_completed = []
    errors_encountered = []

    # ── Helper to send a message and track results ────────────────────────

    def send(user_msg: str, label: str) -> bool:
        """Send a message to the agent, return True if no crash."""
        divider(f"USER: {label}")
        log(f"\nYou: {user_msg}")
        t0 = time.time()
        try:
            loop.process_message(user_msg, state)
            elapsed = time.time() - t0
            log(f"\n  [Completed in {elapsed:.1f}s, tool_calls={state.tool_call_count}]")
            stages_completed.append(label)
            return True
        except Exception as e:
            elapsed = time.time() - t0
            log(f"\n  [EXCEPTION after {elapsed:.1f}s]: {e}")
            import traceback
            log(traceback.format_exc())
            errors_encountered.append(f"{label}: {e}")
            return False

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 1: Load RCM
    # ══════════════════════════════════════════════════════════════════════

    section("STAGE 1: LOAD RCM")

    rcm_path = "/Users/rishi/Downloads/Latest/flask-api/output.xlsx"
    send(
        f"Load the RCM file at {rcm_path} and show me a quick summary — "
        f"how many rows, columns, and processes does it have?",
        "Load RCM"
    )

    # Verify state
    if state.rcm_df is not None:
        log(f"\n  [State check] RCM loaded: {len(state.rcm_df)} rows x {len(state.rcm_df.columns)} cols")
        log(f"  [State check] Columns: {list(state.rcm_df.columns)}")
        if "Process" in state.rcm_df.columns:
            log(f"  [State check] Processes: {state.rcm_df['Process'].unique().tolist()}")
    else:
        log(f"\n  [State check] WARNING: RCM not loaded!")

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 2: AI Suggestions (Gap Analysis)
    # ══════════════════════════════════════════════════════════════════════

    section("STAGE 2: AI SUGGESTIONS (GAP ANALYSIS)")

    send(
        "Run AI gap analysis for the Manufacturing industry. "
        "Show me all the suggestions with their priority and category.",
        "AI Suggestions"
    )

    if state.suggestions_cache:
        log(f"\n  [State check] Suggestions cached: {len(state.suggestions_cache)}")
    else:
        log(f"\n  [State check] WARNING: No suggestions cached")

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 3: Merge Suggestions
    # ══════════════════════════════════════════════════════════════════════

    section("STAGE 3: MERGE SUGGESTIONS")

    if state.suggestions_cache and len(state.suggestions_cache) > 0:
        # Merge the first 3 suggestions (or all if fewer)
        count = min(3, len(state.suggestions_cache))
        indices = ",".join(str(i) for i in range(1, count + 1))
        send(
            f"Merge suggestions {indices} into the RCM. Keep the rest for now.",
            "Merge Suggestions"
        )
        if state.rcm_df is not None:
            log(f"\n  [State check] RCM now: {len(state.rcm_df)} rows")
    else:
        log("  Skipping — no suggestions to merge.")
        stages_completed.append("Merge Suggestions (skipped)")

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 4: Deduplication
    # ══════════════════════════════════════════════════════════════════════

    section("STAGE 4: DEDUPLICATION")

    send(
        "Run deduplication analysis on the RCM. Show me all duplicate pairs found.",
        "Deduplication"
    )

    if state.dedup_cache:
        pairs = state.dedup_cache.get("pairs", [])
        log(f"\n  [State check] Dedup pairs cached: {len(pairs)}")
    else:
        log(f"\n  [State check] Dedup cache: None (may be zero pairs)")

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 5: Test of Design (TOD)
    # ══════════════════════════════════════════════════════════════════════

    section("STAGE 5: TEST OF DESIGN (TOD)")

    evidence_folder = "/Users/rishi/Downloads/Latest/flask-api/evidence_2"
    send(
        f"Run Test of Design using the evidence folder at {evidence_folder}. "
        f"Show me the results for each control — PASS or FAIL.",
        "Test of Design"
    )

    if state.tod_results:
        log(f"\n  [State check] TOD results: {len(state.tod_results)} controls")
    else:
        log(f"\n  [State check] TOD results: None")

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 6: Test of Effectiveness (TOE)
    # ══════════════════════════════════════════════════════════════════════

    section("STAGE 6: TEST OF EFFECTIVENESS (TOE)")

    send(
        f"Run Test of Operating Effectiveness using the evidence folder at {evidence_folder}. "
        f"Company name is 'Acme Corp', prepared by 'SOX Agent', reviewed by 'Audit Lead'. "
        f"Show me the results with deviation rates.",
        "Test of Effectiveness"
    )

    if state.toe_results:
        log(f"\n  [State check] TOE results: {len(state.toe_results)} controls")
    else:
        log(f"\n  [State check] TOE results: None")

    # ══════════════════════════════════════════════════════════════════════
    # STAGE 7: Save final checkpoint
    # ══════════════════════════════════════════════════════════════════════

    section("STAGE 7: SAVE & SUMMARY")

    send(
        "Save the final RCM as a checkpoint called 'final_audit_complete'. "
        "Then give me a brief executive summary of the entire audit.",
        "Save & Summary"
    )

    # ══════════════════════════════════════════════════════════════════════
    # FINAL REPORT
    # ══════════════════════════════════════════════════════════════════════

    elapsed = time.time() - start
    section("PIPELINE TEST RESULTS")

    log(f"  Total duration:     {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log(f"  Total tool calls:   {state.tool_call_count}")
    log(f"  Artifacts created:  {len(state.artifacts)}")
    for a in state.artifacts:
        log(f"    - {a}")
    log(f"  Python execs:       {state.python_exec_count}")
    log(f"\n  Stages completed ({len(stages_completed)}/{7}):")
    for s in stages_completed:
        log(f"    [OK] {s}")
    if errors_encountered:
        log(f"\n  Errors ({len(errors_encountered)}):")
        for e in errors_encountered:
            log(f"    [ERR] {e}")
    else:
        log(f"\n  Errors: None")

    log(f"\n  Final state:")
    if state.rcm_df is not None:
        log(f"    RCM: {len(state.rcm_df)} rows x {len(state.rcm_df.columns)} columns")
    log(f"    Output dir: {state.output_dir}")
    log(f"    Suggestions cached: {len(state.suggestions_cache) if state.suggestions_cache else 0}")
    log(f"    Dedup pairs: {len(state.dedup_cache.get('pairs',[])) if state.dedup_cache else 0}")
    log(f"    TOD results: {len(state.tod_results) if state.tod_results else 0}")
    log(f"    TOE results: {len(state.toe_results) if state.toe_results else 0}")

    if state.plan_scratchpad:
        log(f"\n  Agent's plan scratchpad:")
        log(state.plan_scratchpad)

    result = "ALL STAGES COMPLETED" if len(stages_completed) >= 7 else f"{len(stages_completed)}/7 STAGES"
    log(f"\n  RESULT: {result}")


if __name__ == "__main__":
    main()

    # Save log
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(os.getcwd(), f"agent_live_pipeline_{ts}.txt")
    with open(output_path, "w") as f:
        f.write("\n".join(LOG_LINES))
    print(f"\n  Full log saved to: {output_path}")
