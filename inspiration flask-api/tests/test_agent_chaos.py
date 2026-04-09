"""
CHAOS TEST — Every edge case a real user could throw at the agent.

Covers scenarios NOT tested in the previous human-style test:
  - Nonsensical / gibberish input
  - Asking about results that don't exist yet
  - Single high-level goal (does the agent plan autonomously?)
  - Undoing / reverting changes
  - Out-of-order requests (TOE before TOD, etc.)
  - Wrong enum values (bad industry name)
  - Extremely long rambling instructions
  - Asking agent to explain its own reasoning
  - Re-running a stage that already ran
  - Complex multi-part question in one message
  - Asking for comparison across stages
  - Custom Python analysis with specific requirements
  - Empty / whitespace-only input behavior
  - Contradicting previous instructions
  - Asking about columns that don't exist
  - Natural language query on the dataframe
"""

from __future__ import annotations

import json
import os
import sys
import time
import logging
from datetime import datetime
from typing import Any, Dict

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


class LoggingCallbacks:
    def on_speak(self, text: str) -> None:
        log(f"\nAgent: {text}")

    def on_act(self, tool_name: str, args: Dict[str, Any]) -> None:
        args_str = json.dumps(args, default=str, indent=2)
        if len(args_str) > 400:
            args_str = args_str[:400] + "\n  ... (truncated)"
        log(f"\n  >> Tool: {tool_name}")
        log(f"     Args: {args_str}")

    def on_tool_result(self, tool_name: str, result: Dict[str, Any], duration: float) -> None:
        status = "OK" if not result.get("error") else "ERROR"
        log(f"  << {tool_name} [{status}] {duration:.1f}s")
        summary = {}
        for key in result:
            val = result[key]
            if key in ("_agent_notes", "error"):
                summary[key] = val
            elif isinstance(val, (int, float, bool)):
                summary[key] = val
            elif isinstance(val, str):
                summary[key] = val[:120] + "..." if len(val) > 120 else val
            elif isinstance(val, list):
                summary[key] = f"[{len(val)} items]"
            elif isinstance(val, dict):
                summary[key] = f"{{...{len(val)} keys}}"
        log(f"     Result: {json.dumps(summary, default=str)[:600]}")

    def on_error(self, error: str) -> None:
        log(f"\n  [ERROR] {error}")


def main() -> None:
    start = time.time()

    section("CHAOS TEST — EDGE CASE STRESS TEST")
    log(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Testing with adversarial, edge-case, and unusual prompts\n")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-20s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    from agent import create_agent
    callbacks = LoggingCallbacks()
    loop, state = create_agent(callbacks=callbacks)

    prompts = []
    crashes = []

    def send(user_msg: str, label: str) -> bool:
        divider(f"TEST: {label}")
        log(f"\nYou: {user_msg}")
        prompts.append(label)
        t0 = time.time()
        try:
            loop.process_message(user_msg, state)
            elapsed = time.time() - t0
            log(f"\n  [Done in {elapsed:.1f}s | tools={state.tool_call_count}]")
            return True
        except Exception as e:
            elapsed = time.time() - t0
            log(f"\n  [CRASH after {elapsed:.1f}s]: {e}")
            import traceback
            log(traceback.format_exc())
            crashes.append(f"{label}: {e}")
            return False

    # ══════════════════════════════════════════════════════════════════════
    # 1. ASKING ABOUT RESULTS BEFORE ANYTHING EXISTS
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 1: IMPOSSIBLE / PREMATURE REQUESTS")

    send(
        "show me the TOD results",
        "Ask for TOD results before loading anything"
    )

    send(
        "how many duplicates did we find?",
        "Ask about dedup before running it"
    )

    send(
        "merge suggestion number 5 into the rcm",
        "Merge before suggestions exist"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 2. NONSENSICAL / GIBBERISH
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 2: NONSENSICAL INPUT")

    send(
        "asdfghjkl qwerty lorem ipsum",
        "Complete gibberish"
    )

    send(
        "🔥🔥🔥 do the thing 🔥🔥🔥",
        "Emojis + vague 'do the thing'"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 3. SINGLE HIGH-LEVEL GOAL — does it plan?
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 3: AUTONOMOUS PLANNING")

    send(
        "I need you to load the rcm from /Users/rishi/Downloads/Latest/flask-api/output.xlsx, "
        "then check it for duplicates and show me the results. thats all i need today.",
        "Specific multi-step goal — should plan and execute"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 4. OUT-OF-ORDER REQUESTS
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 4: OUT-OF-ORDER REQUESTS")

    send(
        "run TOE before TOD. use evidence at /Users/rishi/Downloads/Latest/flask-api/evidence_2. "
        "company name is ChaosTest Inc",
        "TOE before TOD — unusual order"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 5. WRONG ENUM / BAD PARAMETER VALUES
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 5: BAD PARAMETERS")

    send(
        "run ai suggestions for the Underwater Basket Weaving industry",
        "Invalid industry name"
    )

    send(
        "run ai suggestions for manufacturing",
        "Correct industry (recovery)"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 6. ASKING AGENT TO EXPLAIN ITSELF
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 6: META / SELF-REFLECTION")

    send(
        "what tools do you have available? list them all",
        "Ask about available tools"
    )

    send(
        "why did 9 controls fail the TOD? what were the common reasons?",
        "Ask for analysis of previous results"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 7. UNDO / REVERT
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 7: UNDO / REVERT")

    send(
        "add a column called 'DELETE_ME' with value 'test'",
        "Add a throwaway column"
    )

    send(
        "actually remove that column. i dont want it",
        "Ask to remove column (no built-in tool for this)"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 8. COMPLEX MULTI-PART QUESTION
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 8: COMPLEX MULTI-PART")

    send(
        "I need three things: (1) how many unique subprocesses are in the rcm, "
        "(2) which control has the highest risk level, and "
        "(3) are there any controls that dont have a description?",
        "Triple question in one message"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 9. NATURAL LANGUAGE DATAFRAME QUERY
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 9: NATURAL LANGUAGE QUERIES")

    send(
        "show me all rows where the subprocess is 'Vendor Management'",
        "Natural language filter"
    )

    send(
        "which control ids have the word 'invoice' in their description?",
        "Keyword search in data"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 10. RE-RUN SOMETHING THAT ALREADY RAN
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 10: RE-RUN EXISTING STAGE")

    send(
        "actually run the dedup analysis again. i want to double check",
        "Re-run deduplication"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 11. CONTRADICTORY INSTRUCTIONS
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 11: CONTRADICTIONS")

    send(
        "merge all the ai suggestions. wait no, dont merge any. "
        "actually just merge numbers 1 and 2",
        "Contradictory then final answer"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 12. CUSTOM PYTHON ANALYSIS
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 12: CUSTOM ANALYSIS VIA PYTHON")

    send(
        "write me a python snippet that groups the rcm by subprocess, "
        "counts rows per group, and tells me which subprocess has the most controls",
        "Custom Python groupby analysis"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 13. EXTREMELY LONG RAMBLING INPUT
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 13: LONG RAMBLING INPUT")

    send(
        "so like i was thinking about this the other day and my manager asked me "
        "about the audit and i told him we were using this new ai agent thing and "
        "he was like ok but does it actually check if the controls are working and "
        "i said yeah it does the test of design and the test of effectiveness and "
        "he said ok but what about the ones that failed and i was like well "
        "we need to look at those more carefully so basically can you just "
        "tell me which controls failed both TOD and TOE because those are "
        "the really bad ones we need to fix first",
        "Rambling but clear goal buried at end"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 14. ASKING ABOUT NON-EXISTENT COLUMNS
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 14: NON-EXISTENT DATA")

    send(
        "show me the value counts for the 'Audit_Outcome' column",
        "Column that doesn't exist"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 15. CROSS-STAGE COMPARISON
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 15: CROSS-STAGE ANALYSIS")

    send(
        "compare the TOD and TOE results side by side. which controls passed "
        "design but failed effectiveness? and vice versa?",
        "Cross-stage comparison"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 16. FINAL — POLITE GOODBYE
    # ══════════════════════════════════════════════════════════════════════

    section("PHASE 16: WRAP UP")

    send(
        "save everything and were done. call it 'chaos_test_final'. thanks!",
        "Final save + polite goodbye"
    )

    # ══════════════════════════════════════════════════════════════════════
    # RESULTS
    # ══════════════════════════════════════════════════════════════════════

    elapsed = time.time() - start
    section("CHAOS TEST RESULTS")

    log(f"  Duration:        {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log(f"  Prompts sent:    {len(prompts)}")
    log(f"  Tool calls:      {state.tool_call_count}")
    log(f"  Python execs:    {state.python_exec_count}")
    log(f"  Artifacts:       {len(state.artifacts)}")
    for a in state.artifacts:
        log(f"    - {a}")

    log(f"\n  All prompts:")
    for i, p in enumerate(prompts, 1):
        status = "CRASH" if any(p in c for c in crashes) else "OK"
        log(f"    {i:2d}. [{status:5s}] {p}")

    if crashes:
        log(f"\n  CRASHES ({len(crashes)}):")
        for c in crashes:
            log(f"    [CRASH] {c}")
    else:
        log(f"\n  Crashes: None")

    log(f"\n  Final state:")
    if state.rcm_df is not None:
        log(f"    RCM: {len(state.rcm_df)} rows x {len(state.rcm_df.columns)} cols")
    else:
        log(f"    RCM: not loaded")
    log(f"    Output dir:  {state.output_dir}")
    log(f"    Suggestions: {len(state.suggestions_cache) if state.suggestions_cache else 0}")
    log(f"    Dedup pairs: {len(state.dedup_cache.get('pairs',[])) if state.dedup_cache else 0}")
    log(f"    TOD results: {len(state.tod_results) if state.tod_results else 0}")
    log(f"    TOE results: {len(state.toe_results) if state.toe_results else 0}")

    if state.plan_scratchpad:
        log(f"\n  Agent's plan scratchpad:")
        log(state.plan_scratchpad)

    result = "ALL PROMPTS HANDLED" if len(crashes) == 0 else f"{len(crashes)} CRASHES"
    log(f"\n  RESULT: {result}")


if __name__ == "__main__":
    main()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(os.getcwd(), f"agent_chaos_test_{ts}.txt")
    with open(output_path, "w") as f:
        f.write("\n".join(LOG_LINES))
    print(f"\n  Full log saved to: {output_path}")
