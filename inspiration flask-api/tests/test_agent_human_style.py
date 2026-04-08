"""
HUMAN-STYLE SOX AGENT TEST — Realistic messy prompts.

Tests the agent with the kind of inputs a real auditor would give:
  - Vague / lazy prompts
  - Typos and broken grammar
  - Mid-flow topic changes
  - Asking random unrelated things
  - Changing mind halfway
  - Giving incomplete info
  - Casual tone
  - Contradictions
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


# ── Logging callbacks ─────────────────────────────────────────────────────────

class LoggingCallbacks:
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

        # Compact result summary
        summary = {}
        for key in result:
            val = result[key]
            if key == "_agent_notes":
                summary[key] = val
            elif key == "error":
                summary[key] = val
            elif isinstance(val, (str, int, float, bool)):
                s = str(val)
                summary[key] = s[:150] + "..." if len(s) > 150 else val
            elif isinstance(val, list):
                summary[key] = f"[{len(val)} items]"
            elif isinstance(val, dict):
                summary[key] = f"{{...{len(val)} keys}}"
        log(f"     Result: {json.dumps(summary, default=str, indent=2)[:800]}")

    def on_error(self, error: str) -> None:
        log(f"\n  [ERROR] {error}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    start = time.time()

    section("HUMAN-STYLE SOX AGENT TEST")
    log(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log(f"Testing with messy, realistic, human-like prompts")
    log(f"This makes REAL OpenAI API calls.\n")

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

    log(f"Agent ready: {len(loop._registry)} tools\n")

    prompts_sent = []
    errors = []

    def send(user_msg: str, label: str) -> bool:
        divider(f"PROMPT: {label}")
        log(f"\nYou: {user_msg}")
        prompts_sent.append(label)
        t0 = time.time()
        try:
            loop.process_message(user_msg, state)
            elapsed = time.time() - t0
            log(f"\n  [Done in {elapsed:.1f}s, total tool_calls={state.tool_call_count}]")
            return True
        except Exception as e:
            elapsed = time.time() - t0
            log(f"\n  [EXCEPTION after {elapsed:.1f}s]: {e}")
            import traceback
            log(traceback.format_exc())
            errors.append(f"{label}: {e}")
            return False

    # ══════════════════════════════════════════════════════════════════════
    # 1. VAGUE / LAZY OPENING — no file path, no details
    # ══════════════════════════════════════════════════════════════════════

    section("1. VAGUE OPENING — 'hey load my rcm'")

    send(
        "hey can u load my rcm file",
        "Vague load request (no path)"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 2. GIVE DIRECTORY INSTEAD OF FILE — common user mistake
    # ══════════════════════════════════════════════════════════════════════

    section("2. GIVE DIRECTORY INSTEAD OF FILE")

    send(
        "its in /Users/rishi/Downloads/Latest/flask-api/ somewhere, "
        "i think its called output or something",
        "Vague path with 'or something'"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 3. RANDOM QUESTION MID-FLOW — totally off topic
    # ══════════════════════════════════════════════════════════════════════

    section("3. RANDOM QUESTION — 'what is PCAOB AS 2201'")

    send(
        "btw quick question — what does pcaob as 2201 say about testing controls? "
        "just a quick summary",
        "Random PCAOB question mid-flow"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 4. BACK TO WORK — TYPOS AND CASUAL
    # ══════════════════════════════════════════════════════════════════════

    section("4. BACK TO WORK — typos, casual")

    send(
        "ok cool thnaks. now run the ai suggestins thing for manufacturing",
        "AI suggestions with typos"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 5. CHANGE MIND — "actually wait"
    # ══════════════════════════════════════════════════════════════════════

    section("5. CHANGE MIND — 'actually wait, how many rows do i have'")

    send(
        "actually wait before we do anything else — how many rows and columns "
        "does my rcm have right now? and show me the first few rows",
        "Interrupt to check data"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 6. VAGUE MERGE REQUEST
    # ══════════════════════════════════════════════════════════════════════

    section("6. VAGUE MERGE — 'keep the high priority ones'")

    send(
        "ok for those ai suggestions, just keep the high priority ones "
        "and merge them in. skip the low and medium ones",
        "Vague merge (by priority not index)"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 7. ASK ABOUT CURRENT STATE — like a confused user
    # ══════════════════════════════════════════════════════════════════════

    section("7. CONFUSED USER — 'wait what did we do so far'")

    send(
        "hold on im confused. what have we done so far? whats the current "
        "state of the rcm? did anything change?",
        "Confused state check"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 8. RUN DEDUP WITH CASUAL LANGUAGE
    # ══════════════════════════════════════════════════════════════════════

    section("8. DEDUP — casual, no tool name used")

    send(
        "check if theres any duplicate risks or controls in there. "
        "some of them look pretty similar to me",
        "Casual dedup request"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 9. CONTRADICTORY / WEIRD REQUEST
    # ══════════════════════════════════════════════════════════════════════

    section("9. WEIRD REQUEST — 'add a column then rename it'")

    send(
        "can you add a new column called 'Audit_Status' with value 'Pending' "
        "for all rows, then actually rename it to 'Review_Status' instead. "
        "also for any row where the process is Procure-to-Pay change it to 'In Review'",
        "Multi-step modify with rename"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 10. RUN TOD — WRONG PATH FIRST, THEN CORRECT
    # ══════════════════════════════════════════════════════════════════════

    section("10. TOD — wrong path first")

    send(
        "run the test of design. evidence is in /Users/rishi/Downloads/evidence_folder",
        "TOD with wrong path"
    )

    # Now give correct path
    send(
        "sorry wrong path. its at /Users/rishi/Downloads/Latest/flask-api/evidence_2",
        "TOD with correct path"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 11. SAVE MID-WAY — not waiting for the end
    # ══════════════════════════════════════════════════════════════════════

    section("11. SAVE MID-WAY — 'save what we have so far'")

    send(
        "save what we have so far just in case. call it 'mid_audit_checkpoint'",
        "Mid-audit save"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 12. TOE WITH INCOMPLETE INFO
    # ══════════════════════════════════════════════════════════════════════

    section("12. TOE — incomplete info, casual")

    send(
        "now run the effectiveness test too. same evidence folder as before. "
        "company is TestCo or whatever",
        "TOE with vague company name"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 13. ASK IT TO DO SOMETHING WITH PYTHON
    # ══════════════════════════════════════════════════════════════════════

    section("13. AD-HOC PYTHON — 'whats the pass rate'")

    send(
        "can you calculate the overall pass rate for TOD and the effectiveness "
        "rate for TOE? just do the math",
        "Ad-hoc analysis request"
    )

    # ══════════════════════════════════════════════════════════════════════
    # 14. FINAL SAVE — LAZY
    # ══════════════════════════════════════════════════════════════════════

    section("14. FINAL SAVE — 'save it and give me a summary'")

    send(
        "ok i think were done. save the final version and give me a quick "
        "summary of everything we found. keep it short",
        "Final save + summary"
    )

    # ══════════════════════════════════════════════════════════════════════
    # RESULTS
    # ══════════════════════════════════════════════════════════════════════

    elapsed = time.time() - start
    section("TEST RESULTS")

    log(f"  Duration:        {elapsed:.1f}s ({elapsed/60:.1f} min)")
    log(f"  Prompts sent:    {len(prompts_sent)}")
    log(f"  Tool calls:      {state.tool_call_count}")
    log(f"  Python execs:    {state.python_exec_count}")
    log(f"  Artifacts:       {len(state.artifacts)}")
    for a in state.artifacts:
        log(f"    - {a}")

    log(f"\n  Prompts sent:")
    for i, p in enumerate(prompts_sent, 1):
        log(f"    {i:2d}. {p}")

    if errors:
        log(f"\n  Errors ({len(errors)}):")
        for e in errors:
            log(f"    [ERR] {e}")
    else:
        log(f"\n  Errors: None")

    log(f"\n  Final state:")
    if state.rcm_df is not None:
        log(f"    RCM: {len(state.rcm_df)} rows x {len(state.rcm_df.columns)} cols")
        log(f"    Columns: {list(state.rcm_df.columns)}")
    log(f"    Output dir:  {state.output_dir}")
    log(f"    Suggestions: {len(state.suggestions_cache) if state.suggestions_cache else 0}")
    log(f"    Dedup pairs: {len(state.dedup_cache.get('pairs',[])) if state.dedup_cache else 0}")
    log(f"    TOD results: {len(state.tod_results) if state.tod_results else 0}")
    log(f"    TOE results: {len(state.toe_results) if state.toe_results else 0}")

    if state.plan_scratchpad:
        log(f"\n  Agent's plan scratchpad:")
        log(state.plan_scratchpad)

    result = "COMPLETE" if len(errors) == 0 else f"{len(errors)} ERRORS"
    log(f"\n  RESULT: {result}")


if __name__ == "__main__":
    main()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(os.getcwd(), f"agent_human_test_{ts}.txt")
    with open(output_path, "w") as f:
        f.write("\n".join(LOG_LINES))
    print(f"\n  Full log saved to: {output_path}")
