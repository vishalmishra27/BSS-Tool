"""
Terminal REPL for the autonomous control testing agent.

Run with::

    cd flask-api
    python -m agent
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict

# Ensure engines/ is on sys.path so bare `import TOE_Engine` etc. work
_agent_pkg_dir = os.path.dirname(os.path.abspath(__file__))
_flask_api_dir = os.path.dirname(_agent_pkg_dir)
if os.path.join(_flask_api_dir, "engines") not in sys.path:
    sys.path.insert(0, os.path.join(_flask_api_dir, "engines"))

from .core.loop import DefaultCallbacks


# ---------------------------------------------------------------------------
# Coloured terminal callbacks
# ---------------------------------------------------------------------------

class TerminalCallbacks(DefaultCallbacks):
    """Pretty-print agent output with ANSI colours."""

    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    DIM = "\033[2m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    def on_speak(self, text: str) -> None:
        print(f"\n{self.CYAN}{self.BOLD}Agent:{self.RESET} {text}")

    def on_act(self, tool_name: str, args: Dict[str, Any]) -> None:
        args_preview = json.dumps(args, default=str)
        if len(args_preview) > 300:
            args_preview = args_preview[:300] + "..."
        print(f"\n  {self.GREEN}>> {tool_name}{self.RESET}"
              f"{self.DIM}({args_preview}){self.RESET}")

    def on_tool_result(self, tool_name: str, result: Dict[str, Any], duration: float) -> None:
        status = "OK" if not result.get("error") else "ERR"
        colour = self.GREEN if status == "OK" else self.RED
        print(f"  {colour}<< {tool_name} [{status}] {duration:.1f}s{self.RESET}")

    def on_error(self, error: str) -> None:
        print(f"\n  {self.RED}[ERROR]{self.RESET} {error}")


# ---------------------------------------------------------------------------
# REPL
# ---------------------------------------------------------------------------

def main() -> None:
    # Basic logging setup
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-20s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    # Reduce noise from HTTP libs
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    from . import create_agent

    callbacks = TerminalCallbacks()
    loop, state = create_agent(callbacks=callbacks)

    print("=" * 70)
    print("  CONTROL TESTING AGENT  (Autonomous)")
    print("  AI-powered interactive audit agent")
    print("=" * 70)
    print()
    print("  Type your request to begin (e.g. 'Load my RCM and check for duplicates')")
    print("  Commands:  /new = new session  |  /quit = exit  |  /state = show state")
    print()

    while True:
        try:
            user_input = input(f"\n{TerminalCallbacks.YELLOW}You:{TerminalCallbacks.RESET} ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        cmd = user_input.lower()
        if cmd in ("/quit", "/exit", "quit", "exit"):
            print("Goodbye!")
            break

        if cmd in ("/new", "/reset"):
            loop, state = create_agent(callbacks=callbacks)
            print("--- New session started ---")
            continue

        if cmd == "/state":
            _print_state(state)
            continue

        loop.process_message(user_input, state)


def _print_state(state) -> None:
    """Dump current session state for debugging."""
    c = TerminalCallbacks
    print(f"\n{c.BOLD}Session State:{c.RESET}")
    if state.rcm_df is not None:
        print(f"  RCM:  {len(state.rcm_df)} rows x {len(state.rcm_df.columns)} columns")
    else:
        print("  RCM:  not loaded")
    print(f"  Output dir:  {state.output_dir or 'not set'}")
    print(f"  Suggestions cached:  {len(state.suggestions_cache) if state.suggestions_cache else 0}")
    print(f"  Dedup pairs cached:  {len(state.dedup_cache.get('pairs', [])) if state.dedup_cache else 0}")
    print(f"  TOD results:  {len(state.tod_results) if state.tod_results else 0}")
    print(f"  TOE results:  {len(state.toe_results) if state.toe_results else 0}")
    print(f"  Tool calls:  {state.tool_call_count}")
    print(f"  Artifacts:  {len(state.artifacts)}")
    if state.plan_scratchpad:
        print(f"  Plan:\n{state.plan_scratchpad}")


if __name__ == "__main__":
    main()
