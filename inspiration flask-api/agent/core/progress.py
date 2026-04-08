"""
Real-time progress tracking for long-running engine operations.

Numbers come from actual batches / items completing inside
ThreadPoolExecutor loops — no fake weights.

Provides:
- Per-phase progress with smoothed ETA
- Overall pipeline progress (weighted)
- Total pipeline elapsed time
- Unified tool progress (TOD/TOE) with elapsed + ETA
- Thread-safe updates via a single shared lock
- SSE-ready snapshots for frontend push
"""

from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("agent.progress")

# Single shared lock — used by scoping progress AND tool progress.
# All writers must use this; avoids the bug of multiple independent locks.
_lock = threading.Lock()

# ── Human-friendly labels for each phase key ──────────────────────────
PHASE_LABELS = {
    "starting":            "Starting Scoping Engine",
    "ingest":              "Parsing Trial Balance",
    "categorize":          "Categorising Accounts",
    "quantitative":        "Quantitative Analysis",
    "qualitative":         "Qualitative Risk Assessment",
    "scoping":             "Applying Scoping Rules",
    "map_to_processes":    "Mapping Accounts to Processes",
    "ingest_sops":         "Parsing SOP Documents",
    "build_embeddings":    "Building Document Embeddings",
    "validate_coverage":   "Validating SOP Coverage",
    "extract_and_map":     "Extracting Controls & Risks",
    "completeness_review": "Completeness Review",
    "export":              "Exporting RCM Workbook",
}

# ── Pipeline definitions ──────────────────────────────────────────────
# Two pipelines: initial scoping (phases 1-5) and downstream (phases 6-10+).
# Weights approximate relative wall-clock duration.
SCOPING_PIPELINE = [
    {"key": "ingest",       "label": "Parse Trial Balance",        "weight": 10},
    {"key": "categorize",   "label": "Categorise Accounts",        "weight": 30},
    {"key": "quantitative", "label": "Quantitative Analysis",      "weight": 10},
    {"key": "qualitative",  "label": "Qualitative Risk Assessment", "weight": 40},
    {"key": "scoping",      "label": "Apply Scoping Rules",        "weight": 10},
]

DOWNSTREAM_PIPELINE = [
    {"key": "map_to_processes",    "label": "Mapping Accounts to Processes",  "weight": 15},
    {"key": "ingest_sops",         "label": "Parsing SOP Documents",          "weight": 25},
    {"key": "build_embeddings",    "label": "Building Document Embeddings",   "weight": 10},
    {"key": "validate_coverage",   "label": "Validating SOP Coverage",        "weight": 10},
    {"key": "extract_and_map",     "label": "Extracting Controls & Risks",    "weight": 25},
    {"key": "completeness_review", "label": "Completeness Review",            "weight": 10},
    {"key": "export",              "label": "Exporting RCM Workbook",         "weight":  5},
]

_SCOPING_KEYS = [p["key"] for p in SCOPING_PIPELINE]
_SCOPING_TOTAL_WEIGHT = sum(p["weight"] for p in SCOPING_PIPELINE)

_DOWNSTREAM_KEYS = [p["key"] for p in DOWNSTREAM_PIPELINE]
_DOWNSTREAM_TOTAL_WEIGHT = sum(p["weight"] for p in DOWNSTREAM_PIPELINE)

# Phases where ETA is meaningless (near-instant fixed sub-steps).
# Showing a jumpy "3s remaining" for a 0.2s operation looks broken.
_INSTANT_PHASES = frozenset({
    "quantitative", "validate_coverage", "completeness_review", "export",
    "scoping",
})

# Phases that are a single blocking call with no granular progress.
# Frontend should show an animated/pulsing bar (indeterminate) instead of
# a percentage bar stuck at 0%.  The backend sets total=1 and current=0
# while the work is in-flight, then current=1 when done.
_INDETERMINATE_PHASES = frozenset({
    "ingest",
})


# ── Duration formatting ───────────────────────────────────────────────

def _fmt_duration(seconds: Optional[float]) -> Optional[str]:
    """Format seconds into a human-readable string.

    Examples: ``"5s"``, ``"1m 23s"``, ``"12m 5s"``.
    Returns ``None`` if input is ``None``.
    """
    if seconds is None:
        return None
    s = max(0, int(round(seconds)))
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    return f"{m}m {s}s"


# ── ETA smoothing ─────────────────────────────────────────────────────

def _smoothed_eta(
    elapsed: float,
    current: int,
    total: int,
    prev_eta: Optional[float],
) -> Optional[float]:
    """ETA with exponential smoothing to avoid jumpy estimates.

    - First update: raw linear ETA.
    - Subsequent: blend 70% new + 30% old for smooth convergence.
    - Returns 0.0 (not None) when current >= total so frontend shows "0s"
      instead of a blank.
    - Returns None only when current <= 0 (haven't started yet).
    """
    if current <= 0:
        return None
    if current >= total:
        return 0.0  # Done — show "0s remaining" rather than blank
    raw_eta = (elapsed / current) * (total - current)
    if prev_eta is None:
        return round(raw_eta, 1)
    smoothed = 0.7 * raw_eta + 0.3 * prev_eta
    return round(max(0, smoothed), 1)


# ── Overall pipeline computation ──────────────────────────────────────

def _compute_overall(phase: str, phase_pct: float) -> Dict:
    """Compute overall pipeline progress given the current phase + its %.

    Automatically selects the correct pipeline (scoping or downstream)
    based on which one contains the phase key.
    """
    # Determine which pipeline this phase belongs to
    if phase in _SCOPING_KEYS:
        pipeline = SCOPING_PIPELINE
        keys = _SCOPING_KEYS
        total_weight = _SCOPING_TOTAL_WEIGHT
    elif phase in _DOWNSTREAM_KEYS:
        pipeline = DOWNSTREAM_PIPELINE
        keys = _DOWNSTREAM_KEYS
        total_weight = _DOWNSTREAM_TOTAL_WEIGHT
    else:
        # Phase not in any pipeline (e.g. "starting") — return zero progress
        return {
            "overall_pct": 0.0,
            "pipeline_step": 0,
            "pipeline_total": 0,
            "steps": [],
        }

    idx = keys.index(phase)

    # Weight of all completed phases
    completed_weight = sum(pipeline[i]["weight"] for i in range(idx))
    # Fractional progress within current phase
    current_weight = pipeline[idx]["weight"] * (phase_pct / 100.0)
    overall_pct = round(((completed_weight + current_weight) / total_weight) * 100, 1)

    # Build checklist for frontend
    steps: List[Dict] = []
    for i, p in enumerate(pipeline):
        if i < idx:
            steps.append({"key": p["key"], "label": p["label"], "status": "done"})
        elif i == idx:
            steps.append({"key": p["key"], "label": p["label"], "status": "active"})
        else:
            steps.append({"key": p["key"], "label": p["label"], "status": "pending"})

    return {
        "overall_pct": overall_pct,
        "pipeline_step": idx + 1,
        "pipeline_total": len(pipeline),
        "steps": steps,
    }


# ── Scoping progress ─────────────────────────────────────────────────

def update_progress(
    state: Any,
    phase: str,
    current: int,
    total: int,
    message: str = "",
    *,
    sub_step: Optional[str] = None,
) -> None:
    """Update ``state.scoping_progress`` in a thread-safe manner.

    Parameters
    ----------
    state : AgentState
    phase : str
        Current phase name (e.g. ``"categorize"``).
    current, total : int
        Items completed / expected.
    message : str, optional
        Human-readable status message.
    sub_step : str, optional
        Label for a sub-operation within the phase.
    """
    now = time.time()

    prev = getattr(state, "scoping_progress", None) or {}

    # Track per-phase start time (resets when phase changes)
    if prev.get("phase") == phase:
        started = prev.get("_started_ts", now)
    else:
        started = now

    # Track total pipeline start time on state itself (survives clear_progress).
    # Only set once — first update_progress call in the pipeline starts the clock.
    if getattr(state, "_pipeline_started_ts", None) is None:
        state._pipeline_started_ts = now
    pipeline_started = state._pipeline_started_ts

    elapsed = now - started
    pipeline_elapsed = now - pipeline_started
    pct = round((current / total) * 100, 1) if total > 0 else 0.0

    # Smoothed ETA for current phase
    # - Skip for near-instant phases (no meaningful ETA on sub-second ops)
    # - Skip for indeterminate phases (single blocking call, no data points)
    if phase in _INSTANT_PHASES or phase in _INDETERMINATE_PHASES:
        eta = 0.0 if current >= total else None
    else:
        prev_eta_raw = prev.get("_eta_raw") if prev.get("phase") == phase else None
        eta = _smoothed_eta(elapsed, current, total, prev_eta_raw)

    # Indeterminate flag — tells frontend to show a pulsing/animated bar
    # instead of a percentage bar stuck at 0% during long blocking calls.
    is_indeterminate = phase in _INDETERMINATE_PHASES and current < total

    # Overall pipeline progress
    overall = _compute_overall(phase, pct)

    progress = {
        "phase": phase,
        "phase_label": PHASE_LABELS.get(phase, phase.replace("_", " ").title()),
        "message": message or f"{phase} ({current}/{total})",
        "indeterminate": is_indeterminate,
        "current": current,
        "total": total,
        "pct": pct,
        "sub_step": sub_step,
        # Phase timing
        "elapsed": _fmt_duration(elapsed),
        "eta": _fmt_duration(eta),
        "elapsed_seconds": round(elapsed),
        "eta_seconds": round(eta) if eta is not None else None,
        # Total pipeline timing
        "pipeline_elapsed": _fmt_duration(pipeline_elapsed),
        "pipeline_elapsed_seconds": round(pipeline_elapsed),
        # Overall pipeline progress
        "overall_pct": overall["overall_pct"],
        "pipeline_step": overall["pipeline_step"],
        "pipeline_total": overall["pipeline_total"],
        "steps": overall["steps"],
        # ISO timestamp
        "started_at": datetime.fromtimestamp(started, tz=timezone.utc).isoformat(),
        # Internal — stripped before sending to frontend
        "_started_ts": started,
        "_eta_raw": eta,
    }

    with _lock:
        state.scoping_progress = progress

    logger.info(
        "PROGRESS  %-22s  %d/%d  (%5.1f%%)  elapsed=%-8s  eta=%-8s  "
        "overall=%5.1f%%  [%d/%d]  pipeline=%s  — %s",
        PHASE_LABELS.get(phase, phase),
        current, total, pct,
        _fmt_duration(elapsed) or "0s",
        _fmt_duration(eta) or "-",
        overall["overall_pct"],
        overall["pipeline_step"],
        overall["pipeline_total"],
        _fmt_duration(pipeline_elapsed),
        message,
    )


def clear_progress(state: Any) -> Optional[Dict]:
    """Clear scoping progress and return the last snapshot.

    NOTE: Does NOT clear ``state._pipeline_started_ts`` — the pipeline
    timer persists across tool invocations so overall elapsed stays accurate.
    Call :func:`finish_pipeline` when the entire pipeline is done.
    """
    with _lock:
        last = getattr(state, "scoping_progress", None)
        state.scoping_progress = None
    return last


def finish_pipeline(state: Any) -> Optional[Dict]:
    """Mark the pipeline as finished and return total timing info.

    Call this when the very last phase completes (``scoping`` or ``export``).
    Clears both progress and the pipeline timer.
    """
    now = time.time()
    pipeline_started = getattr(state, "_pipeline_started_ts", None)
    total_elapsed = (now - pipeline_started) if pipeline_started else None

    last = clear_progress(state)
    state._pipeline_started_ts = None

    result = {
        "pipeline_total_elapsed": _fmt_duration(total_elapsed),
        "pipeline_total_elapsed_seconds": round(total_elapsed) if total_elapsed else None,
        "last_progress": last,
    }
    logger.info("PIPELINE  finished — total elapsed: %s", result["pipeline_total_elapsed"] or "unknown")
    return result


# ── Tool progress (TOD / TOE) ────────────────────────────────────────

def update_tool_progress(
    state: Any,
    tool_name: str,
    current: int,
    total: int,
    message: str = "",
    *,
    controls_done: Optional[List[Dict]] = None,
) -> None:
    """Update ``state.tool_progress`` with elapsed time and ETA.

    This replaces the per-file ``_update_tool_progress`` functions in
    test_of_design.py / test_of_effectiveness.py — one function, one lock,
    consistent timing behaviour.
    """
    now = time.time()

    prev = getattr(state, "tool_progress", None) or {}
    # Preserve start time when the same tool is running; reset on tool change.
    if prev.get("tool") == tool_name:
        started = prev.get("_started_ts", now)
    else:
        started = now

    elapsed = now - started
    pct = round((current / total) * 100, 1) if total > 0 else 0.0

    prev_eta = prev.get("_eta_raw") if prev.get("tool") == tool_name else None
    eta = _smoothed_eta(elapsed, current, total, prev_eta)

    progress = {
        "tool": tool_name,
        "current": current,
        "total": total,
        "pct": pct,
        "message": message,
        "elapsed": _fmt_duration(elapsed),
        "eta": _fmt_duration(eta),
        "elapsed_seconds": round(elapsed),
        "eta_seconds": round(eta) if eta is not None else None,
        "controls_done": controls_done or [],
        # Internal
        "_started_ts": started,
        "_eta_raw": eta,
    }

    with _lock:
        state.tool_progress = progress

    logger.info(
        "TOOL_PROGRESS  %-28s  %d/%d  (%5.1f%%)  elapsed=%-8s  eta=%-8s  — %s",
        tool_name,
        current, total, pct,
        _fmt_duration(elapsed) or "0s",
        _fmt_duration(eta) or "-",
        message,
    )


def clear_tool_progress(state: Any) -> Optional[Dict]:
    """Clear tool progress and return the last snapshot."""
    with _lock:
        last = getattr(state, "tool_progress", None)
        state.tool_progress = None
    return last


# ── Counting wrapper for ThreadPoolExecutor ───────────────────────────

def make_counting_as_completed(
    state: Any,
    phase: str,
    total: int,
    real_as_completed: Callable,
    message_template: str = "{phase} (batch {current}/{total})",
) -> Callable:
    """Drop-in replacement for ``concurrent.futures.as_completed`` that
    calls :func:`update_progress` every time a future completes.
    """
    counter = {"n": 0}

    def _counting(fs, *args, **kwargs):
        for future in real_as_completed(fs, *args, **kwargs):
            counter["n"] += 1
            msg = message_template.format(
                phase=phase, current=counter["n"], total=total,
            )
            update_progress(state, phase, counter["n"], total, msg)
            yield future

    return _counting
