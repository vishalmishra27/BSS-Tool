"""Chat-based tool to change the risk-level derivation logic.

Allows the user to provide custom logic for how risk_level is derived from
Risk Probability and Risk Impact.  For example:
  - "always pick the lower one"
  - "change weights to Low=2, Medium=4, High=8"
  - custom mapping like "Low+Medium = Low"

The tool updates the scoring configuration in agent state so that the next
inference / Quality Comparison run uses the new derivation.  It also
immediately re-derives risk levels for all controls in the loaded RCM.

The risk matrix is never independently hardcoded — it is always computed from
the score weights and band thresholds, so changing weights or bands
automatically changes the matrix.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

from .base import Tool
from .infer_risk_level import (
    DEFAULT_SCORE_MAP, DEFAULT_BANDS,
    build_risk_matrix, compute_risk_score, _score_to_level,
)
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..utils import sanitize_for_json

logger = logging.getLogger("agent.tools.modify_risk_logic")

# Pre-built strategies the user can reference by name
_BUILTIN_STRATEGIES = {
    "weighted": "Weighted non-linear scoring: Low=1, Medium=3, High=6; Score = P x I (default).",
    "max": "Pick the higher of probability and impact.",
    "min": "Pick the lower of probability and impact.",
    "average": "Pick the average (round up) of probability and impact.",
}

# Default ranks used when building custom mappings (legacy strategies)
_DEFAULT_RANK = {"low": 1, "medium": 2, "moderate": 2, "high": 3, "critical": 4, "very high": 4}
_DEFAULT_LABEL = {1: "Low", 2: "Medium", 3: "High", 4: "Critical"}

# Aliases that map to the three canonical input values used in the risk matrix.
_INPUT_ALIASES = {
    "low": "low", "l": "low", "minor": "low", "minimal": "low",
    "medium": "medium", "med": "medium", "moderate": "medium", "m": "medium",
    "high": "high", "h": "high", "major": "high", "significant": "high",
    "very high": "high", "critical": "high",
}


def _apply_strategy_to_df(df, strategy: str, severity_rank: dict,
                          rank_to_label: dict, score_map=None, bands=None):
    """Re-derive risk_level for all rows in df using the given strategy."""
    import math
    has_prob = "risk_probability" in df.columns
    has_impact = "risk_impact" in df.columns
    if not (has_prob and has_impact):
        return 0  # nothing to derive

    updated = 0
    for idx, row in df.iterrows():
        prob = str(row.get("risk_probability", "")).strip().lower()
        impact = str(row.get("risk_impact", "")).strip().lower()

        if strategy == "weighted":
            # Weighted non-linear scoring via configurable weights + bands.
            # The matrix is never looked up separately — it is always
            # the arithmetic product of the weights mapped through the bands.
            norm_p = _INPUT_ALIASES.get(prob, "")
            norm_i = _INPUT_ALIASES.get(impact, "")
            if norm_p and norm_i:
                score, level = compute_risk_score(norm_p, norm_i, score_map, bands)
                if score > 0 and level:
                    df.at[idx, "risk_level"] = level
                    updated += 1
            continue

        # Legacy strategies (max, min, average)
        p_rank = severity_rank.get(prob, 0)
        i_rank = severity_rank.get(impact, 0)

        if p_rank == 0 and i_rank == 0:
            continue  # no valid data

        if strategy == "min":
            # Pick lower; if one is 0 (missing), use the other
            chosen = min(p_rank, i_rank) if (p_rank and i_rank) else max(p_rank, i_rank)
        elif strategy == "average":
            chosen = math.ceil((p_rank + i_rank) / 2)
        else:  # "max"
            chosen = max(p_rank, i_rank)

        label = rank_to_label.get(chosen, "High")
        df.at[idx, "risk_level"] = label
        updated += 1

    return updated


def _parse_weights(raw: str) -> Optional[Dict[str, int]]:
    """Parse weight specification like 'Low=2, Medium=4, High=8'."""
    pairs = re.findall(r'(low|medium|high)\s*[=:]\s*(\d+)', raw, re.IGNORECASE)
    if len(pairs) < 3:
        return None
    return {k.lower(): int(v) for k, v in pairs}


def _parse_bands(raw: str) -> Optional[List[tuple]]:
    """Parse band specification like '1-6=Low, 7-20=Medium, 21-40=High'.

    Anything above the last upper bound is automatically "Critical".
    """
    matches = re.findall(r'(\d+)\s*[-–]\s*(\d+)\s*[=:]\s*(Low|Medium|High)', raw, re.IGNORECASE)
    if len(matches) < 2:
        return None
    bands = []
    for _, upper, label in sorted(matches, key=lambda m: int(m[1])):
        bands.append((int(upper), label.capitalize()))
    return bands


class ModifyRiskLogicTool(Tool):
    """Change how risk_level is derived from Risk Probability + Risk Impact."""

    @property
    def name(self) -> str:
        return "modify_risk_logic"

    @property
    def description(self) -> str:
        return (
            "Change the logic used to derive risk_level from Risk Probability "
            "and Risk Impact columns. Default strategy is 'weighted' (non-linear: "
            "Low=1, Medium=3, High=6; Score=PxI; bands: 1-5=Low, 6-17=Medium, "
            "18-35=High, 36=Critical). Other options: 'max', 'min', 'average', "
            "or a custom mapping (e.g., 'Low+High=Medium'). "
            "You can also change the weights (e.g., 'Low=2, Medium=5, High=10') "
            "or band thresholds (e.g., '1-6=Low, 7-20=Medium, 21-40=High') — "
            "the risk matrix auto-recomputes from these values. "
            "After changing the logic, all controls in the loaded RCM are "
            "immediately re-derived with the new logic."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                "strategy", "string",
                (
                    "The derivation strategy. Built-in options: 'weighted' (non-linear PxI scoring — default), "
                    "'max' (pick higher), 'min' (pick lower), 'average' (average, round up). "
                    "Or provide a natural-language description like 'if either is Critical, "
                    "use Critical; otherwise pick the lower one'."
                ),
                required=True,
            ),
            ToolParameter(
                "custom_mapping", "string",
                (
                    "Optional custom mapping as comma-separated rules. "
                    "Format: 'Low+Low=Low, Low+Medium=Low, Medium+High=High, ...'. "
                    "Each rule maps a Probability+Impact pair to a result level. "
                    "If provided, this overrides the strategy parameter."
                ),
                required=False,
            ),
            ToolParameter(
                "weights", "string",
                (
                    "Optional custom score weights for the weighted strategy. "
                    "Format: 'Low=2, Medium=5, High=10'. Changes the internal "
                    "numeric scores used to compute P × I. "
                    "The risk matrix auto-recomputes from these new weights."
                ),
                required=False,
            ),
            ToolParameter(
                "bands", "string",
                (
                    "Optional custom band thresholds for the weighted strategy. "
                    "Format: '1-6=Low, 7-20=Medium, 21-40=High'. "
                    "Scores above the last upper bound map to Critical. "
                    "The risk matrix auto-recomputes from these new bands."
                ),
                required=False,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        if state.rcm_df is None:
            return "No RCM loaded yet. Upload or load an RCM file first."
        has_prob = "risk_probability" in state.rcm_df.columns
        has_impact = "risk_impact" in state.rcm_df.columns
        if not (has_prob and has_impact):
            return (
                "RCM does not have 'Risk Probability' and 'Risk Impact' columns. "
                "Risk level derivation requires both columns."
            )
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        strategy_raw = str(args.get("strategy", "weighted")).strip().lower()
        custom_mapping_raw = str(args.get("custom_mapping", "") or "").strip()
        weights_raw = str(args.get("weights", "") or "").strip()
        bands_raw = str(args.get("bands", "") or "").strip()

        severity_rank = dict(_DEFAULT_RANK)
        rank_to_label = dict(_DEFAULT_LABEL)

        # ── Handle custom mapping ────────────────────────────────────
        if custom_mapping_raw:
            pair_map = {}
            try:
                for rule in custom_mapping_raw.split(","):
                    rule = rule.strip()
                    if not rule or "=" not in rule:
                        continue
                    lhs, rhs = rule.split("=", 1)
                    lhs = lhs.strip()
                    rhs = rhs.strip()
                    if "+" not in lhs:
                        continue
                    prob_str, impact_str = lhs.split("+", 1)
                    pair_map[(prob_str.strip().lower(), impact_str.strip().lower())] = rhs
            except Exception as e:
                return ToolResult(
                    success=False, data={},
                    error=f"Failed to parse custom_mapping: {e}. Expected format: 'Low+Medium=Medium, High+High=High'",
                )

            if not pair_map:
                return ToolResult(
                    success=False, data={},
                    error="No valid rules parsed from custom_mapping. Expected format: 'Low+Medium=Medium, High+High=High'",
                )

            # Apply custom mapping directly to RCM
            df = state.rcm_df
            updated = 0
            for idx, row in df.iterrows():
                prob = str(row.get("risk_probability", "")).strip().lower()
                impact = str(row.get("risk_impact", "")).strip().lower()
                result = pair_map.get((prob, impact))
                if result is None:
                    # Try reverse
                    result = pair_map.get((impact, prob))
                if result:
                    df.at[idx, "risk_level"] = result
                    updated += 1

            state.custom_risk_severity_rank = severity_rank
            state.custom_risk_rank_to_label = rank_to_label

            return ToolResult(
                success=True,
                data=sanitize_for_json({
                    "mode": "custom_mapping",
                    "rules_parsed": len(pair_map),
                    "controls_updated": updated,
                    "message": f"Applied {len(pair_map)} custom mapping rules. {updated} controls re-derived.",
                }),
                summary=f"Applied custom risk mapping ({len(pair_map)} rules, {updated} controls updated)",
            )

        # ── Handle built-in or natural-language strategies ───────────
        # Map natural-language to built-in strategy names
        strategy = "weighted"  # default — weighted non-linear scoring
        s = strategy_raw
        if s in ("weighted", "non-linear", "nonlinear", "matrix", "p x i", "p*i",
                 "multiply", "multiplication", "product", "score"):
            strategy = "weighted"
        elif s in ("max", "higher", "pick the higher", "pick higher", "maximum"):
            strategy = "max"
        elif s in ("min", "lower", "pick the lower", "pick lower", "minimum"):
            strategy = "min"
        elif s in ("average", "avg", "mean", "average them", "round up"):
            strategy = "average"
        else:
            # Try to infer from the description
            if "lower" in s or "min" in s:
                strategy = "min"
            elif "average" in s or "avg" in s or "mean" in s:
                strategy = "average"
            elif "higher" in s or "max" in s:
                strategy = "max"
            elif "weight" in s or "multiply" in s or "matrix" in s or "non-linear" in s:
                strategy = "weighted"
            else:
                strategy = "weighted"
                logger.info("Could not parse strategy '%s', defaulting to 'weighted'", strategy_raw)

        # ── Parse optional custom weights / bands ────────────────────
        custom_sm = None
        custom_bands = None

        if strategy == "weighted":
            if weights_raw:
                custom_sm = _parse_weights(weights_raw)
                if custom_sm is None:
                    return ToolResult(
                        success=False, data={},
                        error=(
                            f"Could not parse weights from: '{weights_raw}'. "
                            "Expected format: 'Low=2, Medium=5, High=10'."
                        ),
                    )
            if bands_raw:
                custom_bands = _parse_bands(bands_raw)
                if custom_bands is None:
                    return ToolResult(
                        success=False, data={},
                        error=(
                            f"Could not parse bands from: '{bands_raw}'. "
                            "Expected format: '1-6=Low, 7-20=Medium, 21-40=High'."
                        ),
                    )

        # Re-derive all controls
        df = state.rcm_df
        updated = _apply_strategy_to_df(
            df, strategy, severity_rank, rank_to_label,
            score_map=custom_sm, bands=custom_bands,
        )

        # Save custom config in state for downstream use
        state.custom_risk_severity_rank = severity_rank
        state.custom_risk_rank_to_label = rank_to_label
        if strategy == "weighted":
            state.custom_risk_score_map = custom_sm        # None means "use defaults"
            state.custom_risk_bands = custom_bands          # None means "use defaults"

        # Build the auto-computed matrix for display
        effective_sm = custom_sm or DEFAULT_SCORE_MAP
        effective_bands = custom_bands or DEFAULT_BANDS
        matrix = build_risk_matrix(custom_sm, custom_bands)
        matrix_display = {
            f"{p.capitalize()} x {i.capitalize()}": lvl
            for (p, i), lvl in matrix.items()
        }

        return ToolResult(
            success=True,
            data=sanitize_for_json({
                "mode": "strategy",
                "strategy": strategy,
                "strategy_description": _BUILTIN_STRATEGIES.get(strategy, strategy),
                "controls_updated": updated,
                "weights": {k.capitalize(): v for k, v in effective_sm.items()},
                "bands": [
                    {"upper_bound": t, "level": l} for t, l in effective_bands
                ] + [{"upper_bound": "above", "level": "Critical"}],
                "risk_matrix": matrix_display,
                "customised_weights": bool(custom_sm),
                "customised_bands": bool(custom_bands),
                "message": (
                    f"Risk derivation strategy set to '{strategy}'. "
                    f"{updated} controls re-derived from probability/impact."
                    + (f" Custom weights: {effective_sm}." if custom_sm else "")
                    + (f" Custom bands: {effective_bands}." if custom_bands else "")
                ),
            }),
            summary=f"Risk logic set to '{strategy}' — {updated} controls re-derived",
        )
