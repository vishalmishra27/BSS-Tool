"""
Shared data types for the autonomous control testing agent.

All dataclasses, enums, and type aliases used across the agent package.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ToolCategory(Enum):
    """Logical grouping for tools shown to the LLM."""
    ANALYSIS = "analysis"
    DATA = "data"
    FILESYSTEM = "filesystem"
    UTILITY = "utility"


# ---------------------------------------------------------------------------
# Tool-related types
# ---------------------------------------------------------------------------

@dataclass
class ToolParameter:
    """One parameter in a tool's JSON schema."""
    name: str
    type: str                            # "string", "integer", "boolean", "array", "object"
    description: str
    required: bool = True
    enum: Optional[List[str]] = None     # Allowed values
    items: Optional[Dict] = None         # For array element schema
    default: Any = None


@dataclass
class ToolResult:
    """Standardised result returned by every tool execution."""
    success: bool
    data: Dict[str, Any]
    error: Optional[str] = None
    duration_seconds: float = 0.0
    artifacts: List[str] = field(default_factory=list)   # File paths created
    summary: str = ""                                     # One-line human summary


# ---------------------------------------------------------------------------
# Agent state
# ---------------------------------------------------------------------------

@dataclass
class AgentState:
    """
    Complete mutable state for one agent session.

    Passed to tools, memory, reflector, and the agent loop.
    Replaces the global ``agent_state`` dict from sox_agent.py.
    """

    # RCM data
    rcm_df: Any = None                           # pandas DataFrame or None
    original_rcm_df: Any = None                  # original RCM with all original column names (for output)
    output_dir: Optional[str] = None

    # Engine result caches
    suggestions_cache: Optional[List] = None
    dedup_cache: Optional[Dict] = None
    tod_results: Optional[List] = None
    toe_results: Optional[List] = None
    sampling_results: Optional[List] = None

    # Evidence folder tracking (to prevent cross-contamination)
    tod_evidence_folder: Optional[str] = None          # TOD evidence folder (do NOT reuse for TOE)

    # TOD schema cache — schemas generated during TOD, reusable by TOE
    tod_schemas: Optional[Dict] = None               # dict[control_id -> ControlSchema]

    # TOD attribute approval flow
    pending_tod_schemas: Optional[Dict] = None        # Schemas awaiting user approval for TOD
    pending_tod_evidence_folder: Optional[str] = None  # Evidence folder for approved TOD run

    # TOE attribute approval flow
    pending_toe_schemas: Optional[Dict] = None        # Schemas awaiting user approval for TOE
    pending_toe_evidence_folder: Optional[str] = None  # Evidence folder for approved TOE run

    # Document list cache — rows from generate_document_list_excel() for evidence validation
    document_list_rows: Optional[List] = None       # [{control_id, required_documents: [...]}]
    evidence_validated: bool = False                 # True after evidence validation has run

    # Evidence extraction cache — built ONCE during evidence validation (Phase 2),
    # reused by the TOD/TOE engine (Phase 3) to avoid re-extracting files via
    # Document Intelligence.  Keys are absolute file path strings, values are
    # (content_text, doc_type_label, extraction_succeeded) tuples.
    evidence_extract_cache: Optional[Dict] = None

    # Agent scratchpad plan (free-form markdown written by the LLM)
    plan_scratchpad: Optional[str] = None

    # Serialized JSON cache of tool results — used by session-results endpoint for page-refresh restore
    tool_results_cache: Dict = field(default_factory=dict)

    # Scoping engine cache — avoids re-parsing the trial balance on every call
    scoping_engine: Any = None
    scoping_trial_balance_path: Optional[str] = None
    scoping_phase: str = "none"          # "none"|"ingested"|"quantitative_done"|"qualitative_done"|"scoped_done"|"complete"
    scoping_benchmark: Optional[str] = None
    scoping_materiality_pct: Optional[float] = None
    scoping_sop_paths: Optional[List[str]] = None
    scoping_quantitative_results: Optional[Dict] = None  # Cached quant results from quantitative_done phase
    scoping_awaiting_input: bool = False  # True after an intermediate phase result is returned; blocks re-entry until user provides new input
    scoping_progress: Optional[Dict] = None  # Live progress dict polled by frontend (set by update_progress())
    tool_progress: Optional[Dict] = None     # Live progress for TOD/TOE tools polled by frontend
    last_override: Optional[Dict] = None  # Metadata about the most recent user-uploaded override Excel

    # Uploaded documents context — injected by the Node.js backend so the
    # agent knows which files have already been uploaded for this chat/project,
    # even after logout/login or Flask restart.
    uploaded_documents: Optional[List[Dict]] = None

    # Frequency inference — pending inferred frequencies awaiting user approval
    pending_frequency_inferences: Optional[Dict] = None   # {control_id: {raw, inferred, kpmg_mapped, source, confidence}}
    frequency_inference_excel_path: Optional[str] = None   # Path to exported editable Excel

    # Quality Comparison results
    comparison_results: Optional[Any] = None  # QualityComparison.ComparisonReport

    # Risk level overrides — per-control manual overrides set via chat
    risk_level_overrides: Optional[Dict] = None  # {control_id: "High"}

    # Custom risk derivation logic — user-supplied severity rank & label maps
    custom_risk_severity_rank: Optional[Dict] = None   # e.g. {"low": 1, "medium": 2, ...}
    custom_risk_rank_to_label: Optional[Dict] = None   # e.g. {1: "Low", 2: "Medium", ...}

    # Custom weighted-scoring config — if the user changes weights or bands
    # the risk matrix auto-recomputes from these values.
    custom_risk_score_map: Optional[Dict] = None    # e.g. {"low": 1, "medium": 3, "high": 6}
    custom_risk_bands: Optional[list] = None        # e.g. [(5, "Low"), (17, "Medium"), (35, "High")]

    # Risk level inference — pending inferred risk levels awaiting user approval
    pending_risk_level_inferences: Optional[Dict] = None   # {control_id: {inferred_level, source, confidence, ...}}
    risk_level_inference_excel_path: Optional[str] = None   # Path to exported editable Excel
    risk_computation_log: Optional[List] = None             # [{control_id, prob, impact, score, rating, timestamp}]

    # RAG (handbook / reference document) state
    rag_project_id: Optional[str] = None       # Cosmos partition key for RAG chunks
    rag_document_name: Optional[str] = None    # Name of the indexed document (e.g. "ICOFAR_Handbook.pdf")
    rag_chunk_count: Optional[int] = None      # Number of indexed chunks

    # Session tracking
    tool_call_count: int = 0
    python_exec_count: int = 0
    version_count: int = 0
    last_save_path: Optional[str] = None
    artifacts: List[str] = field(default_factory=list)

    def reset(self) -> None:
        """Clear all state for a new session."""
        self.rcm_df = None
        self.original_rcm_df = None
        self.output_dir = None
        self.suggestions_cache = None
        self.dedup_cache = None
        self.tod_results = None
        self.toe_results = None
        self.sampling_results = None
        self.tod_evidence_folder = None
        self.tod_schemas = None
        self.pending_tod_schemas = None
        self.pending_tod_evidence_folder = None
        self.pending_toe_schemas = None
        self.pending_toe_evidence_folder = None
        self.document_list_rows = None
        self.evidence_validated = False
        self.evidence_extract_cache = None
        self.plan_scratchpad = None
        self.tool_results_cache.clear()
        self.scoping_engine = None
        self.scoping_trial_balance_path = None
        self.scoping_phase = "none"
        self.scoping_benchmark = None
        self.scoping_materiality_pct = None
        self.scoping_sop_paths = None
        self.scoping_quantitative_results = None
        self.scoping_awaiting_input = False
        self.scoping_progress = None
        self.tool_progress = None
        self.last_override = None
        self.pending_frequency_inferences = None
        self.frequency_inference_excel_path = None
        self.comparison_results = None
        self.risk_level_overrides = None
        self.custom_risk_severity_rank = None
        self.custom_risk_rank_to_label = None
        self.custom_risk_score_map = None
        self.custom_risk_bands = None
        self.pending_risk_level_inferences = None
        self.risk_level_inference_excel_path = None
        self.risk_computation_log = None
        # RAG state is intentionally NOT reset — indexed docs persist across sessions
        # self.rag_project_id = None
        # self.rag_document_name = None
        # self.rag_chunk_count = None
        self.tool_call_count = 0
        self.python_exec_count = 0
        self.version_count = 0
        self.last_save_path = None
        self.artifacts.clear()
