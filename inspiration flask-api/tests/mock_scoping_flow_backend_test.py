from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import patch

from agent.tools.sox_scoping_engine import SoxScopingEngineTool
from agent.types import AgentState


@dataclass
class FakeConfig:
    api_key: str
    azure_endpoint: str
    azure_api_version: str
    model: str
    embedding_model: str
    vision_model: str
    trial_balance_path: str
    sop_paths: list
    output_path: str
    benchmark: str
    materiality_pct: float
    top_k_chunks: int


class _FA:
    def __init__(self, scope: str):
        self.scope = scope


class FakeEngine:
    def __init__(self, cfg: FakeConfig):
        self.config = cfg
        self.accounts = []
        self.fs_accounts = [_FA("In-scope"), _FA("Out-of-scope")]
        self.process_map = {"Cash": ["Treasury / Cash Management"]}
        self.process_sop_coverage = {"Treasury / Cash Management": {"score": "Strong"}}
        self.called: List[str] = []

    def ingest(self, path: str):
        self.called.append("ingest")
        self.accounts = [{"raw_name": "Cash", "balance": 1000.0}, {"raw_name": "AP", "balance": -500.0}]

    def categorize(self):
        self.called.append("categorize")

    def set_materiality(self, benchmark: str, pct: float):
        self.called.append("set_materiality")

    def compute_threshold(self):
        self.called.append("compute_threshold")

    def run_quantitative(self):
        self.called.append("run_quantitative")

    def run_qualitative(self):
        self.called.append("run_qualitative")

    def run_scoping(self):
        self.called.append("run_scoping")

    def map_to_processes(self):
        self.called.append("map_to_processes")

    def ingest_sops(self, valid_sops: list):
        self.called.append("ingest_sops")

    def _validate_process_sop_coverage(self):
        self.called.append("_validate_process_sop_coverage")

    def extract_and_map(self):
        self.called.append("extract_and_map")

    def _completeness_review(self):
        self.called.append("_completeness_review")

    def export(self, output_path: str):
        self.called.append("export")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("fake excel content")


def run_flow() -> Dict[str, Any]:
    tool = SoxScopingEngineTool()
    state = AgentState()

    with tempfile.TemporaryDirectory() as td:
        tb_path = os.path.join(td, "trial_balance.csv")
        sop_path = os.path.join(td, "SOP_P2P.pdf")
        with open(tb_path, "w", encoding="utf-8") as f:
            f.write("Account,Balance\nCash,1000\nAP,-500\n")
        with open(sop_path, "w", encoding="utf-8") as f:
            f.write("fake sop")

        fake_module = SimpleNamespace(
            Config=FakeConfig,
            SOXScopingEngine=FakeEngine,
            MAX_WORKERS=5,
        )

        with patch("agent.tools.sox_scoping_engine._load_external_engine_module", return_value=fake_module):
            # 1) trial balance only -> benchmark gate
            r1 = tool.execute({"trial_balance_path": tb_path}, state)

            # 2) benchmark only -> materiality % gate
            r2 = tool.execute({"trial_balance_path": tb_path, "benchmark": "Revenue"}, state)

            # 3) benchmark + materiality but no SOP (run_downstream default True) -> SOP gate
            r3 = tool.execute(
                {
                    "trial_balance_path": tb_path,
                    "benchmark": "Revenue",
                    "materiality_pct": 1.5,
                },
                state,
            )

            # 4) invalid SOP path -> invalid SOP gate
            r4 = tool.execute(
                {
                    "trial_balance_path": tb_path,
                    "benchmark": "Revenue",
                    "materiality_pct": 1.5,
                    "sop_paths": [os.path.join(td, "missing.pdf")],
                },
                state,
            )

            # 5) valid SOP path -> success
            r5 = tool.execute(
                {
                    "trial_balance_path": tb_path,
                    "benchmark": "Revenue",
                    "materiality_pct": 1.5,
                    "sop_paths": [sop_path],
                },
                state,
            )

            # 6) optional branch: continue without SOP when run_downstream=False -> success
            r6 = tool.execute(
                {
                    "trial_balance_path": tb_path,
                    "benchmark": "Revenue",
                    "materiality_pct": 1.5,
                    "run_downstream": False,
                },
                state,
            )

    def _view(res):
        return {
            "success": res.success,
            "error": res.error,
            "status": res.data.get("status") if isinstance(res.data, dict) else None,
            "step": res.data.get("step") if isinstance(res.data, dict) else None,
            "question": res.data.get("question") if isinstance(res.data, dict) else None,
            "output_excel": res.data.get("output_excel") if isinstance(res.data, dict) else None,
        }

    return {
        "cases": {
            "1_tb_only": _view(r1),
            "2_benchmark_only": _view(r2),
            "3_missing_sops": _view(r3),
            "4_invalid_sops": _view(r4),
            "5_valid_sops": _view(r5),
            "6_no_sops_run_downstream_false": _view(r6),
        },
        "expected_sequence": [
            "materiality_benchmark",
            "materiality_percentage",
            "sop_paths",
            "sop_paths",
            "success",
            "success",
        ],
    }


if __name__ == "__main__":
    result = run_flow()
    print(json.dumps(result, indent=2))
