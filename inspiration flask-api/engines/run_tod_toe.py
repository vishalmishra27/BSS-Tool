"""
TOD / TOE Runner
==========================================
Credentials are loaded from config.py (edit once, used everywhere).
Set your paths below and run:
    python run_tod_toe.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# ── Load centralised config (API keys) ──
import config


# ═══════════════════════════════════════════════════════════════════════════════
#  RUN SETTINGS (credentials come from config.py)
# ═══════════════════════════════════════════════════════════════════════════════

# ── What to run ──
RUN_TOD = True                                  # run Test of Design
RUN_TOE = True                                  # run Test of Operating Effectiveness

# ── Input paths ──
RCM_PATH          = "RCM_With_Samples.xlsx"     # path to your RCM file
RCM_SHEET_NAME    = None                        # sheet name (None = first sheet)
TOD_EVIDENCE_PATH = "evidence_tod"              # folder: evidence_tod/<control_id>/files...
TOE_EVIDENCE_PATH = "evidence_toe"              # folder: evidence_toe/<control_id>/sample_N/files...

# ── Output paths ──
TOD_WORKPAPER_PATH = "tod_workpaper.xlsx"
TOD_REPORT_PATH    = "tod_report.txt"
TOE_WORKPAPER_PATH = "toe_workpaper.xlsx"
TOE_REPORT_PATH    = "toe_report.txt"

# ── Workpaper metadata ──
COMPANY_NAME = ""                               # e.g. "Acme Corporation"
PREPARED_BY  = ""                               # e.g. "Nisarg Thakkar"
REVIEWED_BY  = ""                               # e.g. "Nimisha Jain"

# ── Performance ──
MAX_WORKERS = 5                                 # parallel API calls

# ── Import engines (after config is loaded) ──
_engine_dir = str(Path(__file__).resolve().parent)
_flask_dir  = str(Path(__file__).resolve().parent.parent)
for _p in (_engine_dir, _flask_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from TOD_Engine import (
    RCMControlTester,
    load_tod_evidence_folder,
    load_toe_evidence_folder,
)


# ═══════════════════════════════════════════════════════════════════════════════
#  RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def _validate_config():
    errors = []
    if not config.AZURE_OPENAI_ENDPOINT:
        errors.append("AZURE_OPENAI_ENDPOINT is empty in config.py")
    if not config.AZURE_OPENAI_API_KEY:
        errors.append("AZURE_OPENAI_API_KEY is empty in config.py")
    if not config.DI_KEY:
        errors.append("DI_KEY is empty in config.py")
    if not config.DI_ENDPOINT:
        errors.append("DI_ENDPOINT is empty in config.py")
    if not RCM_PATH or not Path(RCM_PATH).exists():
        errors.append(f"RCM file not found: {RCM_PATH}")
    if RUN_TOD and not Path(TOD_EVIDENCE_PATH).exists():
        errors.append(f"TOD evidence folder not found: {TOD_EVIDENCE_PATH}")
    if RUN_TOE and not Path(TOE_EVIDENCE_PATH).exists():
        errors.append(f"TOE evidence folder not found: {TOE_EVIDENCE_PATH}")
    return errors


def run():
    print("=" * 60)
    print(" TOD / TOE Runner")
    print("=" * 60)

    # ── Validate ──
    errors = _validate_config()
    if errors:
        print("\nConfiguration errors:")
        for e in errors:
            print(f"  - {e}")
        print("\nEdit engines/config.py and try again.")
        sys.exit(1)

    print(f"\n  OpenAI endpoint:  {config.AZURE_OPENAI_ENDPOINT}")
    print(f"  OpenAI model:     {config.AZURE_OPENAI_DEPLOYMENT}")
    print(f"  Doc Intelligence: {config.DI_ENDPOINT}")
    print(f"  RCM file:         {RCM_PATH}")
    if RUN_TOD: print(f"  TOD evidence:     {TOD_EVIDENCE_PATH}")
    if RUN_TOE: print(f"  TOE evidence:     {TOE_EVIDENCE_PATH}")
    print(f"  Max workers:      {MAX_WORKERS}")
    print()

    # ── Initialize tester ──
    tester = RCMControlTester(
        rcm_path=RCM_PATH,
        sheet_name=RCM_SHEET_NAME,
        openai_api_key=config.AZURE_OPENAI_API_KEY,
        openai_model=config.AZURE_OPENAI_DEPLOYMENT,
        azure_endpoint=config.AZURE_OPENAI_ENDPOINT,
        azure_api_key=config.AZURE_OPENAI_API_KEY,
        azure_deployment=config.AZURE_OPENAI_DEPLOYMENT,
        azure_api_version=config.AZURE_OPENAI_API_VERSION,
    )

    tod_schemas = None
    tod_results = None

    # ══════════════════════════════════════════════════════════════
    #  TOD
    # ══════════════════════════════════════════════════════════════
    if RUN_TOD:
        print("\n" + "=" * 60)
        print(" RUNNING TOD (Test of Design)")
        print("=" * 60 + "\n")

        tod_bank = load_tod_evidence_folder(TOD_EVIDENCE_PATH)
        print(f"  Loaded {sum(len(v) for v in tod_bank.values())} samples across {len(tod_bank)} controls\n")

        tod_results, tod_schemas = tester.test_all_tod(tod_bank, max_workers=MAX_WORKERS)

        # Export
        tester.export_tod_workpaper(
            tod_results, TOD_WORKPAPER_PATH, tod_bank=tod_bank,
            company_name=COMPANY_NAME, prepared_by=PREPARED_BY, reviewed_by=REVIEWED_BY,
        )
        print(f"\n  Workpaper saved to: {TOD_WORKPAPER_PATH}")

        # Summary
        print(f"\n  TOD Summary:")
        print(f"  {'Control ID':>11s} {'Result':<20s} {'Design Effectiveness'}")
        print(f"  {'-' * 11} {'-' * 20} {'-' * 25}")
        for r in tod_results:
            print(f"  {r.control_id:>11s} {r.design_assessment:<20s} {r.design_effectiveness}")

    # ══════════════════════════════════════════════════════════════
    #  TOE
    # ══════════════════════════════════════════════════════════════
    if RUN_TOE:
        print("\n" + "=" * 60)
        print(" RUNNING TOE (Test of Operating Effectiveness)")
        print("=" * 60 + "\n")

        include_control_ids = None
        if tod_results is not None:
            include_control_ids = {
                r.control_id for r in tod_results
                if getattr(r, "result", None) == "PASS"
            }
            print(f"  Parsing TOE evidence for TOD PASS controls only: {len(include_control_ids)}")

        toe_bank = load_toe_evidence_folder(
            TOE_EVIDENCE_PATH,
            include_control_ids=include_control_ids,
        )
        print(f"  Loaded {sum(len(v) for v in toe_bank.values())} samples across {len(toe_bank)} controls\n")

        # Reuse schemas from TOD if available (avoids redundant API calls)
        toe_results = tester.test_all_toe(
            toe_bank, max_workers=MAX_WORKERS,
            pre_schemas=tod_schemas,
            tod_results=tod_results,
        )

        # Export
        tester.export_toe_workpaper(
            toe_results, TOE_WORKPAPER_PATH, toe_bank=toe_bank,
            company_name=COMPANY_NAME, prepared_by=PREPARED_BY, reviewed_by=REVIEWED_BY,
        )
        print(f"\n  Workpaper saved to: {TOE_WORKPAPER_PATH}")

        # Report
        report = tester.generate_toe_report(toe_results)
        with open(TOE_REPORT_PATH, "w") as f:
            f.write(report)
        print(f"  Report saved to:   {TOE_REPORT_PATH}")

        # Summary
        print(f"\n  TOE Summary:")
        print(f"  {'Control ID':>11s} {'Samples':>8s} {'Passed':>7s} "
              f"{'Failed':>7s} {'Dev Rate':>9s} "
              f"{'Effectiveness':<28s} {'Deficiency Type'}")
        print(f"  {'-' * 11} {'-' * 8} {'-' * 7} {'-' * 7} {'-' * 9} {'-' * 28} {'-' * 15}")
        for r in toe_results:
            print(f"  {r.control_id:>11s} {r.total_samples:>8d} "
                  f"{r.passed_samples:>7d} {r.failed_samples:>7d} "
                  f"{r.deviation_rate:>8.1%} "
                  f"{r.operating_effectiveness:<28s} {r.deficiency_type}")

    print("\n" + "=" * 60)
    print(" DONE")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    run()
