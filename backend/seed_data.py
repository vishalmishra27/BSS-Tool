"""
seed_data.py — Seeds bss_tool Postgres tables with data reconstructed from the
BSS Excel workbook (checklist / phases / products / project_activities / testcases
tabs). Run once after schema creation to populate the dashboard with real-looking
data instead of empty tables.

Usage: python seed_data.py
"""
import os
import random
from datetime import date, timedelta
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'bss_tool'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
}

LOBS = ['Prepaid B2C', 'Prepaid B2B', 'Postpaid B2C', 'Postpaid B2B', 'ICT']

PHASE_NAMES = {
    'phase1': 'Initiation & Planning',
    'phase2': 'SRS Finalization',
    'phase3': 'Product Rationalization',
    'phase4': 'Configuration Validation',
    'phase5': 'Data Cleanup & Preparation',
    'phase6': 'UAT Execution',
    'phase7': 'Trial / Dry Run Migrations',
    'phase8': 'Final Migration & Cutover',
    'phase9': 'Post-Migration Stabilization',
}

# status + start/end dates as seen in the "phases" screenshot (30-10-202x end dates,
# Assigned to "Bhanu Pra...", Reporting "Prithvi Raj", wf_id = 1)
PHASE_STATUS = {
    'phase1': 'complete', 'phase2': 'complete', 'phase3': 'complete',
    'phase4': 'complete', 'phase5': 'complete', 'phase6': 'complete',
    'phase7': 'pending', 'phase8': 'pending', 'phase9': 'pending',
}

CHECKLIST_ITEMS = {
    'phase1': [
        'Establish governance structure and key stakeholders',
        'Assign roles, responsibilities, and tool access',
        'Set project timelines and cutover windows',
    ],
    'phase2': [
        'Align SRS with business and functional objectives',
        'Map traceability matrix for all requirements',
        'Document system integration points and exception flows',
        'Approve readiness checklist for build/UAT',
        'Conduct stakeholder walkthroughs and sign-offs',
    ],
    'phase3': [
        'Analyze current product portfolio (B2B/B2C)',
        'Map legacy products to rationalized target products',
        'Define customer transition rules',
        'Approve final rationalized catalog',
    ],
    'phase4': [
        'Validate product configurations (plans, benefits, charges)',
        'Compare parameters across legacy and target systems',
        'Reconcile product setup changes (LoB, bundles)',
        'Test lifecycle scenarios (activation to termination)',
        'Approve configuration sign-off',
    ],
    'phase5': [
        'Identify and purge inactive or duplicate accounts',
        'Validate reference data and lookup tables',
        'Sign-off on cleaned and approved dataset',
    ],
    'phase6': [
        'Execute end-to-end test cases across business journeys',
        'Validate bill formats, tax, usage, and notifications',
        'Track and close UAT defects',
        'Reconcile outputs against expected values',
        'Obtain formal UAT sign-off',
        'Load sample or full-size data into target environment',
        'Perform batch runs and monitor processing issues',
    ],
    'phase7': [
        'Validate post-load data accuracy',
        'Reconcile key KPIs (customer count, balance, status)',
        'Review trial run report with business',
    ],
    'phase8': [
        'Freeze final cutover plan and timings',
        'Execute data extraction, transformation, and loading',
        'Perform reconciliation: legacy vs target',
        'Run critical workflows and validate outputs',
        'Approve go-live readiness report',
    ],
    'phase9': [
        'Monitor system performance and batch jobs',
        'Capture and close post-migration fallout/issues',
        'Reconcile CRM, order management, and charging',
        'Validate first bill run (usage, rental, discounts)',
        'Confirm end-of-stabilization and project closure',
    ],
}

PRODUCT_PREFIXES = {
    'Prepaid B2C': ['TOPUP', 'BUNDLE', 'VOICE', 'DATA'],
    'Prepaid B2B': ['CORP_TOPUP', 'CORP_BUNDLE', 'FLEET'],
    'Postpaid B2C': ['PLAN', 'FAMILY_PLAN', 'ROAMING'],
    'Postpaid B2B': ['ENTERPRISE_PLAN', 'APN', 'LEASED_LINE'],
    'ICT': ['CLOUD', 'FOPOINT', 'INTERCO', 'ACCDEDIE', 'BLR_POIN', 'FRAIS', 'NPP_SRS', 'CISCO'],
}

ASSIGNEES = ['Avni Malvi', 'Sakshi Chandra', 'Bhanu Pratap']
REPORTERS = ['Bhanu Pratap', 'Prithvi Raj']

TEST_TITLES = [
    'Create bug for failed activation flow',
    'Create feature for new plan enrollment',
    'Create enhancement for billing accuracy',
    'Validate system integration response',
    'Verify document upload and parsing',
    'Confirm user access provisioning',
]


def get_db():
    return psycopg2.connect(**DB_CONFIG)


def main():
    conn = get_db()
    cur = conn.cursor()
    rng = random.Random(42)

    # ── phases ──────────────────────────────────────────────────────────────
    print("Seeding phases...")
    cur.execute("DELETE FROM phases")
    base_end = date(2025, 10, 30)
    for i, (pid, status) in enumerate(PHASE_STATUS.items()):
        start = base_end - timedelta(days=(9 - i) * 14)
        cur.execute(
            """INSERT INTO phases (id, phase_id, curr_status, start_dt, end_dt, lob, assigned_to)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (pid, pid, status, start.isoformat(), base_end.isoformat(), 'All LOBs', 'Bhanu Pratap')
        )

    # ── checklist ───────────────────────────────────────────────────────────
    print("Seeding checklist...")
    cur.execute("DELETE FROM checklist_comments")
    cur.execute("DELETE FROM checklist_attachments")
    cur.execute("DELETE FROM checklist")
    ch_id = 1
    for pid, items in CHECKLIST_ITEMS.items():
        phase_complete = PHASE_STATUS[pid] == 'complete'
        for item in items:
            status = 'complete' if phase_complete else rng.choice(['pending', 'in_progress'])
            cur.execute(
                """INSERT INTO checklist (ch_id, phase_id, wf_id, item_text, status)
                   VALUES (%s, %s, %s, %s, %s)""",
                (ch_id, pid, '1', item, status)
            )
            ch_id += 1

    # ── transformation_activities + transformation_lob_progress ───────────
    print("Seeding transformation_activities...")
    cur.execute("DELETE FROM transformation_activities")
    cur.execute("DELETE FROM transformation_lob_progress")
    planned_by_phase = {
        'Initiation & Planning': 80, 'SRS Finalization': 70, 'Product Rationalization': 60,
        'Configuration Validation': 85, 'Data Cleanup & Preparation': 60, 'UAT Execution': 90,
        'Trial / Dry Run Migrations': 75, 'Final Migration & Cutover': 50,
        'Post-Migration Stabilization': 30,
    }
    lob_totals = {lob: {'planned': 0, 'actual': 0, 'n': 0} for lob in LOBS}
    for phase_name, planned in planned_by_phase.items():
        for lob in LOBS:
            jitter = rng.randint(-8, 5)
            actual = max(0, min(100, planned + jitter))
            cur.execute(
                """INSERT INTO transformation_activities (lob, phase_name, planned, actual)
                   VALUES (%s, %s, %s, %s)""",
                (lob, phase_name, planned, actual)
            )
            lob_totals[lob]['planned'] += planned
            lob_totals[lob]['actual'] += actual
            lob_totals[lob]['n'] += 1
    for lob, t in lob_totals.items():
        cur.execute(
            "INSERT INTO transformation_lob_progress (lob, planned, actual) VALUES (%s, %s, %s)",
            (lob, round(t['planned'] / t['n']), round(t['actual'] / t['n']))
        )

    # ── products ────────────────────────────────────────────────────────────
    print("Seeding products...")
    cur.execute("DELETE FROM product_parameters")
    cur.execute("DELETE FROM products")
    pidx = 380
    for lob, prefixes in PRODUCT_PREFIXES.items():
        for prefix in prefixes:
            for n in range(1, 4):
                product_id = f"PO{pidx}MA"
                pidx += 1
                name = f"{prefix}_{n}"
                migration_flag = rng.choice(['migrate', 'migrate', 'migrate', 'purge'])
                status = rng.choice(['configured', 'pending', 'pending'])
                cur.execute(
                    """INSERT INTO products (product_id, product_name, lob, migration_flag, status)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (product_id, name, lob, migration_flag, status)
                )

    # ── uat_cases ───────────────────────────────────────────────────────────
    print("Seeding uat_cases...")
    cur.execute("DELETE FROM uat_cases")
    statuses = ['OPEN', 'CLOSED', 'IN_PROGRESS', 'REOPENED']
    priorities = ['High', 'Medium', 'Low']
    for i in range(1, 50):
        test_case_id = f"TTCRM-{rng.randint(30, 99)}"
        lob = rng.choice(LOBS)
        priority = rng.choice(priorities)
        status = rng.choice(statuses)
        desc = rng.choice(TEST_TITLES)
        cur.execute(
            """INSERT INTO uat_cases (test_case_id, lob, priority, status, description)
               VALUES (%s, %s, %s, %s, %s) ON CONFLICT (test_case_id) DO NOTHING""",
            (f"{test_case_id}-{i}", lob, priority, status, desc)
        )

    # ── main_workflow / stages (single workflow tying phases together) ────
    print("Seeding main_workflow / stages...")
    cur.execute("DELETE FROM stages")
    cur.execute("DELETE FROM main_workflow")
    cur.execute(
        "INSERT INTO main_workflow (id, name, description, status) VALUES (%s, %s, %s, %s)",
        ('1', 'BSS Migration Programme', 'End-to-end BSS migration workflow', 'in_progress')
    )
    for i, (pid, name) in enumerate(PHASE_NAMES.items()):
        cur.execute(
            """INSERT INTO stages (id, main_wf_id, name, status, order_idx)
               VALUES (%s, %s, %s, %s, %s)""",
            (pid, '1', name, PHASE_STATUS[pid], i)
        )

    conn.commit()
    cur.close()
    conn.close()
    print("Done.")


if __name__ == '__main__':
    main()
