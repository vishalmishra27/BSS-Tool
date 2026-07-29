"""
Seed realistic demo data for BPM, UAT, and Product dashboards.
Run while backend is up: python3 seed_demo_data.py
"""
import requests, json, sys

BASE = "http://localhost:5001"

# ── Login ────────────────────────────────────────────────────────────────────
r = requests.post(f"{BASE}/api/auth/login", json={"username": "prog_director", "password": "kpmg1234"})
if r.status_code != 200:
    print(f"Login failed: {r.text}")
    sys.exit(1)
TOKEN = r.json()["token"]
H = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def api(method, path, data=None):
    fn = getattr(requests, method)
    r = fn(f"{BASE}/api/bpm{path}", headers=H, json=data)
    if r.status_code >= 400:
        print(f"  WARN {method.upper()} {path}: {r.status_code} {r.text[:120]}")
    return r.json() if r.status_code < 400 else None

# ── 1. Create BPM Users ─────────────────────────────────────────────────────
print("=== Creating BPM Users ===")
users = [
    {"username": "sarah.mitchell", "password": "kpmg1234", "full_name": "Sarah Mitchell",
     "email": "sarah.mitchell@kpmg.com", "role": "engagement_manager", "organisation": "KPMG Advisory"},
    {"username": "david.chen", "password": "kpmg1234", "full_name": "David Chen",
     "email": "david.chen@kpmg.com", "role": "engagement_manager", "organisation": "KPMG Advisory"},
    {"username": "priya.sharma", "password": "kpmg1234", "full_name": "Priya Sharma",
     "email": "priya.sharma@kpmg.com", "role": "bss_consultant", "organisation": "KPMG Advisory"},
    {"username": "michael.adeyemi", "password": "kpmg1234", "full_name": "Michael Adeyemi",
     "email": "michael.adeyemi@kpmg.com", "role": "bss_consultant", "organisation": "KPMG Advisory"},
    {"username": "emma.wright", "password": "kpmg1234", "full_name": "Emma Wright",
     "email": "emma.wright@kpmg.com", "role": "qa_manager", "organisation": "KPMG Advisory"},
    {"username": "raj.patel", "password": "kpmg1234", "full_name": "Raj Patel",
     "email": "raj.patel@kpmg.com", "role": "data_analyst", "organisation": "KPMG Advisory"},
    {"username": "catherine.dubois", "password": "kpmg1234", "full_name": "Catherine Dubois",
     "email": "catherine.dubois@client.com", "role": "client_sponsor", "organisation": "Telecom Corp"},
    {"username": "thomas.nkosi", "password": "kpmg1234", "full_name": "Thomas Nkosi",
     "email": "thomas.nkosi@client.com", "role": "client_it_lead", "organisation": "Telecom Corp"},
]

user_ids = {}
for u in users:
    res = api("post", "/admin/users", u)
    if res:
        user_ids[u["username"]] = res["id"]
        print(f"  + {u['full_name']} ({u['role']})")
    else:
        # Try to get existing user id
        all_users = api("get", "/admin/users")
        if all_users:
            for au in all_users:
                if au["username"] == u["username"]:
                    user_ids[u["username"]] = au["id"]
                    print(f"  ~ {u['full_name']} already exists (id={au['id']})")
                    break

# Also get the prog_director's own user id
all_u = api("get", "/admin/users")
if all_u:
    for au in all_u:
        if au["username"] not in user_ids:
            user_ids[au["username"]] = au["id"]

# Get manager IDs for assignment
sarah_id = user_ids.get("sarah.mitchell")
david_id = user_ids.get("david.chen")
priya_id = user_ids.get("priya.sharma")
michael_id = user_ids.get("michael.adeyemi")
emma_id = user_ids.get("emma.wright")
raj_id = user_ids.get("raj.patel")

# ── 2. Create BPM Projects ──────────────────────────────────────────────────
print("\n=== Creating BPM Projects ===")
projects = [
    {"name": "BSS Migration — Core Billing Platform",
     "start_date": "2026-01-15", "end_date": "2026-09-30",
     "owner_manager_id": sarah_id},
    {"name": "Network Modernization — 5G Rollout Phase 2",
     "start_date": "2026-03-01", "end_date": "2026-12-15",
     "owner_manager_id": david_id},
    {"name": "Data Reconciliation & Cleanup Sprint",
     "start_date": "2026-05-01", "end_date": "2026-08-31",
     "owner_manager_id": sarah_id},
    {"name": "UAT Automation Framework Setup",
     "start_date": "2026-04-01", "end_date": "2026-07-30",
     "owner_manager_id": david_id},
]

project_ids = []
for p in projects:
    res = api("post", "/projects", p)
    if res:
        project_ids.append(res["id"])
        print(f"  + [{res['code']}] {p['name']}")
    else:
        project_ids.append(None)

# ── 3. Create BPM Tasks ─────────────────────────────────────────────────────
print("\n=== Creating BPM Tasks ===")
tasks = [
    # Project 1 — BSS Migration
    {"project_id": project_ids[0], "title": "Complete CBS-to-CLM data mapping",
     "description": "Map all 2,400 product codes from CBS to CLM service catalog. Include parameter-level mapping for billing attributes.",
     "assignee_id": priya_id, "status": "done", "start_date": "2026-01-20", "end_date": "2026-03-15"},
    {"project_id": project_ids[0], "title": "Configure rating & charging rules",
     "description": "Set up 156 rating rules for prepaid B2B/B2C bundles in the new billing engine.",
     "assignee_id": michael_id, "status": "done", "start_date": "2026-02-01", "end_date": "2026-04-10"},
    {"project_id": project_ids[0], "title": "Migrate subscriber master data (Batch 1 — 2M records)",
     "description": "Execute dry-run migration for prepaid B2C subscribers. Validate name, MSISDN, balance, and plan linkage.",
     "assignee_id": priya_id, "status": "in_progress", "start_date": "2026-04-15", "end_date": "2026-06-30"},
    {"project_id": project_ids[0], "title": "Post-migration billing reconciliation",
     "description": "Run CBS vs CLM billing reconciliation for migrated cohort. Target: <0.1% variance on invoice totals.",
     "assignee_id": raj_id, "status": "in_progress", "start_date": "2026-05-01", "end_date": "2026-07-31"},
    {"project_id": project_ids[0], "title": "Executive sign-off: Go/No-Go for cutover",
     "description": "Present migration readiness scorecard to CTO and Client Sponsor. Requires 98%+ reconciliation match rate.",
     "assignee_id": emma_id, "status": "todo", "start_date": "2026-08-01", "end_date": "2026-08-15"},
    {"project_id": project_ids[0], "title": "Weekly KPI reporting to Steering Committee",
     "description": "Compile and distribute weekly programme KPIs: migration progress, defect count, SLA adherence.",
     "assignee_id": raj_id, "status": "in_progress", "start_date": "2026-02-01", "end_date": "2026-09-30",
     "recurrence_type": "weekly", "recurrence_days": "mon"},

    # Project 2 — Network Modernization
    {"project_id": project_ids[1], "title": "Network topology audit — legacy nodes",
     "description": "Inventory all legacy PSTN/2G nodes across 14 regions. Document interconnect dependencies.",
     "assignee_id": michael_id, "status": "done", "start_date": "2026-03-01", "end_date": "2026-04-30"},
    {"project_id": project_ids[1], "title": "5G core configuration validation",
     "description": "Validate AMF, SMF, UPF parameters against vendor baseline. 340 config parameters across 12 sites.",
     "assignee_id": priya_id, "status": "in_progress", "start_date": "2026-05-01", "end_date": "2026-08-15"},
    {"project_id": project_ids[1], "title": "Integration testing — 5G ↔ BSS interfaces",
     "description": "End-to-end test: subscriber provisioning, real-time charging, and PCRF policy enforcement on 5G core.",
     "assignee_id": emma_id, "status": "todo", "start_date": "2026-07-01", "end_date": "2026-09-30"},
    {"project_id": project_ids[1], "title": "Daily health-check report",
     "description": "Automated daily summary: node uptime, alarm counts, interface latency stats.",
     "assignee_id": raj_id, "status": "in_progress", "start_date": "2026-05-15", "end_date": "2026-12-15",
     "recurrence_type": "daily"},

    # Project 3 — Data Reconciliation
    {"project_id": project_ids[2], "title": "CBS vs OCS balance reconciliation",
     "description": "Reconcile prepaid wallet balances between CBS and OCS for 4.2M subscribers. Flag discrepancies > $0.50.",
     "assignee_id": raj_id, "status": "done", "start_date": "2026-05-01", "end_date": "2026-06-15"},
    {"project_id": project_ids[2], "title": "Duplicate subscriber cleansing",
     "description": "Identify and merge 23,000+ duplicate subscriber records using fuzzy matching on name + MSISDN.",
     "assignee_id": priya_id, "status": "in_progress", "start_date": "2026-06-01", "end_date": "2026-07-31"},
    {"project_id": project_ids[2], "title": "Data quality scorecard — final assessment",
     "description": "Publish final data quality metrics: completeness, accuracy, consistency across all migrated entities.",
     "assignee_id": raj_id, "status": "todo", "start_date": "2026-08-01", "end_date": "2026-08-31"},

    # Project 4 — UAT Automation
    {"project_id": project_ids[3], "title": "Playwright test harness setup",
     "description": "Configure Playwright runner with headless Chromium, screenshot capture, and Excel test-case ingestion.",
     "assignee_id": emma_id, "status": "done", "start_date": "2026-04-01", "end_date": "2026-05-15"},
    {"project_id": project_ids[3], "title": "Write regression suite — 120 test cases",
     "description": "Author automated test cases covering: login, provisioning, billing, payment, and self-care portal flows.",
     "assignee_id": emma_id, "status": "in_progress", "start_date": "2026-05-15", "end_date": "2026-07-15"},
    {"project_id": project_ids[3], "title": "CI/CD pipeline integration",
     "description": "Integrate UAT automation into GitLab CI. Tests trigger on every staging deploy. Results feed UAT Dashboard.",
     "assignee_id": michael_id, "status": "todo", "start_date": "2026-07-01", "end_date": "2026-07-30"},
]

for t in tasks:
    if t["project_id"] is None:
        continue
    res = api("post", "/tasks", t)
    if res:
        print(f"  + [{res['code']}] {t['title'][:60]}... ({t['status']})")

# ── 4. Seed UAT test cases ───────────────────────────────────────────────────
print("\n=== Seeding UAT Test Cases ===")
import psycopg2, psycopg2.extras, os

DB = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'bss_tool'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
}

conn = psycopg2.connect(**DB)
conn.autocommit = True
cur = conn.cursor()

# Check if uat_test_cases table exists
cur.execute("SELECT to_regclass('public.uat_test_cases')")
if cur.fetchone()[0] is None:
    print("  uat_test_cases table does not exist, skipping UAT seed")
else:
    # Check current count
    cur.execute("SELECT COUNT(*) FROM uat_test_cases")
    existing = cur.fetchone()[0]
    if existing < 15:
        uat_cases = [
            ("Subscriber Activation — Prepaid B2C", "Broadband", "High", "Closed"),
            ("Subscriber Activation — Postpaid B2B", "Digital Services", "High", "Closed"),
            ("Bundle Purchase — Data + Voice Combo", "ICT", "High", "Closed"),
            ("Top-Up via USSD — Prepaid Wallet", "Broadband", "High", "Closed"),
            ("Invoice Generation — Postpaid Monthly Cycle", "Digital Services", "High", "Open"),
            ("Payment Processing — Mobile Money", "ICT", "Medium", "Open"),
            ("Self-Care Portal — Plan Change", "Broadband", "Medium", "Closed"),
            ("IVR Balance Enquiry — Real-Time Rating", "Digital Services", "Medium", "Closed"),
            ("Roaming Activation — Partner Network", "ICT", "Medium", "Open"),
            ("Credit Limit Breach — Barring Trigger", "Broadband", "High", "Closed"),
            ("Loyalty Points Redemption", "Digital Services", "Low", "Open"),
            ("Number Portability — MNP Import", "ICT", "High", "Closed"),
            ("Family Plan — Shared Data Pool", "Broadband", "Medium", "Open"),
            ("Enterprise SLA Reporting", "Digital Services", "Medium", "Closed"),
            ("Bulk SMS Provisioning — B2B", "ICT", "Low", "Closed"),
            ("Dunning Process — Payment Reminder", "Broadband", "High", "Closed"),
            ("VAS Subscription — Music Streaming", "Digital Services", "Low", "Open"),
            ("PCRF Policy Enforcement — Fair Usage", "ICT", "High", "Closed"),
            ("Customer Migration — Legacy to New BSS", "Broadband", "High", "Open"),
            ("Interconnect Settlement — CDR Matching", "Digital Services", "High", "Closed"),
            ("Bill Dispute Resolution Workflow", "ICT", "Medium", "Closed"),
            ("Emergency Credit Activation", "Broadband", "Medium", "Open"),
            ("SIM Swap — Identity Verification", "Digital Services", "High", "Closed"),
            ("Data Rollover — Month-End Carry Forward", "ICT", "Medium", "Closed"),
            ("Dealer Commission Calculation", "Broadband", "Low", "Open"),
        ]
        # Clear old minimal data and insert fresh
        cur.execute("DELETE FROM uat_test_cases")
        for tc in uat_cases:
            cur.execute(
                "INSERT INTO uat_test_cases (test_case_name, lob, priority, status) VALUES (%s, %s, %s, %s)",
                tc
            )
        print(f"  + Inserted {len(uat_cases)} UAT test cases")
    else:
        print(f"  ~ {existing} test cases already exist, skipping")

# ── 5. Seed Product data ─────────────────────────────────────────────────────
print("\n=== Seeding Product Data ===")
cur.execute("SELECT to_regclass('public.products')")
if cur.fetchone()[0] is None:
    print("  products table does not exist, skipping Product seed")
else:
    cur.execute("SELECT COUNT(*) FROM products")
    existing = cur.fetchone()[0]
    if existing < 10:
        products = [
            ("Unlimited Data 5G — Consumer", "Broadband", "migrate", "matched", 81),
            ("Voice & Data Bundle — Prepaid", "Broadband", "migrate", "matched", 92),
            ("Enterprise SIP Trunk — 100 Lines", "ICT", "migrate", "matched", 75),
            ("IoT Connectivity — M2M SIM", "Digital Services", "migrate", "in_progress", 60),
            ("Roaming Pack — Africa", "Broadband", "migrate", "matched", 88),
            ("Mobile Money Wallet — Standard", "Digital Services", "migrate", "in_progress", 45),
            ("Legacy ISDN BRI — Residential", "ICT", "purge", "matched", 100),
            ("2G Voice Only — Legacy Prepaid", "Broadband", "purge", "matched", 100),
            ("Dial-Up Internet — Phase Out", "Digital Services", "purge", "matched", 100),
            ("Fixed Wireless — Rural", "ICT", "migrate", "matched", 70),
            ("Cloud PBX — SME Package", "Digital Services", "migrate", "in_progress", 55),
            ("SD-WAN Enterprise — Premium", "ICT", "migrate", "matched", 85),
            ("Youth Bundle — Social Media Pack", "Broadband", "migrate", "matched", 90),
            ("Corporate Fleet — Bulk Voice", "ICT", "migrate", "in_progress", 40),
            ("Premium Content — Video Streaming", "Digital Services", "migrate", "matched", 78),
        ]
        cur.execute("DELETE FROM products")
        for p in products:
            try:
                cur.execute(
                    "INSERT INTO products (product_name, lob, action, config_status, completion_pct) VALUES (%s, %s, %s, %s, %s)",
                    p
                )
            except Exception as e:
                # Schema might differ — try simpler insert
                try:
                    cur.execute(
                        "INSERT INTO products (product_name, lob, rationalization_action, configuration_status) VALUES (%s, %s, %s, %s)",
                        (p[0], p[1], p[2], p[3])
                    )
                except Exception:
                    print(f"  WARN: Could not insert product '{p[0]}': {e}")
        print(f"  + Inserted {len(products)} products")
    else:
        print(f"  ~ {existing} products already exist, skipping")

cur.close()
conn.close()
print("\nDone! Realistic demo data seeded.")
