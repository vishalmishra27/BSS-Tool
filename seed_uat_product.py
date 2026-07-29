"""
Seed realistic UAT and Product data matching the actual DB schema.
"""
import psycopg2, os

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

# ── 1. UAT Cases ─────────────────────────────────────────────────────────────
print("=== Seeding UAT Test Cases ===")
cur.execute("DELETE FROM uat_cases")

uat_cases = [
    ("TC-001", "Broadband",        "High",   "Closed",  "Subscriber Activation — Prepaid B2C with KYC validation"),
    ("TC-002", "Broadband",        "High",   "Closed",  "Top-Up via USSD — Prepaid wallet balance update"),
    ("TC-003", "Broadband",        "High",   "Closed",  "Bundle Purchase — Data + Voice combo pack activation"),
    ("TC-004", "Broadband",        "Medium", "Closed",  "Self-Care Portal — Plan upgrade with proration"),
    ("TC-005", "Broadband",        "High",   "Closed",  "Credit Limit Breach — Barring trigger and notification"),
    ("TC-006", "Broadband",        "Medium", "Open",    "Family Plan — Shared data pool provisioning"),
    ("TC-007", "Broadband",        "High",   "Open",    "Customer Migration — Legacy CBS to new BSS cutover"),
    ("TC-008", "Broadband",        "Low",    "Open",    "Dealer Commission — Monthly calculation and payout"),
    ("TC-009", "Digital Services", "High",   "Closed",  "Subscriber Activation — Postpaid B2B enterprise"),
    ("TC-010", "Digital Services", "High",   "Closed",  "Invoice Generation — Postpaid monthly billing cycle"),
    ("TC-011", "Digital Services", "Medium", "Closed",  "IVR Balance Enquiry — Real-time rating engine check"),
    ("TC-012", "Digital Services", "High",   "Closed",  "Interconnect Settlement — CDR matching and rating"),
    ("TC-013", "Digital Services", "Medium", "Closed",  "Enterprise SLA Reporting — Monthly KPI extraction"),
    ("TC-014", "Digital Services", "High",   "Open",    "Number Portability — MNP import with 4-hour SLA"),
    ("TC-015", "Digital Services", "Low",    "Open",    "Loyalty Points Redemption — Partner catalog sync"),
    ("TC-016", "Digital Services", "Medium", "Closed",  "SIM Swap — Identity verification workflow"),
    ("TC-017", "ICT",              "High",   "Closed",  "5G Core Provisioning — AMF/SMF/UPF subscriber flow"),
    ("TC-018", "ICT",              "High",   "Closed",  "Payment Processing — Mobile Money real-time credit"),
    ("TC-019", "ICT",              "Medium", "Closed",  "Roaming Activation — Partner network handshake"),
    ("TC-020", "ICT",              "High",   "Closed",  "PCRF Policy Enforcement — Fair Usage throttle"),
    ("TC-021", "ICT",              "Medium", "Open",    "VAS Subscription — Music streaming with trial period"),
    ("TC-022", "ICT",              "High",   "Closed",  "Bulk SMS Provisioning — B2B enterprise gateway"),
    ("TC-023", "ICT",              "Medium", "Closed",  "Data Rollover — Month-end carry forward calculation"),
    ("TC-024", "ICT",              "Low",    "Open",    "Emergency Credit Activation — Threshold trigger"),
    ("TC-025", "ICT",              "High",   "Closed",  "Dunning Process — 3-stage payment reminder workflow"),
    ("TC-026", "Broadband",        "High",   "Closed",  "Prepaid-to-Postpaid Migration — Balance transfer"),
    ("TC-027", "Digital Services", "Medium", "Defect",  "Bill Dispute Resolution — Ticket escalation loop"),
    ("TC-028", "ICT",              "High",   "Defect",  "CDR Mediation — Duplicate record filtering failure"),
    ("TC-029", "Broadband",        "High",   "Reopened","4G Data Throttle — Speed cap not enforcing at limit"),
    ("TC-030", "Digital Services", "Medium", "Open",    "Customer 360 View — Cross-system data aggregation"),
]

for tc in uat_cases:
    cur.execute(
        "INSERT INTO uat_cases (test_case_id, lob, priority, status, description) VALUES (%s, %s, %s, %s, %s)",
        tc
    )
print(f"  + Inserted {len(uat_cases)} UAT test cases")

# ── 2. Products ──────────────────────────────────────────────────────────────
print("\n=== Seeding Products ===")
cur.execute("DELETE FROM product_parameters")
cur.execute("DELETE FROM products")

products = [
    ("PRD-001", "Unlimited Data 5G — Consumer",        "Broadband",        "migrate"),
    ("PRD-002", "Voice & Data Bundle — Prepaid",        "Broadband",        "migrate"),
    ("PRD-003", "Family Plan — Shared 50GB",            "Broadband",        "migrate"),
    ("PRD-004", "Youth Bundle — Social Media Pack",     "Broadband",        "migrate"),
    ("PRD-005", "Prepaid-to-Postpaid Migration Offer",  "Broadband",        "migrate"),
    ("PRD-006", "Enterprise SIP Trunk — 100 Lines",     "ICT",              "migrate"),
    ("PRD-007", "SD-WAN Enterprise — Premium",          "ICT",              "migrate"),
    ("PRD-008", "IoT Connectivity — M2M SIM",           "ICT",              "migrate"),
    ("PRD-009", "Cloud PBX — SME Package",              "ICT",              "migrate"),
    ("PRD-010", "Fixed Wireless — Rural Broadband",     "ICT",              "migrate"),
    ("PRD-011", "Mobile Money Wallet — Standard",       "Digital Services",  "migrate"),
    ("PRD-012", "Premium Content — Video Streaming",    "Digital Services",  "migrate"),
    ("PRD-013", "Corporate Fleet — Bulk Voice",         "Digital Services",  "migrate"),
    ("PRD-014", "Legacy ISDN BRI — Residential",        "ICT",              "purge"),
    ("PRD-015", "2G Voice Only — Legacy Prepaid",       "Broadband",        "purge"),
    ("PRD-016", "Dial-Up Internet — Phase Out",         "Digital Services",  "purge"),
    ("PRD-017", "Pager Service — Discontinued",         "ICT",              "purge"),
]

for p in products:
    cur.execute(
        "INSERT INTO products (product_id, product_name, lob, migration_flag, status) VALUES (%s, %s, %s, %s, 'configured')",
        p
    )
print(f"  + Inserted {len(products)} products")

# ── 3. Product Parameters ───────────────────────────────────────────────────
print("\n=== Seeding Product Parameters ===")
param_names = [
    "Advance Rental", "Authority Matrix", "BS Type", "Benefits",
    "Billing ID", "Penalties", "Product Relationships Dependencies",
    "Provisioning Code", "Rentals", "Sellable Non Sellable",
]

import random
random.seed(42)

for p in products:
    pid = p[0]
    lob = p[2]
    is_purge = p[3] == "purge"
    for param in param_names:
        total = random.randint(60, 85)
        if is_purge:
            matched = total  # purge products fully matched
        else:
            matched = random.randint(int(total * 0.65), total)
        status = "good" if matched == total else ("in_progress" if matched > total * 0.7 else "pending")
        cur.execute(
            "INSERT INTO product_parameters (param_name, lob, product_id, status, matched, total) VALUES (%s, %s, %s, %s, %s, %s)",
            (param, lob, pid, status, matched, total)
        )

print(f"  + Inserted {len(products) * len(param_names)} product parameters")

cur.close()
conn.close()
print("\nDone! UAT + Product demo data seeded.")
