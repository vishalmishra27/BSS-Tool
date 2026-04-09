"""
seed_users.py — Creates the `users` table (if missing) and seeds demo accounts.

Run once from the backend directory:
    python seed_users.py

Demo credentials (all share the same password):
    Password: kpmg1234
"""

import os
import sys
import bcrypt
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'bss_tool'),
    'user':     os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'postgres'),
}

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      VARCHAR(80)  NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    full_name     VARCHAR(120) NOT NULL,
    email         VARCHAR(120),
    role          VARCHAR(60)  NOT NULL,
    organisation  VARCHAR(120) DEFAULT 'KPMG Advisory',
    is_active     BOOLEAN      DEFAULT TRUE,
    created_at    TIMESTAMPTZ  DEFAULT NOW(),
    last_login    TIMESTAMPTZ
);
"""

DEMO_USERS = [
    {
        'username':     'prog_director',
        'full_name':    'Programme Director',
        'email':        'prog.director@kpmg.com',
        'role':         'programme_director',
        'organisation': 'KPMG Advisory',
    },
    {
        'username':     'eng_manager',
        'full_name':    'Engagement Manager',
        'email':        'eng.manager@kpmg.com',
        'role':         'engagement_manager',
        'organisation': 'KPMG Advisory',
    },
    {
        'username':     'bss_consultant',
        'full_name':    'BSS Consultant',
        'email':        'bss.consultant@kpmg.com',
        'role':         'bss_consultant',
        'organisation': 'KPMG Advisory',
    },
    {
        'username':     'qa_manager',
        'full_name':    'QA / Test Manager',
        'email':        'qa.manager@kpmg.com',
        'role':         'qa_manager',
        'organisation': 'KPMG Advisory',
    },
    {
        'username':     'data_analyst',
        'full_name':    'Data Analyst',
        'email':        'data.analyst@kpmg.com',
        'role':         'data_analyst',
        'organisation': 'KPMG Advisory',
    },
    {
        'username':     'client_sponsor',
        'full_name':    'Client Programme Sponsor',
        'email':        'sponsor@client.com',
        'role':         'client_sponsor',
        'organisation': 'Client Organisation',
    },
    {
        'username':     'client_it',
        'full_name':    'Client IT Lead',
        'email':        'it.lead@client.com',
        'role':         'client_it_lead',
        'organisation': 'Client Organisation',
    },
    {
        'username':     'client_ops',
        'full_name':    'Client Operations Lead',
        'email':        'ops.lead@client.com',
        'role':         'client_operations',
        'organisation': 'Client Organisation',
    },
]

DEFAULT_PASSWORD = 'kpmg1234'


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt()).decode()


def main():
    print(f"Connecting to {DB_CONFIG['database']} at {DB_CONFIG['host']}:{DB_CONFIG['port']} …")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"ERROR: Could not connect to database: {e}")
        sys.exit(1)

    cur = conn.cursor()

    # Create table
    cur.execute(CREATE_TABLE)
    conn.commit()
    print("✓ users table ready")

    # Seed users
    pw_hash = hash_password(DEFAULT_PASSWORD)
    inserted = 0
    skipped = 0

    for u in DEMO_USERS:
        try:
            cur.execute(
                """
                INSERT INTO users (username, password_hash, full_name, email, role, organisation)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (username) DO NOTHING
                """,
                (u['username'], pw_hash, u['full_name'], u['email'], u['role'], u['organisation']),
            )
            if cur.rowcount:
                print(f"  + {u['username']:<20} ({u['role']})")
                inserted += 1
            else:
                print(f"  ~ {u['username']:<20} already exists, skipped")
                skipped += 1
        except Exception as e:
            print(f"  ! {u['username']}: {e}")

    conn.commit()
    cur.close()
    conn.close()

    print(f"\nDone — {inserted} inserted, {skipped} skipped.")
    print(f"Login with any demo account using password: {DEFAULT_PASSWORD}")


if __name__ == '__main__':
    main()
