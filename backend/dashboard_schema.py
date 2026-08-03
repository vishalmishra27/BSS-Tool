"""
dashboard_schema.py — idempotent schema for the migration-dashboard tables
(phases, checklist, products, uat_cases, transformation_*, main_workflow,
stages, workflow_comments). These tables were previously created by hand on
the original dev DB, which is why a fresh environment (e.g. `phases` /
`uat_cases` "does not exist") fails when running seed_data.py / seed_uat_product.py.

Called once on backend boot from app.py, same pattern as bpm_endpoints.ensure_bpm_schema.
"""

import logging

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS main_workflow (
    id          VARCHAR(20) PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    description TEXT,
    status      VARCHAR(30) DEFAULT 'in_progress'
);

CREATE TABLE IF NOT EXISTS stages (
    id          VARCHAR(20) PRIMARY KEY,
    main_wf_id  VARCHAR(20) REFERENCES main_workflow(id) ON DELETE CASCADE,
    name        VARCHAR(200) NOT NULL,
    status      VARCHAR(30) DEFAULT 'pending',
    order_idx   INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS phases (
    id           VARCHAR(20) PRIMARY KEY,
    phase_id     VARCHAR(20) UNIQUE NOT NULL,
    stage_id     VARCHAR(20) REFERENCES stages(id) ON DELETE SET NULL,
    curr_status  VARCHAR(30) DEFAULT 'pending',
    start_dt     DATE,
    end_dt       DATE,
    lob          VARCHAR(60),
    assigned_to  VARCHAR(120)
);

CREATE TABLE IF NOT EXISTS checklist (
    ch_id     SERIAL PRIMARY KEY,
    phase_id  VARCHAR(20) REFERENCES phases(phase_id) ON DELETE CASCADE,
    wf_id     VARCHAR(20),
    item_text TEXT NOT NULL,
    status    VARCHAR(30) DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS checklist_comments (
    id         SERIAL PRIMARY KEY,
    ch_id      INTEGER REFERENCES checklist(ch_id) ON DELETE CASCADE,
    username   VARCHAR(120) DEFAULT 'Programme User',
    comment    TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS checklist_attachments (
    id          SERIAL PRIMARY KEY,
    ch_id       INTEGER REFERENCES checklist(ch_id) ON DELETE CASCADE,
    file_name   VARCHAR(255) NOT NULL,
    file_path   TEXT NOT NULL,
    uploaded_by VARCHAR(120) DEFAULT 'Programme User',
    uploaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workflow_comments (
    id              SERIAL PRIMARY KEY,
    phase_id        VARCHAR(20) REFERENCES phases(phase_id) ON DELETE CASCADE,
    username        VARCHAR(120) DEFAULT 'Programme User',
    action          VARCHAR(60) DEFAULT 'Commented',
    comment         TEXT NOT NULL,
    attachment_name VARCHAR(255),
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transformation_activities (
    id         SERIAL PRIMARY KEY,
    lob        VARCHAR(60) NOT NULL,
    phase_name VARCHAR(120) NOT NULL,
    planned    NUMERIC(5,1) DEFAULT 0,
    actual     NUMERIC(5,1) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS transformation_lob_progress (
    id      SERIAL PRIMARY KEY,
    lob     VARCHAR(60) UNIQUE NOT NULL,
    planned NUMERIC(5,1) DEFAULT 0,
    actual  NUMERIC(5,1) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS products (
    product_id      VARCHAR(30) PRIMARY KEY,
    product_name    VARCHAR(200) NOT NULL,
    lob             VARCHAR(60) NOT NULL,
    migration_flag  VARCHAR(20) DEFAULT 'migrate',
    status          VARCHAR(30) DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS product_parameters (
    id         SERIAL PRIMARY KEY,
    param_name VARCHAR(120) NOT NULL,
    lob        VARCHAR(60),
    status     VARCHAR(30) DEFAULT 'pending'
);

CREATE TABLE IF NOT EXISTS uat_cases (
    test_case_id VARCHAR(40) PRIMARY KEY,
    lob          VARCHAR(60),
    priority     VARCHAR(20) DEFAULT 'Medium',
    status       VARCHAR(30) DEFAULT 'OPEN',
    description  TEXT,
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);
"""


def ensure_dashboard_schema(query_fn):
    """Create migration-dashboard tables if missing. Called once on backend boot."""
    query_fn(SCHEMA_SQL, fetch=None)
    logger.info(
        "Dashboard schema ensured (phases, checklist, products, uat_cases, "
        "transformation_*, main_workflow, stages, workflow_comments)"
    )
