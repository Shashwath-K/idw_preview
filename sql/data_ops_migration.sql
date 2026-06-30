-- ============================================================
-- Data Operations Schema Migration
-- Creates tables for batch SQL ELT execution management
-- ============================================================

CREATE SCHEMA IF NOT EXISTS data_ops;

-- ------------------------------------------------------------
-- 1. batch — top-level grouping of SQL files
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_ops.batch (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(255) NOT NULL,
    description     TEXT,
    environment     VARCHAR(20) NOT NULL DEFAULT 'DEV'
                        CHECK (environment IN ('DEV', 'QA', 'UAT', 'PROD')),
    status          VARCHAR(20) NOT NULL DEFAULT 'draft'
                        CHECK (status IN ('draft', 'running', 'completed', 'failed')),
    created_by      VARCHAR(255) NOT NULL DEFAULT 'system',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_batch_status ON data_ops.batch (status);
CREATE INDEX IF NOT EXISTS idx_batch_environment ON data_ops.batch (environment);

-- ------------------------------------------------------------
-- 2. sql_file — individual .sql files attached to a batch
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_ops.sql_file (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID NOT NULL REFERENCES data_ops.batch(id) ON DELETE CASCADE,
    file_name       VARCHAR(512) NOT NULL,
    file_path       VARCHAR(1024) NOT NULL,
    execution_order INTEGER NOT NULL DEFAULT 0,
    checksum        VARCHAR(128),
    file_size       BIGINT,
    content         TEXT,
    uploaded_by     VARCHAR(255) NOT NULL DEFAULT 'system',
    uploaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sql_file_batch ON data_ops.sql_file (batch_id);
CREATE INDEX IF NOT EXISTS idx_sql_file_order ON data_ops.sql_file (batch_id, execution_order);

-- ------------------------------------------------------------
-- 3. batch_execution — one row per execution run of a batch
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_ops.batch_execution (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id        UUID NOT NULL REFERENCES data_ops.batch(id) ON DELETE CASCADE,
    status          VARCHAR(20) NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'completed', 'failed')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    triggered_by    VARCHAR(255) NOT NULL DEFAULT 'system'
);

CREATE INDEX IF NOT EXISTS idx_execution_batch ON data_ops.batch_execution (batch_id);
CREATE INDEX IF NOT EXISTS idx_execution_status ON data_ops.batch_execution (status);

-- ------------------------------------------------------------
-- 4. execution_log — per-file result within an execution run
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS data_ops.execution_log (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    execution_id    UUID NOT NULL REFERENCES data_ops.batch_execution(id) ON DELETE CASCADE,
    sql_file_id     UUID REFERENCES data_ops.sql_file(id) ON DELETE SET NULL,
    file_name       VARCHAR(512) NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'success', 'failed')),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    duration_ms     INTEGER,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_log_execution ON data_ops.execution_log (execution_id);

-- ============================================================
-- Done
-- ============================================================
