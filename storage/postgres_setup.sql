-- =============================================================================
-- Regular PostgreSQL tables for anomaly detection results and agent audit trail
-- These are NOT hypertables – they store low-volume, long-lived records.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. anomaly_logs  –  every detected anomaly event
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anomaly_logs (
    id                   BIGSERIAL    PRIMARY KEY,
    symbol               VARCHAR(20)  NOT NULL,
    anomaly_type         VARCHAR(50),
    severity             VARCHAR(20)  CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    detection_method     VARCHAR(50),
    anomaly_score        DECIMAL(10, 6),
    price_at_anomaly     DECIMAL(12, 4),
    volume_at_anomaly    DECIMAL(20, 4),
    context              JSONB,
    detected_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_at           TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomaly_logs_symbol_ts
    ON anomaly_logs (symbol, detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_logs_severity
    ON anomaly_logs (severity);

-- -----------------------------------------------------------------------------
-- 2. root_cause_reports  –  LLM-generated root-cause analysis per anomaly
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS root_cause_reports (
    id                BIGSERIAL    PRIMARY KEY,
    anomaly_log_id    BIGINT       REFERENCES anomaly_logs (id),
    symbol            VARCHAR(20)  NOT NULL,
    summary           TEXT,
    full_report       TEXT,
    confidence_score  DECIMAL(4, 3),
    root_cause_type   VARCHAR(100),
    evidence          JSONB,
    recommendations   TEXT[],
    agent_model       VARCHAR(100),
    tokens_used       INTEGER,
    generated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    created_at        TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_root_cause_anomaly_id
    ON root_cause_reports (anomaly_log_id);

CREATE INDEX IF NOT EXISTS idx_root_cause_symbol_ts
    ON root_cause_reports (symbol, generated_at DESC);

-- -----------------------------------------------------------------------------
-- 3. agent_audit_trail  –  step-by-step trace of agent tool calls
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_audit_trail (
    id           BIGSERIAL    PRIMARY KEY,
    report_id    BIGINT       REFERENCES root_cause_reports (id),
    step_name    VARCHAR(100),
    step_input   JSONB,
    step_output  JSONB,
    tool_calls   JSONB,
    duration_ms  INTEGER,
    executed_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
