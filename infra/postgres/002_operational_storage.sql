ALTER TABLE customer_profiles
    ADD COLUMN IF NOT EXISTS normal_behavior JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ;

ALTER TABLE transaction_history
    ADD COLUMN IF NOT EXISTS raw_event JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS processing_status VARCHAR(16) NOT NULL
        DEFAULT 'received',
    ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'transaction_history_processing_status_check'
    ) THEN
        ALTER TABLE transaction_history
            ADD CONSTRAINT transaction_history_processing_status_check
            CHECK (processing_status IN ('received', 'scored', 'failed'));
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS risk_decisions (
    record_id VARCHAR(140) PRIMARY KEY,
    input_event_id VARCHAR(64) NOT NULL UNIQUE,
    transaction_id VARCHAR(64) NOT NULL UNIQUE,
    customer_id VARCHAR(64) NOT NULL,
    transaction_payload JSONB NOT NULL,
    feature_values JSONB NOT NULL,
    rules_contribution INTEGER NOT NULL CHECK (
        rules_contribution BETWEEN 0 AND 70
    ),
    rule_hits JSONB NOT NULL,
    ruleset_version VARCHAR(80) NOT NULL,
    rule_override_action VARCHAR(40),
    anomaly_contribution INTEGER NOT NULL CHECK (
        anomaly_contribution BETWEEN 0 AND 30
    ),
    anomaly_raw_score DOUBLE PRECISION NOT NULL,
    anomaly_deviations JSONB NOT NULL,
    anomaly_level VARCHAR(32) NOT NULL,
    anomaly_inference_time_ms DOUBLE PRECISION NOT NULL CHECK (
        anomaly_inference_time_ms >= 0
    ),
    model_version VARCHAR(80) NOT NULL,
    uncapped_score INTEGER NOT NULL CHECK (
        uncapped_score BETWEEN 0 AND 100
    ),
    final_score INTEGER NOT NULL CHECK (final_score BETWEEN 0 AND 100),
    score_policy_version VARCHAR(80) NOT NULL,
    score_override_action VARCHAR(40),
    score_category VARCHAR(16) NOT NULL CHECK (
        score_category IN ('low', 'medium', 'high')
    ),
    effective_category VARCHAR(16) NOT NULL CHECK (
        effective_category IN ('low', 'medium', 'high')
    ),
    recommended_action VARCHAR(40) NOT NULL CHECK (
        recommended_action IN (
            'approve',
            'additional_verification',
            'hold_for_review'
        )
    ),
    override_applied BOOLEAN NOT NULL,
    explanation JSONB NOT NULL,
    decision_policy_version VARCHAR(80) NOT NULL,
    processing_latency_ms DOUBLE PRECISION NOT NULL CHECK (
        processing_latency_ms >= 0
    ),
    processed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS risk_decisions_customer_time_idx
    ON risk_decisions (customer_id, processed_at DESC);

CREATE INDEX IF NOT EXISTS risk_decisions_category_time_idx
    ON risk_decisions (effective_category, processed_at DESC);

CREATE TABLE IF NOT EXISTS ruleset_versions (
    version VARCHAR(80) PRIMARY KEY,
    content_sha256 CHAR(64) NOT NULL UNIQUE,
    configuration JSONB NOT NULL,
    registered_by VARCHAR(120) NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS model_versions (
    version VARCHAR(80) PRIMARY KEY,
    algorithm VARCHAR(80) NOT NULL,
    artifact_sha256 CHAR(64) NOT NULL UNIQUE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    registered_by VARCHAR(120) NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id VARCHAR(140) PRIMARY KEY,
    decision_record_id VARCHAR(140) NOT NULL UNIQUE
        REFERENCES risk_decisions(record_id),
    customer_id VARCHAR(64) NOT NULL,
    transaction_id VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'open' CHECK (
        status IN ('open', 'assigned', 'resolved')
    ),
    assigned_to VARCHAR(120),
    assigned_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS fraud_alerts_status_time_idx
    ON fraud_alerts (status, created_at DESC);

CREATE TABLE IF NOT EXISTS analyst_reviews (
    review_id VARCHAR(140) PRIMARY KEY,
    alert_id VARCHAR(140) NOT NULL UNIQUE REFERENCES fraud_alerts(alert_id),
    analyst_id VARCHAR(120) NOT NULL,
    outcome VARCHAR(32) NOT NULL CHECK (
        outcome IN (
            'confirmed_fraud',
            'legitimate',
            'needs_more_information'
        )
    ),
    notes TEXT,
    reviewed_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rejected_events (
    record_id VARCHAR(300) PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS outbox_events (
    outbox_id VARCHAR(300) PRIMARY KEY,
    record_id VARCHAR(300) NOT NULL,
    topic VARCHAR(249) NOT NULL,
    message_key VARCHAR(300) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    UNIQUE (record_id, topic)
);

CREATE INDEX IF NOT EXISTS outbox_events_pending_idx
    ON outbox_events (record_id, created_at)
    WHERE published_at IS NULL;

CREATE TABLE IF NOT EXISTS audit_history (
    audit_id BIGSERIAL PRIMARY KEY,
    entity_type VARCHAR(40) NOT NULL,
    entity_id VARCHAR(300) NOT NULL,
    action VARCHAR(80) NOT NULL,
    actor VARCHAR(120) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS audit_history_entity_time_idx
    ON audit_history (entity_type, entity_id, occurred_at DESC);
