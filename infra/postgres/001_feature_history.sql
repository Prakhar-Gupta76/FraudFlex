CREATE TABLE IF NOT EXISTS customer_profiles (
    customer_id VARCHAR(64) PRIMARY KEY,
    home_country VARCHAR(120),
    home_region VARCHAR(120),
    usual_countries TEXT[] NOT NULL DEFAULT '{}',
    usual_regions TEXT[] NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_history (
    event_id VARCHAR(64) PRIMARY KEY,
    transaction_id VARCHAR(64) NOT NULL UNIQUE,
    customer_id VARCHAR(64) NOT NULL,
    account_id VARCHAR(64) NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL,
    merchant_category VARCHAR(64) NOT NULL,
    device_id VARCHAR(64) NOT NULL,
    region VARCHAR(120) NOT NULL,
    country VARCHAR(120) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL CHECK (
        latitude BETWEEN -90 AND 90
    ),
    longitude DOUBLE PRECISION NOT NULL CHECK (
        longitude BETWEEN -180 AND 180
    ),
    authentication_result VARCHAR(16) NOT NULL CHECK (
        authentication_result IN ('success', 'failure', 'challenged')
    ),
    failed_attempts_last_10m INTEGER NOT NULL DEFAULT 0 CHECK (
        failed_attempts_last_10m >= 0
    ),
    transaction_time TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS transaction_history_customer_time_idx
    ON transaction_history (customer_id, transaction_time DESC);

CREATE INDEX IF NOT EXISTS transaction_history_device_time_idx
    ON transaction_history (device_id, transaction_time DESC);

CREATE INDEX IF NOT EXISTS transaction_history_merchant_time_idx
    ON transaction_history (merchant_id, transaction_time DESC);

CREATE TABLE IF NOT EXISTS device_deny_list (
    device_id VARCHAR(64) PRIMARY KEY,
    reason TEXT NOT NULL,
    added_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS merchant_risk_profiles (
    merchant_id VARCHAR(64) PRIMARY KEY,
    fraud_rate DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (
        fraud_rate BETWEEN 0 AND 1
    ),
    sample_size INTEGER NOT NULL DEFAULT 0 CHECK (sample_size >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
