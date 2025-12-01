DROP TABLE IF EXISTS fraud_alerts; 
DROP TABLE IF EXISTS rules; 
DROP TABLE IF EXISTS schemas; 
DROP TABLE IF EXISTS pipelines; 

CREATE TABLE IF NOT EXISTS user_stats (
    user_id INTEGER,
    avg_spend FLOAT,
    txn_count INTEGER,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE pipelines (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    kafka_topic VARCHAR(255),
    status VARCHAR(50) DEFAULT 'stopped',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE schemas (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipelines(id),
    schema_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE rules (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipelines(id),
    rule_expression TEXT NOT NULL,  -- e.g. "amount > 5000 AND currency = 'USD'"
    severity VARCHAR(50) DEFAULT 'warning',
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE fraud_alerts (
    id SERIAL PRIMARY KEY,
    pipeline_id INTEGER REFERENCES pipelines(id),  -- Links back to the config
    severity VARCHAR(50),                          -- critical, warning, etc.
    rule_description TEXT,                         -- "Amount > 5000"
    alert_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    transaction_data JSONB NOT NULL                -- ⭐️ The Dynamic Payload
);