CREATE TABLE IF NOT EXISTS job (
    q VARCHAR(120) NOT NULL,
    type VARCHAR(20) NOT NULL,
    execution_intervall SMALLINT DEFAULT 60,
    next_execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_time_executed TIMESTAMP,
    PRIMARY KEY (q, type)
)