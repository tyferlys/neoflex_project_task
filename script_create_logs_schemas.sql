CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE logs.dags_logs (
    dag_id              VARCHAR(255),
    time_start          TIMESTAMP,
    time_end            TIMESTAMP,
    status              VARCHAR(255),
    error_message       TEXT,
    PRIMARY KEY (dag_id, time_start)
);