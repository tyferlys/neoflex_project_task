CREATE SCHEMA IF NOT EXISTS ds;

CREATE TABLE IF NOT EXISTS ds.MD_EXCHANGE_RATE_D (
    data_actual_date        DATE NOT NULL,
    data_actual_end_date    DATE,
    currency_rk             INTEGER NOT NULL,
    reduced_cource          FLOAT,
    code_iso_num            VARCHAR(3),
    PRIMARY KEY (data_actual_date, currency_rk)
);