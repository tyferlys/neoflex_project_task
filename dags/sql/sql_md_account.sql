CREATE SCHEMA IF NOT EXISTS ds;

CREATE TABLE IF NOT EXISTS ds.MD_ACCOUNT_D (
    data_actual_date        DATE NOT NULL,
    data_actual_end_date    DATE,
    account_rk              INTEGER NOT NULL,
    account_number          VARCHAR(20) NOT NULL,
    char_type               VARCHAR(1) NOT NULL,
    currency_rk             INTEGER NOT NULL,
    currency_code           VARCHAR(3) NOT NULL,
    PRIMARY KEY (data_actual_date, account_rk)
);