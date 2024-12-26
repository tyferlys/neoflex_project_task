CREATE SCHEMA IF NOT EXISTS ds;

CREATE TABLE IF NOT EXISTS ds.FT_BALANCE_F (
    on_date     DATE NOT NULL,
    account_rk  INTEGER NOT NULL,
    currency_rk INTEGER,
    balance_out FLOAT,
    PRIMARY KEY (on_date, account_rk)
);