CREATE SCHEMA IF NOT EXISTS ds;

CREATE TABLE IF NOT EXISTS ds.FT_POSTING_F (
    oper_date           DATE NOT NULL,
    credit_account_rk   INTEGER NOT NULL,
    debet_account_rk    INTEGER NOT NULL,
    credit_amount       FLOAT,
    debet_amount        FLOAT
);