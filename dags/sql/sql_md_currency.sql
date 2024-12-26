

CREATE TABLE IF NOT EXISTS ds.MD_CURRENCY_D (
    currency_rk             INTEGER NOT NULL,
    data_actual_date        DATE NOT NULL,
    data_actual_end_date    DATE,
    currency_code           VARCHAR(3),
    code_iso_char           VARCHAR(3),
    PRIMARY KEY (currency_rk, data_actual_date)
);