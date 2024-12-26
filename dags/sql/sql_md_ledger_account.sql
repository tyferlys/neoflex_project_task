CREATE SCHEMA IF NOT EXISTS ds;


CREATE TABLE IF NOT EXISTS ds.MD_LEDGER_ACCOUNT_S (
    chapter                         CHAR(1),
    chapter_name                    VARCHAR(16),
    section_number                  INTEGER,
    section_name                    VARCHAR(22),
    subsection_name                 VARCHAR(21),
    ledger1_account                 INTEGER,
    ledger1_account_name            VARCHAR(47),
    ledger_account                  INTEGER NOT NULL,
    ledger_account_name             VARCHAR(153),
    characteristic                  CHAR(1),
    is_resident                     INTEGER,
    is_reserve                      INTEGER,
    is_reserved                     INTEGER,
    is_loan                         INTEGER,
    is_reserved_assets              INTEGER,
    is_overdue                      INTEGER,
    is_interest                     INTEGER,
    pair_account                    VARCHAR(5),
    start_date                      DATE NOT NULL,
    end_date                        DATE,
    is_rub_only                     INTEGER,
    min_term                        INTEGER,
    min_term_measure                VARCHAR(1),
    max_term                        INTEGER,
    max_term_measure                VARCHAR(1),
    ledger_acc_full_name_translit   VARCHAR(1),
    is_revaluation                  VARCHAR(1),
    is_correct                      VARCHAR(1),
    PRIMARY KEY (ledger_account, start_date)
);
