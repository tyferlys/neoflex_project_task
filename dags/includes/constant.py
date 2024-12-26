import pandas as pd

metadata_dag_balance = {
    "tablename": "ft_balance_f",
    "schema": "ds",
    "directory_to_file": f"/opt/airflow/source/ft_balance_f.csv",
    "dtype": {
        "ON_DATE": pd.StringDtype(),
        "ACCOUNT_RK": pd.Int64Dtype(),
        "CURRENCY_RK": pd.Int64Dtype(),
        "BALANCE_OUT": pd.Float64Dtype()
    },
    "list_pk": ["on_date", "account_rk"],
    "list_not_null": ["on_date", "account_rk"],
    "list_date": ["on_date"],
    "list_length": {},
    "sql_script": "sql/sql_ft_balance.sql"
}

metadata_dag_posting = {
    "tablename": "ft_posting_f",
    "schema": "ds",
    "directory_to_file": f"/opt/airflow/source/ft_posting_f.csv",
    "dtype": {
        "OPER_DATE": pd.StringDtype(),
        "CREDIT_ACCOUNT_RK": pd.Int64Dtype(),
        "DEBET_ACCOUNT_RK": pd.Int64Dtype(),
        "CREDIT_AMOUNT": pd.Float64Dtype(),
        "DEBET_AMOUNT": pd.Float64Dtype()
    },
    "list_pk": [],
    "list_not_null": ["oper_date", "credit_account_rk", "debet_account_rk"],
    "list_date": ["oper_date"],
    "list_length": {},
    "sql_script": "sql/sql_ft_posting.sql"
}

metadata_dag_account = {
    "tablename": "md_account_d",
    "schema": "ds",
    "directory_to_file": "/opt/airflow/source/md_account_d.csv",
    "dtype": {
        "DATA_ACTUAL_DATE": pd.StringDtype(),
        "DATA_ACTUAL_END_DATE": pd.StringDtype(),
        "ACCOUNT_RK": pd.Int64Dtype(),
        "ACCOUNT_NUMBER": pd.StringDtype(),
        "CHAR_TYPE": pd.StringDtype(),
        "CURRENCY_RK": pd.Int64Dtype(),
        "CURRENCY_CODE": pd.StringDtype()
    },
    "list_pk": ["data_actual_date", "account_rk"],
    "list_not_null": ["data_actual_date", "data_actual_end_date", "account_rk", "account_rk",
                      "account_number", "char_type", "currency_rk", "currency_code"],
    "list_date": ["data_actual_date", "data_actual_end_date"],
    "list_length": {
        "account_number": [0, 20],
        "char_type": [0, 1],
        "currency_code": [0, 3],
    },
    "sql_script": "sql/sql_md_account.sql"
}

metadata_dag_currency = {
    "tablename": "md_currency_d",
    "schema": "ds",
    "directory_to_file": "/opt/airflow/source/md_currency_d.csv",
    "dtype": {
        "CURRENCY_RK": pd.Int64Dtype(),
        "DATA_ACTUAL_DATE": pd.StringDtype(),
        "DATA_ACTUAL_END_DATE": pd.StringDtype(),
        "CURRENCY_CODE": pd.StringDtype(),
        "CODE_ISO_CHAR": pd.StringDtype(),
    },
    "list_pk": ["currency_rk", "data_actual_date"],
    "list_not_null": ["currency_rk", "data_actual_date"],
    "list_date": ["data_actual_date", "data_actual_end_date"],
    "list_length": {
        "currency_code": [0, 3],
        "code_iso_char": [0, 3]
    },
    "sql_script": "sql/sql_md_currency.sql"
}

metadata_exchange_rate = {
    "tablename": "md_exchange_rate_d",
    "schema": "ds",
    "directory_to_file": "/opt/airflow/source/md_exchange_rate_d.csv",
    "dtype": {
        "DATA_ACTUAL_DATE": pd.StringDtype(),
        "DATA_ACTUAL_END_DATE": pd.StringDtype(),
        "CURRENCY_RK": pd.Int64Dtype(),
        "REDUCED_COURCE": pd.Float64Dtype(),
        "CODE_ISO_NUM": pd.StringDtype(),
    },
    "list_pk": ["data_actual_date", "currency_rk"],
    "list_not_null": ["data_actual_date", "currency_rk"],
    "list_date": ["data_actual_date", "data_actual_end_date"],
    "list_length": {
        "code_iso_char": [0, 3]
    },
    "sql_script": "sql/sql_md_exchange_rate.sql"
}

metadata_ledger_account = {
    "tablename": "md_ledger_account_s",
    "schema": "ds",
    "directory_to_file": "/opt/airflow/source/md_ledger_account_s.csv",
    "dtype": {
        "CHAPTER": pd.StringDtype(),
        "CHAPTER_NAME": pd.StringDtype(),
        "SECTION_NUMBER": pd.Int64Dtype(),
        "SECTION_NAME": pd.StringDtype(),
        "SUBSECTION_NAME": pd.StringDtype(),
        "LEDGER1_ACCOUNT": pd.Int64Dtype(),
        "LEDGER1_ACCOUNT_NAME": pd.StringDtype(),
        "LEDGER_ACCOUNT": pd.Int64Dtype(),
        "LEDGER_ACCOUNT_NAME": pd.StringDtype(),
        "CHARACTERISTIC": pd.StringDtype(),
        "IS_RESIDENT": pd.Int64Dtype(),
        "IS_RESERVE": pd.Int64Dtype(),
        "IS_RESERVED": pd.Int64Dtype(),
        "IS_LOAN": pd.Int64Dtype(),
        "IS_RESERVED_ASSETS": pd.Int64Dtype(),
        "IS_OVERDUE": pd.Int64Dtype(),
        "IS_INTEREST": pd.Int64Dtype(),
        "PAIR_ACCOUNT": pd.StringDtype(),
        "START_DATE": pd.StringDtype(),
        "END_DATE": pd.StringDtype(),
        "IS_RUB_ONLY": pd.Int64Dtype(),
        "MIN_TERM": pd.StringDtype(),
        "MIN_TERM_MEASURE": pd.StringDtype(),
        "MAX_TERM": pd.StringDtype(),
        "MAX_TERM_MEASURE": pd.StringDtype(),
        "LEDGER_ACC_FULL_NAME_TRANSLIT": pd.StringDtype(),
        "IS_REVALUATION": pd.StringDtype(),
        "IS_CORRECT": pd.StringDtype()
    },
    "list_pk": ["ledger_account", "start_date"],
    "list_not_null": ["ledger_account", "start_date"],
    "list_date": ["start_date", "end_date"],
    "list_length": {
        "chapter": [1, 1],
        "chapter_name": [0, 16],
        "section_name": [0, 22],
        "subsection_name": [0, 21],
        "ledger1_account_name": [0, 47],
        "ledger_account_name": [0, 153],
        "characteristic": [1, 1],
        "pair_account": [0, 5],
        "min_term": [0, 1],
        "min_term_measure": [0, 1],
        "max_term": [0, 1],
        "max_term_measure": [0, 1],
        "ledger_acc_full_name_translit": [0, 1],
        "is_revaluation": [0, 1],
        "is_correct": [0, 1]
    },
    "sql_script": "sql/sql_md_ledger_account.sql"
}