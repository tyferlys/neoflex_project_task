-- PROCEDURE: ds.fill_account_turnover_f(date)

-- DROP PROCEDURE IF EXISTS ds.fill_account_turnover_f(date);

CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(
	i_ondate date)
LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
	UPDATE logs.dags_logs
	SET status = 'Начало заполнения витрины account_turnover'
	WHERE dag_id = 'fill_account_turnover'
		AND time_start = (SELECT time_start FROM logs.dags_logs WHERE dag_id = 'fill_account_turnover' ORDER BY time_start DESC LIMIT 1);

	DELETE FROM dm.dm_account_turnover_f
	WHERE on_date = i_OnDate;

	WITH
	cource_account AS (
	  SELECT  a.account_rk,
	          e.reduced_cource,
	          e.data_actual_date,
	          e.data_actual_end_date
	  FROM ds.md_account_d a
	  INNER JOIN ds.md_exchange_rate_d e USING(currency_rk)
	),
	account_credit AS (
		SELECT	p.oper_date as on_date,
				p.credit_account_rk as account_rk,
				SUM(p.credit_amount) as credit_amount,
				SUM(p.credit_amount * COALESCE(ca.reduced_cource, 1)) as credit_amount_rub
		FROM ds.ft_posting_f p
		LEFT JOIN cource_account ca
			ON p.credit_account_rk = ca.account_rk
				AND p.oper_date >= ca.data_actual_date
				AND p.oper_date <= ca.data_actual_end_date
		WHERE p.oper_date = i_OnDate
		GROUP BY oper_date, credit_account_rk
	),
	account_debet AS (
		SELECT	p.oper_date as on_date,
				p.debet_account_rk as account_rk,
				SUM(p.debet_amount) as debet_amount,
				SUM(p.debet_amount * COALESCE(ca.reduced_cource, 1)) as debet_amount_rub
		FROM ds.ft_posting_f p
		LEFT JOIN cource_account ca
			ON p.debet_account_rk = ca.account_rk
				AND p.oper_date >= ca.data_actual_date
				AND p.oper_date <= ca.data_actual_end_date
		WHERE p.oper_date = i_OnDate
		GROUP BY oper_date, debet_account_rk
	)

	INSERT INTO dm.dm_account_turnover_f(
		on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
	)
	SELECT	COALESCE(c.on_date, d.on_date),
			COALESCE(c.account_rk, d.account_rk),
			c.credit_amount,
			c.credit_amount_rub,
			d.debet_amount,
			d.debet_amount_rub
	FROM account_credit c
	FULL JOIN account_debet d
		ON c.account_rk = d.account_rk;

	UPDATE logs.dags_logs
	SET status = 'Конец заполнения витрины account_turnover'
	WHERE dag_id = 'fill_account_turnover'
		AND time_start = (SELECT time_start FROM logs.dags_logs WHERE dag_id = 'fill_account_turnover' ORDER BY time_start DESC LIMIT 1);
END;
$BODY$;

ALTER PROCEDURE ds.fill_account_turnover_f(date)
    OWNER TO airflow;


DO $$
BEGIN
FOR i IN 0..30 LOOP
	CALL ds.fill_account_turnover_f('2018-01-01'::date + i);
END LOOP;
END;
$$;