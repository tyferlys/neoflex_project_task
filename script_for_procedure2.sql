WITH cource_account AS (
	SELECT	a.account_rk,
			e.reduced_cource,
			e.data_actual_date,
			e.data_actual_end_date
	FROM ds.md_account_d a
	INNER JOIN ds.md_exchange_rate_d e USING(currency_rk)
)

INSERT INTO dm.dm_account_balance_f
SELECT	b.*,
		b.balance_out * COALESCE(c.reduced_cource, 1) as balance_out_rub
FROM ds.ft_balance_f b
INNER JOIN ds.md_account_d a
	ON b.account_rk = a.account_rk
LEFT JOIN cource_account c
	ON b.account_rk = c.account_rk
		AND b.on_date >= c.data_actual_date
		AND b.on_date <= c.data_actual_end_date
WHERE b.on_date = '2017-12-31'
ORDER BY account_rk

------------------------------


-- PROCEDURE: ds.fill_account_balance_f(date)

-- DROP PROCEDURE IF EXISTS ds.fill_account_balance_f(date);

CREATE OR REPLACE PROCEDURE ds.fill_account_balance_f(
	i_ondate date
)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	time_start_work TIMESTAMP := NOW();
BEGIN
	INSERT INTO logs.dags_logs
	VALUES ('fill_account_balance_f', time_start_work, NULL, 'Заполнение витрины account_balance в процессе', NULL);

	DELETE FROM dm.dm_account_balance_f
	WHERE on_date = i_OnDate;

	WITH
	filtered_turnover AS (
		SELECT	i_OnDate as on_date,
				sa.account_rk,
				ma.credit_amount,
				ma.credit_amount_rub,
				ma.debet_amount,
				ma.debet_amount_rub,
				sa.currency_rk,
				sa.char_type
		FROM ds.md_account_d sa
		LEFT JOIN dm.dm_account_turnover_f ma
			ON ma.account_rk = sa.account_rk AND ma.on_date = i_OnDate
		WHERE i_OnDate >= sa.data_actual_date AND i_OnDate <= sa.data_actual_end_date
	)

	INSERT INTO dm.dm_account_balance_f
	SELECT	ft.on_date,
			ft.account_rk,
			ft.currency_rk,
		CASE
			WHEN ft.char_type = 'А' THEN COALESCE(b.balance_out, 0) + COALESCE(ft.debet_amount, 0) - COALESCE(ft.credit_amount, 0)
			WHEN ft.char_type = 'П' THEN COALESCE(b.balance_out, 0) - COALESCE(ft.debet_amount, 0) + COALESCE(ft.credit_amount, 0)
			ELSE NULL
		END as balance_out,
		CASE
			WHEN ft.char_type = 'А' THEN COALESCE(b.balance_out_rub, 0) + COALESCE(ft.debet_amount_rub, 0) - COALESCE(ft.credit_amount_rub, 0)
			WHEN ft.char_type = 'П' THEN COALESCE(b.balance_out_rub, 0) - COALESCE(ft.debet_amount_rub, 0) + COALESCE(ft.credit_amount_rub, 0)
			ELSE NULL
		END as balance_out_rub
	FROM filtered_turnover ft
	LEFT JOIN dm.dm_account_balance_f b
		ON ft.account_rk = b.account_rk
			AND b.on_date = ft.on_date - INTERVAL '1 day';

	UPDATE logs.dags_logs
	SET status = 'Заполнения витрины account_balance закончилось успешно', time_end = NOW()
	WHERE dag_id = 'fill_account_balance_f' AND time_start = time_start_work;
EXCEPTION
	WHEN OTHERS THEN
		PERFORM dblink_exec(
		    'host=localhost dbname=project_neoflex user=airflow password=airflow',
		    'INSERT INTO logs.dags_logs
		     VALUES (''fill_account_balance_f'', ' || quote_literal(time_start_work) || ', ' || quote_literal(NOW()) || ', ''Заполнение витрины account_balance закончилось с ошибкой'',' || quote_literal(SQLERRM) ||');'
		);

		RAISE EXCEPTION 'Ошибка при выполнении процедуры: %', SQLERRM;
END;
$BODY$;

ALTER PROCEDURE ds.fill_account_balance_f(date)
    OWNER TO airflow;


DO $$
BEGIN
FOR i IN 0..30 LOOP
	PERFORM pg_sleep(0.2);
	RAISE NOTICE 'Итерация %s', i;
	CALL ds.fill_account_balance_f('2018-01-01'::date + i);
	COMMIT;
END LOOP;
END;
$$;