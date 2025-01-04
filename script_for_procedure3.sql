-- PROCEDURE: ds.fill_f101_round_f(date)

-- DROP PROCEDURE IF EXISTS ds.fill_f101_round_f(date);

CREATE OR REPLACE PROCEDURE ds.fill_f101_round_f(
	i_ondate date)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	start_date DATE := i_OnDate - INTERVAL '1 month';
	end_date DATE := i_OnDate - INTERVAL '1 day';
BEGIN
	UPDATE logs.dags_logs
	SET status = 'Начало заполнения витрины f101_round'
	WHERE dag_id = 'fill_f101_round'
		AND time_start = (SELECT time_start FROM logs.dags_logs WHERE dag_id = 'fill_f101_round' ORDER BY time_start DESC LIMIT 1);

	DELETE FROM dm.dm_f101_round_f
	WHERE from_date = start_date AND to_date = end_date;

	WITH
	account_data AS (
		SELECT	a.account_rk,
				a.currency_code::integer,
				a.char_type,
				la.chapter,
				la.ledger_account
		FROM ds.md_account_d a
		INNER JOIN ds.md_ledger_account_s la
			ON LEFT(a.account_number, 5) = la.ledger_account::varchar
	),
	data_balance AS (
		SELECT	ad.chapter,
				ad.ledger_account,
				ad.char_type as characteristic,
				SUM(
					CASE
						WHEN ad.currency_code IN (810, 643) AND ab.on_date = start_date - INTERVAL '1 day'
							THEN balance_out_rub
						ELSE
							NULL
					END
				) AS balance_in_rub,
				SUM(
					CASE
						WHEN ad.currency_code NOT IN (810, 643) AND ab.on_date = start_date - INTERVAL '1 day'
							THEN balance_out_rub
						ELSE
							NULL
					END
				) AS balance_in_val,
				SUM(
					CASE
						WHEN ab.on_date = start_date - INTERVAL '1 day'
							THEN balance_out_rub
						ELSE
							NULL
					END
				) AS balance_in_total,
				SUM(
					CASE
						WHEN ad.currency_code IN (810, 643) AND ab.on_date = end_date
							THEN balance_out_rub
						ELSE
							NULL
					END
				) AS balance_out_rub,
				SUM(
					CASE
						WHEN ad.currency_code NOT IN (810, 643) AND ab.on_date = end_date
							THEN balance_out_rub
						ELSE
							NULL
					END
				) AS balance_out_val,
				SUM(
					CASE
						WHEN ab.on_date = end_date
							THEN balance_out_rub
						ELSE
							NULL
					END
				) AS balance_out_total
		FROM dm.dm_account_balance_f ab
		INNER JOIN account_data ad
			ON ab.account_rk = ad.account_rk
		GROUP BY ad.chapter, ad.ledger_account, ad.char_type
	),
	data_turn AS (
		SELECT	ad.chapter,
				ad.ledger_account,
				ad.char_type as characteristic,
				SUM(
					CASE
						WHEN ad.currency_code IN (810, 643) AND ac.on_date >= start_date AND ac.on_date <= end_date
							THEN debet_amount_rub
						ELSE
							NULL
					END
				) AS turn_deb_rub,
				SUM(
					CASE
						WHEN ad.currency_code NOT IN (810, 643) AND ac.on_date >= start_date AND ac.on_date <= end_date
							THEN debet_amount_rub
						ELSE
							NULL
					END
				) AS turn_deb_val,
				SUM(
					CASE
						WHEN ac.on_date >= start_date AND ac.on_date <= end_date
							THEN debet_amount_rub
						ELSE
							NULL
					END
				) AS turn_deb_total,
				SUM(
					CASE
						WHEN ad.currency_code IN (810, 643) AND ac.on_date >= start_date AND ac.on_date <= end_date
							THEN credit_amount_rub
						ELSE
							NULL
					END
				) AS turn_cre_rub,
				SUM(
					CASE
						WHEN ad.currency_code NOT IN (810, 643) AND ac.on_date >= start_date AND ac.on_date <= end_date
							THEN credit_amount_rub
						ELSE
							NULL
					END
				) AS turn_cre_val,
				SUM(
					CASE
						WHEN ac.on_date >= start_date AND ac.on_date <= end_date
							THEN credit_amount_rub
						ELSE
							NULL
					END
				) AS turn_cre_total
		FROM dm.dm_account_turnover_f ac
		RIGHT JOIN account_data ad
			ON ac.account_rk = ad.account_rk
		GROUP BY ad.chapter, ad.ledger_account, ad.char_type
	)

	INSERT INTO dm.dm_f101_round_f(
		from_date, to_date, chapter, ledger_account, characteristic, balance_in_rub, balance_in_val, balance_in_total,
		turn_deb_rub, turn_deb_val, turn_deb_total, turn_cre_rub, turn_cre_val, turn_cre_total, balance_out_rub,
		balance_out_val, balance_out_total
	)
	SELECT	start_date as from_date,
			end_date as to_date,
			b.chapter,
			b.ledger_account,
			b.characteristic,
			b.balance_in_rub,
			b.balance_in_val,
			b.balance_in_total,
			t.turn_deb_rub,
			t.turn_deb_val,
			t.turn_deb_total,
			t.turn_cre_rub,
			t.turn_cre_val,
			t.turn_cre_total,
			b.balance_out_rub,
			b.balance_out_val,
			b.balance_out_total
	FROM data_balance b
	INNER JOIN data_turn t
		ON b.ledger_accounat = t.ledger_account;

	UPDATE logs.dags_logs
	SET status = 'Конец заполнения витрины f101_round'
	WHERE dag_id = 'fill_f101_round'
		AND time_start = (SELECT time_start FROM logs.dags_logs WHERE dag_id = 'fill_f101_round' ORDER BY time_start DESC LIMIT 1);
END;
$BODY$;

