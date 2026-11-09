-- Statement executed by src/bin/txn_batch_pg.rs (extracted from its BATCH_SQL constant); one statement per window, mixed accounts.
-- $1 accounts uuid[], $2 tx ids uuid[], $3 amounts bigint[].
-- Result columns: status, transaction_id, account_id, amount, before_balance, after_balance.
WITH input AS (
  SELECT acc, tx, amt, ord
  FROM unnest($1::uuid[], $2::uuid[], $3::bigint[]) WITH ORDINALITY AS i(acc, tx, amt, ord)
),
probe AS (                                   -- one probe per row: does it already exist?
  SELECT i.*, (l.transaction_id IS NOT NULL) AS ex,
         l.amount AS o_amt, l.before_balance AS o_before, l.after_balance AS o_after
  FROM input i LEFT JOIN ledger l ON l.account_id = i.acc AND l.transaction_id = i.tx
),
valid AS (                                   -- fresh, amount > 0, account exists
  SELECT p.* FROM probe p
  WHERE NOT p.ex AND p.amt > 0
    AND EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = p.acc)
),
agg  AS (SELECT acc, sum(amt)::bigint AS s FROM valid GROUP BY acc),
upd  AS (UPDATE accounts a SET balance = a.balance + agg.s
         FROM agg WHERE a.account_id = agg.acc
         RETURNING a.account_id, a.balance - agg.s AS start_bal),
calc AS (SELECT v.acc, v.tx, v.amt,
                u.start_bal + sum(v.amt) OVER (PARTITION BY v.acc ORDER BY v.ord) - v.amt AS before_bal
         FROM valid v JOIN upd u ON u.account_id = v.acc),
ins  AS (INSERT INTO ledger(transaction_id, account_id, amount, before_balance, after_balance)
         SELECT tx, acc, amt, before_bal, before_bal + amt FROM calc
         RETURNING transaction_id, account_id, amount, before_balance, after_balance)
SELECT 'applied', transaction_id, account_id, amount, before_balance, after_balance FROM ins
UNION ALL SELECT 'existing', tx, acc, o_amt, o_before, o_after FROM probe WHERE ex
UNION ALL SELECT 'rejected:invalid_amount', tx, acc, amt, NULL, NULL FROM probe WHERE NOT ex AND amt <= 0
UNION ALL SELECT 'rejected:unknown_account', tx, acc, amt, NULL, NULL FROM probe p
          WHERE NOT ex AND amt > 0 AND NOT EXISTS (SELECT 1 FROM accounts a WHERE a.account_id = p.acc);
