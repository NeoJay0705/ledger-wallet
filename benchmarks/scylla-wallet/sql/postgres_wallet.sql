CREATE TABLE accounts (
    account_id BIGINT NOT NULL CHECK (account_id >= 0),
    balance BYTEA NOT NULL CHECK (octet_length(balance) = 8),
    version BIGINT NOT NULL CHECK (version >= 0),
    write_clock BIGINT NOT NULL CHECK (write_clock >= 0),
    CONSTRAINT accounts_pkey PRIMARY KEY (account_id)
);

CREATE TABLE ledger (
    account_id BIGINT NOT NULL CHECK (account_id >= 0),
    tx_id BIGINT NOT NULL CHECK (tx_id >= 0),
    operation SMALLINT NOT NULL CHECK (operation IN (0, 1)),
    amount BYTEA NOT NULL CHECK (octet_length(amount) = 8),
    outcome SMALLINT NOT NULL CHECK (outcome BETWEEN 0 AND 3),
    balance_after BYTEA NOT NULL CHECK (octet_length(balance_after) = 8),
    version BIGINT NOT NULL CHECK (version >= 0),
    write_clock BIGINT NOT NULL CHECK (write_clock >= 0),
    CONSTRAINT ledger_pkey PRIMARY KEY (account_id, tx_id)
);

CREATE FUNCTION wallet_commit(
    p_account_id BIGINT,
    p_tx_id BIGINT,
    p_operation SMALLINT,
    p_amount BYTEA,
    p_outcome SMALLINT,
    p_balance_after BYTEA,
    p_version BIGINT,
    p_write_clock BIGINT
)
RETURNS TABLE (
    did_insert BOOLEAN,
    current_balance BYTEA,
    current_version BIGINT,
    current_write_clock BIGINT,
    record_operation SMALLINT,
    record_amount BYTEA,
    record_outcome SMALLINT,
    record_balance_after BYTEA,
    record_version BIGINT,
    record_write_clock BIGINT
)
LANGUAGE plpgsql
AS $$
DECLARE
    inserted BOOLEAN;
    changed_rows BIGINT;
BEGIN
    IF p_account_id < 0 OR p_tx_id < 0
       OR octet_length(p_amount) <> 8
       OR octet_length(p_balance_after) <> 8
       OR p_operation NOT IN (0, 1)
       OR p_outcome NOT BETWEEN 0 AND 3
       OR p_version < 0 OR p_write_clock < 0 THEN
        RAISE EXCEPTION 'wallet commit arguments are invalid';
    END IF;

    INSERT INTO ledger (
        account_id, tx_id, operation, amount, outcome,
        balance_after, version, write_clock
    ) VALUES (
        p_account_id, p_tx_id, p_operation, p_amount, p_outcome,
        p_balance_after, p_version, p_write_clock
    )
    ON CONFLICT (account_id, tx_id) DO NOTHING
    RETURNING TRUE INTO inserted;

    IF inserted IS TRUE THEN
        UPDATE accounts
        SET balance = p_balance_after,
            version = p_version,
            write_clock = p_write_clock
        WHERE account_id = p_account_id;
        GET DIAGNOSTICS changed_rows = ROW_COUNT;
        IF changed_rows <> 1 THEN
            RAISE EXCEPTION 'wallet account update did not affect exactly one row';
        END IF;

        RETURN QUERY SELECT
            TRUE,
            p_balance_after,
            p_version,
            p_write_clock,
            p_operation,
            p_amount,
            p_outcome,
            p_balance_after,
            p_version,
            p_write_clock;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        FALSE,
        a.balance,
        a.version,
        a.write_clock,
        l.operation,
        l.amount,
        l.outcome,
        l.balance_after,
        l.version,
        l.write_clock
    FROM accounts AS a
    CROSS JOIN ledger AS l
    WHERE a.account_id = p_account_id
      AND l.account_id = p_account_id
      AND l.tx_id = p_tx_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'wallet duplicate row or account is missing';
    END IF;
END;
$$;
