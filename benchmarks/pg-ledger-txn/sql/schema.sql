-- Ledger schema used by src/bin/txn_batch_pg.rs
CREATE TABLE accounts(account_id uuid PRIMARY KEY, balance bigint NOT NULL DEFAULT 0);
CREATE TABLE ledger(
  transaction_id uuid NOT NULL,
  account_id     uuid NOT NULL REFERENCES accounts(account_id),
  amount         bigint NOT NULL CHECK (amount > 0),
  before_balance bigint NOT NULL,
  after_balance  bigint NOT NULL,
  PRIMARY KEY (account_id, transaction_id),
  CHECK (after_balance = before_balance + amount));
