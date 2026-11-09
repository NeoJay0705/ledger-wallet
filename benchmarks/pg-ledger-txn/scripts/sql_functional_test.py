#!/usr/bin/env python3
"""Functional test of sql/txn_batch.sql on a throwaway PostgreSQL container (no published port).

Needs Docker (default image: the pinned PostgreSQL 18.6 digest used by the benchmarks; override with PG_IMAGE).
Checks: applied chain, existing (original record, also when the retry carries another amount),
rejected:invalid_amount, rejected:unknown_account, no mutation on replay, duplicate key inside one
input fails with 23505 and rolls back. Exit code 0 = pass.
"""
import os, pathlib, subprocess, sys, time, uuid

ROOT = pathlib.Path(__file__).resolve().parent.parent
IMAGE = os.environ.get('PG_IMAGE', 'postgres@sha256:afc7e2d441324c0388fa80c3d24f733b4194a4eb7f47dd8ee2b08eb1a24a647c')  # PostgreSQL 18.6, same image as the benchmarks
NAME = 'pg-ledger-txn-sqltest-' + uuid.uuid4().hex[:8]
u = lambda n: f'00000000-0000-0000-0000-{n:012d}'
A1, A2, A3, A9 = u(1), u(2), u(3), u(9)          # A9 never exists
TX = {k: u(100 + i) for i, k in enumerate(['a1', 'a2', 'a3', 'b1', 'z1', 'c1', 'c2', 'd1', 'd2', 'e1'])}

def psql(sql, check=True):
    p = subprocess.run(['docker', 'exec', '-i', NAME, 'psql', '-U', 'postgres', '-h', '127.0.0.1', '-X', '-At', '-F', '|',
                        '-v', 'ON_ERROR_STOP=1'], input=sql, text=True, capture_output=True)
    if check and p.returncode != 0:
        raise RuntimeError(p.stderr)
    return p

def window(rows):
    """rows: [(account, tx_key, amount)] -> psql result of the statement."""
    arr = lambda vals, cast: "ARRAY[" + ",".join(f"'{v}'" if cast != 'bigint' else str(v) for v in vals) + f"]::{cast}[]"
    stmt = (ROOT / 'sql/txn_batch.sql').read_text()
    stmt = stmt.replace('$1::uuid[]', arr([r[0] for r in rows], 'uuid')).replace('$2::uuid[]', arr([TX[r[1]] for r in rows], 'uuid')) \
               .replace('$3::bigint[]', arr([r[2] for r in rows], 'bigint')).rstrip().rstrip(';')
    return psql(stmt + ';', check=False)   # data-modifying CTEs must be top level: sort in Python

def lines(p):
    if p.returncode != 0:
        raise RuntimeError(p.stderr)
    inv = {v: k for k, v in TX.items()}
    out = []
    for l in p.stdout.strip().splitlines():
        s, tx, acc, amt, b, a = l.split('|')
        out.append((s, inv[tx], int(acc[-12:]), int(amt), b, a))
    return sorted(out, key=lambda x: (x[2], x[1]))

def balances():
    return psql("SELECT account_id, balance FROM accounts ORDER BY 1;").stdout.strip().splitlines()

def check(name, got, want):
    ok = got == want
    print(('PASS ' if ok else 'FAIL ') + name)
    if not ok:
        print('  got :', got); print('  want:', want)
    return ok

def main():
    subprocess.run(['docker', 'run', '-d', '--name', NAME, '-e', 'POSTGRES_PASSWORD=x', IMAGE], check=True, capture_output=True)
    ok = True
    try:
        for _ in range(60):
            if subprocess.run(['docker', 'exec', NAME, 'pg_isready', '-U', 'postgres', '-h', '127.0.0.1'], capture_output=True).returncode == 0:
                time.sleep(2); break
            time.sleep(1)
        psql((ROOT / 'sql/schema.sql').read_text())
        psql(f"INSERT INTO accounts VALUES ('{A1}',1),('{A2}',0),('{A3}',0); "
             f"INSERT INTO ledger VALUES ('{TX['a1']}','{A1}',1,0,1);")
        r1 = lines(window([(A1, 'a1', 1), (A1, 'a2', 1), (A1, 'a3', 2), (A2, 'b1', 1), (A9, 'z1', 1), (A3, 'c1', 0), (A3, 'c2', 1)]))
        ok &= check('first window (existing / applied chain / rejected)', r1, [
            ('existing', 'a1', 1, 1, '0', '1'), ('applied', 'a2', 1, 1, '1', '2'), ('applied', 'a3', 1, 2, '2', '4'),
            ('applied', 'b1', 2, 1, '0', '1'), ('rejected:invalid_amount', 'c1', 3, 0, '', ''), ('applied', 'c2', 3, 1, '0', '1'),
            ('rejected:unknown_account', 'z1', 9, 1, '', '')])
        ok &= check('balances after first window', balances(), [f'{A1}|4', f'{A2}|1', f'{A3}|1'])
        r2 = lines(window([(A1, 'a1', 999), (A1, 'a2', 1), (A9, 'z1', 1)]))
        ok &= check('replay returns original records (a1 retried with amount 999), no mutation', r2, [
            ('existing', 'a1', 1, 1, '0', '1'), ('existing', 'a2', 1, 1, '1', '2'), ('rejected:unknown_account', 'z1', 9, 1, '', '')])
        ok &= check('balances unchanged after replay', balances(), [f'{A1}|4', f'{A2}|1', f'{A3}|1'])
        r3 = lines(window([(A2, 'd2', 5), (A2, 'd1', 7)]))   # input order defines the chain, not tx id order
        ok &= check('before/after chain follows input order', r3, [('applied', 'd1', 2, 7, '6', '13'), ('applied', 'd2', 2, 5, '1', '6')])
        p = window([(A3, 'e1', 1), (A3, 'e1', 1)])
        ok &= check('duplicate key inside one input fails with unique violation (23505)', ('23505' in p.stderr or 'duplicate key' in p.stderr), True)
        ok &= check('balances unchanged after failed statement', balances(), [f'{A1}|4', f'{A2}|13', f'{A3}|1'])
        n = psql("SELECT count(*) FROM ledger;").stdout.strip()
        ok &= check('ledger rows (a1 + a2 a3 b1 c2 d1 d2)', n, '7')
    finally:
        subprocess.run(['docker', 'rm', '-f', '-v', NAME], capture_output=True)
    print('RESULT', 'PASS' if ok else 'FAIL')
    return 0 if ok else 1

if __name__ == '__main__':
    sys.exit(main())
