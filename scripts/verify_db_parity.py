"""Row-level parity check between two Postgres databases.

Compares, for every table in the named schemas:
  - presence
  - column list (name + type, in ordinal order)
  - count(*)
  - an ORDER-INDEPENDENT checksum of every row:
        sum( ('x'||substr(md5(t::text),1,8))::bit(32)::bigint )
    one sequential pass, no sort, digest computed server-side.

Exit code 0 = identical, 1 = any difference.
"""
import os, sys, time
import psycopg2

SCHEMAS = tuple(os.environ.get("SCHEMAS", "app,data,ref,public").split(","))
A_NAME, B_NAME = os.environ.get("A_NAME", "A"), os.environ.get("B_NAME", "B")

TABLES_SQL = """
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_type='BASE TABLE' AND table_schema = ANY(%s)
ORDER BY 1,2
"""
COLS_SQL = """
SELECT table_schema||'.'||table_name, ordinal_position, column_name, data_type
FROM information_schema.columns
WHERE table_schema = ANY(%s)
ORDER BY 1, ordinal_position
"""

def connect(dsn):
    c = psycopg2.connect(dsn, connect_timeout=30)
    c.set_session(readonly=True, autocommit=True)
    return c

def fetch(conn, sql, args=None):
    cur = conn.cursor(); cur.execute(sql, args); r = cur.fetchall(); cur.close(); return r

def probe(conn, schema, table):
    q = ('SELECT count(*), '
         'sum((\'x\'||substr(md5(t::text),1,8))::bit(32)::bigint) '
         'FROM "%s"."%s" t' % (schema, table))
    cur = conn.cursor(); cur.execute(q); n, ck = cur.fetchone(); cur.close()
    return n, (ck if ck is not None else 0)

def main():
    a = connect(os.environ["A_DSN"]); b = connect(os.environ["B_DSN"])
    ta = {(s, t) for s, t in fetch(a, TABLES_SQL, (list(SCHEMAS),))}
    tb = {(s, t) for s, t in fetch(b, TABLES_SQL, (list(SCHEMAS),))}

    print("schemas checked : %s" % ", ".join(SCHEMAS))
    print("tables in %-7s: %d" % (A_NAME, len(ta)))
    print("tables in %-7s: %d" % (B_NAME, len(tb)))

    bad = 0
    only_a, only_b = sorted(ta - tb), sorted(tb - ta)
    for s, t in only_a:
        print("MISSING in %s : %s.%s" % (B_NAME, s, t)); bad += 1
    for s, t in only_b:
        print("EXTRA   in %s : %s.%s" % (B_NAME, s, t)); bad += 1

    ca = {}; cb = {}
    for k, _, name, typ in fetch(a, COLS_SQL, (list(SCHEMAS),)): ca.setdefault(k, []).append((name, typ))
    for k, _, name, typ in fetch(b, COLS_SQL, (list(SCHEMAS),)): cb.setdefault(k, []).append((name, typ))

    common = sorted(ta & tb)
    print("\ncomparing %d common tables ...\n" % len(common))
    t0 = time.time()
    for i, (s, t) in enumerate(common, 1):
        key = "%s.%s" % (s, t)
        if ca.get(key) != cb.get(key):
            print("COLUMNS  %-58s differ" % key); bad += 1
        try:
            na, cka = probe(a, s, t)
            nb, ckb = probe(b, s, t)
        except Exception as e:
            print("ERROR    %-58s %s" % (key, str(e).strip().splitlines()[0])); bad += 1; continue
        if na != nb:
            print("COUNT    %-58s %s=%d  %s=%d" % (key, A_NAME, na, B_NAME, nb)); bad += 1
        elif cka != ckb:
            print("CHECKSUM %-58s %s=%s  %s=%s" % (key, A_NAME, cka, B_NAME, ckb)); bad += 1
        else:
            print("ok       %-58s %10d rows" % (key, na))
        if i % 25 == 0:
            sys.stdout.flush()

    print("\n%s" % ("-" * 72))
    print("elapsed %.1fs" % (time.time() - t0))
    if bad:
        print("RESULT: %d DIFFERENCE(S) — do NOT delete" % bad); return 1
    print("RESULT: IDENTICAL across %d tables — safe to delete %s" % (len(common), A_NAME))
    return 0

sys.exit(main())
