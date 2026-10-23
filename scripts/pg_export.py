"""Export a Postgres database to a directory: DDL snapshot + CSV per table.

Data goes out with COPY ... TO STDOUT WITH CSV HEADER, so it round-trips
through COPY ... FROM without type-guessing. Schema is captured as text for
reference and for restoring anything the target app's own migrations don't build
(indexes, constraints, sequence positions).
"""
import os, sys, io, json, pathlib, psycopg2
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DSN = os.environ["DSN"]; OUT = pathlib.Path(os.environ["OUT"])
SKIP = {"pg_catalog", "information_schema"}
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "data").mkdir(exist_ok=True)

c = psycopg2.connect(DSN, connect_timeout=30); c.set_session(readonly=True, autocommit=True)
def q(sql, a=None):
    cur = c.cursor(); cur.execute(sql, a); r = cur.fetchall(); cur.close(); return r

tables = q("""SELECT table_schema, table_name FROM information_schema.tables
              WHERE table_type='BASE TABLE' AND table_schema NOT IN %s
              AND table_schema NOT LIKE 'pg_%%' ORDER BY 1,2""", (tuple(SKIP),))

meta = {"tables": [], "indexes": [], "constraints": [], "sequences": [], "columns": {}, "views": []}

for s, t in tables:
    cols = q("""SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position""", (s, t))
    meta["columns"]["%s.%s" % (s, t)] = [list(x) for x in cols]
    n = q('SELECT count(*) FROM "%s"."%s"' % (s, t))[0][0]
    meta["tables"].append({"schema": s, "table": t, "rows": n})
    path = OUT / "data" / ("%s.%s.csv" % (s, t))
    cur = c.cursor()
    with io.open(path, "w", encoding="utf-8", newline="") as fh:
        cur.copy_expert('COPY (SELECT * FROM "%s"."%s") TO STDOUT WITH CSV HEADER' % (s, t), fh)
    cur.close()
    print("  %-46s %8d rows  %8d bytes" % ("%s.%s" % (s, t), n, path.stat().st_size))

meta["indexes"]    = [list(r) for r in q("SELECT schemaname, indexname, indexdef FROM pg_indexes WHERE schemaname NOT IN %s ORDER BY 1,2", (tuple(SKIP),))]
meta["constraints"]= [list(r) for r in q("""SELECT n.nspname, rel.relname, con.conname, pg_get_constraintdef(con.oid)
                        FROM pg_constraint con JOIN pg_class rel ON rel.oid=con.conrelid
                        JOIN pg_namespace n ON n.oid=rel.relnamespace
                        WHERE n.nspname NOT IN %s ORDER BY 1,2,3""", (tuple(SKIP),))]
meta["views"]      = [list(r) for r in q("SELECT schemaname, viewname, definition FROM pg_views WHERE schemaname NOT IN %s", (tuple(SKIP),))]
seqs = q("SELECT sequence_schema, sequence_name FROM information_schema.sequences WHERE sequence_schema NOT IN %s", (tuple(SKIP),))
for ss, sn in seqs:
    lv = q('SELECT last_value, is_called FROM "%s"."%s"' % (ss, sn))[0]
    meta["sequences"].append([ss, sn, lv[0], lv[1]])

io.open(OUT / "schema.json", "w", encoding="utf-8").write(json.dumps(meta, ensure_ascii=False, indent=1, default=str))
print("\ntables=%d  rows=%d  sequences=%d  indexes=%d  constraints=%d"
      % (len(meta["tables"]), sum(t["rows"] for t in meta["tables"]),
         len(meta["sequences"]), len(meta["indexes"]), len(meta["constraints"])))
print("written to %s" % OUT)
