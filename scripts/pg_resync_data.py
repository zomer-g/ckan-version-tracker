"""Refresh an already-migrated copy: truncate the target's tables and re-COPY
every row from the source.

Schema is assumed to exist on the target already (put there by the original
pg_dump, or by the app's own migrations). Only data moves, so this is the tool
for the cutover window: run it once the source has stopped taking writes, and
the copy is current to the row.

Foreign keys are honoured without superuser tricks. xhostd's per-channel role is
not a superuser, so `session_replication_role = replica` is unavailable; instead
tables are loaded in repeated passes and any table whose load fails on a foreign
key is retried after its parents land. The loop stops when a pass makes no
progress, which surfaces a genuine cycle instead of hanging.

Rows move in COPY's BINARY format, not CSV. CSV round-tripping broke on a bytea
column holding real image bytes ("extra data after last expected column"), and
text escaping is a whole class of bug this does not need: binary is exact, needs
no quoting or encoding decisions, and both ends run the same Postgres major
version. It does require identical column order and types, which the parity
check already proves.

Columns are listed explicitly rather than using SELECT * / COPY table. A STORED
generated column is returned by SELECT * but is NOT accepted by COPY FROM, so the
star form sends one field more than the target expects ("row field count is 10,
expected 9"). pg_dump has always known to skip these; this tool has to as well.
"""
import io
import os
import sys

import psycopg2

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

SRC = os.environ["SRC_DSN"]
DST = os.environ["DST_DSN"]
SCHEMAS = tuple(os.environ.get("SCHEMAS", "public").split(","))
APPLY = os.environ.get("APPLY") == "1"

src = psycopg2.connect(SRC, connect_timeout=30); src.set_session(readonly=True, autocommit=True)
dst = psycopg2.connect(DST, connect_timeout=30); dst.autocommit = False


def rows(conn, sql, args=None):
    cur = conn.cursor(); cur.execute(sql, args); out = cur.fetchall(); cur.close(); return out


def copyable_columns(conn, schema, table):
    """Live, non-generated columns in attribute order — exactly what COPY accepts."""
    return [
        r[0] for r in rows(conn,
            """SELECT a.attname FROM pg_attribute a
               WHERE a.attrelid = %s::regclass
                 AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = ''
               ORDER BY a.attnum""", ('"%s"."%s"' % (schema, table),))
    ]


tables = [
    (s, t) for s, t in rows(src,
        """SELECT table_schema, table_name FROM information_schema.tables
           WHERE table_type='BASE TABLE' AND table_schema = ANY(%s) ORDER BY 1,2""",
        (list(SCHEMAS),))
]
print("source tables: %d" % len(tables))

if not APPLY:
    print("\nDRY RUN — set APPLY=1 to write. Would truncate and reload:")
    for s, t in tables:
        n = rows(src, 'SELECT count(*) FROM "%s"."%s"' % (s, t))[0][0]
        print("   %-46s %8d rows" % ("%s.%s" % (s, t), n))
    sys.exit(0)

qualified = ", ".join('"%s"."%s"' % (s, t) for s, t in tables)
cur = dst.cursor()
cur.execute("TRUNCATE %s CASCADE" % qualified)
print("truncated %d tables on target" % len(tables))

pending = list(tables)
loaded, total_rows = [], 0
while pending:
    progressed = []
    failed = []
    for s, t in pending:
        src_cols = copyable_columns(src, s, t)
        dst_cols = copyable_columns(dst, s, t)
        if src_cols != dst_cols:
            failed.append(((s, t), "column lists differ: source=%s target=%s" % (src_cols, dst_cols)))
            continue
        collist = ", ".join('"%s"' % c for c in src_cols)
        buf = io.BytesIO()
        sc = src.cursor()
        sc.copy_expert('COPY (SELECT %s FROM "%s"."%s") TO STDOUT (FORMAT BINARY)' % (collist, s, t), buf)
        sc.close()
        buf.seek(0)
        try:
            cur.execute("SAVEPOINT load")
            cur.copy_expert('COPY "%s"."%s" (%s) FROM STDIN (FORMAT BINARY)' % (s, t, collist), buf)
            cur.execute("RELEASE SAVEPOINT load")
            n = rows(src, 'SELECT count(*) FROM "%s"."%s"' % (s, t))[0][0]
            print("   loaded %-44s %8d rows" % ("%s.%s" % (s, t), n))
            progressed.append((s, t)); total_rows += n
        except psycopg2.Error as exc:
            cur.execute("ROLLBACK TO SAVEPOINT load")
            failed.append(((s, t), str(exc).strip().splitlines()[0]))
    if not progressed:
        print("\nno progress; unresolved:")
        for (s, t), err in failed:
            print("   %-46s %s" % ("%s.%s" % (s, t), err))
        dst.rollback(); sys.exit(1)
    loaded += progressed
    pending = [k for k, _ in failed]

# sequences follow the data, or the next insert collides with a restored row
seqs = rows(src, """SELECT sequence_schema, sequence_name FROM information_schema.sequences
                    WHERE sequence_schema = ANY(%s)""", (list(SCHEMAS),))
for ss, sn in seqs:
    last, called = rows(src, 'SELECT last_value, is_called FROM "%s"."%s"' % (ss, sn))[0]
    cur.execute('SELECT setval(%s, %s, %s)', ("%s.%s" % (ss, sn), last, called))
print("sequences aligned: %d" % len(seqs))

dst.commit()
print("\ncommitted — %d tables, %d rows" % (len(loaded), total_rows))
