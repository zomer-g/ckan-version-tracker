"""A sampling stamp must not be part of a row's identity.

A source that records WHEN it looked at an item writes that moment into the row.
Hashed, it makes every pass a fresh identity and the archive files an unchanged
item again on every run — the plan register grew to 178,241 rows for 38,111
plans that way. Excluded, the identity is the content; refreshed on conflict, the
row still says when it was last confirmed.
"""
from app.services.append_store import build_insert, row_hash

COLS = ["מספר תכנית", "סטטוס", "תאריך דגימה"]
STAMP = "תאריך דגימה"


def _row(plan="203-1303767", status="בבדיקה תכנונית", stamp="2026-08-08T08:06:46Z"):
    return {"מספר תכנית": plan, "סטטוס": status, STAMP: stamp}


# ── the identity ─────────────────────────────────────────────────────────────

def test_two_passes_over_an_unchanged_plan_are_one_identity():
    a = row_hash(_row(stamp="2026-08-08T08:06:46Z"), COLS, exclude=(STAMP,))
    b = row_hash(_row(stamp="2026-08-14T21:49:03Z"), COLS, exclude=(STAMP,))
    assert a == b


def test_without_the_exclusion_they_are_two_identities():
    # The bug, pinned: this is what produced 4.68 copies per plan.
    a = row_hash(_row(stamp="2026-08-08T08:06:46Z"), COLS)
    b = row_hash(_row(stamp="2026-08-14T21:49:03Z"), COLS)
    assert a != b


def test_a_real_change_is_still_a_new_identity():
    a = row_hash(_row(status="בבדיקה תכנונית"), COLS, exclude=(STAMP,))
    b = row_hash(_row(status="הפקדה להתנגדויות/השגות"), COLS, exclude=(STAMP,))
    assert a != b


def test_exclusion_does_not_drop_the_column_from_the_row():
    sql, params = build_insert("t", COLS, [_row()], key_col=None, keyless=True,
                               stamp_col=STAMP)
    assert "2026-08-08T08:06:46Z" in params
    assert '"תאריך דגימה"' in sql


# ── the conflict clause ──────────────────────────────────────────────────────

def test_the_stamp_is_refreshed_on_conflict():
    sql, _ = build_insert("t", COLS, [_row()], key_col=None, keyless=True,
                          stamp_col=STAMP)
    assert 'DO UPDATE SET "תאריך דגימה" = EXCLUDED."תאריך דגימה"' in sql


def test_no_stamp_column_keeps_do_nothing():
    # Every other source must behave exactly as before.
    sql, _ = build_insert("t", COLS, [_row()], key_col=None, keyless=True)
    assert "DO NOTHING" in sql and "DO UPDATE" not in sql


def test_a_stamp_the_table_does_not_carry_is_ignored():
    # A manifest naming a column the CSV never produced must not build SQL
    # referring to it.
    sql, _ = build_insert("t", ["a", "b"], [{"a": "1", "b": "2"}],
                          key_col=None, keyless=True, stamp_col="לא קיים")
    assert "DO NOTHING" in sql and "לא קיים" not in sql


def test_only_the_stamp_is_updated_never_content():
    sql, _ = build_insert("t", COLS, [_row()], key_col=None, keyless=True,
                          stamp_col=STAMP)
    update = sql.split("DO UPDATE SET", 1)[1]
    assert '"סטטוס"' not in update and '"מספר תכנית"' not in update


# ── the trap DO UPDATE brings with it ────────────────────────────────────────

def test_one_chunk_cannot_touch_the_same_row_twice():
    """Postgres raises if DO UPDATE hits a row twice in one statement, so two
    readings of one unchanged plan must collapse before the SQL is built."""
    rows = [_row(stamp="2026-08-08T08:06:46Z"), _row(stamp="2026-08-14T21:49:03Z")]
    sql, _ = build_insert("t", COLS, rows, key_col=None, keyless=True,
                          stamp_col=STAMP)
    assert sql.count("),(") == 0          # one VALUES tuple, not two
    assert sql.count("DO UPDATE") == 1


def test_two_genuinely_different_plans_both_survive_the_chunk():
    rows = [_row(plan="203-1303767"), _row(plan="101-1207158")]
    sql, _ = build_insert("t", COLS, rows, key_col=None, keyless=True,
                          stamp_col=STAMP)
    assert sql.count("),(") == 1          # two VALUES tuples


def test_keyed_tables_are_untouched_by_the_stamp_option():
    # A keyed table conflicts on its key; the stamp option must not change that
    # target, only what happens on conflict.
    sql, _ = build_insert("t", COLS, [_row()], key_col="מספר תכנית", keyless=False,
                          stamp_col=STAMP)
    assert 'ON CONFLICT ("מספר תכנית")' in sql


# ── every writer must pass it, or the fix reverts for that path alone ────────

def test_no_append_rows_call_site_forgets_the_stamp():
    """The first attempt fixed two of seven call sites and looked correct: the
    tests passed, the streaming loaders deduped, and the very next production
    run still added a full 37,039-row copy — because the scraper reaches NEON
    through push_version, not through those loaders. A missing stamp_col is
    invisible at every level except the row count days later, so it is checked
    here instead of trusted."""
    import ast
    import pathlib

    root = pathlib.Path(__file__).resolve().parent.parent / "app"
    missing = []
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            f = node.func
            if not (isinstance(f, ast.Attribute) and f.attr == "append_rows"):
                continue
            if not any(k.arg == "stamp_col" for k in node.keywords):
                missing.append(f"{path.relative_to(root.parent)}:{node.lineno}")
    assert not missing, (
        "append_rows called without stamp_col — a sampled source writing "
        "through these paths will duplicate every row on every pass:\n  "
        + "\n  ".join(missing))
