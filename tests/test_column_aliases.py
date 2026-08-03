"""Unit tests for the column-caption index (no DB needed).

Covers the pure half of app/services/column_aliases.py: reading a GovMap layer's
field dictionary out of the documentation bundle OVER already archives, and
attaching those captions to catalog columns. The write half (upsert + version
checkpoint) needs a live append DB and is exercised on deploy.
"""
import asyncio
import io
import os
import sys
import zipfile

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.services import append_store as A  # noqa: E402
from app.services import column_aliases as CA  # noqa: E402

# A real sidecar, trimmed: layer 217940's dictionary as the scraper writes it.
FIELDS_CSV = (
    "layer_id,machine_name,hebrew_alias,type,is_served,display_order\n"
    "217940,name,שם האתר,text,1,1\n"
    "217940,description,תיאור,text,1,2\n"
    "217940,x,,text,1,5\n"                  # served, but GovMap never captioned it
    "217940,no,מספר,,0,\n"                  # documented, never served
    "217940,objectId,objectId,text,1,0\n"   # alias echoes the name → no alias
)


def _bundle(*csvs: tuple[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("symbology_index.csv", "layer_id,caption\n217940,אנדרטאות\n")
        z.writestr("אנדרטאות_217940.sld", "<StyledLayerDescriptor/>")
        for name, body in csvs:
            z.writestr(name, "﻿" + body)
    return buf.getvalue()


# ── parsing the dictionary ───────────────────────────────────────────────────

def test_parse_keeps_only_real_captions():
    d = CA.parse_fields_csv(FIELDS_CSV)
    assert d == {"name": "שם האתר", "description": "תיאור", "no": "מספר"}


def test_parse_ignores_alias_that_echoes_the_machine_name():
    # GovMap echoes the machine name when it has no caption; that is NOT a label.
    assert "objectId" not in CA.parse_fields_csv(FIELDS_CSV)


def test_dictionary_found_by_suffix_not_by_name():
    # The sidecar is named after the layer's Hebrew caption, so it can only be
    # located by its _fields.csv suffix.
    blob = _bundle(("אנדרטאות_מ.א._רמת_הנגב_fields.csv", FIELDS_CSV))
    assert CA.dictionary_from_bundle(blob)["name"] == "שם האתר"


def test_bundle_without_a_dictionary_is_empty_not_an_error():
    assert CA.dictionary_from_bundle(_bundle()) == {}


def test_several_dictionaries_in_one_bundle_merge():
    other = ("layer_id,machine_name,hebrew_alias,type,is_served,display_order\n"
             "23,pop_total,סה״כ אוכלוסייה,text,1,1\n")
    blob = _bundle(("a_fields.csv", FIELDS_CSV), ("b_fields.csv", other))
    d = CA.dictionary_from_bundle(blob)
    assert d["name"] == "שם האתר" and d["pop_total"] == "סה״כ אוכלוסייה"


# ── the version resource that carries it ─────────────────────────────────────

def test_doc_values_reads_the_list_valued_mapping():
    maps = {"_symbology": ["r2:datasets/x/v1/a_symbology.zip"], "נתוני הסורק": "r2:x"}
    assert CA._doc_values(maps) == ["r2:datasets/x/v1/a_symbology.zip"]


def test_doc_values_accepts_a_bare_value_and_skips_non_storage():
    assert CA._doc_values({"_symbology": "r2:k"}) == ["r2:k"]
    assert CA._doc_values({"_symbology": "not-a-storage-value"}) == []
    assert CA._doc_values({}) == []


# ── attaching captions to catalog columns ────────────────────────────────────

def test_apply_labels_only_the_columns_it_knows():
    cols = [{"name": "name", "type": "text"}, {"name": "geom", "type": "geometry"}]
    out = CA.apply(cols, {"name": "שם האתר"})
    assert out[0]["alias"] == "שם האתר"
    assert "alias" not in out[1]


def test_apply_does_not_mutate_the_shared_column_list():
    # The lists come from append_store's catalog helpers and are reused across
    # catalog records — labelling one record must not label them all.
    cols = [{"name": "name", "type": "text"}]
    CA.apply(cols, {"name": "שם האתר"})
    assert cols == [{"name": "name", "type": "text"}]


def test_apply_without_aliases_returns_the_columns_untouched():
    cols = [{"name": "a", "type": "text"}]
    assert CA.apply(cols, None) is cols
    assert CA.apply(cols, {}) is cols


# ── writing: the live table decides, never the dictionary ────────────────────

class FakeConn:
    """Enough asyncpg surface for _write: a table's live columns + a log."""

    def __init__(self, live: dict[tuple[str, str], list[str]]):
        self.live = live
        self.deleted: list[tuple] = []
        self.inserted: list[tuple] = []

    async def fetch(self, _sql, schema, table):
        return [{"column_name": c} for c in self.live.get((schema, table), [])]

    async def execute(self, _sql, *args):
        self.deleted.append(args)

    async def executemany(self, _sql, rows):
        self.inserted.extend(rows)


def test_write_matches_case_insensitively_and_skips_absent_columns():
    conn = FakeConn({("idx", "govmap_1_x"): ["ShemYishuv", "pop_total", "geom"]})
    n = asyncio.run(CA._write(conn, [("idx", "govmap_1_x")],
                              {"shemyishuv": "שם יישוב",   # differs only in case
                               "pop_total": "אוכלוסייה",
                               "no": "מספר"},               # documented, not a column
                              "govmap"))
    assert n == 2
    written = {r[2]: r[3] for r in conn.inserted}
    # The PHYSICAL column name is written, not the dictionary's spelling.
    assert written == {"ShemYishuv": "שם יישוב", "pop_total": "אוכלוסייה"}


def test_write_clears_a_tables_captions_before_rewriting_them():
    # A field that lost its caption upstream must lose it here too, so the
    # rewrite is delete-then-insert rather than an upsert of survivors.
    conn = FakeConn({("idx", "t"): ["a"]})
    asyncio.run(CA._write(conn, [("idx", "t")], {}, "govmap"))
    assert conn.deleted == [("idx", "t")]
    assert conn.inserted == []


# ── the caption reaches the copy-to-AI DDL ───────────────────────────────────

def test_ddl_shows_the_caption_next_to_the_name():
    ddl = A.format_schema_ddl([{
        "table": "govmap_23_x",
        "description": "אזורים סטטיסטיים",
        "columns": [{"name": "shem_yishuv", "type": "text", "alias": "שם יישוב"},
                    {"name": "objectId", "type": "text"}],
    }])
    assert "shem_yishuv text,  -- שם יישוב" in ddl
    # A column with no caption gets no trailing comment (and mixed case still
    # comes out quoted, which is the only form SQL accepts).
    assert '"objectId" text\n' in ddl
