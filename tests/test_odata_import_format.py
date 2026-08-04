"""Regression tests for the מידע לעם (odata) import: blank formats + streaming.

Two defects kept "רשימת כתובות בישראל עם קואורדינטות" — 548,157 Israeli street
addresses with ITM coordinates, published as ``כתובות.xlsx`` — out of /data:

1. odata declares that resource with ``format: ""`` and no datastore, and both
   the admin UI and the importer keyed importability off that field alone. The
   file was shown as "לא ניתן לייבוא" and the endpoint rejected it, even though
   its name and download URL both end in ``.xlsx``.
2. The XLSX parser read every sheet whole, then built a second full copy of the
   cells as text. Peak Python heap for that file was ~240MB on a 512MB dyno,
   and the pass took minutes.

So: formats are now inferred from the file extension when the declared one is
blank, and XLSX is read as a stream.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services import odata_import as odi  # noqa: E402

# The resource exactly as odata's package_search returns it.
ADDRESSES_RESOURCE = {
    "id": "19c5be7f-e3d3-4fc6-9d65-c1d84e3256e8",
    "name": "כתובות.xlsx",
    "format": "",
    "datastore_active": False,
    "url": "https://www.odata.org.il/dataset/c4263736-9d34-4d6e-909e-4e4281652728"
           "/resource/19c5be7f-e3d3-4fc6-9d65-c1d84e3256e8/download/-3.xlsx",
}


# ── format inference ────────────────────────────────────────────────────────

def test_blank_format_falls_back_to_the_file_name():
    r = ADDRESSES_RESOURCE
    assert odi.infer_format(r["format"], r["name"], r["url"]) == "XLSX"
    assert odi.is_supported_file_format(r["format"], r["name"], r["url"])


def test_blank_format_falls_back_to_the_url_when_the_name_has_no_extension():
    assert odi.infer_format("", "כתובות", ADDRESSES_RESOURCE["url"]) == "XLSX"


def test_declared_format_wins_over_the_extension():
    # The publisher's own label is authoritative when it is one we support.
    assert odi.infer_format("CSV", "data.xlsx", None) == "CSV"


def test_extension_comes_from_the_path_not_the_query_string():
    assert odi.infer_format("", None, "https://x/f.csv?sig=abc.pdf") == "CSV"


def test_unsupported_stays_unsupported():
    assert odi.infer_format("", "report.pdf", "https://x/report.pdf") == ""
    assert not odi.is_supported_file_format("", "report.pdf")
    assert not odi.is_supported_file_format("PDF", "report.pdf")
    # No extension anywhere, nothing declared → nothing to go on.
    assert odi.infer_format("", "כתובות", None) == ""


# ── streaming vs. eager parsing ─────────────────────────────────────────────

def _sheet(header_offset=0, n=5):
    """A gov-style sheet: ``header_offset`` junk rows, a header, then data."""
    rows = [["דוח מרכז למיפוי ישראל", None, None] for _ in range(header_offset)]
    rows.append(["city", "street", "X"])
    rows += [[f"עיר {i}", f"רחוב {i}", 210427.4 + i] for i in range(n)]
    return rows


def test_stream_and_eager_parsers_agree():
    for offset in (0, 1, 3):
        rows = _sheet(header_offset=offset)
        eager_cols, eager_data = odi._rows_to_table(rows)
        stream_cols, stream_data = odi._stream_table(iter(rows))
        assert eager_cols == stream_cols == ["city", "street", "X"]
        assert list(stream_data) == eager_data
        # Every cell arrives as text — the odata tables are all-text by design.
        assert all(isinstance(v, str) for r in eager_data for v in r)


def test_stream_holds_only_the_scan_window():
    """The generator must not have consumed the source before it is iterated."""
    consumed = []

    def _rows():
        for r in _sheet(n=10_000):
            consumed.append(1)
            yield r

    columns, gen = odi._stream_table(_rows())
    assert columns == ["city", "street", "X"]
    # Only the header-scan window has been pulled so far, not all 10k rows.
    assert len(consumed) <= odi._HEADER_SCAN_ROWS
    assert sum(1 for _ in gen) == 10_000


def test_header_less_sheet_gets_synthetic_columns():
    rows = [[1, 2, 3], [4, 5, 6]]
    assert odi._rows_to_table(rows)[0] == ["col_1", "col_2", "col_3"]
    columns, gen = odi._stream_table(iter(rows))
    assert columns == ["col_1", "col_2", "col_3"]
    # A header-less file keeps ALL its rows — none is eaten as a header.
    assert list(gen) == [["1", "2", "3"], ["4", "5", "6"]]


def test_empty_input():
    assert odi._rows_to_table([]) == ([], [])
    columns, gen = odi._stream_table(iter([]))
    assert columns == [] and list(gen) == []


def test_batches_reach_the_loader_without_blocking_the_event_loop():
    """_load_rows pulls batches through a thread, so a minutes-long parse cannot
    freeze the app. The wrapper must still deliver every batch, in order."""
    import asyncio
    import threading

    loop_thread = threading.get_ident()
    seen_in = []

    def _slow_batches():
        for i in range(5):
            seen_in.append(threading.get_ident())
            yield [(f"row{i}",)]

    async def _drain():
        return [b async for b in odi._aiter_batches(_slow_batches())]

    out = asyncio.run(_drain())
    assert out == [[("row0",)], [("row1",)], [("row2",)], [("row3",)],
                   [("row4",)]]
    assert all(t != loop_thread for t in seen_in)


def _write_xlsx(path, sheets):
    """sheets: {name: rows} → a real .xlsx on disk."""
    import openpyxl
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for name, rows in sheets.items():
        ws = wb.create_sheet(name)
        for r in rows:
            ws.append(r)
    wb.save(path)
    wb.close()


def test_xlsx_loads_lazily_and_keeps_hebrew_headers(tmp_path):
    p = str(tmp_path / "addresses.xlsx")
    _write_xlsx(p, {
        "notes": [["הערות"], ["מקור: מרכז למיפוי"]],   # narrow junk tab
        "addresses": [["עיר", "רחוב", "X", "Y"]] +
                     [[f"עיר {i}", f"רחוב {i}", 210427.4 + i, 634575.6 + i]
                      for i in range(500)],
    })
    columns, batches = odi._open_for_load(p, "XLSX")
    # The widest sheet wins, and Hebrew header names survive as column names.
    assert columns == odi.append_store.safe_column_names(["עיר", "רחוב", "X", "Y"])
    assert len(columns) == 4
    # Batches are produced lazily — a generator, not a materialised list.
    assert not isinstance(batches, list)
    rows = [r for batch in batches for r in batch]
    assert len(rows) == 500
    assert rows[0][0] == "עיר 0"


def test_xlsx_release_the_workbook_when_the_generator_is_exhausted(tmp_path):
    p = str(tmp_path / "small.xlsx")
    _write_xlsx(p, {"s": [["a", "b"], [1, 2]]})
    columns, gen = odi._stream_xlsx(p)
    assert columns == ["a", "b"]
    assert list(gen) == [["1", "2"]]
    # The read handle is closed, so the file can be replaced (Windows would
    # raise PermissionError if openpyxl still held it).
    os.replace(p, p + ".done")
