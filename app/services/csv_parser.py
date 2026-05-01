"""Parse CSV content into CKAN Datastore-compatible fields and records."""

import csv
import io
import logging
import re
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Max records per datastore_create/upsert batch
BATCH_SIZE = 5000


def parse_csv(content: bytes) -> tuple[list[dict], list[dict]]:
    """
    Parse CSV bytes into (fields, records) for CKAN Datastore.

    Returns:
        fields: [{"id": "col_name", "type": "text|integer|numeric|date"}, ...]
        records: [{"col_name": value, ...}, ...]
    """
    text = _decode(content)
    dialect = _detect_dialect(text)
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)

    if not reader.fieldnames:
        return [], []

    records: list[dict] = []
    for row in reader:
        cleaned = {k.strip(): _clean_value(v) for k, v in row.items() if k}
        records.append(cleaned)

    field_names = [f.strip() for f in reader.fieldnames if f]
    fields = _detect_field_types(field_names, records)

    # Cast values to detected types
    records = _cast_records(records, fields)

    return fields, records


def _decode(content: bytes) -> str:
    """Decode bytes trying UTF-8-BOM, UTF-8, then Windows-1255 (Hebrew)."""
    for encoding in ("utf-8-sig", "utf-8", "windows-1255", "iso-8859-8", "latin-1"):
        try:
            return content.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            continue
    return content.decode("utf-8", errors="replace")


def _detect_dialect(text: str) -> csv.Dialect:
    """Detect CSV dialect (comma vs tab vs semicolon)."""
    try:
        sample = text[:8192]
        return csv.Sniffer().sniff(sample, delimiters=",;\t|")
    except csv.Error:
        return csv.excel


def _clean_value(value: str | None) -> str | None:
    """Strip whitespace, return None for empty strings."""
    if value is None:
        return None
    v = value.strip()
    return v if v else None


def _detect_field_types(field_names: list[str], records: list[dict]) -> list[dict]:
    """Detect CKAN Datastore field types by scanning EVERY record.

    Sampling a prefix is unsafe: a single text value in row 9,000 of an
    otherwise-integer column demotes the column to text. If we mis-detect
    "integer" from a 100-row sample and that text value lands in a later
    batch, CKAN's typed INSERT returns 409 Conflict on the entire batch
    (no per-row error → all 2,500 rows fail). Records are already in
    memory at this point, so the full scan is essentially free.
    """
    fields = []

    for name in field_names:
        values = [r.get(name) for r in records if r.get(name) is not None]
        field_type = _infer_type(values)
        fields.append({"id": name, "type": field_type})

    return fields


def _infer_type(values: list[str]) -> str:
    """Infer CKAN Datastore type from a list of string values."""
    if not values:
        return "text"

    # Try integer
    if all(_is_integer(v) for v in values):
        return "integer"

    # Try numeric (float)
    if all(_is_numeric(v) for v in values):
        return "numeric"

    # Try date
    if all(_is_date(v) for v in values):
        return "date"

    return "text"


def _is_integer(v: str) -> bool:
    try:
        int(v.replace(",", ""))
        return True
    except (ValueError, AttributeError):
        return False


def _is_numeric(v: str) -> bool:
    try:
        float(v.replace(",", ""))
        return True
    except (ValueError, AttributeError):
        return False


_DATE_PATTERNS = [
    re.compile(r"^\d{4}-\d{2}-\d{2}"),  # 2026-04-10 or 2026-04-10T...
    re.compile(r"^\d{2}/\d{2}/\d{4}$"),  # 10/04/2026
    re.compile(r"^\d{2}\.\d{2}\.\d{4}$"),  # 10.04.2026
]


def _is_date(v: str) -> bool:
    return any(p.match(v) for p in _DATE_PATTERNS)


def _cast_records(records: list[dict], fields: list[dict]) -> list[dict]:
    """Cast string values to their detected types.

    On cast failure (a value the detector said was castable but isn't —
    should be impossible after the full-scan detector but kept as a
    belt-and-suspenders), fall back to NULL rather than the original
    string. CKAN datastore rejects a typed INSERT outright if any row
    has the wrong type, returning 409 for the whole batch — losing one
    cell to NULL is better than dropping 2,500 rows.
    """
    type_map = {f["id"]: f["type"] for f in fields}
    casted = []

    for row in records:
        new_row: dict[str, Any] = {}
        for key, val in row.items():
            if val is None:
                new_row[key] = None
            elif type_map.get(key) == "integer":
                try:
                    new_row[key] = int(val.replace(",", ""))
                except (ValueError, AttributeError):
                    new_row[key] = None
            elif type_map.get(key) == "numeric":
                try:
                    new_row[key] = float(val.replace(",", ""))
                except (ValueError, AttributeError):
                    new_row[key] = None
            else:
                new_row[key] = val
        casted.append(new_row)

    return casted


def batch_records(records: list[dict], batch_size: int = BATCH_SIZE) -> list[list[dict]]:
    """Split records into batches for chunked upload."""
    return [records[i : i + batch_size] for i in range(0, len(records), batch_size)]
