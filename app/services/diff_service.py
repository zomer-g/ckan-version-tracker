import logging
from typing import Any

import dictdiffer

logger = logging.getLogger(__name__)

# Fields to exclude from diff display
NOISY_FIELDS = {
    "metadata_modified",
    "metadata_created",
    "revision_id",
    "tracking_summary",
    "num_tags",
    "num_resources",
}


def compute_metadata_diff(old_meta: dict, new_meta: dict) -> list[dict]:
    """
    Compute a structured diff between two metadata snapshots.
    Returns a list of changes, each with type, field, old_value, new_value.
    """
    changes = []

    for diff_type, field, values in dictdiffer.diff(old_meta, new_meta):
        field_str = _field_to_str(field)

        # Skip noisy fields
        if any(noisy in field_str for noisy in NOISY_FIELDS):
            continue

        if diff_type == "change":
            old_val, new_val = values
            changes.append({
                "type": "changed",
                "field": field_str,
                "old_value": _truncate(old_val),
                "new_value": _truncate(new_val),
            })
        elif diff_type == "add":
            for key, val in values:
                changes.append({
                    "type": "added",
                    "field": f"{field_str}.{key}" if field_str else str(key),
                    "old_value": None,
                    "new_value": _truncate(val),
                })
        elif diff_type == "remove":
            for key, val in values:
                changes.append({
                    "type": "removed",
                    "field": f"{field_str}.{key}" if field_str else str(key),
                    "old_value": _truncate(val),
                    "new_value": None,
                })

    return changes


def _field_to_str(field) -> str:
    if isinstance(field, (list, tuple)):
        return ".".join(str(f) for f in field)
    return str(field)


def _truncate(value: Any, max_len: int = 500) -> Any:
    if isinstance(value, str) and len(value) > max_len:
        return value[:max_len] + "..."
    return value
