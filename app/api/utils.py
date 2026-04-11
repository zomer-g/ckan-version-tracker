import re
import uuid

from fastapi import HTTPException


def parse_uuid(value: str, label: str = "ID") -> uuid.UUID:
    """Parse a UUID string, raising 400 if invalid."""
    try:
        return uuid.UUID(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {label}")


def sanitize_ckan_name(name: str) -> str:
    """Create a CKAN-safe dataset name (lowercase alphanumeric, hyphens, underscores)."""
    safe = re.sub(r"[^a-z0-9_-]", "-", name.lower())
    safe = re.sub(r"-+", "-", safe).strip("-")
    return safe[:80]
