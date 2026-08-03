"""A manifest that calls itself neon_eligible must actually reach the SQL console.

Every hardcoded tabular kind (registries, munidata, emun, servicescompass) sets
``archive_neon`` in its own config branch, which is what makes approve_request
default the dataset to the dual R2+NEON plan rather than plain R2. Manifest
sources had no such branch: they declared ``neon_eligible: true``, OVER agreed
they were eligible — and then approve_request saw no opt-in in the config, chose
"r2", and apply_storage_target popped the NEON half back off. The rows were
archived as files and never became queryable, and nothing in the approval flow
said so. This is the same trap the note on TABULAR_SCRAPER_KINDS describes.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest

from app.api.datasets import _apply_registry_match, storage_target_of
from app.services import source_registry as sr


def _manifest(**over):
    base = {
        "manifest_version": 1,
        "id": "toyneon",
        "label_he": "מקור צעצוע",
        "label_en": "Toy Source",
        "site_url": "https://toy.example.org/",
        "badge": {"bg": "#e0f2fe", "fg": "#075985", "accent": "#0ea5e9"},
        "neon_eligible": True,
        "default_config": {"download_files": True},
        "url_patterns": [{"regex": r"^https?://toy\.example\.org/x$"}],
    }
    base.update(over)
    return sr.validate_manifest(base)


def _match(manifest):
    return sr.match_manifests("https://toy.example.org/x", [manifest])


def test_a_neon_eligible_manifest_turns_on_the_dual_write():
    config = _apply_registry_match(_match(_manifest()), {})
    assert config["archive_neon"] is True


def test_the_approval_card_then_shows_the_neon_plan():
    """storage_target_of is what the pending-request card renders, so this is
    the difference between an admin seeing 'R2' and 'R2 + NEON'."""
    config = _apply_registry_match(_match(_manifest()), {})
    assert "neon" in storage_target_of(config)


def test_a_file_only_manifest_is_left_alone():
    """A source that archives PDFs has no rows to query; forcing the dual write
    would promise a SQL console with nothing in it."""
    config = _apply_registry_match(_match(_manifest(neon_eligible=False)), {})
    assert "archive_neon" not in config


def test_an_explicit_caller_choice_still_wins():
    """An admin tuning one dataset must not be overridden by the default."""
    config = _apply_registry_match(_match(_manifest()), {"archive_neon": False})
    assert config["archive_neon"] is False


def test_the_manifest_config_still_comes_through():
    config = _apply_registry_match(_match(_manifest()), {})
    assert config["download_files"] is True
    assert config["kind"] == "toyneon"


def test_the_real_telegram_manifest_is_neon_eligible():
    """The source this was found on: one flat row per message, and the whole
    point of collecting a ministry's channel is searching what it said."""
    manifests = [m for m in [_telegram_manifest()] if m]
    if not manifests:
        pytest.skip("govscraper checkout not available")
    config = _apply_registry_match(
        sr.match_manifests("https://t.me/MOHreport", manifests), {},
    )
    assert config["archive_neon"] is True


def _telegram_manifest():
    import os.path
    import sys

    root = os.path.join(os.path.dirname(__file__), "..", "..", "GOV scraper")
    if not os.path.isdir(root):
        return None
    sys.path.insert(0, os.path.abspath(root))
    try:
        from govscraper.scrapers.telegram.manifest import MANIFEST
    except Exception:
        return None
    return sr.validate_manifest(MANIFEST)
