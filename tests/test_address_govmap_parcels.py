"""נדל"ן לעם — folding GovMap's swept parcel addresses into the spine.

The third external address source, and the one whose coordinate is the WEAKEST
while its parcel link is the strongest. Both halves of that are pinned here,
because the merge is only an improvement if it keeps them apart: every row it
carries is a parcel centroid (353 addresses were measured sharing one point),
and every row also carries the parcel GovMap itself says the address sits on.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import inspect  # noqa: E402

from app.services import address_govmap_parcels as gp  # noqa: E402


# ── never overwrite a better point ────────────────────────────────────────────
def test_an_existing_point_is_never_overwritten():
    """The register is exact to the centimetre and the geocoder measured a
    median 1.5 m. A parcel centroid is "somewhere on the plot" and has no
    business displacing either."""
    src = inspect.getsource(gp.merge)
    assert "o.point IS NULL" in src, "the point UPDATE must be restricted to empty points"


def test_the_point_is_stamped_apart_from_the_geocoder():
    """'govmap' is the geocoder's stamp. Reusing it here would erase the only
    signal that tells a 1.5 m geocode from a whole-parcel centroid — which is
    the entire reason point_source exists."""
    assert gp.POINT_SOURCE == "govmap_parcel"
    src = inspect.getsource(gp.merge)
    assert "point_source = '" in src
    assert "'govmap'" not in src


def test_the_insert_cannot_clobber_an_existing_row():
    src = inspect.getsource(gp.merge)
    assert "ON CONFLICT (address_key) DO NOTHING" in src


# ── the parcel, which is what this source is actually authoritative about ─────
def test_the_parcel_link_is_filled_only_where_there_is_none():
    src = inspect.getsource(gp.merge)
    assert "o.parcel_key IS NULL" in src


def test_the_parcel_link_is_labelled_and_not_passed_off_as_point_in_polygon():
    """parcel_match already means 'pip' (exact) or 'street' (approximate). A
    link that came from GovMap's own answer is neither, and a reader auditing a
    parcel has to be able to tell which of the three produced it."""
    src = inspect.getsource(gp.merge)
    assert "parcel_match = '" in src
    assert "'pip'" not in src


def test_a_parcel_is_resolved_once_per_parcel_not_once_per_address():
    """Up to 353 addresses share one parcel centroid, and over_parcel_at() is a
    spatial lookup. Resolving per address would multiply the cost by the number
    of doorways on the plot."""
    src = inspect.getsource(gp.resolve_parcels)
    assert "DISTINCT ON (parcel_object_id)" in src


def test_an_unanswered_parcel_is_still_marked_resolved():
    """GovMap's index and the shape layer are separate publications; a parcel
    in one and not the other resolves to NULL legitimately. Without the flag,
    every merge would ask about it again forever."""
    src = inspect.getsource(gp.resolve_parcels)
    assert "parcel_resolved = true" in src
    assert "NOT parcel_resolved" in src


def test_resolution_happens_before_the_merge_reads_parcel_keys():
    """A merge that ran before resolution would fill no parcel — and then find
    nothing left to fill next time, because the POINT is already set and the
    row is no longer new."""
    src = inspect.getsource(gp.merge)
    assert "await resolve_parcels()" in src
    assert src.index("await resolve_parcels()") < src.index("UPDATE public")


# ── keying ────────────────────────────────────────────────────────────────────
def test_the_settlement_joins_on_the_cbs_code_not_on_a_name():
    """438 of 439 swept settlement codes resolve in over_settlements and the
    names agree exactly. So the seam the municipal layers have to work around
    — matching a town by its spelling — simply does not exist here, and
    reintroducing it would be a regression."""
    src = inspect.getsource(gp._keyed_cte)
    assert "g.settlement_code" in src
    assert "over_settlement_code(" not in src


def test_an_unresolvable_street_is_dropped_rather_than_invented():
    """11% of swept rows name their street 'רח' 7002' — GovMap's street code as
    a name. The spine's own build owns the xy:/zip: fallback keys for
    street-less addresses; minting more of them from here would create rows a
    rebuild cannot reconcile."""
    src = inspect.getsource(gp._distinct_cte)
    assert "street_key IS NOT NULL" in src


def test_the_address_key_matches_the_spine_format():
    """`{settlement}|{street_key}|{house}{suffix}|{entrance}` — anything else
    creates a duplicate row ON CONFLICT cannot catch."""
    src = inspect.getsource(gp.merge)
    assert "d.sc || '|' || d.street_key || '|' ||" in src
    assert "d.house_num::text || '|'" in src


def test_the_settlement_name_comes_from_the_code():
    src = inspect.getsource(gp.merge)
    assert "over_settlements t ON t.code = d.sc" in src


def test_one_doorway_cannot_be_inserted_twice_in_one_pass():
    """Two address objects can name the same doorway; DISTINCT ON collapses
    them on the key the insert uses."""
    src = inspect.getsource(gp._distinct_cte)
    assert "DISTINCT ON (sc, street_key, house_num)" in src


def test_the_distinct_prefers_the_copy_that_has_a_parcel():
    """DISTINCT ON without ORDER BY keeps an arbitrary member of the group — so
    a doorway listed twice could keep the parcel-less copy and lose the link
    this source exists to supply."""
    src = inspect.getsource(gp._distinct_cte)
    assert "ORDER BY sc, street_key, house_num, (parcel_key IS NULL)" in src


# ── ingest ────────────────────────────────────────────────────────────────────
def test_ingest_is_idempotent_on_govmaps_own_address_id():
    """The sweep is restartable and re-walks ids after a crash, so the same
    address WILL arrive twice. It must land on the same row."""
    src = inspect.getsource(gp.record_batch)
    assert "ON CONFLICT (address_objectid) DO UPDATE" in src


def test_resending_an_address_does_not_reset_its_parcel_resolution():
    """A parcel's geometry does not change because the sweep walked past it
    twice, and re-resolving hundreds of thousands of parcels on every restart
    would be the entire cost of this feature."""
    src = inspect.getsource(gp.record_batch)
    # The assignment, not the word — the docstring says why it is absent.
    assert "parcel_resolved =" not in src
    assert "parcel_resolved=" not in src


def test_a_row_without_both_ids_is_dropped_not_stored_half_keyed():
    assert gp._row({"parcel_object_id": 1}) is None
    assert gp._row({"address_objectid": 1}) is None
    row = gp._row({"address_objectid": "485659", "parcel_object_id": "17",
                   "settlement_code": "1349", "house_num": "6",
                   "lat": "31.3003588", "lon": "35.0780688"})
    assert row is not None and row[0] == 485659 and row[1] == 17


def test_garbage_numbers_do_not_abort_a_batch():
    """One malformed field in a 15-row checkpoint must not cost the other 14."""
    row = gp._row({"address_objectid": "1", "parcel_object_id": "2",
                   "settlement_code": "", "house_num": "לא ידוע", "lat": None})
    assert row is not None
    assert row[2] is None and row[6] is None and row[7] is None


# ── what must not happen ──────────────────────────────────────────────────────
def test_a_dry_run_writes_nothing():
    src = inspect.getsource(gp.merge)
    assert "if dry_run:" in src
    assert src.index("if dry_run:") < src.index("UPDATE public")


def test_the_report_does_not_write():
    src = inspect.getsource(gp.report)
    for verb in ("INSERT", "UPDATE ", "DELETE"):
        assert verb not in src, f"{verb} has no business in a report"


def test_the_raw_ledger_is_kept_out_of_the_public_console():
    """The console runs free-form SQL, so hiding the table from /data hides the
    signpost and not the table. What the public gets is the reconciled result
    in over_re_addresses."""
    src = inspect.getsource(gp.ensure_tables)
    assert "_revoke_from_public_console" in src


# ── a rebuild must not delete what the merge added ────────────────────────────
def test_the_merge_is_a_build_stage():
    """`addresses` TRUNCATE+INSERTs the spine from the two source files alone,
    so a rebuild deletes every address this source discovered and nothing else
    puts them back."""
    from app.services import nadlan_index as ni
    assert "govmap_parcels" in ni.STAGES
    assert "govmap_parcels" in ni._BUILDERS


def test_it_is_in_the_default_set():
    from app.services import nadlan_index as ni
    assert "govmap_parcels" not in ni.DEFAULT_SKIP


def test_it_runs_after_the_better_point_sources_and_before_pip():
    """Each filler only fills a NULL point, so between two sources for the same
    doorway the earlier stage wins. A parcel centroid must therefore come after
    the municipal registers — and before `pip`, which links the rows it adds."""
    from app.services import nadlan_index as ni
    assert ni.STAGES.index("addresses") < ni.STAGES.index("govmap_parcels")
    assert ni.STAGES.index("municipal") < ni.STAGES.index("govmap_parcels")
    assert ni.STAGES.index("govmap_parcels") < ni.STAGES.index("pip")
