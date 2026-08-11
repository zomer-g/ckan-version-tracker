"""Offline tests for the נדל"ן לעם index: the street matcher and the invariants
that keep the crosswalk honest. No database — `_resolve_streets` is deliberately
pure so the matching rules can be asserted with plain dicts.

The rule under the most scrutiny here is the ambiguity guard. The measured seam
this index exists to close is the gazetteer's street spellings against the
postal/address-list spellings (56% raw overlap), and the lever that closes it —
matching on the distinctive last word — is also the one that can mis-link
'דוד המלך' to 'שלמה המלך'. So a low-confidence variant that could pick either
street must pick NEITHER.
"""
from app.services import data_catalog, nadlan_index as ni


def _canon(sc, name, n=1, in_addr=True, in_post=False, street_id=None):
    return {"sc": sc, "name": name, "n": n, "in_addr": in_addr,
            "in_post": in_post, "street_id": street_id}


def _gaz(sc, name, n=1, code=None, name_en=None, sname="עיר"):
    return {"sc": sc, "name": name, "n": n, "code": code,
            "name_en": name_en, "sname": sname}


def _by_key(rows):
    return {r[0]: r for r in rows}


# ── canonical side ────────────────────────────────────────────────────────────
def test_canonical_street_keys_are_deterministic_text():
    streets, _aliases, _un = ni._resolve_streets([_canon(7900, "אבימלך")], [])
    # A deterministic key (not a surrogate id) is what lets a TRUNCATE+INSERT
    # rebuild reproduce the crosswalk without breaking anything downstream.
    assert _by_key(streets)["7900-אבימלך"][2] == "אבימלך"


def test_spelling_variants_collapse_onto_one_street():
    # geresh/spacing/hyphen differences must not create two streets.
    streets, _a, _u = ni._resolve_streets(
        [_canon(7900, "ג'בר", n=5), _canon(7900, "ג׳בר", n=2)], [])
    assert len(streets) == 1
    assert streets[0][2] == "ג'בר"       # the more frequent spelling wins


def test_postal_and_address_list_merge_into_one_street():
    streets, _a, _u = ni._resolve_streets(
        [_canon(7900, "רוטשילד", n=9, in_addr=True),
         _canon(7900, "רוטשילד", n=3, in_addr=False, in_post=True, street_id="079277")], [])
    row = _by_key(streets)["7900-רוטשילד"]
    assert row[7] is True and row[8] is True      # in_postal, in_address_list
    assert row[5] == "079277"                     # postal_street_id carried over


# ── the gazetteer seam ────────────────────────────────────────────────────────
def test_gazetteer_full_name_matches_the_short_canonical_name():
    """The whole point of the index: 'שמואל יבניאלי' must reach 'יבניאלי'."""
    streets, aliases, unmatched = ni._resolve_streets(
        [_canon(7900, "יבניאלי")],
        [_gaz(7900, "שמואל יבניאלי", code="79000600")])
    assert len(streets) == 1                       # not two separate streets
    assert unmatched == []
    assert _by_key(streets)["7900-יבניאלי"][6] == "79000600"   # gaztir code attached
    assert _by_key(streets)["7900-יבניאלי"][9] is True         # in_gazetteer
    # The gazetteer's own spelling is now an alias of the canonical street.
    assert ("שמואליבניאלי", "7900-יבניאלי") in {(a[1], a[2]) for a in aliases}


def test_street_type_prefix_does_not_split_a_street():
    streets, _a, unmatched = ni._resolve_streets(
        [_canon(7900, "רוטשילד")], [_gaz(7900, "שדרות רוטשילד")])
    assert len(streets) == 1 and unmatched == []


def test_ambiguous_last_token_links_to_neither_street():
    """'דוד המלך' and 'שלמה המלך' both end in 'המלך'.

    A variant that could designate either street must designate neither —
    otherwise the crosswalk silently attaches a property to the wrong road."""
    streets, aliases, _u = ni._resolve_streets(
        [_canon(7900, "דוד המלך"), _canon(7900, "שלמה המלך")], [])
    keyed = {(a[0], a[1]): a[2] for a in aliases}
    assert (7900, "המלך") not in keyed             # poisoned, not arbitrated
    # The unambiguous full forms still resolve.
    assert keyed[(7900, "דודהמלך")] == "7900-דודהמלך"
    assert keyed[(7900, "שלמההמלך")] == "7900-שלמההמלך"


def test_unmatched_gazetteer_street_still_becomes_a_real_street():
    """A street only the gazetteer knows is real; it just has no counterpart."""
    streets, _a, unmatched = ni._resolve_streets(
        [_canon(7900, "אבימלך")], [_gaz(7900, "רחוב שאיש לא רשם", n=4)])
    assert len(streets) == 2
    # ...and it is recorded as a visible, shrinkable work queue rather than lost.
    assert len(unmatched) == 1
    assert unmatched[0][2] == "gazetteer" and unmatched[0][4] == 4


def test_streets_are_scoped_per_settlement():
    streets, aliases, _u = ni._resolve_streets(
        [_canon(7900, "הרצל"), _canon(5000, "הרצל")], [])
    assert len(streets) == 2
    assert {a[0] for a in aliases} == {7900, 5000}
    # An identical name in another town must never merge.
    assert {s[0] for s in streets} == {"7900-הרצל", "5000-הרצל"}


def test_rows_with_no_settlement_code_are_dropped():
    streets, _a, _u = ni._resolve_streets([_canon(None, "הרצל")], [_gaz(None, "הרצל")])
    assert streets == []


# ── the official street file (רשות האוכלוסין, with synonyms) ─────────────────
def _off(sc, name, code, status="official"):
    return {"sc": sc, "name": name, "official_code": code, "status": status}


def test_official_code_merges_spellings_the_ladder_would_keep_apart():
    """'רוטשילד' and 'שדרות רוטשילד' are one street; the file says so by code."""
    official = [_off(7900, "שדרות רוטשילד", 103),
                _off(7900, "רוטשילד", 103, "synonym of 103")]
    streets, _a, _u = ni._resolve_streets(
        [_canon(7900, "שדרות רוטשילד", n=9), _canon(7900, "רוטשילד", n=2)], [], official)
    assert len(streets) == 1
    assert streets[0][10] == 103          # official_code lands on the row


def test_published_synonym_links_a_gazetteer_name_the_ladder_misses():
    """A transliteration variant no normalization rule could infer.

    Real pair from Petah Tikva, official code 115: 'ברנדייס' is also written
    'בראנדיס'. They share no token and normalize differently, so every heuristic
    rule fails — only the published synonym connects them. 373 such pairs exist
    in Petah Tikva alone (spelling variants, landmark names like
    'מלון עדן' → 'שטמפפר יהושע', and old names like 'בתי בורשטיין' → 'המכבים')."""
    official = [_off(7900, "ברנדייס", 115),
                _off(7900, "בראנדיס", 115, "synonym of 115")]
    # Without the file the gazetteer spelling has no counterpart...
    _s, _a, before = ni._resolve_streets(
        [_canon(7900, "ברנדייס")], [_gaz(7900, "בראנדיס")])
    assert len(before) == 1
    # ...with it, it resolves onto the canonical street instead of forking one.
    streets, _a2, after = ni._resolve_streets(
        [_canon(7900, "ברנדייס")], [_gaz(7900, "בראנדיס")], official)
    assert after == [] and len(streets) == 1


def test_a_name_ambiguous_in_the_source_is_ignored():
    """If the file maps one spelling to two official codes, trust neither."""
    official = [_off(7900, "הרצל", 1), _off(7900, "הרצל", 2)]
    assert ni._official_code_index(official) == {}


def test_official_rows_are_optional_and_change_nothing_when_absent():
    """The file is a layer on top; without it the ladder must behave as before."""
    args = ([_canon(7900, "יבניאלי")], [_gaz(7900, "שמואל יבניאלי")])
    a = ni._resolve_streets(*args)
    b = ni._resolve_streets(*args, ())
    assert a == b


def test_two_streets_sharing_a_representative_spelling_do_not_collide():
    """Distinct official codes must never end up on one street_key."""
    official = [_off(7900, "הרצל", 1), _off(7900, "הרצל תיאודור", 2)]
    streets, _a, _u = ni._resolve_streets(
        [_canon(7900, "הרצל"), _canon(7900, "הרצל תיאודור")], [], official)
    keys = [s[0] for s in streets]
    assert len(keys) == len(set(keys)) == 2


# ── invariants ────────────────────────────────────────────────────────────────
def test_every_table_has_a_hebrew_title_in_the_data_catalog():
    """A new over_re_ table must not surface in /data under its raw name."""
    missing = [t for t in ni.ALL_TABLES if t not in data_catalog._OVER_TITLES]
    assert missing == [], f"missing /data titles for: {missing}"


def test_all_tables_are_over_prefixed_so_the_catalog_picks_them_up():
    # data_catalog._over_index_records() only collects tables starting 'over_'.
    assert all(t.startswith("over_") for t in ni.ALL_TABLES)


def test_every_stage_has_a_builder():
    assert set(ni.STAGES) == set(ni._BUILDERS)


def test_default_build_skips_the_two_expensive_opt_ins():
    """source_indexes is a one-off and pip is the only stage with material Neon
    compute cost — neither may run just because someone pressed rebuild."""
    default = [s for s in ni.STAGES if s not in ("source_indexes", "pip")]
    assert "pip" not in default and "source_indexes" not in default
    assert default == ["parcels", "gazetteer", "postal_localities",
                       "streets", "addresses", "zip5"]


def test_every_ddl_column_added_after_launch_has_a_migration():
    """CREATE TABLE IF NOT EXISTS does not evolve an existing table.

    A column added to _DDL after the table shipped is absent in production until
    an explicit ALTER runs, and the build then dies on "column does not exist" —
    which is exactly what happened to `zip_level`. Anything in _COLUMN_ADDITIONS
    must name a real table and appear in that table's DDL."""
    ddl = "\n".join(ni._DDL)
    for table, column, _type in ni._COLUMN_ADDITIONS:
        assert table in ni.ALL_TABLES, f"{table} is not a known table"
        assert column in ddl, f"{column} is migrated but missing from _DDL"


def test_every_source_constant_is_defined_and_well_formed():
    """A source referenced only inside an f-string in a build stage fails at RUN
    time, not import — so an undefined constant reaches production and dies mid
    build. (POSTAL_LOCALITY_SRC did exactly that: a patch was lost, the module
    still imported, and the stage raised NameError in prod.)"""
    for name in ("PARCELS_SRC", "GAZTIR_SRC", "POSTAL_SRC",
                 "POSTAL_LOCALITY_SRC", "ADDR_SRC"):
        src = getattr(ni, name, None)
        assert isinstance(src, tuple) and len(src) == 2, f"{name} missing/malformed"
        schema, table = src
        assert schema in ("public", "odata", "idx"), f"{name} has odd schema {schema}"
        assert table and isinstance(table, str)


def test_build_stage_sql_references_only_defined_names():
    """Compile every stage function so a NameError in an f-string is caught here."""
    import inspect
    for stage, fn in ni._BUILDERS.items():
        src = inspect.getsource(fn)
        for const in ("PARCELS_SRC", "GAZTIR_SRC", "POSTAL_SRC",
                      "POSTAL_LOCALITY_SRC", "ADDR_SRC"):
            if const in src:
                assert getattr(ni, const, None) is not None, \
                    f"stage {stage} references undefined {const}"


def test_postal_synonym_pairs_link_without_mixing_codespaces():
    """Israel Post publishes PAIRS, רשות האוכלוסין publishes shared CODES.

    The two number streets differently, so the pairs must never enter the
    official-code namespace — they only say "whatever street `name` resolves to,
    `synonym` names too". Real pair: סנש חנה / חנה סנש."""
    syn = [{"sc": 7900, "name": "סנש חנה", "synonym": "חנה סנש"}]
    # 'חנה סנש' is a word-order variant, so the ladder's token_set already links
    # it; use a pair the ladder cannot reach to prove the file does the work.
    syn2 = [{"sc": 7900, "name": "קפלנסקי שלמה", "synonym": "קפלן"}]
    _s, _a, before = ni._resolve_streets(
        [_canon(7900, "קפלנסקי שלמה")], [_gaz(7900, "קפלן")])
    assert len(before) == 1, "the ladder should not reach this on its own"

    streets, _a2, after = ni._resolve_streets(
        [_canon(7900, "קפלנסקי שלמה")], [_gaz(7900, "קפלן")], (), syn2)
    assert after == [] and len(streets) == 1
    assert syn  # the order-variant pair is harmless either way


def test_postal_synonyms_are_ignored_when_neither_side_is_known():
    """A pair naming two streets we have never seen must create nothing."""
    syn = [{"sc": 7900, "name": "רחוב דמיוני", "synonym": "שם אחר"}]
    streets, aliases, _u = ni._resolve_streets([_canon(7900, "אבימלך")], [], (), syn)
    assert len(streets) == 1
    assert all("דמיוני" not in (a[3] or "") for a in aliases)


def test_an_exact_name_beats_another_streets_inferred_variant_either_order():
    """Haifa has 'דרך עכו' AND a street simply named 'עכו'.

    Asking for 'עכו' must return the street called 'עכו' — its exact name (100)
    outranks the other's inferred no_type variant (85). The old rule poisoned the
    key on `>=`, so the winner depended on which street the loop met first and
    the exact name lost whenever it arrived second. 26 real streets were affected.
    """
    for order in ([_canon(8200, "דרך עכו"), _canon(8200, "עכו")],
                  [_canon(8200, "עכו"), _canon(8200, "דרך עכו")]):
        streets, aliases, _u = ni._resolve_streets(order, [])
        keyed = {(a[0], a[1]): a[2] for a in aliases}
        assert keyed.get((8200, "עכו")) == "8200-עכו", \
            f"exact name lost for order {[c['name'] for c in order]}"
        assert len(streets) == 2


def test_a_genuine_tie_between_two_streets_still_designates_neither():
    """The guard must survive the fix: equal weight from two streets = poison."""
    streets, aliases, _u = ni._resolve_streets(
        [_canon(7900, "דוד המלך"), _canon(7900, "שלמה המלך")], [])
    keyed = {(a[0], a[1]): a[2] for a in aliases}
    assert (7900, "המלך") not in keyed
    assert len(streets) == 2
