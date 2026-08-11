"""Offline tests for the settlement index's pure logic (no DB): normalization
and inflection generation."""
from app.services import settlement_index as si


def test_norm_collapses_spacing_hyphen_geresh():
    # spaces, ASCII hyphen, Hebrew maqaf, and both geresh forms all collapse
    assert si.norm("תל אביב-יפו") == "תלאביביפו"
    assert si.norm("תל-אביב־יפו") == "תלאביביפו"
    assert si.norm("אבו ג'ווייעד") == si.norm("אבו ג׳ווייעד")
    assert si.norm("  Tel  Aviv-Yafo ") == "telavivyafo"
    assert si.norm(None) == "" and si.norm("") == ""


def test_strip_paren():
    assert si.strip_paren("אבו ג'ווייעד (שבט)") == "אבו ג'ווייעד"
    assert si.strip_paren("מגדל (קיבוץ)") == "מגדל"


def test_aliases_include_official_and_prefixes():
    al = si.aliases_for({"code": 5000, "name": "תל אביב-יפו"})
    keys = {k: (kind, w) for k, _, kind, w in al}
    assert keys["תלאביביפו"] == ("official", 100)         # base form, top weight
    assert "בתלאביביפו" in keys and keys["בתלאביביפו"][0] == "prefix"  # ב prefix
    assert "לתלאביביפו" in keys                            # ל prefix


def test_aliases_no_paren_variant():
    al = si.aliases_for({"code": 967, "name": "אבו ג'ווייעד (שבט)"})
    keys = {k for k, _, _, _ in al}
    assert si.norm("אבו ג'ווייעד") in keys                # paren stripped
    assert si.norm("אבו ג'ווייעד (שבט)") in keys          # full official too


def test_aliases_english_and_translit_no_hebrew_prefix():
    al = si.aliases_for({"code": 5000, "name": "תל אביב-יפו",
                         "name_en": "Tel Aviv-Yafo", "translit": "TEL AVIV-YAFO"})
    by_kind = {}
    for k, _, kind, _ in al:
        by_kind.setdefault(kind, []).append(k)
    assert "telavivyafo" in by_kind.get("english", [])
    # Latin forms must NOT get Hebrew prefixes
    assert not any(k.startswith("בtel") for k, _, _, _ in al)


def test_manual_aliases_resolve_known_gaps():
    recs = si.load_seed()
    rows = si.manual_alias_rows(recs)
    # admin-prefix rows ('מ.א. X') keep the bare variant as their surface — same
    # convention as the auto layer — so exclude them when keying by surface.
    by_variant = {surf: (code, kind, w) for _, code, surf, kind, w in rows
                  if kind != "manual_admin_prefix"}
    # short form + rename must be present, weighted above prefixed guesses
    assert "תל אביב" in by_variant and by_variant["תל אביב"][1] == "manual"
    assert by_variant["תל אביב"][0] == 5000
    assert "נצרת עילית" in by_variant and by_variant["נצרת עילית"][0] == 1061
    # base manual = weight 85; prefixed manual variants ('בתל אביב') = 55
    assert by_variant["תל אביב"][2] == 85
    assert "בתל אביב" in by_variant and by_variant["בתל אביב"] == (5000, "manual_prefix", 55)


def test_authorities_seed_and_aliases():
    auth = si._load_json(si.AUTH_SEED_PATH)
    assert len(auth) > 200  # ~257 authorities
    # regional councils (מועצה אזורית) present — the ones missing from settlements
    assert any(a.get("municipal_status") == "מועצה אזורית" for a in auth)
    # the same alias engine applies (prefix + norm)
    r = auth[0]
    keys = {k for k, _, _, _ in si.aliases_for(r)}
    assert si.norm(r["name"]) in keys


def test_authority_crosswalk_targets_are_real_authorities():
    """Every entry of the 2022 Interior-Ministry crosswalk must name an authority
    that exists in the seed — otherwise the variant silently resolves to nothing."""
    manual = si._load_json(si.AUTH_MANUAL_PATH)
    assert len(manual) > 50
    names = {a["name"] for a in si._load_json(si.AUTH_SEED_PATH)}
    assert [m for m in manual if m["name"] not in names] == []
    # and each variant must actually ADD something: it must not already be an
    # auto-generated alias of the authority it points at (dead weight otherwise).
    auth = si._load_json(si.AUTH_SEED_PATH)
    auto = {}
    for a in auth:
        for k, _, _, _ in si.aliases_for(a):
            auto.setdefault(k, set()).add(a["code"])
    code = {a["name"]: a["code"] for a in auth}
    redundant = [m for m in manual if code[m["name"]] in auto.get(si.norm(m["variant"]), ())]
    assert redundant == []


def test_authority_crosswalk_resolves_on_both_indexes():
    auth = si._load_json(si.AUTH_SEED_PATH)
    manual = si._load_json(si.AUTH_MANUAL_PATH)
    rows = si.manual_rows(manual, auth)
    by_variant = {surf: code for _, code, surf, kind, _ in rows if kind == "manual"}
    # ktiv-haser city spelling, and a regional council's pre-rename name
    assert by_variant["קרית גת"] == next(a["code"] for a in auth if a["name"] == "קריית גת")
    assert by_variant["עמק לוד"] == next(a["code"] for a in auth if a["name"] == "שדות דן")
    # 'מ.א. עמק לוד' resolves too — the curated layer gets the administrative
    # prefixes ('מ.א.' / 'עיריית' / …) expanded, like the auto layer.
    keys = {k for k, _, _, _, _ in rows}
    assert si.norm("מ.א. עמק לוד") in keys and si.norm("עיריית קרית גת") in keys
    # the same file applied to the SETTLEMENT seed resolves the city spellings…
    recs = si.load_seed()
    s_rows = {surf: c for _, c, surf, kind, _ in si.manual_rows(manual, recs, quiet=True)
              if kind == "manual"}
    assert s_rows["קרית גת"] == next(r["code"] for r in recs if r["name"] == "קריית גת")
    # …and quietly drops the regional councils, which are not settlements
    assert "עמק לוד" not in s_rows


def test_swap_variants_bounded_and_symmetric():
    assert si.swap_variants(si.norm("בוסתאן אל-מרג'")) >= {si.norm("בוסטאן אל-מרג'")}
    assert si.swap_variants("אבג") == set()          # no ת/ט → nothing
    # at most _MAX_SWAPS letters flip at once: 3 positions → 3 singles + 3 pairs
    assert len(si.swap_variants("תתת")) == 6
    assert "טטט" not in si.swap_variants("תתת")


def test_ktiv_swap_layer_never_hijacks_a_real_name():
    """The layer is applied blanket-wide, so its safety rests entirely on the two
    guards — assert them against the real seeds, not a fixture."""
    for seed, extra in ((si.load_seed(), si.manual_alias_rows(si.load_seed())),
                        (si._load_json(si.AUTH_SEED_PATH), [])):
        base = si.dedupe_aliases(
            [(k, r["code"], s, kind, w) for r in seed for k, s, kind, w in si.aliases_for(r)] + extra)
        real = {k for k, _, _, _, _ in base}
        swaps = si.ktiv_swap_rows(base)
        # guard 1: never shadows a key the index genuinely knows
        assert not [r for r in swaps if r[0] in real]
        # guard 2: one code per key, and always the bottom weight tier
        assert len({r[0] for r in swaps}) == len(swaps)
        assert {r[4] for r in swaps} == {30}
        assert {r[3] for r in swaps} == {"ktiv_swap"}


def test_ktiv_swap_resolves_the_taw_tet_spelling():
    auth = si._load_json(si.AUTH_SEED_PATH)
    base = si.dedupe_aliases(
        [(k, a["code"], s, kind, w) for a in auth for k, s, kind, w in si.aliases_for(a)]
        + si.manual_rows(si._load_json(si.AUTH_MANUAL_PATH), auth))
    by_key = {r[0]: r[1] for r in base + si.ktiv_swap_rows(base)}
    code = next(a["code"] for a in auth if a["name"] == "בוסתן אל-מרג'")
    for spelling in ["בוסטאן אל-מרג'", "בוסטאן אל מרג", "מ.א. בוסטאן אל-מרג"]:
        assert by_key.get(si.norm(spelling)) == code, spelling


def test_dedupe_aliases_keeps_highest_weight():
    rows = [("k", 1, "a", "manual_prefix", 55), ("k", 1, "b", "official", 100),
            ("k", 2, "c", "manual", 85)]
    out = {(v, c): (surf, w) for v, c, surf, _, w in si.dedupe_aliases(rows)}
    assert out[("k", 1)] == ("b", 100)     # official wins regardless of order
    assert out[("k", 2)] == ("c", 85)      # other codes untouched


def test_seed_loads_and_generates():
    recs = si.load_seed()
    assert len(recs) > 1000
    # every record yields at least an official alias
    r = next(x for x in recs if x["name"] == "אבו גוש")
    keys = {k for k, _, _, _ in si.aliases_for(r)}
    assert si.norm("אבו גוש") in keys
    assert si.norm("באבו גוש") in keys  # prefixed
