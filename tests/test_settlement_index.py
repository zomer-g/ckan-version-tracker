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
    by_variant = {surf: (code, kind, w) for _, code, surf, kind, w in rows}
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


def test_seed_loads_and_generates():
    recs = si.load_seed()
    assert len(recs) > 1000
    # every record yields at least an official alias
    r = next(x for x in recs if x["name"] == "אבו גוש")
    keys = {k for k, _, _, _ in si.aliases_for(r)}
    assert si.norm("אבו גוש") in keys
    assert si.norm("באבו גוש") in keys  # prefixed
