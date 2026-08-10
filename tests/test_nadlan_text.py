"""Offline tests for נדל"ן לעם's pure key/parsing/aliasing logic (no DB).

These rules are where a property crosswalk silently goes wrong — a house number
parsed from the wrong end, a street alias that mis-links two different streets,
a parcel key that drifts between rebuilds — so they are asserted here rather
than discovered in the data.
"""
from app.services import nadlan_text as nt


# ── keys ──────────────────────────────────────────────────────────────────────
def test_as_int_handles_the_two_source_conventions():
    # The parcels layer stores ints as floats-in-text; the postal file zero-pads.
    assert nt.as_int("3287.0") == 3287
    assert nt.as_int("00043") == 43
    assert nt.as_int("6390") == 6390
    assert nt.as_int("") is None and nt.as_int(None) is None
    assert nt.as_int("א") is None


def test_parcel_key_is_deterministic_and_defaults_the_suffix():
    assert nt.parcel_key("6390", "0", "308") == "6390-0-308"
    assert nt.parcel_key("6390.0", None, "308.0") == "6390-0-308"   # float text
    assert nt.parcel_key("6390", "", "308") == "6390-0-308"         # blank suffix → 0
    assert nt.parcel_key("7100", "2", "15") == "7100-2-15"          # real suffixes exist
    # A malformed row must produce no key at all rather than a key nothing matches.
    assert nt.parcel_key("abc", "0", "308") is None
    assert nt.parcel_key("6390", "0", "") is None


def test_gp_key_drops_the_suffix_because_the_gazetteer_has_none():
    assert nt.gp_key("6390", "308") == "6390-308"
    assert nt.gp_key("6390.0", "308.0") == "6390-308"
    # Two real parcels sharing a gush/parcel pair collapse here — that is exactly
    # the ambiguity the spine flags rather than hides.
    assert nt.gp_key("7100", "15") == nt.gp_key("7100", "15")


def test_split_parcel_key_round_trips():
    assert nt.split_parcel_key("6390-0-308") == (6390, 0, 308)
    assert nt.split_parcel_key("6390-308") is None
    assert nt.split_parcel_key("x-0-1") is None


# ── house numbers ─────────────────────────────────────────────────────────────
def test_parse_house_number_positional_rule():
    # Postal: zero-padded, starts with a digit → first run.
    assert nt.parse_house_number("00043") == (43, None)
    # Address list is dirty free text and the number trails → last run.
    assert nt.parse_house_number("דוד אבידן 10") == (10, None)
    # A letter glued to the number is a distinct doorway, not noise.
    assert nt.parse_house_number("12א") == (12, "א")
    # A range starts with a digit, so the FIRST number wins.
    assert nt.parse_house_number("5-7") == (5, None)
    assert nt.parse_house_number("") == (None, None)
    assert nt.parse_house_number(None) == (None, None)
    assert nt.parse_house_number("ללא מספר") == (None, None)


# ── street aliasing ───────────────────────────────────────────────────────────
def _kinds(name):
    return {k: (kind, w) for k, _s, kind, w in nt.street_aliases_for(name)}


def test_street_type_words_are_stripped():
    al = _kinds("שדרות רוטשילד")
    assert al["שדרותרוטשילד"] == ("official", 100)
    assert al["רוטשילד"][0] == "no_type"
    # "שדרות" must not be shortened by the "שד" rule (longest-first).
    assert "רותרוטשילד" not in al


def test_strip_street_type_refuses_to_gut_a_short_name():
    # A street genuinely called "הדרך" must survive.
    assert nt.strip_street_type(nt.norm("הדרך")) == nt.norm("הדרך")


def test_paren_and_type_strippings_compose():
    # The useful form must be reachable at HIGH weight, not only via last_token.
    al = _kinds("רח' הרצל (הישנה)")
    assert al["הרצל"][1] == 85
    assert al["רחהרצלהישנה"] == ("official", 100)


def test_last_token_is_offered_but_at_the_bottom_weight():
    al = _kinds("שמואל יבניאלי")
    assert al["שמואליבניאלי"] == ("official", 100)
    assert al["יבניאלי"] == ("last_token", 40)   # the gazetteer-gap lever
    # Ordering matters: an exact hit must always beat the guess.
    assert al["שמואליבניאלי"][1] > al["יבניאלי"][1]


def test_definite_article_variant():
    assert "זית" in _kinds("הזית")


def test_aliases_are_unique_and_skip_junk():
    assert nt.street_aliases_for("") == []
    assert nt.street_aliases_for("?") == []      # the sources' "unknown" sentinel
    keys = [k for k, _s, _k, _w in nt.street_aliases_for("הרצל")]
    assert len(keys) == len(set(keys))


# ── mode sniffing ─────────────────────────────────────────────────────────────
def test_sniff_gush_helka_forms():
    assert nt.sniff_mode("גוש 6390 חלקה 308")["parsed"] == {"gush": 6390, "helka": 308}
    assert nt.sniff_mode("6390/308")["parsed"] == {"gush": 6390, "helka": 308}
    assert nt.sniff_mode("גוש 6390 חלקה 308")["mode"] == "gush_helka"


def test_sniff_zip_and_the_ambiguous_five_digits():
    assert nt.sniff_mode("4935048")["mode"] == "zip"
    amb = nt.sniff_mode("49350")
    # 5 digits is a legitimate ZIP5 *and* a legitimate gush — surface both
    # rather than guessing.
    assert amb["mode"] == "zip"
    assert amb["alternatives"] == [{"mode": "gush", "parsed": {"gush": 49350}}]


def test_sniff_point_and_address():
    p = nt.sniff_mode("32.08,34.88")
    assert p["mode"] == "point" and p["parsed"] == {"lat": 32.08, "lon": 34.88}
    # A swapped pair is obviously a coordinate; accept it rather than mapping
    # the user into the sea.
    assert nt.sniff_mode("34.88,32.08")["parsed"] == {"lat": 32.08, "lon": 34.88}
    assert nt.sniff_mode("אבימלך 8 פתח תקווה")["mode"] == "address"
    assert nt.sniff_mode("")["mode"] == "empty"
