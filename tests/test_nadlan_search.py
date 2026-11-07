"""נדל"ן לעם's general search box: what a typed string is read as, how a
free-text address is split, and that one failing group never sinks the rest."""
import asyncio

from app.services import nadlan_query, nadlan_search as ns


def _kinds(q):
    return [r["kind"] for r in ns.interpret(q)]


def test_every_plausible_reading_is_kept():
    assert _kinds("3/2025") == ["gush_helka", "tender"]       # both are real
    assert ns.interpret("3/2025")[1]["parsed"]["tender_id"] == 20250003
    assert "tender" in _kinds("20250003")
    assert _kinds("64239") == ["zip", "gush"]
    assert _kinds("6638/120") == ["gush_helka"]
    assert _kinds("32.08, 34.78") == ["point"]


def test_plan_numbers_are_recognised():
    for q in ("תא/3598", "507-0391482", "5/53/14", "תכנית רג/340", "גב/מק/281כ"):
        assert "plan" in _kinds(q), q
    assert ns.interpret("תכנית רג/340")[-1]["parsed"] == {"number": "רג/340", "norm": "רג340"}
    assert ns.plan_norm("תא/ 3598") == ns.plan_norm("תא 3598") == "תא3598"


def test_hebrew_free_text_is_text():
    assert _kinds("דיזנגוף 50 תל אביב") == ["text"]
    assert _kinds("רמת החייל") == ["text"]
    assert ns.interpret("   ") == []


def test_an_address_splits_on_a_known_settlement_at_either_end():
    codes = {"ירושלים": 3000, "תל אביב יפו": 5000, "תל": 1}
    assert ns.split_address("המגיד 14 ירושלים".split(), codes) == \
        {"city": "ירושלים", "street": "המגיד", "number": "14"}
    assert ns.split_address("תל אביב יפו רחוב דיזנגוף 50א".split(), codes) == \
        {"city": "תל אביב יפו", "street": "דיזנגוף", "number": "50א"}
    assert ns.split_address("שדרות רוטשילד".split(), codes) is None   # no settlement
    assert ns.split_address(["ירושלים"], codes) is None                # no street


def test_a_failing_group_is_reported_and_the_rest_still_answer(monkeypatch):
    async def _boom(*a, **k):
        raise RuntimeError("timeout")

    async def _plans(norm, text):
        return [{"source": "taba_bounds", "number": "תא/3598", "name": None}]

    monkeypatch.setattr(ns, "_tenders", _boom)
    monkeypatch.setattr(ns, "_plans", _plans)
    monkeypatch.setattr(nadlan_query, "by_gush_helka", _boom)
    out = asyncio.run(ns.search("תא/3598"))
    assert out["plans"][0]["number"] == "תא/3598" and out["errors"] == {}
    out = asyncio.run(ns.search("3/2025"))
    assert set(out["errors"]) == {"tenders", "properties:gush_helka"}
    assert out["properties"] == [] and out["tenders"] == []


def test_the_plan_sql_normalises_the_same_way_and_binds_its_parameters():
    by_num, by_name = ns._plans_sql("number"), ns._plans_sql("name")
    assert "$2" not in by_num and "$1" in by_num          # one argument: the norm
    assert "$2" in by_name and "ILIKE" in by_name
    assert "taba_bounds" not in by_name                    # that layer has no name
    assert by_num.count("LIMIT") == 1
