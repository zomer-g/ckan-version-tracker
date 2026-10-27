"""Diary-owner extraction (app/services/ocal_owners.py) and the
contact-with-the-voter expenses parser (app/services/ocal_mk_expenses.py).

The titles below are real diary_sources titles from the migrated corpus.
"""
import io

import openpyxl

from app.services.ocal_mk_expenses import match_owner, parse_workbook
from app.services.ocal_owners import (
    PeopleIndex, canonical_keys, extract_owners, learn_names, name_key, names_match,
    owner_key_for,
)

PEOPLE = PeopleIndex([
    ("1", "מאי גולן"), ("2", "בדרה גולן פלורה מאי"), ("3", "איתמר בן גביר"),
    ("4", "סגלוביץ יואב"), ("5", "רפאל (רפול) אנגל"), ("6", "יעקב מרגי"),
    ("7", "ינון אהרוני"), ("8", "אבי כהן סקלי"), ("9", "גדי אריאלי"),
])


def _names(owners):
    return [o["name"] for o in owners]


# ── name matching ────────────────────────────────────────────────────────────

def test_names_match_is_order_insensitive_and_allows_extra_given_names():
    assert names_match("מאי גולן", "גולן מאי")
    assert names_match("מאי גולן", "בדרה גולן פלורה מאי")
    assert names_match("בצלאל סמוטריץ'", "סמוטריץ בצלאל יואל")
    assert name_key("ישראל כ\"ץ") == name_key("ישראל כץ")
    assert not names_match("מאי גולן", "יאיר גולן")
    # A single shared word is never enough.
    assert not names_match("גולן", "יאיר גולן")


# ── title extraction ─────────────────────────────────────────────────────────

def test_minister_and_director_general_are_both_owners_with_roles():
    owners = extract_owners(
        "יומני השר לביטחון לאומי, איתמר בן גביר ומנכ\"ל המשרד לביטחון לאומי, "
        "רפאל (רפול) אנגל, לשנת 2024 (רבעון רביעי)", None, PEOPLE)
    assert _names(owners) == ["איתמר בן גביר", "רפאל אנגל"]
    assert owners[0]["role"] == "השר לביטחון לאומי"
    assert owners[0]["person_id"] == "3"
    assert owners[1]["role"].startswith("מנכ\"ל המשרד לביטחון לאומי")


def test_second_owner_role_is_its_own_clause():
    owners = extract_owners(
        "יומן שר הרווחה, יעקב מרגי ומנכ\"ל משרד הרווחה, ינון אהרוני, לשנת 2023 (רבעון שלישי)",
        None, PEOPLE)
    assert _names(owners) == ["יעקב מרגי", "ינון אהרוני"]
    assert owners[1]["role"] == "מנכ\"ל משרד הרווחה"


def test_title_spelling_is_kept_for_knesset_ordered_people():
    owners = extract_owners(
        "יומן השר לביטחון הפנים, עומר בר לב, סגן השר לביטחון הפנים יואב סגלוביץ ומנכ\"ל "
        "המשרד לביטחון הפנים, תומר לוטן לשנת 2021", None, PEOPLE)
    assert _names(owners) == ["עומר בר לב", "יואב סגלוביץ", "תומר לוטן"]


def test_may_golan_is_not_mistaken_for_the_month_may():
    owners = extract_owners("יומן השרה מאי גולן לחודש אוקטובר 2023", None, PEOPLE)
    assert _names(owners) == ["מאי גולן"]


def test_topic_phrases_and_companies_are_not_people():
    owners = extract_owners(
        "יומן מנכ\"ל משרד החדשנות, המדע והטכנולוגיה, גדי אריאלי, לשנת 2024 (רבעון שני)",
        None, PEOPLE)
    assert _names(owners) == ["גדי אריאלי"]
    owners = extract_owners(
        "משרד האנרגיה - פגישות עם נציגי דלק, שברון, כי\"ל, בז\"ן, כרמל אוליפינים וגדיב תעשיות",
        None, PEOPLE)
    assert [o["kind"] for o in owners] == ["subject"]


def test_two_names_joined_by_vav_split():
    owners = extract_owners(
        "יומן שר התפוצות, נחמן שי, ומנכל\"י משרד התפוצות, דביר כהנא וציונה קניג-יאיר, לשנת 2021",
        None, PEOPLE)
    assert _names(owners) == ["נחמן שי", "דביר כהנא", "ציונה קניג-יאיר"]


def test_name_after_honorific_without_comma():
    owners = extract_owners(
        "יומן המשנה למנכ\"ל משרד הבריאות פרופ' איתמר גרוטו לשנים 2016 ו 2017", None, PEOPLE)
    assert _names(owners) == ["איתמר גרוטו"]
    assert owners[0]["role"] == "המשנה למנכ\"ל משרד הבריאות"


def test_unnamed_diary_falls_back_to_its_office():
    owners = extract_owners(
        "עיריית חדרה - יומן ראש הרשות המקומית ו/או האיזורית והמנכ\"ל - מחצית ראשונה 2022",
        None, PEOPLE)
    assert owners == [{"name": "עיריית חדרה – ראש הרשות", "person_id": None, "role": None,
                       "kind": "subject", "method": "subject"}]
    owners = extract_owners("יומן ראש עיריית כפר סבא — סופי עותק של יומן ראש העיר", None, PEOPLE)
    assert _names(owners) == ["ראש עיריית כפר סבא"]


def test_learned_names_are_found_where_another_title_runs_them_into_the_role():
    titles = ["יומן שר הכלכלה והתעשיה, עמיר פרץ, לשנת 2020 (רבעון שני)",
              "יומן שר הכלכלה עמיר פרץ לשנת 2020 (רבעון שלישי)"]
    assert extract_owners(titles[1], None, PEOPLE)[0]["kind"] == "subject"
    learned = learn_names(titles, PEOPLE)
    index = PeopleIndex([("1", "מאי גולן")] + [(None, n) for n in learned])
    owners = extract_owners(titles[1], None, index)
    assert _names(owners) == ["עמיר פרץ"]
    assert owners[0]["role"] == "שר הכלכלה"


def test_canonical_keys_merge_spellings_of_one_person():
    owners = [{"name": "מאי גולן", "kind": "person"},
              {"name": "בדרה גולן פלורה מאי", "kind": "person"},
              {"name": "יאיר גולן", "kind": "person"}]
    canon = canonical_keys(owners)
    keys = {o["name"]: canon[owner_key_for(o)] for o in owners}
    assert keys["מאי גולן"] == keys["בדרה גולן פלורה מאי"]
    assert keys["יאיר גולן"] != keys["מאי גולן"]


# ── expenses workbook parsing ────────────────────────────────────────────────

def _xlsx(rows, title="גיליון1"):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = title
    for r in rows:
        ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_wide_layout_one_row_per_mk():
    data = _xlsx([
        ["הוצאות חברי הכנסת מתקציב קשר עם הציבור לשנת 2023"],
        [],
        ["שם חבר הכנסת", "סיעה", "אתר אינטרנט ורשתות חברתיות", "משרד מחוץ לכנסת", "סה\"כ"],
        ["בן גביר איתמר", "עוצמה יהודית", "40,000", 12000.5, 52000.5],
        ["גולן מאי", "הליכוד", 1000, None, 1000],
        ["סה\"כ", "", 41000, 12000.5, 53000.5],
    ])
    out = parse_workbook(data, "mk.xlsx")
    rows = out["rows"]
    assert out["sheets"][0]["layout"] == "wide"
    assert {r["year"] for r in rows} == {2023}
    ben_gvir = [r for r in rows if r["mk_name"] == "בן גביר איתמר"]
    assert {(r["category"], r["amount"], r["is_total"]) for r in ben_gvir} == {
        ("אתר אינטרנט ורשתות חברתיות", 40000.0, False),
        ("משרד מחוץ לכנסת", 12000.5, False),
        ("סה\"כ", 52000.5, True),
    }
    assert ben_gvir[0]["faction"] == "עוצמה יהודית"
    # The sheet's total row is not an MK.
    assert not any(r["mk_name"].startswith("סה") for r in rows)


def test_long_layout_with_merged_mk_cells():
    data = _xlsx([
        ["שם ח\"כ", "סוג הוצאה", "שם הספק", "סכום"],
        ["מאי גולן", "פרסום", "חברה א", 500],
        [None, "ייעוץ משפטי", "משרד עו\"ד ב", "1,200"],
        ["יאיר גולן", "פרסום", "חברה ג", 300],
    ], title="2024")
    rows = parse_workbook(data, "x.xlsx")["rows"]
    assert [(r["mk_name"], r["category"], r["description"], r["amount"], r["year"]) for r in rows] == [
        ("מאי גולן", "פרסום", "חברה א", 500.0, 2024),
        ("מאי גולן", "ייעוץ משפטי", "משרד עו\"ד ב", 1200.0, 2024),
        ("יאיר גולן", "פרסום", "חברה ג", 300.0, 2024),
    ]


def test_expense_names_link_to_diary_owners():
    owners = [("p:גולנ מאי", "מאי גולן"), ("p:בנ גביר איתמר", "איתמר בן גביר"),
              ("p:גולנ יאיר", "יאיר גולן")]
    assert match_owner("בדרה גולן פלורה מאי", owners) == "p:גולנ מאי"
    assert match_owner("בן גביר איתמר", owners) == "p:בנ גביר איתמר"
    assert match_owner("לפיד יאיר", owners) is None


# ── the Knesset dataset as archived by OVER ──────────────────────────────────

from app.services.ocal_mk_expenses import category_resolver, clean_category, rows_from_over  # noqa: E402


def test_nicknames_and_spellings_of_one_mk_match():
    assert names_match("אבי דיכטר", "דיכטר אברהם משה")
    assert names_match("דבי ביטון", "ביטון דבורה")
    assert names_match("טלי גוטליב", "גוטליב רויטל")
    assert names_match("חילי טרופר", "טרופר יחיאל משה")
    assert names_match("קטי קטרין שטרית", "שיטרית קטרין")
    assert names_match("משה סולומון", "סלומון משה")
    assert names_match("ואליד אלהואשלה", "אל הואשלה ואליד")
    # ...without merging different people who share a surname.
    assert not names_match("חיים ביטון", "ביטון מיכאל מרדכי")
    assert not names_match("מאיר כהן", "מירב כהן")
    assert not names_match("דוד ביטן", "דוד אמסלם")


def test_clipped_and_numbered_headings_are_restored():
    resolve = category_resolver(["צריכת מדיה כתובה ודיגיטלית", "הוצאות שירותי כבלים"])
    assert resolve("צריכת מדיה כתובה ודי") == "צריכת מדיה כתובה ודיגיטלית"
    assert resolve('"הוצאות שירותי כבלים') == "הוצאות שירותי כבלים"
    assert resolve("ייעוץ  סקרים וחוות") == "ייעוץ סקרים וחוות"  # no full form known
    assert clean_category("מחשב (2)") == "מחשב"
    assert clean_category("טלפון נייד 2)") == "טלפון נייד"


def test_rows_from_over_keeps_refunds_and_uses_summary_only_where_no_detail():
    detail = [
        {"שנת הדוח": "2024", "שם חבר הכנסת": "גולן מאי", "שם סעיף הוצאה": "צריכת מדיה כתובה ודיגיטלית",
         "שם בית עסק/ ספק": "ספק א", "תאריך ביצוע/ תאריך חשבונית": "2024-03-01", 'סכום בש"ח': "1000",
         "פרטים/ הערות": "", "אשראי": "X", "אסמכתאות לעסקה": "https://example.org/r/1"},
        {"שנת הדוח": "2024", "שם חבר הכנסת": "גולן מאי", "שם סעיף הוצאה": "צריכת מדיה כתובה ודי",
         "שם בית עסק/ ספק": "ספק א", "תאריך ביצוע/ תאריך חשבונית": "2024-03-05", 'סכום בש"ח': "-200",
         "פרטים/ הערות": "זיכוי", "אשראי": "", "אסמכתאות לעסקה": ""},
    ]
    summary = [
        {"שנת הדוח": "2024", "שם חבר הכנסת": "גולן מאי", "שם סעיף הוצאה": "כיבוד קל", 'סכום בש"ח': "50"},
        {"שנת הדוח": "2023", "שם חבר הכנסת": "גולן מאי", "שם סעיף הוצאה": "מחשב (2)", 'סכום בש"ח': "300"},
    ]
    d, s = rows_from_over(detail, summary)
    assert [(r["category"], r["amount"], str(r["expense_date"])) for r in d] == [
        ("צריכת מדיה כתובה ודיגיטלית", 1000.0, "2024-03-01"),
        ("צריכת מדיה כתובה ודיגיטלית", -200.0, "2024-03-05"),
    ]
    assert d[0]["receipt_url"] == "https://example.org/r/1" and d[0]["supplier"] == "ספק א"
    assert d[1]["description"] == "זיכוי"
    # 2024 has transactions → its summary row is dropped; 2023 has none → kept.
    assert [(r["year"], r["category"], r["amount"]) for r in s] == [(2023, "מחשב", 300.0)]
