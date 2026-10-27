"""נדל"ן לעם — the information cubes that hang off one parcel.

The envelope answers "what IS this property" (its identity in every codespace)
plus two describing layers that are cheap enough to carry on every answer: the
א"ס and the deals summary. Everything else OVER holds about a piece of ground —
its תב"ע, the רמ"י tenders and plans around it, the crime in its statistical
area, the noise over it — is far too much to compute for every result of a
2 km radius search. So each of those is a CUBE: listed up front, fetched only
when someone opens it, for one parcel at a time.

Three grains reach a parcel, and every cube declares which one it uses:

* ``spatial``  — a GovMap layer mirrored into ``idx`` with a PostGIS ``geom``.
  The parcel centroid is put INSIDE the layer's polygons (``radius_m = 0``) or
  the layer's features within ``radius_m`` are listed (point layers such as
  shelters, where "inside" can never hit). One code path, bbox-first on the
  layer's GiST index, so a layer that covers a different town costs an index
  probe and nothing more.
* ``stat_area`` — the crime register (משטרת ישראל), published per CBS
  statistical area. It is compared against the settlement and the whole
  country, per 1,000 residents, because an absolute count of cases in one area
  means nothing on its own.
* ``gush_helka`` — already served by the deals block; listed here only so the
  page can present every cube the same way.

Nothing here is hardcoded to a physical table. A GovMap layer's ``idx`` table is
``<ckan_name>_<first 8 hex of the dataset id>`` (``index_mirror.table_name``),
so it is FOUND by that suffix; the crime corpus is one table per year and is
found by its column signature, like the deals corpus. A cube whose table is not
on this deployment reports ``available: false`` rather than failing — the same
rule the deals block follows.

Columns are not curated either. Each layer publishes whatever its GIS had, and
the honest presentation is all of it, labelled with the Hebrew caption GovMap
itself shows (``over_column_aliases``) where one is known.
"""
from __future__ import annotations

import asyncio
import logging
import re
import time

from app.services import append_store, column_aliases, nadlan_query
from app.services.append_store import _qi
from app.services.index_mirror import PG_EXT_SCHEMA
from app.services.nadlan_index import PARCELS_TABLE

logger = logging.getLogger(__name__)

_X = _qi(PG_EXT_SCHEMA)

#: How many features a spatial cube returns. A parcel sits inside a handful of
#: plans at most; a radius layer near a city centre can hit dozens, and the
#: nearest are the ones that answer "what is around my house".
MAX_FEATURES = 25

#: Longest value handed to the page. Some layers carry a whole document in a
#: text field; a cube is a summary with a link to the full row, not a reader.
MAX_VALUE_CHARS = 400

# Columns that are bookkeeping, geometry or GIS plumbing — never what a reader
# opened the cube for. Compared lower-case.
_NOISE_COLS = {
    "_id", "_row_hash", "_first_seen", "row_hash", "first_seen", "last_seen",
    "geometry_wkt", "geom", "objectid", "object_id", "fid", "ogc_fid", "gid",
    "globalid", "global_id", "shape", "shape_area", "shape_length", "shape_leng",
    "shape__area", "shape__length", "shape_le_1", "st_area(shape)",
    "st_length(shape)", "symbol", "symbolid", "symbol_id",
}

# ── the registry ──
# (id, group, title, grain, dataset ids, radius_m, description)
# Every dataset id here was looked up in the OVER catalog; the layer number is
# in the comment so the GovMap source can be found by hand.
_SPATIAL: list[tuple[str, str, str, list[str], int, str]] = [
    # תכנון
    ("mavat_plans", "תכנון", 'תוכניות במבא"ת (קווים כחולים)',
     ["b105ae22-a6f5-4cd0-b37e-c63dc5da40c3"], 0,       # lay=203, מינהל התכנון
     'כל תוכנית במערכת מבא"ת של מינהל התכנון שהקו הכחול שלה מכיל את החלקה, '
     'מאושרת או בהליך. זו השכבה הרשמית של גבולות התוכניות.'),
    ("taba_plots", "תכנון", 'מגרשי תב"ע',
     ["68b8e2fe-d757-4478-aa46-ee756001f6bc"], 0,       # lay=11, רמ"י
     'המגרש בתוכנית בניין העיר החלה על הנקודה: מספר תוכנית, ייעוד ושטח. '
     'השכבה של רמ"י, רצף מגרשי תב"ע.'),
    ("taba_bounds", "תכנון", 'גבולות תב"ע',
     ["2156f30e-cb97-4737-a2f5-bd030b61c944"], 0,       # lay=357, רמ"י
     'כל התוכניות שגבולן מכיל את החלקה. מספר התוכנית הוא המפתח לחיפוש '
     'במערכת מבא"ת של מינהל התכנון.'),
    ("moch_taba", "תכנון", 'תב"ע — משרד הבינוי והשיכון',
     ["be92c1d4-4c75-4ac7-bccf-3d8259be30c3"], 0,       # lay=336
     'תוכניות של משרד הבינוי והשיכון החלות על הנקודה.'),
    ("moch_plots", "תכנון", "מגרשים — משרד הבינוי והשיכון",
     ["9ddf9d2f-d090-4596-8223-1bdbf4a0e305"], 0,       # lay=335
     "המגרש בתוכניות של משרד הבינוי והשיכון שהחלקה בתוכו."),
    ("plans_municipal", "תכנון", "גבולות תוכניות עירוניים",
     ["f514654e-236f-495b-8019-64459d9dca09",           # גבעת זאב, lay=228069
      "7c640256-ac4c-456c-ada8-385c981b395e"], 0,       # אשקלון (אינדקס ישן), lay=217578
     "שכבות גבולות תוכניות שפרסמו רשויות מקומיות בודדות."),
    ("plots_municipal", "תכנון", "מגרשים עירוניים",
     ["c1a9e9cc-ef3a-49e1-b53a-852b3d2f47aa",           # נס ציונה, lay=211932
      "e5e95a91-1346-4db0-b8df-1a2b30c685a5",           # מספרי מגרשים, lay=212147
      "29b97437-a083-4b4b-9e4c-351046b2eaae"], 0,       # מגרשים, lay=221390
     "שכבות מגרשים שפרסמו רשויות מקומיות בודדות."),
    ("land_use_municipal", "תכנון", "ייעודי קרקע עירוניים",
     ["07033f8e-dcbf-4e71-8c7a-b8e47bca2dc3",           # חיפה, lay=217697
      "79ee02fd-f90f-4bfa-be0c-a96c5283e356",           # כפר סבא, lay=228802
      "b0dcdfb2-c009-4e3a-95e6-0b45bc7985c1",           # אשקלון, lay=232748
      "c43d47e0-3d24-46cc-b696-a248aa756779",           # חדרה, lay=240427
      "3bea12fb-5d11-4998-96a8-532955da6143",           # ערד, lay=238220
      "584129bd-0434-4365-bee6-99a364f21eaf",           # גבעת זאב, lay=228044
      "11d1751b-1b22-4866-a9c3-bd222796e1ac",           # בנימינה-גבעת עדה
      "508e0323-1d31-45b0-9819-20ee385b74de",           # אבו בסמה, lay=225679
      "9bbe3c6d-091a-4b53-a569-a43faab42e5d"], 0,       # באר שבע מסחר/תעסוקה
     "שכבות ייעודי קרקע שפרסמו רשויות מקומיות בודדות. קיימות רק לחלק מהיישובים."),
    ("urban_renewal", "תכנון", "מתחמי התחדשות עירונית",
     ["99f41abe-a18f-400b-96a5-e3f366fc1094",           # lay=200720
      "3606a895-1d9e-46ff-a0f2-7bbb99be24a3"], 0,       # אילת, lay=234493
     "האם החלקה בתוך מתחם התחדשות עירונית מוכרז (פינוי-בינוי, עיבוי)."),
    ("green_building", "תכנון", "בנייה ירוקה בתב\"ע",
     ["d3965af2-c01f-44d1-a739-8a79a20d2c53"], 0,       # lay=268
     "תוכניות שמחייבות תקן בנייה ירוקה."),
    ("planning_area", "תכנון", "מרחב תכנון (ועדה מקומית)",
     ["e9dbbf16-712b-49eb-979f-8f8a07df343b",           # lay=202
      "3b674f5a-1e54-418e-aa98-49584640f4fb",           # תחום מרחב תכנון, lay=212191
      "e28228c5-d8d6-4d66-bf93-cb340b9310d6",           # מרחב תכנון, lay=232240
      "2d72d7cb-7cca-4832-ac68-bd69f545c443"], 0,       # ועדות אזוריות, lay=210772
     "הוועדה המקומית לתכנון ובנייה שבתחומה החלקה — הכתובת להיתרים ולהתנגדויות."),
    # רמ"י
    ("rmi_tender_plots", 'רמ"י', 'מכרזי רמ"י — מגרשים',
     ["d38f49ea-99fc-448a-9c0c-fa79aa577db7"], 300,     # lay=363
     'מגרשים ששווקו או משווקים במכרזי רשות מקרקעי ישראל, בתוך 300 מ׳.'),
    ("rmi_tender_bounds", 'רמ"י', 'מכרזי רמ"י — גבולות',
     ["0ef99574-e510-4fa8-bb2e-7ce3d213b55a"], 0,       # lay=362
     "גבולות מכרזי המקרקעין החלים על הנקודה."),
    ("rmi_marketing_plans", 'רמ"י', 'תוכניות לשיווק',
     ["05381f7f-651e-49c3-9d89-eb21b2e4d3f9"], 0,       # lay=359
     'תוכניות שרמ"י מייעדת לשיווק.'),
    ("rmi_plans_prep", 'רמ"י', "תוכניות בעבודה ובהכנה",
     ["df3ee3f5-1985-4e8d-815e-0236bb1563b3"], 0,       # lay=361
     'תוכניות שרמ"י מקדמת וטרם אושרו — מה עשוי להשתנות בסביבה.'),
    ("rmi_master_plans", 'רמ"י', "תוכניות אב, מתאר ושלד",
     ["db6e6df1-a523-4cfa-af9e-99ded86433c0"], 0,       # lay=358
     "תוכניות-העל החלות על האזור."),
    ("rmi_surveys", 'רמ"י', 'סקרים',
     ["8b3b74de-f71b-4f4d-b80a-a865e3f7ec01"], 0,       # lay=360
     'סקרים שרמ"י ערכה בשטח.'),
    ("rmi_regions", 'רמ"י', 'מרחב רמ"י',
     ["ca5372bb-1f60-4476-a574-5772fb78faaa"], 0,       # lay=356
     'המרחב של רשות מקרקעי ישראל שאחראי על השטח.'),
    ("rmi_appeals", 'רמ"י', "החלטות ועדת השגות על שומות",
     ["2abf506a-cd3d-4a71-843b-36daf0573fdc"], 300,     # lay=242719
     'החלטות ועדת ההשגות על שומות רמ"י בסביבה (300 מ׳).'),
    ("controlled_price", 'רמ"י', "מגרשים במחיר מפוקח",
     ["b85a7bc3-4455-4600-86f7-343b138fa6c7"], 300,     # lay=200558
     "מגרשים שהוקצו לתוכנית מחיר מפוקח / דירה בהנחה, בתוך 300 מ׳."),
    # קדסטר ושיפוט
    ("cadastre_processes", "קדסטר ושיפוט", "תהליכי קדסטר",
     ["295a8254-9135-44a2-814a-556350d69b8d"], 0,       # lay=240040
     "הליכי הסדר, פרצלציה וחלוקה פתוחים בשטח. חלקה בתהליך עשויה לקבל מספר חדש."),
    ("tazar", "קדסטר ושיפוט", 'תצ"ר — תוכניות לצורכי רישום',
     ["469807c0-e92f-4cb2-84f8-4b80126b7a49"], 0,       # lay=81
     'תצ"ר שגבולה מכיל את החלקה: התוכנית שלפיה נרשמה (או תירשם) החלוקה בטאבו.'),
    ("jurisdiction", "קדסטר ושיפוט", "גבולות שיפוט",
     ["fc1f42d4-58d7-42bc-884d-03c53f470214"], 0,       # lay=212192
     "הרשות המקומית שבתחום השיפוט שלה החלקה — מי גובה ארנונה ומי נותן שירותים."),
    ("village_lands", "קדסטר ושיפוט", "גבולות אדמות הכפרים",
     ["3cb47dde-5bf6-4d2e-b93b-8c8bbeeb6841"], 0,       # lay=187
     "גבולות אדמות הכפרים ההיסטוריים שהשטח היה חלק מהם."),
    # סביבה
    ("airport_noise", "סביבה", "רעש משדות תעופה",
     ["91b518f8-bf2c-4042-8fc3-092819c6213e"], 0,       # lay=301
     "האם הנקודה בתוך מתחם חשיפה לרעש מטוסים."),
    ("traffic_noise_day", "סביבה", "רעש תחבורה ביום",
     ["0dc57594-c352-4e3d-8cd4-00cafa7441f9"], 0,       # lay=308
     "מפלס רעש התחבורה בשעות היום, בגובה 5 מ׳."),
    ("traffic_noise_night", "סביבה", "רעש תחבורה בלילה",
     ["7464913d-195d-4c04-b0a5-9c29f9ed7686"], 0,       # lay=309
     "מפלס רעש התחבורה בשעות הלילה, בגובה 5 מ׳."),
    ("soil_tests", "סביבה", "אזורי בדיקות קרקע",
     ["96bfffc8-8cef-4eb6-90cb-c350948b689e",           # lay=210779
      "2fc8fd80-4288-45db-99ed-08e23e034278"], 0,       # lay=223980
     "אזורים שבהם נדרשות בדיקות קרקע לפני תכנון ובנייה."),
    # מורשת
    ("antiquities", "מורשת", "אתרי עתיקות מוכרזים",
     ["c09a6f4e-3dc5-4716-b5d7-bc662d85c7f7",           # רשות העתיקות, lay=212189
      "9f1bfd3e-0f8e-468d-9ffb-8e4b01707ebb",           # lay=182
      "ca51b60f-4bfe-4ca7-8997-9c4693e06938"], 0,       # lay=246737
     "האם החלקה בתוך אתר עתיקות מוכרז — מחייב תיאום עם רשות העתיקות לפני חפירה."),
    ("preservation", "מורשת", "מבנים ואתרים לשימור",
     ["b732397f-d5a2-4b57-b6d8-c58bb1c27c0b",           # lay=213420
      "f1d4ec94-5202-442e-8074-c91b43a2cbbd",           # lay=213572
      "8b88b2e8-cb11-4757-9696-aa4591f120af",           # באר שבע
      "0d35049d-3feb-4d00-93ba-12eb0ce12f45",           # קרית טבעון
      "2323fe1b-665e-416e-8703-dd1df81603ab"], 100,     # בית אל
     "מבנים ואתרים ברשימות השימור, בתוך 100 מ׳ מהחלקה."),
    # שכונה
    ("edu_shelters", "שכונה", "מקלטים במוסדות חינוך",
     ["d0dfe5f7-c170-43e6-a35c-80496d55dd2e"], 500,     # lay=240413
     "מקלטים במוסדות חינוך בתוך 500 מ׳."),
]

CRIME_ID = "crime"


def registry() -> list[dict]:
    """Every cube, in display order, without touching the database."""
    out: list[dict] = [{
        "id": CRIME_ID, "group": "ביטחון אישי", "title": "נתוני פשיעה",
        "grain": "stat_area", "radius_m": None,
        "description": ("תיקי פשיעה שפתחה משטרת ישראל באזור הסטטיסטי של החלקה, "
                        "לעומת היישוב כולו והממוצע הארצי, לכל 1,000 תושבים."),
        "datasets": [],
    }]
    for cid, group, title, ds, radius, desc in _SPATIAL:
        out.append({"id": cid, "group": group, "title": title, "grain": "spatial",
                    "radius_m": radius, "description": desc, "datasets": ds})
    return out


_BY_ID = {c["id"]: c for c in registry()}


def cube(cube_id: str) -> dict | None:
    return _BY_ID.get(cube_id)


# ── finding the tables ──
_TABLES_TTL = 600.0
_cols_cache: dict[str, tuple[float, dict[str, list[str]]]] = {}
_lock = asyncio.Lock()

#: The crime register's signature. One table per year, every year the same.
CRIME_COLS = {"StatisticAreaKod", "YeshuvKod", "StatisticGroup", "Year", "Quarter"}

#: Where a GovMap layer can live with a PostGIS ``geom``, in order of preference.
#: ``idx`` is the index mirror every layer is meant to reach; ``public.append_*``
#: is the archive table, which ``append_geometry`` also gives a ``geom`` (and a
#: GiST index) — and which is the ONLY copy for a layer too large for the
#: mirror's size gate. The national רצף layers (1.09 M plots for רצף מגרשי תב"ע)
#: are exactly that size, so a cube that looked in idx alone would report them
#: missing while their geometry sat one schema over.
GEO_SCHEMAS = ("idx", "public")


def _id8(dataset_id: str) -> str:
    return dataset_id.replace("-", "")[:8].lower()


async def _schema_cols(schema: str) -> dict[str, list[str]]:
    """``{table: [columns]}`` for one schema, cached. One information_schema
    read per schema per ten minutes, shared by every cube and the crime lookup."""
    hit = _cols_cache.get(schema)
    if hit and time.monotonic() - hit[0] < _TABLES_TTL:
        return hit[1]
    async with _lock:
        hit = _cols_cache.get(schema)
        if hit and time.monotonic() - hit[0] < _TABLES_TTL:
            return hit[1]
        try:
            raw = await append_store.schema_table_columns(schema)
        except Exception:  # noqa: BLE001 — schema absent on this deployment
            logger.warning("nadlan layers: schema %s unavailable", schema, exc_info=True)
            raw = {}
        cols = {t: [c["name"] if isinstance(c, dict) else c for c in cs]
                for t, cs in raw.items()}
        _cols_cache[schema] = (time.monotonic(), cols)
        return cols


async def _geo_tables() -> dict[str, tuple[str, str, list[str]]]:
    """``{id8: (schema, table, [columns])}`` for every table that has a ``geom``.

    Keyed on the dataset-id suffix both ``index_mirror.table_name`` and the
    append tables end in, so a layer re-scraped under a new ckan_name still
    resolves. The first schema in GEO_SCHEMAS that holds the layer wins."""
    found: dict[str, tuple[str, str, list[str]]] = {}
    for schema in GEO_SCHEMAS:
        for table, names in sorted((await _schema_cols(schema)).items()):
            if "geom" not in names:
                continue
            if schema == "public" and not table.startswith("append_"):
                continue
            suffix = table.rsplit("_", 1)[-1].lower()
            if len(suffix) == 8 and suffix not in found:
                found[suffix] = (schema, table, names)
    return found


async def crime_tables() -> list[str]:
    """Every public table carrying the crime signature, newest year first is
    decided later (by the ``Year`` the rows carry, not by the table name)."""
    return sorted(t for t, names in (await _schema_cols("public")).items()
                  if CRIME_COLS <= set(names))


def invalidate_caches() -> None:
    _cols_cache.clear()


async def _resolve(c: dict) -> list[tuple[str, str, str, list[str]]]:
    """``[(dataset_id, schema, table, columns)]`` for the cube's datasets on hand."""
    geo = await _geo_tables()
    return [(ds, *geo[_id8(ds)]) for ds in c["datasets"] if _id8(ds) in geo]


async def catalog() -> list[dict]:
    """The registry plus whether each cube can be answered here."""
    idx = await _geo_tables()
    crime = await crime_tables()
    out = []
    for c in registry():
        if c["grain"] == "stat_area":
            available = bool(crime)
        else:
            available = any(_id8(d) in idx for d in c["datasets"])
        out.append({k: v for k, v in c.items() if k != "datasets"}
                   | {"available": available})
    return out


# ── the parcel a cube is asked about ──
async def parcel_row(gush: int, helka: int, suffix: int | None) -> dict | None:
    rows = await nadlan_query.by_gush_helka(gush, helka, suffix)
    return rows[0] if rows else None


def _deg(radius_m: float) -> float:
    """A bbox half-width in degrees that is never SMALLER than radius_m in
    Israel (a degree of longitude is ~94 km at 32°N, of latitude ~111 km)."""
    return max(radius_m, 1.0) / 90_000.0


#: מבא"ת addresses a plan by its INTERNAL id, never by the public plan number
#: (``mavat.iplan.gov.il/SV4/1/{id}/310``; the number "קא/300" cannot be put in a
#: URL). So a link is built only from a field that is named as that id and
#: holds one — a guessed link to the wrong plan is worse than no link.
_MAVAT_ID_KEY = re.compile(r"(^|_)(mavat|pl_?id|plan_?id|mavat_?code|id_?mavat)($|_)", re.I)
_MAVAT_ID_VAL = re.compile(r"^[1-9][0-9]{9}$")
MAVAT_PLAN_URL = "https://mavat.iplan.gov.il/SV4/1/{id}/310"


def _link_for(key: str, value: str) -> str | None:
    """A URL a field's value can be opened at, or None."""
    if re.match(r"^https?://", value):
        return value
    if _MAVAT_ID_KEY.search(key) and _MAVAT_ID_VAL.match(value):
        return MAVAT_PLAN_URL.format(id=value)
    return None


def _clean(v) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.lower() in ("null", "none", "nan"):
        return None
    return s if len(s) <= MAX_VALUE_CHARS else s[:MAX_VALUE_CHARS] + "…"


def _current(columns: list[str]) -> str:
    """The rows still in the source. An append table keeps every version of a
    row and closes the superseded ones with ``last_seen``; without this a
    re-drawn plot would answer twice, its old outline beside its new one."""
    return " AND l.last_seen IS NULL" if "last_seen" in columns else ""


async def spatial_cube(c: dict, parcel: dict) -> dict:
    """The features of every dataset in the cube that touch this parcel."""
    tables = await _resolve(c)
    radius = float(c["radius_m"] or 0)
    aliases = await column_aliases.load_map()
    results: list[dict] = []
    sources: list[dict] = []
    # Identical features (the same row mirrored twice, or two versions that
    # differ only in a dropped bookkeeping column) are one answer.
    seen: set[tuple] = set()
    for ds, schema, table, columns in tables:
        cols = [x for x in columns if x.lower() not in _NOISE_COLS]
        if not cols:
            continue
        sel = ", ".join(f"l.{_qi(x)}::text AS {_qi(x)}" for x in cols)
        # Inside for a polygon cube; within the radius, nearest first, for a
        # point cube. The && against an expanded centroid is what lets the
        # layer's GiST index answer before any exact predicate runs.
        pred = (f"{_X}.ST_Intersects(l.geom, k.centroid)" if radius == 0 else
                f"{_X}.ST_DWithin(l.geom::{_X}.geography, "
                f"k.centroid::{_X}.geography, {radius})")
        sql = f"""
            WITH k AS (
              SELECT centroid FROM public.{_qi(PARCELS_TABLE)}
              WHERE parcel_key = $1 AND centroid IS NOT NULL
            )
            SELECT {sel},
                   round({_X}.ST_Distance(l.geom::{_X}.geography,
                                         k.centroid::{_X}.geography)::numeric, 1)
                     AS "__distance_m"
            FROM k JOIN {_qi(schema)}.{_qi(table)} l
              ON l.geom OPERATOR({_X}.&&) {_X}.ST_Expand(k.centroid, {_deg(radius)})
             AND {pred}{_current(columns)}
            ORDER BY "__distance_m"
            LIMIT {MAX_FEATURES}
        """
        try:
            rows = await nadlan_query._fetch(sql, parcel["parcel_key"])
        except Exception as e:  # noqa: BLE001 — one bad layer must not sink the cube
            logger.warning("nadlan layers: %s/%s failed: %s", c["id"], table, e)
            continue
        labels = aliases.get((schema, table), {})
        console = (f'SELECT * FROM {_qi(schema)}.{_qi(table)} l WHERE {_X}.ST_DWithin('
                   f'l.geom::{_X}.geography, {_X}.ST_SetSRID({_X}.ST_MakePoint('
                   f'{parcel["lon"]}, {parcel["lat"]}), 4326)::{_X}.geography, '
                   f'{max(radius, 1.0)}) LIMIT 200') if parcel.get("lat") is not None else \
            f"SELECT * FROM {_qi(schema)}.{_qi(table)} LIMIT 200"
        sources.append({"dataset_id": ds, "table": f"{schema}.{table}",
                        "page_url": f"{nadlan_query.SITE}/versions/{ds}",
                        "row_url": nadlan_query._console_url(console)})
        for r in rows:
            fields = [{"key": k, "label": labels.get(k) or k, "value": _clean(r.get(k))}
                      for k in cols]
            fields = [f for f in fields if f["value"] is not None]
            for f in fields:
                link = _link_for(f["key"], f["value"])
                if link:
                    f["url"] = link
            sig = (ds, tuple((f["key"], f["value"]) for f in fields))
            if fields and sig not in seen:
                seen.add(sig)
                results.append({"dataset_id": ds, "distance_m": float(r["__distance_m"] or 0),
                                "fields": fields})
    results.sort(key=lambda f: f["distance_m"])
    return {"features": results[:MAX_FEATURES], "count": len(results),
            "sources": sources, "available": bool(tables)}


async def probe(parcel: dict) -> dict[str, int]:
    """How many features each spatial cube would return, in ONE round trip.

    This is what lets the page show "3" on a closed cube without opening it:
    a count on a GiST bbox probe per table, all UNIONed, so the cost is that of
    the index lookups and never of reading a row's attributes."""
    parts: list[str] = []
    for c in registry():
        if c["grain"] != "spatial":
            continue
        radius = float(c["radius_m"] or 0)
        for _, schema, table, cols in await _resolve(c):
            pred = (f"{_X}.ST_Intersects(l.geom, k.centroid)" if radius == 0 else
                    f"{_X}.ST_DWithin(l.geom::{_X}.geography, "
                    f"k.centroid::{_X}.geography, {radius})")
            parts.append(
                f"SELECT {nadlan_query._lit(c['id'])} AS cube, count(*) AS n "
                f"FROM k JOIN {_qi(schema)}.{_qi(table)} l ON l.geom OPERATOR({_X}.&&) "
                f"{_X}.ST_Expand(k.centroid, {_deg(radius)}) AND {pred}{_current(cols)}")
    if not parts:
        return {}
    sql = (f"WITH k AS (SELECT centroid FROM public.{_qi(PARCELS_TABLE)} "
           f"WHERE parcel_key = $1 AND centroid IS NOT NULL) "
           f"SELECT cube, sum(n)::int AS n FROM ({' UNION ALL '.join(parts)}) u GROUP BY cube")
    try:
        rows = await nadlan_query._fetch(sql, parcel["parcel_key"])
    except Exception as e:  # noqa: BLE001 — counts are a nicety; the cubes still open
        logger.warning("nadlan layers: probe failed: %s", e)
        return {}
    return {r["cube"]: r["n"] for r in rows}


# ── crime ──
def _per_1000(n: int | None, pop: int | None) -> float | None:
    return round(n * 1000 / pop, 1) if n is not None and pop else None


_years_cache: dict[str, str] = {}


async def _table_years(tables: list[str]) -> dict[str, str]:
    """table -> the year its rows carry. One cheap sample per table, once per
    process: a year's table never changes which year it is."""
    out: dict[str, str] = {t: _years_cache[t] for t in tables if t in _years_cache}
    for t in tables:
        if t in out:
            continue
        try:
            rows = await nadlan_query._fetch(
                f'SELECT max("Year") AS y FROM (SELECT "Year" FROM public.{_qi(t)} LIMIT 50) s')
        except Exception:  # noqa: BLE001
            continue
        y = (rows[0]["y"] or "").strip() if rows else ""
        if y:
            out[t] = _years_cache[t] = y
    return out


async def crime_cube(parcel: dict, year: str | None = None) -> dict:
    """Crime cases in the parcel's statistical area against its settlement and
    the country, by offence group.

    The register keys its area as the national 8-digit code (settlement * 10000
    + area: ``62000113`` is area 113 of בת ים), which is exactly the
    ``yishuv_stat`` the א"ס block already resolves — so the join is an equality
    on a published code, not a name match.

    The three scopes come from ONE scan of the year's table (~400k rows), with
    ``count(*) FILTER`` per scope, so the comparison is between counts taken the
    same way from the same rows."""
    tables = await crime_tables()
    if not tables:
        return {"available": False}
    years = await _table_years(tables)
    if not years:
        return {"available": False}
    ordered = sorted(years.items(), key=lambda kv: kv[1], reverse=True)
    table, chosen = next(((t, y) for t, y in ordered if y == year), ordered[0])

    areas = await nadlan_query.stat_areas([parcel["parcel_key"]])
    area = areas.get(parcel["parcel_key"]) or {}
    area_code = area.get("yishuv_stat")
    sc = parcel.get("settlement_code")

    rows = await nadlan_query._fetch(f"""
        SELECT nullif(btrim("StatisticGroup"), '') AS grp,
               count(*) FILTER (WHERE btrim("StatisticAreaKod") = $1) AS area,
               count(*) FILTER (WHERE btrim("YeshuvKod") = $2)        AS settlement,
               count(*)                                              AS national,
               max(nullif(btrim("StatisticArea"), ''))
                 FILTER (WHERE btrim("StatisticAreaKod") = $1)       AS area_name
        FROM public.{_qi(table)}
        GROUP BY 1
    """, str(area_code or ""), str(sc or ""))

    pops = await nadlan_query._fetch(
        "SELECT (SELECT population FROM public.over_settlements WHERE code = $1) AS s, "
        "(SELECT sum(population) FROM public.over_settlements) AS n", int(sc or 0))
    pop_s = pops[0]["s"] if pops else None
    pop_n = int(pops[0]["n"]) if pops and pops[0]["n"] else None
    pop_a = area.get("population")

    groups = []
    tot = {"area": 0, "settlement": 0, "national": 0}
    area_name = None
    for r in rows:
        for k in tot:
            tot[k] += r[k]
        area_name = area_name or r["area_name"]
        groups.append({"group": r["grp"] or "לא מסווג",
                       "area": r["area"], "settlement": r["settlement"],
                       "national": r["national"],
                       "area_per_1000": _per_1000(r["area"], pop_a),
                       "settlement_per_1000": _per_1000(r["settlement"], pop_s),
                       "national_per_1000": _per_1000(r["national"], pop_n)})
    groups.sort(key=lambda g: g["national"], reverse=True)

    notes = []
    if area_code is None:
        notes.append("לא נמצא אזור סטטיסטי לחלקה, ולכן מוצגים רק נתוני היישוב והארץ.")
    elif tot["area"] == 0:
        notes.append("אין תיקים רשומים לקוד האזור הזה בשנה שנבחרה. ייתכן שהמשטרה "
                     "מפרסמת אותו על חלוקת אזורים אחרת, או שהאזור קטן מכדי לפרסם.")
    if area.get("main_function") and "מגורים" not in str(area.get("main_function")):
        notes.append(f"ייעוד האזור הוא {area['main_function']}: באזור שאינו מגורים "
                     "שיעור לתושב מנופח, כי התיקים נפתחים במקום העבירה ולא במקום המגורים.")
    return {
        "available": True,
        "year": chosen,
        "years": [y for _, y in ordered],
        "table": f"public.{table}",
        "row_url": nadlan_query._console_url(
            f'SELECT * FROM public.{_qi(table)} WHERE "StatisticAreaKod" = '
            f"{nadlan_query._lit(area_code or '')} LIMIT 200"),
        "scopes": {
            "area": {"code": area_code, "name": area_name or area.get("settlement_name"),
                     "population": pop_a, "total": tot["area"],
                     "per_1000": _per_1000(tot["area"], pop_a)},
            "settlement": {"code": sc, "name": parcel.get("locality_name"),
                           "population": pop_s, "total": tot["settlement"],
                           "per_1000": _per_1000(tot["settlement"], pop_s)},
            "national": {"population": pop_n, "total": tot["national"],
                         "per_1000": _per_1000(tot["national"], pop_n)},
        },
        "groups": groups,
        "notes": notes,
    }
