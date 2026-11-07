"""נדל"ן לעם — one search box for every identifier a property question starts from.

The page had four rigid forms (map, address, zip, gush/helka). This is the box
that takes whatever was typed and answers from every connected register at
once, on the field that register is keyed on:

    a point            "32.08, 34.78"              → the parcels under it
    a gush/helka       "6638/120", "גוש 6638 חלקה 120"
    a gush             "6638"                      → its parcels
    a zip              "6423906", "64239"          → the addresses behind it
    an address         "דיזנגוף 50 תל אביב"        → the parcels it sits on
    a tender           "3/2025", "20250003"        → מכרזי רמ"י, its lots
    a plan             "תא/3598", "507-0391482"    → מבא"ת and גבולות תב"ע
    free text          "רמת החייל"                 → streets, plan names

Ambiguity is the normal case, not the edge: "3/2025" is a gush/helka AND a
tender number, "5/53/14" is a plan, "64239" is a zip AND a gush. So
``interpret`` returns EVERY plausible reading, ``search`` runs them all side by
side, and the page shows each group that found something. Nothing is guessed
away.

Every group is bounded (a small limit, a statement timeout in the callers'
``_fetch``) and runs concurrently; one group failing never sinks the others —
it comes back with an ``error`` and the rest still answer.
"""
from __future__ import annotations

import asyncio
import logging
import re

from app.services import nadlan_query, nadlan_text

logger = logging.getLogger(__name__)

#: Results per group. The box is for finding, the property card for reading.
PROPERTY_LIMIT = 10
TENDER_LIMIT = 10
PLAN_LIMIT = 8
STREET_LIMIT = 8
PLAN_PARCEL_SAMPLE = 12

#: The plan registers the crosswalk holds (nadlan_layer_links), with the field
#: that carries the plan number and, where there is one, its name.
PLAN_SOURCES: list[dict] = [
    {"id": "mavat", "dataset_id": "b105ae22-a6f5-4cd0-b37e-c63dc5da40c3",
     "title": 'מבא"ת — מינהל התכנון', "number": "pl_number", "name": "pl_name",
     "url": "pl_url", "status": "station_desc"},
    {"id": "taba_bounds", "dataset_id": "2156f30e-cb97-4737-a2f5-bd030b61c944",
     "title": 'גבולות תב"ע — רמ"י', "number": "tochnit", "name": None,
     "url": None, "status": None, "settlement": "shemishuv"},
]

_HEB = re.compile(r"[א-ת]")
_TENDER_NUM = re.compile(r"^\s*(\d{1,4})\s*/\s*((?:19|20)\d{2})\s*$")
_TENDER_ID = re.compile(r"^\s*((?:19|20)\d{2})(\d{4})\s*$")
# מבא"ת's national numbering: three digits, a dash, seven digits.
_MAVAT_NUM = re.compile(r"^\s*(\d{3})\s*-\s*(\d{7})\s*$")
# A local plan number: a Hebrew committee prefix with slashes ("תא/3598",
# "גב/מק/281כ"), or digits joined by two or more slashes ("5/53/14").
_LOCAL_PLAN = re.compile(r"^\s*(?:[א-ת\"׳״']{1,6}\s*/\s*)+[\dא-ת/\-\s]*\d[\dא-ת/\-\s]*$")
_SLASHED_DIGITS = re.compile(r"^\s*\d+(?:\s*/\s*\d+){2,}[א-ת]?\s*$")
_PLAN_WORD = re.compile(r"^\s*(?:תכנית|תוכנית|תב\"ע|תב״ע)\s+(.+)$")
_HOUSE = re.compile(r"^(\d{1,4})([א-ת])?$")


def plan_norm(s: str) -> str:
    """A plan number with its punctuation and spaces dropped, so "תא/ 3598",
    "תא/3598" and "תא 3598" are one number."""
    return re.sub(r"[\s/\\\-.\"'׳״]", "", (s or "").lower())


def interpret(q: str) -> list[dict]:
    """Every plausible reading of ``q``, most specific first. Pure."""
    s = (q or "").strip()
    if not s:
        return []
    out: list[dict] = []
    sniff = nadlan_text.sniff_mode(s)
    mode = sniff["mode"]
    if mode in ("point", "gush_helka", "zip", "gush"):
        out.append({"kind": mode, "parsed": sniff["parsed"]})
        for alt in sniff["alternatives"]:
            out.append({"kind": alt["mode"], "parsed": alt["parsed"]})

    m = _TENDER_NUM.match(s)
    if m:
        n, year = int(m.group(1)), int(m.group(2))
        out.append({"kind": "tender", "parsed": {"tender_id": year * 10_000 + n,
                                                  "tender_number": f"{n}/{year}"}})
    m = _TENDER_ID.match(s)
    if m:
        out.append({"kind": "tender", "parsed": {
            "tender_id": int(s.strip()),
            "tender_number": f"{int(m.group(2))}/{m.group(1)}"}})

    pm = _PLAN_WORD.match(s)
    plan_text = pm.group(1).strip() if pm else s
    if (_MAVAT_NUM.match(plan_text) or _LOCAL_PLAN.match(plan_text)
            or _SLASHED_DIGITS.match(plan_text) or pm):
        out.append({"kind": "plan", "parsed": {"number": plan_text,
                                               "norm": plan_norm(plan_text)}})

    if mode == "address" and _HEB.search(s) and not any(o["kind"] == "plan" for o in out):
        out.append({"kind": "text", "parsed": {"text": s}})
    return out


def split_address(tokens: list[str], settlement_spans: dict[str, int]) -> dict | None:
    """``{city, street, number}`` from the words of a free-text address, given
    which word spans are a known settlement (``span -> code``). Pure.

    The settlement is the longest known span at the START or the END ("תל אביב
    דיזנגוף 50" / "דיזנגוף 50 תל אביב"); the house number is the token that
    looks like one; the street is what is left. None when no settlement is
    found or nothing is left for the street."""
    n = len(tokens)
    best: tuple[int, int, int] | None = None   # (length, start, end)
    for length in range(n, 0, -1):
        for start in (0, n - length):
            span = " ".join(tokens[start:start + length])
            if span in settlement_spans and (best is None or length > best[0]):
                best = (length, start, start + length)
        if best:
            break
    if not best:
        return None
    _, a, b = best
    city = " ".join(tokens[a:b])
    rest = tokens[:a] + tokens[b:]
    number = None
    street_words = []
    for t in rest:
        if number is None and _HOUSE.match(t):
            number = t
        else:
            street_words.append(t)
    street = " ".join(w for w in street_words if w not in ("רחוב", "רח'", "רח׳"))
    if not street:
        return None
    return {"city": city, "street": street, "number": number}


def _tokens(s: str) -> list[str]:
    s = re.sub(r"[,،]", " ", s)
    return [t for t in s.split() if t]


def _spans(tokens: list[str]) -> list[str]:
    """Candidate settlement spans: every run of up to 4 words at either end."""
    out: list[str] = []
    n = len(tokens)
    for length in range(1, min(4, n) + 1):
        for start in (0, n - length):
            span = " ".join(tokens[start:start + length])
            if span not in out:
                out.append(span)
    return out


# ── the groups ──
async def _properties(kind: str, parsed: dict) -> tuple[list[dict], list[dict] | None]:
    addrs = None
    if kind == "point":
        parcels = await nadlan_query.by_point(parsed["lat"], parsed["lon"], 0.0, PROPERTY_LIMIT)
    elif kind == "gush_helka":
        parcels = await nadlan_query.by_gush_helka(parsed["gush"], parsed["helka"])
    elif kind == "gush":
        parcels = await nadlan_query.by_gush_helka(parsed["gush"], 0)
    elif kind == "zip":
        addrs, parcels = await nadlan_query.by_zip(parsed["zip"])
    elif kind == "address":
        addrs, parcels = await nadlan_query.by_address(
            parsed["city"], parsed["street"], parsed.get("number"))
    else:
        return [], None
    return parcels[:PROPERTY_LIMIT], addrs


async def _resolve_address(text: str) -> dict | None:
    """A free-text address split into its parts, the settlement checked
    against the gazetteer (``over_settlement_code``)."""
    toks = _tokens(text)
    if len(toks) < 2:
        return None
    spans = _spans(toks)
    rows = await nadlan_query._fetch(
        "SELECT s AS span, public.over_settlement_code(s) AS code "
        "FROM unnest($1::text[]) AS s", spans)
    known = {r["span"]: r["code"] for r in rows if r.get("code")}
    return split_address(toks, known)


async def _tenders(parsed: dict) -> list[dict]:
    from app.services import michrazim
    if not await michrazim.is_ready():
        return []
    res = await michrazim.search_lots({"tender_id": parsed["tender_id"]},
                                      limit=200, sort="year_desc")
    lots = res.get("data") or []
    if not lots:
        return []
    first = lots[0]
    won = [x for x in lots if x.get("amount_basis")]
    return [{
        "tender_id": str(parsed["tender_id"]),
        "tender_number": first.get("tender_number") or parsed["tender_number"],
        "settlement": first.get("settlement"),
        "tender_type": first.get("tender_type"),
        "purpose": first.get("purpose"),
        "status": first.get("status"),
        "committee_date": first.get("committee_date"),
        "lots": len(lots),
        "lots_won": len(won),
        "units": sum(int(x.get("units") or 0) for x in lots),
        "tender_url": first.get("tender_url") or michrazim.TENDER_URL.format(
            id=parsed["tender_id"]),
        "lot_names": [x.get("lot_name") for x in lots[:TENDER_LIMIT] if x.get("lot_name")],
    }]


def _plans_sql(by: str) -> str:
    """One statement over the crosswalked plan layers, matched on the plan
    number (normalised, exact or prefix) or on the plan name (contains)."""
    from app.services.nadlan_layer_links import FEATURES_TABLE, STATE_TABLE
    parts = []
    for src in PLAN_SOURCES:
        num = f"f.attrs->>'{src['number']}'"
        name = f"f.attrs->>'{src['name']}'" if src["name"] else "NULL"
        if by == "number":
            cond = (f"regexp_replace(lower({num}), '[[:space:]/\\\\.\"''׳״-]', '', 'g') "
                    f"LIKE $1 || '%'")
        else:
            if not src["name"]:
                continue
            cond = f"{name} ILIKE '%' || $2 || '%'"
        settlement = f"f.attrs->>'{src['settlement']}'" if src.get("settlement") else "NULL"
        url = f"f.attrs->>'{src['url']}'" if src["url"] else "NULL"
        status = f"f.attrs->>'{src['status']}'" if src["status"] else "NULL"
        parts.append(f"""
            SELECT '{src['id']}' AS source, {num} AS number, {name} AS name,
                   {settlement} AS settlement, {url} AS url, {status} AS status,
                   cardinality(f.parcels) AS parcel_count,
                   f.parcels[1:{PLAN_PARCEL_SAMPLE}] AS parcels,
                   (regexp_replace(lower({num}), '[[:space:]/\\\\.\"''׳״-]', '', 'g') = $1) AS exact
            FROM public."{FEATURES_TABLE}" f
            JOIN public."{STATE_TABLE}" s
              ON s.dataset_id = f.dataset_id AND s.generation = f.generation
            WHERE f.dataset_id = '{src['dataset_id']}' AND {cond}""")
    if not parts:
        return ""
    return (" UNION ALL ".join(parts)
            + f" ORDER BY exact DESC, parcel_count DESC NULLS LAST LIMIT {PLAN_LIMIT}")


async def _plans(norm: str, text: str | None) -> list[dict]:
    sql_num = _plans_sql("number")
    rows = await nadlan_query._fetch(sql_num, norm, text or "") if norm else []
    if not rows and text and len(text) >= 3:
        sql_name = _plans_sql("name")
        if sql_name:
            rows = await nadlan_query._fetch(sql_name, plan_norm(text), text)
    titles = {s["id"]: s["title"] for s in PLAN_SOURCES}
    out = []
    seen = set()
    for r in rows:
        key = (r["source"], r["number"], r["name"])
        if key in seen:
            continue
        seen.add(key)
        parcels = []
        for k in (r.get("parcels") or []):
            sp = nadlan_text.split_parcel_key(k)
            if sp:
                parcels.append({"gush": sp[0], "suffix": sp[1], "helka": sp[2]})
        out.append({"source": r["source"], "source_title": titles.get(r["source"]),
                    "number": r["number"], "name": r["name"], "settlement": r["settlement"],
                    "url": r["url"], "status": r["status"],
                    "parcel_count": r["parcel_count"], "parcels": parcels,
                    "exact": bool(r["exact"])})
    return out


async def _guard(name: str, coro):
    try:
        return name, await coro, None
    except Exception as e:  # noqa: BLE001 — one group must not sink the search
        logger.warning("nadlan search: %s failed: %s", name, e)
        return name, None, str(e)[:200]


async def search(q: str) -> dict:
    """Every reading of ``q``, run side by side. Returns
    ``{q, interpretations, properties, tenders, plans, streets, errors}``;
    ``properties`` are full envelopes (the same the four forms return)."""
    readings = interpret(q)
    errors: dict[str, str] = {}
    out: dict = {"q": q, "interpretations": readings, "properties": [],
                 "addresses": None, "tenders": [], "plans": [], "streets": [],
                 "address_parse": None}

    # A free-text reading first needs its address split (one round trip).
    text = next((r["parsed"]["text"] for r in readings if r["kind"] == "text"), None)
    if text:
        try:
            parsed = await _resolve_address(text)
        except Exception as e:  # noqa: BLE001
            parsed, errors["address"] = None, str(e)[:200]
        if parsed:
            out["address_parse"] = parsed
            readings.append({"kind": "address", "parsed": parsed})

    jobs = []
    prop_kinds = [r for r in readings if r["kind"] in
                  ("point", "gush_helka", "gush", "zip", "address")]
    for r in prop_kinds:
        jobs.append(_guard(f"properties:{r['kind']}", _properties(r["kind"], r["parsed"])))
    for r in readings:
        if r["kind"] == "tender":
            jobs.append(_guard("tenders", _tenders(r["parsed"])))
        elif r["kind"] == "plan":
            jobs.append(_guard("plans", _plans(r["parsed"]["norm"], r["parsed"]["number"])))
    if text:
        jobs.append(_guard("plans", _plans("", text)))
        street_q = (out["address_parse"] or {}).get("street") or text
        jobs.append(_guard("streets", nadlan_query.suggest_streets(street_q, None, STREET_LIMIT)))

    parcels: list[dict] = []
    seen_keys: set[str] = set()
    addrs_all: list[dict] = []
    for name, res, err in await asyncio.gather(*jobs):
        if err:
            errors[name] = err
            continue
        if name.startswith("properties:"):
            ps, addrs = res
            for p in ps:
                k = p.get("parcel_key")
                if k and k not in seen_keys:
                    seen_keys.add(k)
                    p["_found_by"] = name.split(":", 1)[1]
                    parcels.append(p)
            if addrs:
                addrs_all.extend(addrs)
        elif name == "tenders":
            out["tenders"].extend(res)
        elif name == "plans":
            have = {(p["source"], p["number"], p["name"]) for p in out["plans"]}
            out["plans"].extend(p for p in res if (p["source"], p["number"], p["name"]) not in have)
        elif name == "streets":
            out["streets"] = res

    if parcels:
        found_by = {p["parcel_key"]: p.pop("_found_by") for p in parcels}
        try:
            env = await nadlan_query.property_envelope(
                parcels[:PROPERTY_LIMIT], addresses=addrs_all or None)
            for e in env:
                e["found_by"] = found_by.get(e.get("parcel_key"))
            if any(r["kind"] == "address" for r in prop_kinds):
                env = nadlan_query.rank_address_results(env)
            out["properties"] = env
        except Exception as e:  # noqa: BLE001
            errors["properties"] = str(e)[:200]
    out["plans"] = out["plans"][:PLAN_LIMIT]
    out["errors"] = errors
    return out
