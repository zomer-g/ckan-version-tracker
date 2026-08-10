"""Enrichment pipeline for imported יומן לעם (Ocal) diaries — ports Ocal's
entityExtractor / crossReferencer / eventMatcher (the FREE stages, no LLM).

Run after a source's events are upserted (see ocal_import.import_resource):
  1. extract_entities_for_source — Stage 1 owner-link + Stage 2 participant /
     location / title parse (heuristic, method 'owner' / 'participant_parse').
  2. cross_reference_for_source — does person B's own diary confirm a meeting
     that A's diary says B attended? (entity_cross_refs).
  3. refresh_entity_matview — REFRESH MATERIALIZED VIEW CONCURRENTLY.
  4. find_matches_for_source — group cross-diary duplicate events (similar_events
     + diary_events.match_group_id).

Stage 3 (AI-NER, ``ai_ner_for_source``) is the PAID LLM stage — a faithful port
of Ocal's ``stageAiNer``. Like the legacy service it is OFF the automatic path
(Ocal's pipeline.ts imported with skipAI=true) and runs only on admin demand, or
automatically when ``settings.ocal_ai_ner_auto`` is set. It reuses the same LLM
keys as /api/cbs/ask (DeepSeek preferred, then Anthropic).

All writes go to the ocal DB (app/services/ocal_db.py). Thresholds/method strings
match Ocal exactly so the enrichment is consistent with the migrated corpus.
"""
from __future__ import annotations

import json
import logging
import re
import unicodedata

from app.config import settings
from app.services import ocal_db

logger = logging.getLogger(__name__)

# ── text normalisation + name cleaning (ported from entityExtractor.ts) ──────
_INVIS = re.compile("[​-‏﻿­  ‪-‮⁠]")
HONORIFIC_RE = re.compile(
    r"^(פרופ[׳']?|ד[״\"]ר|עו[״\"]ד|מנכ[״\"]ל|מנמ[״\"]ל|מנה[״\"]ל|ח[״\"]כ|גב[׳']|הרב|הגב|שרה?|מר|דר)\s+",
    re.IGNORECASE)
SPLIT_RE = re.compile(r"[,;\n|/\\]+")
PARTICIPANT_KEYS_RE = re.compile(r"משתתפ|נוכח|מוזמנ|participant|attendee|invitee", re.IGNORECASE)
LOCATION_KEYS_RE = re.compile(r"מקום|מיקום|location|place|venue|כתובת|address", re.IGNORECASE)
ORG_PHRASE_RE = re.compile(
    r"(משרד|ועד[תה]|רשות|עיריי?ת|מועצ[תה]|הסתדרות|בנק|אוניברסיט[תה])\s+"
    r"([֐-׿\"'״׳\-]+(?:\s+[֐-׿\"'״׳\-]+){0,3})")

# title normalisation (ported from eventMatcher.ts, shared with cross-ref)
_NIKUD_RE = re.compile("[֑-ׇ]")
_ZW_RE = re.compile("[​-‏﻿]")
_PREFIX_RE = re.compile(
    r"^(פגישה עם |פגישת |ישיבה בנושא |ישיבת |דיון בנושא |דיון על |ביקור ב|השתתפות ב|נוכחות ב)")
_ABBREV = {"רה״מ": "ראש הממשלה", "מנכ״ל": "מנהל כללי", "סמנכ״ל": "סגן מנהל כללי"}


def normalize_text(s: str | None) -> str:
    s = unicodedata.normalize("NFC", s or "")
    s = _INVIS.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def clean_name(s: str | None) -> str:
    return normalize_text(HONORIFIC_RE.sub("", (s or "").strip()))


def normalize_title(s: str | None) -> str:
    t = unicodedata.normalize("NFC", s or "")
    t = _NIKUD_RE.sub("", t)
    t = _ZW_RE.sub("", t)
    for k, v in _ABBREV.items():
        t = re.sub(re.escape(k).replace('״', '["״]'), v, t)
    t = _PREFIX_RE.sub("", t)
    return re.sub(r"\s+", " ", t).strip().lower()


def jaccard(a: str, b: str) -> float:
    A, B = set(a.split()), set(b.split())
    if not A and not B:
        return 1.0
    inter = len(A & B)
    union = len(A) + len(B) - inter
    return inter / union if union else 0.0


def _best(name: str, registry: list[tuple]) -> tuple:
    """registry: list of (norm_name, id, canonical). → (id, canonical, score)."""
    best_id = best_name = None
    best = 0.0
    for nn, rid, canon in registry:
        s = jaccard(name, nn)
        if s > best:
            best, best_id, best_name = s, rid, canon
    return best_id, best_name, best


async def _load_registries():
    people = await ocal_db.fetch("SELECT id, name FROM people WHERE name IS NOT NULL")
    orgs = await ocal_db.fetch("SELECT id, name FROM organizations WHERE name IS NOT NULL")
    preg = [(normalize_text(p["name"]).lower(), p["id"], p["name"])
            for p in people if len((p["name"] or "").strip()) >= 2]
    oreg = [(normalize_text(o["name"]).lower(), o["id"], o["name"])
            for o in orgs if len((o["name"] or "").strip()) >= 2]
    return preg, oreg


_EE_INSERT = """
INSERT INTO event_entities
    (event_id, entity_type, entity_id, entity_name, role, raw_mention, confidence, extraction_method)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (event_id, entity_type, entity_name, role) DO NOTHING
"""


# ── Stage 1 + Stage 2 ────────────────────────────────────────────────────────

async def extract_entities_for_source(source_id, *, clear_existing: bool = False) -> dict:
    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        if clear_existing:
            await conn.execute(
                "DELETE FROM event_entities WHERE event_id IN "
                "(SELECT id FROM diary_events WHERE source_id=$1)", source_id)
        # Stage 1 — owner link (confidence 1.0, method 'owner').
        owner = await conn.execute("""
            INSERT INTO event_entities
                (event_id, entity_type, entity_id, entity_name, role, confidence, extraction_method)
            SELECT e.id, 'person', s.person_id, p.name, 'owner', 1.0::real, 'owner'
            FROM diary_events e
            JOIN diary_sources s ON s.id = e.source_id
            JOIN people p ON p.id = s.person_id
            WHERE e.source_id = $1 AND s.person_id IS NOT NULL
            ON CONFLICT (event_id, entity_type, entity_name, role) DO NOTHING
        """, source_id)

    preg, oreg = await _load_registries()
    rows = await ocal_db.fetch(
        "SELECT id, title, location, participants, other_fields "
        "FROM diary_events WHERE source_id=$1 AND is_active", source_id)

    to_insert: list[tuple] = []
    seen: set[tuple] = set()

    def add(event_id, etype, eid, ename, role, raw, conf):
        ename = (ename or "").strip()
        if not ename:
            return
        key = (event_id, etype, ename, role)
        if key in seen:
            return
        seen.add(key)
        to_insert.append((event_id, etype, eid, ename, role, raw, float(conf), "participant_parse"))

    for ev in rows:
        eid = ev["id"]
        other = ev["other_fields"] or {}
        # gather participant / location text from mapped fields + other_fields keys
        p_text = [ev["participants"] or ""]
        l_text = [ev["location"] or ""]
        if isinstance(other, dict):
            for k, v in other.items():
                if v in (None, ""):
                    continue
                if PARTICIPANT_KEYS_RE.search(str(k)):
                    p_text.append(str(v))
                elif LOCATION_KEYS_RE.search(str(k)):
                    l_text.append(str(v))
        title = ev["title"] or ""

        # ── participants → role 'participant'
        for raw in SPLIT_RE.split(" , ".join(t for t in p_text if t)):
            nm = clean_name(raw)
            if len(nm) < 2:
                continue
            key = nm.lower()
            pid, pcanon, ps = _best(key, preg)
            if ps >= 0.85:
                add(eid, "person", pid, pcanon, "participant", raw.strip(), 0.9)
            elif ps >= 0.6:
                add(eid, "person", pid, pcanon, "participant", raw.strip(), 0.7)
            else:
                oid, ocanon, os = _best(key, oreg)
                if os >= 0.6:
                    add(eid, "organization", oid, ocanon, "participant", raw.strip(), 0.9 if os >= 0.85 else 0.7)
                else:
                    add(eid, "person", None, nm, "participant", raw.strip(), 0.5)

        # ── location → role 'location'
        for raw in SPLIT_RE.split(" , ".join(t for t in l_text if t)):
            nm = clean_name(raw)
            if len(nm) < 2:
                continue
            low = nm.lower()
            if re.match(r"^https?:?$", low) or low.startswith("www.") or "@" in low or re.match(r"^\d+$", low):
                continue
            oid, ocanon, os = _best(low, oreg)
            if os >= 0.6:
                add(eid, "organization", oid, ocanon, "location", raw.strip(), 0.9 if os >= 0.85 else 0.7)
            else:
                add(eid, "place", None, nm, "location", raw.strip(), 0.9)

        # ── title mentions → role 'mentioned'
        tnorm = normalize_text(title).lower()
        if len(tnorm) >= 3:
            for nn, rid, canon in preg:
                if len(nn) >= 3 and nn in tnorm:
                    add(eid, "person", rid, canon, "mentioned", None, 0.8)
            for nn, rid, canon in oreg:
                if len(nn) >= 3 and nn in tnorm:
                    add(eid, "organization", rid, canon, "mentioned", None, 0.8)
            for m in ORG_PHRASE_RE.finditer(title):
                phrase = clean_name(m.group(0))
                if len(phrase) < 4:
                    continue
                oid, ocanon, os = _best(phrase.lower(), oreg)
                if os >= 0.7:
                    add(eid, "organization", oid, ocanon, "mentioned", m.group(0), 0.8)
                else:
                    add(eid, "organization", None, phrase, "mentioned", m.group(0), 0.6)

    inserted = 0
    if to_insert:
        async with pool.acquire() as conn:
            for i in range(0, len(to_insert), 500):
                await conn.executemany(_EE_INSERT, to_insert[i:i + 500])
        inserted = len(to_insert)
    logger.info("ocal_enrich: entities for %s — owner=%s parsed=%d", source_id, owner, inserted)
    return {"owner": owner, "parsed": inserted}


# ── Cross-referencing ─────────────────────────────────────────────────────────

_XREF_UPSERT = """
INSERT INTO entity_cross_refs
    (event_entity_id, source_event_id, target_person_id, target_source_id,
     status, matched_event_id, match_method, match_score, event_date)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
ON CONFLICT (event_entity_id, target_source_id) DO UPDATE SET
    status=EXCLUDED.status, matched_event_id=EXCLUDED.matched_event_id,
    match_method=EXCLUDED.match_method, match_score=EXCLUDED.match_score,
    event_date=EXCLUDED.event_date
"""
_XREF_TITLE_THRESHOLD = 0.4


async def cross_reference_for_source(source_id, *, is_resync: bool = False) -> dict:
    src = await ocal_db.fetchrow(
        "SELECT id, person_id FROM diary_sources WHERE id=$1", source_id)
    if not src:
        return {"written": 0}
    if is_resync:
        await ocal_db.execute(
            "DELETE FROM entity_cross_refs WHERE source_event_id IN "
            "(SELECT id FROM diary_events WHERE source_id=$1)", source_id)

    mentions = await ocal_db.fetch("""
        SELECT ee.id AS event_entity_id, ee.event_id, ee.entity_id AS person_id,
               de.event_date, de.title, de.match_group_id
        FROM event_entities ee
        JOIN diary_events de ON de.id = ee.event_id
        WHERE de.source_id = $1 AND de.is_active
          AND ee.entity_type = 'person' AND ee.entity_id IS NOT NULL
          AND ee.confidence >= 0.5 AND ee.role IN ('participant','mentioned')
    """, source_id)
    own_person = src["person_id"]
    mentions = [m for m in mentions if m["person_id"] != own_person and m["event_date"]]
    if not mentions:
        return {"written": 0}

    person_ids = list({m["person_id"] for m in mentions})
    tgt_rows = await ocal_db.fetch("""
        SELECT id, person_id, first_event_date, last_event_date
        FROM diary_sources
        WHERE is_enabled AND person_id = ANY($1::uuid[])
          AND first_event_date IS NOT NULL AND last_event_date IS NOT NULL
    """, person_ids)
    by_person: dict = {}
    for r in tgt_rows:
        by_person.setdefault(r["person_id"], []).append(r)

    pool = await ocal_db.get_pool()
    out: list[tuple] = []
    async with pool.acquire() as conn:
        for m in mentions:
            for tgt in by_person.get(m["person_id"], []):
                if tgt["id"] == source_id:
                    continue
                d = m["event_date"]
                if d < tgt["first_event_date"] or d > tgt["last_event_date"]:
                    continue
                found, method, score, matched_id = await _find_match_in_source(
                    conn, m, tgt["id"], d)
                out.append((
                    m["event_entity_id"], m["event_id"], m["person_id"], tgt["id"],
                    "confirmed" if found else "unconfirmed", matched_id, method, score, d,
                ))
        written = 0
        for i in range(0, len(out), 200):
            await conn.executemany(_XREF_UPSERT, out[i:i + 200])
            written += len(out[i:i + 200])
    logger.info("ocal_enrich: cross-refs for %s — %d written", source_id, written)
    return {"written": written}


async def _find_match_in_source(conn, mention, target_source_id, event_date):
    # Method 1 — same match_group in the target source.
    if mention["match_group_id"]:
        row = await conn.fetchrow(
            "SELECT id FROM diary_events WHERE source_id=$1 AND match_group_id=$2 AND is_active LIMIT 1",
            target_source_id, mention["match_group_id"])
        if row:
            return True, "match_group", 1.0, row["id"]
    # Method 2 — best title similarity on the same date.
    evs = await conn.fetch(
        "SELECT id, title FROM diary_events WHERE source_id=$1 AND event_date=$2 AND is_active",
        target_source_id, event_date)
    my = normalize_title(mention["title"])
    best_id, best = None, 0.0
    for e in evs:
        s = jaccard(my, normalize_title(e["title"]))
        if s > best:
            best, best_id = s, e["id"]
    if best >= _XREF_TITLE_THRESHOLD:
        return True, "title_similarity", float(best), best_id
    return False, None, None, None


# ── Event matching ────────────────────────────────────────────────────────────
_MATCH_THRESHOLD = 0.65


async def find_matches_for_source(source_id, *, is_resync: bool = False) -> dict:
    own = await ocal_db.fetch(
        "SELECT id, source_id, title, event_date, match_group_id "
        "FROM diary_events WHERE source_id=$1 AND is_active AND event_date IS NOT NULL", source_id)
    if not own:
        return {"groups": 0}
    dates = list({e["event_date"] for e in own})
    others = await ocal_db.fetch(
        "SELECT id, source_id, title, event_date, match_group_id "
        "FROM diary_events WHERE source_id<>$1 AND is_active AND event_date = ANY($2::date[])",
        source_id, dates)
    by_date: dict = {}
    for e in others:
        by_date.setdefault(e["event_date"], []).append(e)

    pool = await ocal_db.get_pool()
    created = joined = 0
    async with pool.acquire() as conn:
        for ev in own:
            if ev["match_group_id"]:
                continue
            my = normalize_title(ev["title"])
            if len(my.split()) < 2 and len(my) < 6:
                continue
            best, best_ev = 0.0, None
            for oe in by_date.get(ev["event_date"], []):
                s = jaccard(my, normalize_title(oe["title"]))
                if s > best:
                    best, best_ev = s, oe
            if best < _MATCH_THRESHOLD or not best_ev:
                continue
            if best_ev["match_group_id"]:
                gid = best_ev["match_group_id"]
                await conn.execute(
                    "UPDATE similar_events SET "
                    "  grouped_event_ids = array_append(grouped_event_ids, $2::uuid), "
                    "  involved_source_ids = CASE WHEN $3::uuid = ANY(involved_source_ids) "
                    "     THEN involved_source_ids ELSE array_append(involved_source_ids, $3::uuid) END, "
                    "  total_events = total_events + 1 WHERE id=$1",
                    gid, ev["id"], ev["source_id"])
                await conn.execute(
                    "UPDATE diary_events SET match_group_id=$1 WHERE id=$2", gid, ev["id"])
                joined += 1
            else:
                longer = ev["title"] if len(ev["title"] or "") >= len(best_ev["title"] or "") else best_ev["title"]
                gid = await conn.fetchval(
                    "INSERT INTO similar_events "
                    "(representative_event_id, event_date, common_title, grouped_event_ids, total_events, involved_source_ids) "
                    "VALUES ($1,$2,$3,$4,2,$5) RETURNING id",
                    best_ev["id"], ev["event_date"], longer,
                    [ev["id"], best_ev["id"]], [ev["source_id"], best_ev["source_id"]])
                await conn.execute(
                    "UPDATE diary_events SET match_group_id=$1 WHERE id = ANY($2::uuid[])",
                    gid, [ev["id"], best_ev["id"]])
                created += 1
    logger.info("ocal_enrich: matching for %s — created=%d joined=%d", source_id, created, joined)
    return {"created": created, "joined": joined}


# ── Stage 3: AI-NER (paid LLM) — faithful port of Ocal's stageAiNer ───────────
# Verbatim from Ocal server/src/services/entityExtractor.ts (AI_NER_PROMPT).
_AI_NER_PROMPT = (
    "You are an expert at named entity recognition (NER) for Israeli government "
    "diary events (יומן) written in Hebrew.\n\n"
    "ENTITY TYPES:\n"
    "- person: Named individuals — strip honorifics (שר, ח\"כ, ד\"ר, פרופ', עו\"ד, "
    "מנכ\"ל, הרב, גב', מר) and return only the personal name\n"
    "- organization: Government ministries (משרד...), companies, unions, committees, "
    "political parties\n"
    "- place: Cities, buildings, venues (NOT generic room numbers like \"חדר 105\")\n\n"
    "RULES:\n"
    "- Generic role references without a proper name (\"ראש המחלקה\", \"נציג הוועדה\") → skip\n"
    "- Return full ministry name including ה- prefix: \"משרד הביטחון\" not \"ביטחון\"\n"
    "- Omit entities with confidence below 0.4\n"
    "- Each entity gets a \"role\": \"participant\" (person/org present at event), "
    "\"location\" (place), or \"mentioned\" (referenced but not present)\n\n"
    "INPUT: A JSON array where each element has \"id\" (event UUID) and \"text\" "
    "(Hebrew event text).\n\n"
    "OUTPUT: A JSON array. Each element:\n"
    '{"id":"<event_id>","entities":[{"name":"<Hebrew name>","type":"person"|'
    '"organization"|"place","role":"participant"|"location"|"mentioned","raw":'
    '"<exact substring>","confidence":0.0-1.0}]}\n\n'
    "Return ONLY the JSON array, no explanation. Empty entities array if nothing found."
)

_DEEPSEEK_BASE_URL = "https://api.deepseek.com"
_DEEPSEEK_MODEL = "deepseek-chat"
_ANTHROPIC_MODEL = "claude-opus-4-8"


def ai_ner_provider() -> str | None:
    """Same selection as /api/cbs/ask: DeepSeek preferred, then Anthropic."""
    if settings.deepseek_api_key:
        return "deepseek"
    if settings.anthropic_api_key:
        return "anthropic"
    return None


def ai_ner_available() -> bool:
    return bool(settings.ocal_ai_ner_enabled and ai_ner_provider())


def _parse_ner_json(raw: str) -> list:
    """Tolerant JSON extraction — the model may wrap the array in ```json fences,
    or return {"results": [...]} / {"data": [...]} instead of a bare array."""
    raw = (raw or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```[a-zA-Z]*\n?", "", raw)
        raw = re.sub(r"\n?```$", "", raw).strip()
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        # last resort — grab the outermost [ ... ] span
        i, j = raw.find("["), raw.rfind("]")
        if i >= 0 and j > i:
            try:
                obj = json.loads(raw[i:j + 1])
            except (ValueError, TypeError):
                return []
        else:
            return []
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        return obj.get("results") or obj.get("data") or obj.get("events") or []
    return []


async def _ai_ner_call(payload: list[dict]) -> list:
    """One LLM round-trip: [{id,text}] → [{id,entities:[...]}]. Provider-agnostic."""
    provider = ai_ner_provider()
    user = json.dumps(payload, ensure_ascii=False)
    if provider == "deepseek":
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.deepseek_api_key, base_url=_DEEPSEEK_BASE_URL)
        resp = await client.chat.completions.create(
            model=_DEEPSEEK_MODEL, temperature=0,
            max_tokens=settings.ocal_ai_ner_max_tokens,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": _AI_NER_PROMPT},
                {"role": "user", "content": user},
            ],
        )
        return _parse_ner_json(resp.choices[0].message.content or "")
    if provider == "anthropic":
        from anthropic import AsyncAnthropic
        client = AsyncAnthropic(api_key=settings.anthropic_api_key)
        resp = await client.messages.create(
            model=_ANTHROPIC_MODEL, max_tokens=settings.ocal_ai_ner_max_tokens,
            system=_AI_NER_PROMPT,
            messages=[{"role": "user", "content": user}],
        )
        text = "".join(b.text for b in resp.content if getattr(b, "type", None) == "text")
        return _parse_ner_json(text)
    return []


_AI_ROLES = {"participant", "location", "mentioned"}
_AI_TYPES = {"person", "organization", "place"}


async def ai_ner_for_source(source_id, *, errors: list | None = None) -> int:
    """Stage 3 — LLM NER over a source's events (extraction_method='ai_ner').

    Batches events (settings.ocal_ai_ner_batch), builds a title+location+
    other_fields text per event, asks the LLM for entities, resolves each to the
    people/organizations registry by Jaccard (>=0.7 → entity_id + canonical name),
    and inserts into event_entities (ON CONFLICT DO NOTHING). Every batch failure
    is soft — logged + recorded in ``errors`` — so one bad call never aborts."""
    if errors is None:
        errors = []
    if not ai_ner_available():
        logger.info("ocal_enrich: AI-NER skipped for %s (no provider / disabled)", source_id)
        return 0

    preg, oreg = await _load_registries()
    batch = max(1, int(settings.ocal_ai_ner_batch))
    pool = await ocal_db.get_pool()
    offset = 0
    total_inserted = 0

    while True:
        events = await ocal_db.fetch(
            "SELECT id, title, location, other_fields FROM diary_events "
            "WHERE source_id=$1 AND is_active ORDER BY id LIMIT $2 OFFSET $3",
            source_id, batch, offset)
        if not events:
            break
        offset += len(events)

        payload: list[dict] = []
        for e in events:
            other = e["other_fields"] or {}
            other_text = ""
            if isinstance(other, dict):
                other_text = " | ".join(
                    str(v) for v in other.values()
                    if isinstance(v, str) and v.strip())
            text = "\n".join(
                normalize_text(t) for t in (e["title"], e["location"], other_text) if t)
            if text.strip():
                payload.append({"id": str(e["id"]), "text": text})

        if not payload:
            if len(events) < batch:
                break
            continue

        # Map the string id we sent back to the real UUID; the model must only
        # attach entities to events from THIS batch (guards hallucinated ids).
        id_map = {str(e["id"]): e["id"] for e in events}

        try:
            parsed = await _ai_ner_call(payload)
        except Exception as exc:  # noqa: BLE001 — soft per-batch
            errors.append(f"AI NER batch error at offset {offset}: {exc}")
            logger.warning("ocal_enrich: AI-NER batch failed at offset %d: %s", offset, exc)
            if len(events) < batch:
                break
            continue

        rows: list[tuple] = []
        seen: set[tuple] = set()
        for item in parsed:
            if not isinstance(item, dict):
                continue
            ev_id = id_map.get(str(item.get("id")))
            if ev_id is None:
                continue
            for ent in item.get("entities") or []:
                if not isinstance(ent, dict):
                    continue
                name = ent.get("name")
                try:
                    conf = float(ent.get("confidence", 0))
                except (ValueError, TypeError):
                    conf = 0.0
                etype = ent.get("type")
                role = ent.get("role")
                if not name or conf < 0.4 or etype not in _AI_TYPES or role not in _AI_ROLES:
                    continue
                cleaned = clean_name(normalize_text(name))
                if not cleaned:
                    continue
                low = cleaned.lower()
                entity_id, resolved = None, cleaned
                if etype == "person":
                    pid, pcanon, ps = _best(low, preg)
                    if ps >= 0.7:
                        entity_id, resolved = pid, pcanon
                elif etype == "organization":
                    oid, ocanon, os = _best(low, oreg)
                    if os >= 0.7:
                        entity_id, resolved = oid, ocanon
                key = (ev_id, etype, resolved, role)
                if key in seen:
                    continue
                seen.add(key)
                rows.append((ev_id, etype, entity_id, resolved, role,
                             ent.get("raw"), conf, "ai_ner"))

        if rows:
            async with pool.acquire() as conn:
                for i in range(0, len(rows), 500):
                    await conn.executemany(_EE_INSERT, rows[i:i + 500])
            total_inserted += len(rows)

        if len(events) < batch:
            break

    logger.info("ocal_enrich: AI-NER for %s — inserted=%d (%s)",
                source_id, total_inserted, ai_ner_provider())
    return total_inserted


# ── Materialized view + orchestrator ──────────────────────────────────────────

async def refresh_entity_matview() -> None:
    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        try:
            async with conn.transaction():
                await conn.execute("SET LOCAL statement_timeout = 120000")
                await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_entity_counts")
        except Exception as e:  # noqa: BLE001 — matview may be missing / busy
            logger.warning("ocal_enrich: mv_entity_counts refresh skipped: %s", e)


async def enrich_source(source_id, *, is_resync: bool = False,
                        run_ai: bool = False, refresh_matview: bool = True) -> dict:
    """Full enrichment chain for one source. Each stage is non-fatal.

    The free stages (entities → cross-refs → matview → matching) always run.
    ``run_ai`` adds Stage 3 (paid LLM NER) between the free entity pass and
    cross-referencing — so the cross-ref/matview see the AI-added entities too —
    mirroring the legacy admin "run entity extraction (with AI)" action. AI-NER
    is a no-op when disabled or no LLM key is set."""
    out = {}
    try:
        out["entities"] = await extract_entities_for_source(source_id, clear_existing=is_resync)
        if run_ai and ai_ner_available():
            ai_errors: list = []
            out["ai_ner"] = {
                "inserted": await ai_ner_for_source(source_id, errors=ai_errors),
                "provider": ai_ner_provider(),
                "errors": ai_errors,
            }
        out["cross_refs"] = await cross_reference_for_source(source_id, is_resync=is_resync)
        # Skipped in bulk (scan) runs — the caller refreshes once at the end
        # instead of once per source (the refresh scans ~1M entity rows).
        if refresh_matview:
            await refresh_entity_matview()
    except Exception:  # noqa: BLE001
        logger.exception("ocal_enrich: entities/cross-ref chain failed for %s", source_id)
    try:
        out["matches"] = await find_matches_for_source(source_id, is_resync=is_resync)
    except Exception:  # noqa: BLE001
        logger.exception("ocal_enrich: matching failed for %s", source_id)
    return out
