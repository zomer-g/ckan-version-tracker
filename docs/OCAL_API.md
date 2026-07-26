# יומן לעם API — `/api/ocal`

The public read API for **יומן לעם** (Ocal), migrated into OVER
(over.org.il). It mirrors the endpoints that powered the standalone
ocal.org.il site (`/api/public/*` there → `/api/ocal/*` here) and queries the
migrated Ocal database (a dedicated Neon Postgres; see
`app/services/ocal_db.py`).

- **Base URL:** `https://www.over.org.il/api/ocal`
- **Auth:** none. All endpoints are public, rate-limited per IP.
- **Format:** JSON, UTF-8. Dates are `YYYY-MM-DD`; timestamps are ISO-8601.
- **Availability:** returns `503` when `OCAL_DATABASE_URL` is not configured.

> Processed-data notice: like the rest of the "לעם" projects, this exposes
> **processed / derived** calendar data, not raw primary government sources.
> Each source carries its links back to the upstream CKAN resource.

---

## Endpoints at a glance

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/ocal/events` | Full-text + faceted event search (paginated) |
| GET | `/api/ocal/events/{id}` | One event |
| GET | `/api/ocal/events/{id}/entities` | Extracted entities for an event |
| GET | `/api/ocal/events/{id}/cross-refs` | Cross-diary verification references |
| GET | `/api/ocal/events/{id}/matches` | Same-day cross-diary duplicate events |
| GET | `/api/ocal/sources` | Enabled diary sources |
| GET | `/api/ocal/sources/{id}` | One diary source |
| GET | `/api/ocal/calendar` | Events within a calendar window |
| GET | `/api/ocal/stats` | Hero counters (events / sources / orgs) |
| GET | `/api/ocal/entities` | Top entities by event count |
| GET | `/api/ocal/content` | Site CMS key/value content |
| GET | `/api/ocal/download/source/{id}` | Single-diary CSV/JSON export |
| POST | `/api/ocal/download/bulk` | Multi-diary ZIP export |

---

## `GET /api/ocal/events`

Main search. All query params optional.

| Param | Type | Notes |
|---|---|---|
| `q` | string | Hebrew/English full-text. Supports `AND`/`OR`/`NOT`; a token with a geresh/gershayim (e.g. `מח"ש`) matches the abbreviation exactly, others match as prefixes. |
| `from_date`, `to_date` | `YYYY-MM-DD` | Inclusive range on `event_date`. |
| `source_ids` | string | Comma-separated source UUIDs. |
| `location` | string | `ILIKE` substring. |
| `participants` | string | `ILIKE` substring. |
| `entity_names` | string | `\|\|`-separated entity names (matched case-insensitively). |
| `cross_ref_status` | `confirmed`\|`unconfirmed` | Only events with a cross-ref of this status. |
| `page` | int ≥ 1 | Default 1. |
| `per_page` | int 1–500 | Default 50. |
| `sort` | `date_asc`\|`date_desc`\|`relevance` | Default: `relevance` when `q` is set, else `date_desc`. |

**Response**
```json
{
  "data": [
    {
      "id": "…", "title": "…", "start_time": "…", "end_time": "…",
      "location": "…", "participants": "…", "event_date": "2026-01-15",
      "source_name": "…", "source_color": "#3B82F6", "source_reviewed": true,
      "match_count": 3,
      "top_entities": [{"name": "…", "type": "person"}],
      "cross_ref_summary": {"confirmed": 1, "unconfirmed": 0, "total": 1}
    }
  ],
  "pagination": {"page": 1, "per_page": 50, "total": 1234, "total_pages": 25}
}
```

## `GET /api/ocal/calendar`

| Param | Type | Notes |
|---|---|---|
| `date` | `YYYY-MM-DD` | **Required.** Anchor date. |
| `view` | `month`\|`week`\|`4day`\|`day` | Default `month`. Determines the window (month view includes surrounding weeks). |
| `source_ids` | string | Comma-separated UUIDs. |
| `entity_names` | string | Comma-separated names. |
| `max_date` | `YYYY-MM-DD` | Caps the window end (e.g. hide future events). |

**Response:** `{ "events": [...], "date_range": {"from","to"}, "event_counts": {"2026-01-15": 4, ...} }`

## `GET /api/ocal/sources`

`{ "data": [ { ...diary_source, "person_name": "…", "organization_name": "…" } ] }` — enabled sources, ordered by name.

## `GET /api/ocal/stats`

`{ "total_events": N, "total_sources": N, "total_organizations": N }` (cached 5 min).

## `GET /api/ocal/entities`

Top 200 entities by event count. Params: `source_ids`, `type` (`person`|`organization`|`place`), `from_date`, `to_date`. Unfiltered requests read the `mv_entity_counts` materialized view; filtered requests run live. `{ "data": [ {"entity_name","entity_type","entity_id","event_count"} ] }`.

## `GET /api/ocal/content`

`{ "content": { "<key>": <json-or-string>, ... } }` — the site CMS values.

## Downloads

- `GET /api/ocal/download/source/{id}?format=csv|json&from_date=&to_date=` — one diary as a CSV (UTF-8 BOM, Hebrew headers) or JSON file.
- `POST /api/ocal/download/bulk` with body `{ "source_ids": ["uuid", …], "format": "csv"|"json", "from_date"?, "to_date"? }` — a ZIP with one file per diary (max 1000 sources).

---

## Notes / parity with the legacy site

- The Hebrew tsquery construction (geresh stripping, prefix vs exact match, boolean operators) matches the legacy `search_vector` trigger (Ocal migration 021). Keep them aligned if either changes.
- The MK-expenses layer (`/expenses` on the legacy site) is **not** ported yet.
- Admin endpoints (`/api/admin/*` on the legacy site) are ported separately under OVER's authenticated admin surface.
