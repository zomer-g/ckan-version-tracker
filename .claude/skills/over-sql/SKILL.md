---
name: over-sql
description: "Write, run and hand over SQL against the גרסאות לעם / OVER data console (over.org.il/data — the whole-site Postgres: every dataset table, the knesset schema, the idx collection indexes, GovMap layers with PostGIS). Use this whenever the user asks for a query, a number, a cross-source comparison, a map, or a chart out of OVER's data — phrasings like 'תבנה לי שאילתא', 'כמה X יש', 'תצליב בין המאגרים', 'תראה לי את זה על מפה', 'למה השאילתה נכשלת', or any question naming a dataset/layer/table on over.org.il. Also use when a query times out, returns nothing, or errors with `column ... does not exist`, and when writing new examples for the /data page. The corpus is all-text Hebrew columns, machine-named map layers and a 10-second statement timeout, so a query written from intuition usually fails on the first try — this skill has the verified patterns and the runner script that proves a query works before it reaches the user."
---

# SQL על מאגר גרסאות לעם

The /data console is a read-only window onto one Postgres holding every queryable
table on the site. This skill is about writing SQL that actually runs there —
and about not handing the user a query you have not seen return rows.

## The one rule

**Run every query against production before showing it to the user.** Not because
the SQL might be malformed — because this corpus violates every assumption a
well-formed query makes. Columns are text that look numeric. Dates are strings in
three different formats. Map layers publish machine names nobody would guess.
There is a 10-second statement timeout that a plain three-table join can exceed.
A query that "looks right" is wrong often enough that untested SQL is a guess
dressed up as an answer.

Use `scripts/run_sql.py` (bundled here) — it handles base64, the www redirect,
UTF-8 output on Windows, and prints timing so you see the timeout coming.

```bash
python .claude/skills/over-sql/scripts/run_sql.py query.sql
```

Write the SQL to a file rather than echoing it into the script — Hebrew
identifiers and their double quotes do not survive shell quoting, and you end up
running a query you did not write. Space calls ~3 s apart: 20/minute.

## Two surfaces, one engine

The public console at `/data` and the MCP server at `/data/mcp` run the same
catalog, the same DDL and the same read-only SELECT path. Everything in this
skill applies to both; only the plumbing differs.

| | `/data` console | `/data/mcp` |
|---|---|---|
| how you call it | `POST /api/tables/sql` (base64) | `run_sql` tool |
| statement timeout | 10 s | 20 s |
| rows | 1,000 | 1,000 (`max_rows`, default 200) |
| discovery | `GET /api/tables`, `information_schema` | `list_schemas` → `list_tables` (searches COLUMN names and Hebrew aliases) → `describe_schema` |

When the MCP is connected to your session, prefer its tools — `list_tables`
finds a table by a field name, which is the search you actually want, and
`describe_schema` hands back the exact quoted identifiers plus the cast traps
and the site's helper functions. When it is not, use the runner script; the
console is the same database.

## The console's contract

| | |
|---|---|
| endpoint | `POST https://www.over.org.il/api/tables/sql`, body `{"sql_b64": "<base64 of the SQL>"}` |
| host | the **www** host — `over.org.il` answers 307 and urllib drops the POST body |
| allowed | ONE `SELECT` or `WITH` statement, read-only role, no DDL/DML |
| `search_path` | `public, knesset, idx, odata, ocal, extensions` — schema-qualify only on a name collision |
| statement timeout | **10 s** (60 s on `/api/tables/export.csv`) |
| rows | 1,000 in the browser, 200,000 via export |
| rate limit | 20/min for SQL, 6/min for CSV export |

Catalog for discovery: `GET https://www.over.org.il/api/tables` returns every
table with its title, columns (with Hebrew `alias` where known) and row estimate.
Cache it to a file — it is ~1.2 MB and you will grep it repeatedly.

## Where the data lives

- **`public.append_*`** — one table per tracked dataset (or per resource). Every
  column is `text`, named exactly as the source named it, usually in Hebrew.
- **`idx.*`** — the collection indexes: one table per GovMap layer, per scraped
  file corpus. All text, plus `geometry_wkt` (text) and `geom` (PostGIS,
  EPSG:4326) where the source is spatial.
- **`knesset.*`** — the 48 ODATA mirror tables. The exception to everything
  below: lower-case names, real types (`integer`, `timestamptz`, `boolean`).
- **`public.over_*`** — OVER's own reference index (settlements, authorities,
  table profiles, column aliases). These are the join keys of the whole corpus.
- **`odata.*` / `ocal.*`** — imported מידע לעם resources and יומן לעם data.

## Naming: what breaks first

Hebrew column names need double quotes, and the quotes must contain the exact
characters the source used:

- **Gershayim** — `"סה״כ הכנסות"` uses ״ (U+05F4), not two ASCII quotes. Copy the
  name from the catalog rather than typing it.
- **63-byte truncation** — Postgres clips identifiers by BYTES and a Hebrew
  letter is two, so long Hebrew headers arrive cut mid-word:
  `"גרעון מצטבר נטו (גרעון מצטבר בניכו"`. Use it exactly as stored.
- **GovMap layers are machine-named.** Since 2026-07-30 a layer's columns are the
  source's field names (`shem_yishuv`, `pop_total`, `ata_shem`), not the Hebrew
  captions GovMap shows on its own map. If a caption is what you know, look up
  the machine name:

```sql
SELECT table_name, column_name, alias FROM public.over_column_aliases
WHERE alias LIKE '%אוכלוסי%'
```

The catalog API returns the same mapping as `alias` on each column, and the
console's autocomplete searches it — so "what is the population column called"
is a lookup, never a guess.

## Typing: everything is text

Cast at the point of use, and defend the cast:

```sql
"סה״כ הכנסות"::numeric                    -- fine when the column is always numeric
NULLIF(pop_total, '')::numeric            -- an empty string is NOT null and WILL raise
NULLIF(NULLIF(x,'')::numeric, 0)          -- and a zero denominator is its own bug
```

Dates arrive as strings in whatever format the publisher chose — `DD/MM/YYYY` in
רשם החברות, `DD.MM.YYYY` in החלטות הממשלה. `to_date` raises on the first
malformed row (727k rows with 16 bad ones is enough to kill the query), so filter
the shape first:

```sql
WHERE "תאריך התאגדות" ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
  AND to_date("תאריך התאגדות", 'DD/MM/YYYY') >= DATE '2024-07-01'
```

Prefer `[0-9]` over `\d`: these queries get embedded in TypeScript template
literals on the /data page, where `\d` silently becomes `d`.

## Joining across sources — the settlement index

Two registers naming the same town will not join on the name. One writes
`תל אביב - יפו`, the other `תל אביב -יפו`; one keys by CBS code, the other by a
free-text field with trailing spaces; and some towns were renamed
(נצרת עילית → נוף הגליל). OVER's index exists to make that join possible:

```sql
over_settlement(q)        -- canonical CBS name  ('פ"ת' → 'פתח תקווה')
over_settlement_code(q)   -- CBS code (integer)  ('נצרת עילית' → 1061)
over_authority(q) / over_authority_code(q)   -- same, for local authorities
public.over_settlements   -- code, name, district, population, year
```

Resolve BOTH sides to the code and join on that. Never join two sources on a raw
Hebrew name — it silently drops the rows that spell it differently, and the drop
is invisible in the result.

The live list of these functions is printed at the top of every schema dump
(`describe_schema`, the copy-to-AI button, `/api/tables/schema.txt`), read from
`pg_proc` — so if a resolver has been added since this skill was written, the
schema knows and this file may not.

## Staying inside 10 seconds

These are the moves that turned timeouts into 2-second queries in practice:

**Filter as text before parsing.** A full scan of 727k rows costs ~1.2 s; the
same scan with `to_date` on every row exceeds the timeout. `WHERE "תאריך" LIKE
'%/2024'` first, parse the survivors.

**Aggregate the big table, then join.** Reduce 727k rows to one row per
settlement in a CTE, and join the small result to everything else. Joining the
raw table to a small CTE invites a nested loop that rescans it per key.

**`WITH x AS MATERIALIZED (...)`** when you need row-level output from a big
table. Without it the planner may inline the CTE and rescan per outer row — this
alone was the difference between a timeout and 2.4 s.

**Spatial:** `ST_DWithin(geom::geography, pt, 20000)` for metres (without
`::geography` the distance is in degrees and meaningless). `ST_Collect` is cheap;
`ST_Union` dissolves and is not — use Union only when you actually need one
merged shape, and only over tens of geometries, not thousands.

## Spatial work

A result is mappable when some column's CONTENT looks like WKT or GeoJSON —
`geometry_wkt`, `ST_AsText(geom)`, `ST_AsGeoJSON(geom)`. There is no map without
one; a reference table like `over_settlements` has no geometry at all, and the
map chip stays grey.

**Giving a non-spatial dataset geography.** Most registers have no coordinates
but do name a place. Resolve the place to a settlement code, borrow that
settlement's shape from a map layer, and an ordinary table answers a distance
question. The statistical-areas layer (`idx.govmap_23_d882fbdb_493df16d`,
`shem_yishuv`) is the usual donor.

**Colouring a map.** The /data map colours by a category column (a colour per
value) or by any numeric column (a sequential ramp over its own min→max, with a
legend). The first numeric column of the result is what it opens on, so put the
measure you want to see first. For counts with one dominant outlier, tell the
user about the "לפי התפלגות" (quantile) option — a linear ramp puts everything
else in the lightest bucket.

**Points at the same address stack invisibly.** Four ballots in one school are
one dot; aggregate to the location and carry the extremes in their own columns,
or the map hides the very rows that matter.

## The workflow

1. **Find the tables.** Search the cached catalog by title, column name or Hebrew
   alias. `information_schema.columns` works too, and is the only way to see a
   table that the catalog has not refreshed yet.
2. **Look at the data before writing logic.** `SELECT * FROM t LIMIT 5` tells you
   the date format, the empty-string convention and whether a "code" column is
   really numeric. Two minutes here saves three failed queries.
3. **Draft, run, read the row count.** Zero rows is a failure, not a result —
   find out which predicate emptied it before showing anything.
4. **Hand over the query with its numbers.** Say how long it took and how many
   rows it returned. If the answer rests on an inference (see below), say so in
   the same breath.

## Honesty about what the data can answer

The corpus will let you build a chain of joins that looks authoritative and is
actually an inference. When that happens, the inference is fine — presenting it
as a fact is not. Two habits:

**State the coverage of every derived join.** "49 of the 122 polling-place
addresses in Haifa could be geocoded" is the difference between a map and a
misleading map. Measure it (`count(*) FILTER (WHERE EXISTS ...)`) rather than
estimating it.

**Name the field that does not exist.** If the user asks for something by
neighbourhood and no table has a neighbourhood field, say that first, then offer
the closest defensible proxy and its limits. Do not let a plausible chain of
joins imply a field that was never published.

## Answering the user

Reply in Hebrew (the user's language), with the SQL in a fenced ```sql block.
Explain the two or three non-obvious moves in the query — the join key, the guard
that stops a cast from raising, the reason a CTE is MATERIALIZED — because those
are what the user will need to change when they adapt it. Skip the tour of
standard SQL.

## Reference

`references/recipes.md` — verified query patterns: cross-source joins on
settlement, spatial analysis of non-spatial registers, the elections corpus and
its ballot/polling-station join key, unpivoting wide party tables, per-capita
metrics, and the timeout rewrites. Read it when the task resembles one of those;
each recipe is a query that has been run against production.
