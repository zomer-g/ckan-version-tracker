# Table Profiler — dataset profiling, enrichment & standardization

Per-table mechanism that samples every SQL table, computes deterministic
statistics in SQL first (min/max of numeric & date fields, date-format
detection, keywords, recurring-entity detection), then enriches with an LLM
(Hebrew summary + per-column semantic type). Phase 2 adds a standardization
layer that normalizes recurring fields (localities, dates, …).

## Decisions (locked with the user, 2026-07-27)

1. **Storage — everything together.** Profiles live IN the append DB, table
   `public.over_table_profiles`, so they're queryable from the same SQL console
   as the data. The `over_` prefix is reserved for OVER-generated artifacts.
2. **Normalization output — VIEWS now, `over_*` columns later.** Phase 2 starts
   with non-destructive views/mapping tables; the end state adds physical
   `over_<col>` companion columns on the raw table (e.g. a uniform ISO date, a
   canonical locality) so dates/entities can be queried directly. Source columns
   are never mutated.
3. **Scope — pilot of 20 tables spanning ALL source types** before any wide run.

## Why SQL-first matters here

`append_*` tables store **every value as `text`** (see `append_store` docstring).
So a column's declared Postgres type says nothing about whether it holds numbers
or dates — the profiler SNIFFS this from a sample, then computes exact min/max
over the full table with regex-guarded casts (`::numeric`, `to_timestamp(col,
fmt)`). Typed schemas (knesset/idx/odata) get ranges directly.

## Architecture

- **`app/services/table_profiler.py`** — the engine.
  - Storage: `ensure_profile_table`, `upsert_profile`, `save_enrichment`,
    `get_profile`, `coverage`.
  - Pure logic (unit-tested, no DB): `looks_numeric`, `detect_date_format`
    (python + Postgres format pair, day-first preference, ambiguity flag),
    `tokenize_he`/`extract_keywords`, `classify_entity_heuristic`,
    `table_signature`, `select_pilot`.
  - SQL profiler: `_sample_column_values` (TABLESAMPLE on >1M-row tables) →
    `_classify_columns` → `_aggregate` (one scan: count, per-col non-null,
    numeric/date min/max/avg) + `_pg_stats_distinct` (no scan) + `_top_values`
    per text column → `profile_table`.
  - LLM: `llm_provider` (DeepSeek→Anthropic, mirrors `ocal_enrich`),
    budget-guarded `enrich_profile` (via `llm_budget.reserve_llm_call`).
  - Orchestration: `run_pilot(db, n, enrich, force)` — signature-skip unchanged.
- **`app/api/admin.py`** — `POST /api/admin/profiler/run` (`scope=pilot` bg /
  `scope=table` sync), `GET /api/admin/profiler/coverage`,
  `GET /api/admin/profiler/profile`.
- **`scripts/run_profiler_pilot.py`** — local runner (needs `APPEND_DATABASE_URL`).
- **`tests/test_table_profiler.py`** — 18 offline tests.

## Profile record shape (`sql_profile` JSONB)

```
row_count, column_count, geometry_columns, candidate_key,
keywords: [{token, count}],
columns: { <col>: {
    detected_kind: numeric|date|text|empty,
    non_null, fill_rate, distinct_est, distinct_ratio,
    entity_guess: {guess, confidence, evidence},
    # numeric: min, max, avg, numeric_rate
    # date:    min, max (ISO), date_format {python, postgres, match_rate, ambiguous}
    # text:    top_values: [{value, count}]
}}
```
`date_parse_specs` (top-level) = `{col: {python, postgres, …}}` → **feeds phase 2**.

## Running the pilot

The append DB + LLM keys live on Render, not in the local `.env`. Two ways:

- **Deployed:** `POST /api/admin/profiler/run {"scope":"pilot","n":20}` (admin),
  then poll `GET /api/admin/profiler/coverage`.
- **Local:** set `APPEND_DATABASE_URL` (+ optional key), then
  `python scripts/run_profiler_pilot.py --n 20`.

## Access (shipped)

- `GET /api/tables/{table}/profile` — public, one table's profile (404 if unprofiled).
- `profile` is also embedded in `GET /api/tables/{table}/detail`.
- MCP tool `get_table_profile(table, schema)` on the OVER server.
- /data table-detail screen: `ProfilePanel` (frontend/src/components/ProfilePanel.tsx)
  — collapsible summary + tags/keywords + per-field table (type, min/max, detected
  date format, recurring-entity label, fill%, distinct, AI description).

## Status

- [x] Storage table + model-free DDL in append DB
- [x] SQL deterministic profiler (min/max, date-format, top-values, entities, keywords)
- [x] Native-type path: declared timestamp/numeric columns get exact MIN/MAX (no sniff)
- [x] LLM enrichment (budget-guarded, provider-agnostic)
- [x] Admin endpoints + standalone pilot runner
- [x] Offline tests (22, green)
- [x] **20-table pilot RUN on prod** — 18/20 profiled+enriched across all buckets
      (2 errored, logged); verified gov-decisions dates 1993-01-03..2026-07-19
- [x] API + MCP + /data ProfilePanel — verified live on prod
- [ ] Re-run pilot with the native-type fix (force=true) to refresh knesset date ranges
- [ ] Investigate the 2 pilot errors (likely a large idx/govmap layer timeout)
- [ ] Admin review/approve UI (`status` → `approved`)
- [ ] Scheduler pass (whole-catalog, budgeted)
- [ ] Phase 2 — standardization (alias index + `over_*` canonical columns)

## Phase 2 — standardization (design, not yet built)

- **Alias/inflection index** per entity type: extend `cbs_gazetteer.aliases` for
  localities; new `entity_alias(entity_type, variant, canonical_id, canonical_name)`
  for רשויות/corporations/people. Matching reuses `ocal_enrich`
  `normalize_text`/`jaccard`; misses queue to LLM/admin → become aliases
  (self-improving).
- **Canonical dates** from the locked per-column `date_parse_specs` (deterministic,
  not re-guessed).
- **Output:** views first; then physical `over_<col>` columns (uniform ISO date,
  canonical locality/authority/corp) added to the raw table for direct querying.
