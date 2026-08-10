# נדל"ן לעם — the property-level spatial crosswalk

Four heavy layers already live in OVER, each holding **one leg** of Israeli
property identity and none of them joined to the others. This project derives the
thin crosswalk between them, so any one spatial identity resolves to all the
others.

| layer | table | rows | size | identifies |
|---|---|---|---|---|
| חלקות shape | `public.append_shape_ff3176b1` | 1,100,209 | 4,584 MB | גוש-חלקה → polygon |
| גזטיר הנכסים | `odata.gaztir_41720377` | 3,680,555 | 524 MB | גוש-חלקה → יישוב + רחוב |
| קובץ המיקוד | `odata."00a9749e…_2a021675"` | 515,311 | 60 MB | כתובת → מיקוד |
| רשימת כתובות | `odata."ac1ae1fa…_19c5be7f"` | 548,156 | 57 MB | כתובת → נקודה (ITM) |

Public page: `/projects/nadlan`. Public API: `/api/nadlan/*`.
Crosswalk tables: `public.over_re_*` (queryable from `/data` and the MCP).

## What the sources actually are

Each of these was measured against production, and each one invalidated an
approach that looked obvious first:

1. **The parcels layer is already PostGIS-ready** — `geom geometry(Polygon,4326)`
   with a GiST index plus a btree on `"GUSH_NUM"`. Nothing needs building; its
   4.58 GB simply must stay out of the crosswalk.
2. **Point-in-parcel linking is precise.** A 3,000-point sample resolved in 5.1 s
   with **90.4 %** landing inside a parcel. אבימלך 8 פתח תקווה → gush 6319
   parcel 225.
3. **The postal file uses a foreign codespace.** `LocationID` is Israel Post's
   own id: **0** of its (code, name) pairs match `public.over_settlements`. It is
   bridged by NAME once, into `over_re_postal_localities`; nothing downstream
   joins on the raw id.
4. **The gazetteer uses the CBS codespace** — 1,208 of 1,227 distinct
   `SettlmentID` values are valid CBS codes.
5. **The gazetteer has no house number**, so it resolves to a *street*, never to
   a building. The precise address→parcel link therefore has to be spatial.
6. **The gazetteer also has no gush suffix.** Parcels carry `GUSH_SUFFI` (~3 % of
   rows are non-zero); the gazetteer does not. So it attaches on the suffix-less
   `gp_key`, and the **0.63 %** of gush/parcel pairs covering more than one real
   parcel are flagged `gp_ambiguous` and downgraded to `confidence:
   "approximate"` rather than silently collapsed.
7. **The postal file covers 91 localities only** (500,700 distinct ZIP7, last
   updated 2017).
8. **The address list covers 325 cities and 30 % of it has no coordinates**
   (164,089 rows at `X='0'`). Its `number` column is dirty — `'דוד אבידן 10'`.
9. **Both files use `'?'` as "street unknown"** — 43,004 postal rows and 7,055
   address rows. These must NOT collapse into one bucket per house number:
   אבן יהודה `'?'` 1 carries **three different ZIP7s**, i.e. three real
   addresses. An unresolvable street therefore keys on whatever actually
   distinguishes the row — its zip, else its coordinate, else the raw text.
10. **The existing settlement resolver already closes the locality seam** —
    `over_settlement_code()` resolves **325/325** address-list cities and
    **91/91** postal localities.

## The street index, and why it earns its place

The naming gap is narrow and directed. Distinct (locality, street) pairs: postal
24,170 · gazetteer 35,989 · address list 33,009. Raw-name overlap:

- address list ∩ postal = 24,170 = **100 %** (same naming universe)
- postal ∩ gazetteer = 13,444 (**56 %**), address ∩ gazetteer = 21,152 (**64 %**)

So the canonical spelling is taken from the postal/address side (it is also what
a user types) and the gazetteer's spellings become weighted aliases.

### The authoritative layer: רשות האוכלוסין's street file

data.gov.il `israel-streets-synom` (resource `bf185c7f…`, 152,130 rows,
**1,312 localities**, keyed by the **CBS** code — 1,270/1,312 resolve in
`over_settlements`) publishes each street once as `official` and again under
every `synonym of <code>` spelling. For Petah Tikva alone that is 1,166 official
streets and **2,395 synonyms**, including both name orders (`יוסף ספיר` /
`ספיר יוסף`), the type-prefixed form (`דרך ספיר`) and the bare surname (`ספיר`) —
the entire heuristic ladder, as published fact.

It does **not** replace the ladder: the two fail on different names.

**Country-wide** (the file is now tracked, so this is the whole corpus —
30,995 canonical, 34,734 gazetteer, 152,130 official rows):

| matching the gazetteer | all localities | localities the address/postal files cover |
|---|---|---|
| raw name | 61.1 % | — |
| heuristic ladder alone | 72.7 % | 79.4 % |
| **ladder + official file** | **75.6 %** | **82.6 %** |

⚠ **These supersede the two-city figures this section used to quote**
(96.5 % / 91.9 % for פתח תקווה and חיפה). Those were real for those cities and
badly unrepresentative: the big cities are exactly where all three sources are
richest. Measure on the corpus, not on Tel Aviv-sized samples.

**The remaining gap is coverage, not naming.** 2,938 gazetteer streets (8.5 %)
sit in localities the address list and postal file do not cover *at all* — the
address list has 321 localities and the gazetteer 430 — so they cannot match a
canonical street by construction. **99.7 % of them are in the official file's
1,312 localities**, which is precisely what the file adds beyond the match rate:
those streets now get a canonical name and a real code instead of nothing.

The file also **merges duplicate canonical streets** the ladder kept apart —
39,825 → 37,815 country-wide (2,010 duplicates), and **35,168 of the 37,815
streets now carry a real `official_code`**, the first genuine street identifier
in the project.

What only the file can reach: 373 Petah Tikva synonym pairs share **no token**
with their official name — transliteration variants (`ברנדייס` / `בראנדיס` /
`ברנדס`), landmarks (`מלון עדן` → `שטמפפר יהושע`) and historical names
(`בתי בורשטיין` → `המכבים`). No normalization rule could infer any of them.

Weights: a published `official` name is trusted like an exact match (100); a
published `synonym` sits at 92 — above every heuristic guess, below an exact hit.

The table is found by **column signature**, not by name
(`find_official_streets_table()`), because the resource can arrive either as a
tracked CKAN dataset or through the admin odata import. If it is absent the
build silently falls back to the ladder alone.

**Tracked as** `public.append_israel_streets_synom_bf185c7f` — OVER dataset
`bf185c7f-0000-4000-8000-57fb2bd99088`, 152,130 rows, all columns `text` with
trailing spaces on every value (`'official '`, `'ירושלים '`), which the loader
strips. Note the dataset's **XML** sibling (`e3a63f81-…`) is a decoy: the
scraper misrouted it as a KML, archived it raw and loaded **0 rows**.

**Petah Tikva, for reference** (822 canonical streets, 876 gazetteer names) —
a best case, not a typical one:

| approach | matched | |
|---|---|---|
| raw name | 538 | 61.4 % |
| normalization only | 547 | 62.4 % |
| heuristic ladder | 821 | 93.7 % |

The 267 cross-spelling links are overwhelmingly a systematic **name-order**
convention difference — the gazetteer writes `אברהם הרצפלד`, the other files
write `הרצפלד אברהם` — caught by the `token_set` rule. In that city the riskiest
rule (`last_token`) produced **zero** links.

The alias ladder (`nadlan_text.street_aliases_for`), strongest first:
`official` 100 · `no_paren` 90 · `no_type` 85 · `no_he` 70 · `token_set` 60 ·
`last_token` 40.

`last_token` is the lever that closes the remaining gap and also the one that can
mis-fire — `דוד המלך` and `שלמה המלך` both end in `המלך`. So a variant that could
designate either street designates **neither**: the builder poisons ambiguous
keys instead of arbitrating them (`tests/test_nadlan_index.py`).

Everything unmatched lands in `over_re_street_unmatched` — a visible, shrinkable
work queue rather than a silent loss.

## Tables

| table | grain |
|---|---|
| `over_re_parcels` | one row per חלקה; PK `parcel_key` = `gush-suffix-parcel` |
| `over_re_parcel_gazetteer` | the gazetteer rolled up to parcel grain |
| `over_re_streets` / `_street_aliases` / `_street_unmatched` | canonical street (+ `official_code`) + inflections |
| `over_re_postal_localities` | Israel-Post locality id → CBS code |
| `over_re_addresses` | the address spine (postal ∪ address list) |
| `over_re_zip5` | zip → locality rollup |
| `over_re_build_state` | per-stage watermark, timing, row counts |

### Locating a property on the map

The polygons are not copied into the spine (that is the 4.58 GB), but every mode
can return them: pass `geometry=true` to `/parcel`, `/point`, `/zip` or
`/address` and each result carries its parcel outline as GeoJSON **on the same
envelope as its identity**. So a property found by מיקוד or by כתובת is exactly
as locatable on the map as one found by clicking — the identity and the shape
never diverge.

Bounded twice so this stays cheap: the gush list drives the source table's
existing `"GUSH_NUM"` btree (a text equality, so no cast defeats the index), then
the key list narrows to the exact parcels. `ST_SimplifyPreserveTopology` at ~2 m
takes a real 3,067-char urban polygon down to 257 — 200 parcels cost ~60 KB
rather than megabytes. `MAX_GEOMETRIES` caps it at 200.

On the page the map is shown for **every** tab, not just the map tab, and fits
itself to the results when the search carried no map pin. A parcel without a
polygon falls back to a centroid marker, and the page says how many of the
results are drawn with real boundaries rather than leaving it to be guessed.

Two deliberate choices:

- **`ST_PointOnSurface`, not `ST_Centroid`** — a concave parcel's centroid can
  fall outside it, which would quietly make "the point of this property" wrong.
- **`lat`/`lon` stored alongside `centroid`** — the asyncpg geometry codec hands
  back hex EWKB, so the API reads the two floats and only spatial predicates
  touch the geometry.
- **TRUNCATE+INSERT, never DROP** — the read-only console role keeps its grant
  (the `site_index` rationale) and `over_gush_helka()` keeps its return type.

## SQL functions (visible in every `/data` schema dump and to the MCP)

`append_store.sql_helper_functions()` reads `over_%` live from `pg_proc`, so
these need no further wiring:

```
over_parcel_key(gush, suffix, parcel) / over_parcel_key(gush, parcel)
over_house_num(q)            over_house_suffix(q)
over_street_key(sc, q)       over_street(sc, q)
over_parcel_at(lat, lon)     over_parcels_near(lat, lon, radius_m)
over_gush_helka(gush, helka) -> SETOF over_re_parcels
over_zip(city, street, house)  over_address_parcel(city, street, house)
```

Every parameter is prefixed `p_`. In a `LANGUAGE sql` function an unqualified
name matching both a parameter and a column resolves to the **column**, so
`WHERE p.gush = gush` would be a tautology returning the whole table.

## Building it

```bash
# default set — skips the two expensive opt-ins
curl -X POST 'https://www.over.org.il/api/admin/nadlan/build' -H "Authorization: Bearer $TOKEN"
# one-off, and the only stage with material Neon compute cost
curl -X POST 'https://www.over.org.il/api/admin/nadlan/build?stages=source_indexes' -H "Authorization: Bearer $TOKEN"
curl -X POST 'https://www.over.org.il/api/admin/nadlan/build?stages=pip' -H "Authorization: Bearer $TOKEN"
curl 'https://www.over.org.il/api/admin/nadlan/state' -H "Authorization: Bearer $TOKEN"
```

Stages run in dependency order: `source_indexes → parcels → gazetteer →
postal_localities → streets → addresses → zip5 → pip`.

`source_indexes` and `pip` are **excluded from the default** on purpose. The
first is a one-off that never needs repeating; the second is 384k point-in-polygon
probes and the only stage whose cost is material on a compute-billed Neon plan
(~98 % of the bill is compute). `pip` is batched (2,000 rows), **geohash-ordered**
so consecutive batches touch spatially adjacent polygon pages and the buffer
cache actually hits, and resumable — a row is done once `parcel_match` is set.

None of the three `odata` source tables had a single index before this; a
one-gush gazetteer aggregate took 6.3 s. `ensure_source_indexes()` fixes that for
every `/data` user, not just this project.

## Verifying

```bash
pytest tests/test_nadlan_text.py tests/test_nadlan_index.py tests/test_nadlan_api.py
```

Golden fixtures, all confirmed against production:

- אבימלך 8, פתח תקווה → gush 6319 parcel 225
- gush 14271 parcel 2 → כרם בן זמרה, `LEGAL_AREA` 66277, `LOCALITY_I` 664
- חפץ חיים 43, פתח תקווה → ZIP7 `4935048`

After a build, `/api/nadlan/stats` publishes the coverage percentages, and the
page shows them. Showing the gaps is the point of the project, not an
embarrassment.

## Later

Real-estate transactions attach as one more thin table pointing at `parcel_key`
and/or `address_key` — both deterministic text derived from source values, so
they survive every rebuild. The alias layer *is* the matcher that will resolve a
raw transaction row to them, which is the strongest reason the street index is a
general resolver rather than a one-off join.
