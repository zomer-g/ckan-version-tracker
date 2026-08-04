# Verified query patterns

Every query here has been run against production. Timings are from a real run;
treat anything over ~7 s as needing a rewrite before you hand it to a user (the
console kills a statement at 10 s).

- [Finding your way around](#finding-your-way-around)
- [Cross-source joins on settlement](#cross-source-joins-on-settlement)
- [Spatial analysis of a non-spatial register](#spatial-analysis-of-a-non-spatial-register)
- [Map layers](#map-layers)
- [The elections corpus](#the-elections-corpus)
- [Wide tables: unpivoting without naming the columns](#wide-tables-unpivoting-without-naming-the-columns)
- [Timeout rewrites](#timeout-rewrites)

---

## Finding your way around

```sql
-- every table in the two main schemas
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_schema IN ('public', 'knesset') ORDER BY 1, 2

-- a table's columns and types
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'kns_bill' ORDER BY ordinal_position

-- which machine-named column carries a Hebrew idea
SELECT table_name, column_name, alias FROM public.over_column_aliases
WHERE alias LIKE '%אוכלוסי%'

-- which tables can be queried spatially
SELECT table_name FROM information_schema.columns
WHERE table_schema = 'idx' AND column_name = 'geom'
```

## Cross-source joins on settlement

The registers disagree on how to write a town, and some key by CBS code instead
of by name. Resolve both sides to the code. (~2.8 s)

```sql
WITH co AS (          -- רשם החברות: keyed by CBS code already
  SELECT "קוד ישוב"::int AS code, count(*) AS companies
  FROM public.append_ica_companies_cc6286ac
  WHERE "סטטוס חברה" = 'פעילה' AND "קוד ישוב" IS NOT NULL
  GROUP BY 1
),
am AS (               -- רשם העמותות: keyed by a free-text spelling
  SELECT over_settlement_code("כתובת - ישוב") AS code, count(*) AS amutot
  FROM public.append_moj_amutot_73f3cd78
  WHERE "סטטוס עמותה" = 'רשומה'
  GROUP BY 1
)
SELECT s.name AS יישוב, s.population AS אוכלוסייה,
       round(1000.0 * am.amutot / s.population, 1) AS עמותות_ל1000,
       round(1000.0 * co.companies / s.population, 1) AS חברות_ל1000
FROM public.over_settlements s
JOIN co USING (code) JOIN am USING (code)
WHERE s.population >= 20000
ORDER BY עמותות_ל1000 DESC
```

**Showing the user why the index is needed** — every spelling in a register that
differs from the official name. 122 rows on the amutot register; each is a row a
plain name-join would have dropped:

```sql
SELECT "כתובת - ישוב" AS כפי_שכתוב, over_settlement("כתובת - ישוב") AS לפי_הלמס,
       count(*) AS עמותות
FROM public.append_moj_amutot_73f3cd78
WHERE over_settlement("כתובת - ישוב") IS NOT NULL
  AND over_settlement("כתובת - ישוב") <> "כתובת - ישוב"
GROUP BY 1, 2 ORDER BY עמותות DESC
```

A map layer joins the same way — `over_settlement_code(city_desc)` on the
clinics layer, `over_settlement_code(shem_yishuv)` on the statistical areas.
Resolution is good but not total: 1,200 of 1,257 distinct names on the
statistical-areas layer resolve. Report the residue when it matters.

## Spatial analysis of a non-spatial register

The register has no coordinates but names a place; borrow the shape. (~2 s)

```sql
WITH pt AS (SELECT ST_SetSRID(ST_MakePoint(34.7818, 32.0853), 4326)::geography AS p),
near AS (             -- settlements whose built-up areas fall in the radius
  SELECT over_settlement_code(a.shem_yishuv) AS code,
         round((min(ST_Distance(a.geom::geography, pt.p)) / 1000)::numeric, 1) AS km
  FROM idx.govmap_23_d882fbdb_493df16d a, pt
  WHERE ST_DWithin(a.geom::geography, pt.p, 20000)
  GROUP BY 1
),
new_co AS (           -- text filter FIRST, parse only the survivors
  SELECT "קוד ישוב"::int AS code, count(*) AS companies
  FROM public.append_ica_companies_cc6286ac
  WHERE "תאריך התאגדות" LIKE '%/2024'
    AND to_date("תאריך התאגדות", 'DD/MM/YYYY') >= DATE '2024-07-01'
  GROUP BY 1
)
SELECT s.name AS יישוב, n.km AS מרחק_קמ, c.companies AS חברות_חדשות
FROM near n JOIN new_co c USING (code)
JOIN public.over_settlements s ON s.code = n.code
ORDER BY חברות_חדשות DESC
```

Row-level output from the same shape needs `MATERIALIZED`, or it times out:

```sql
co AS MATERIALIZED (
  SELECT "שם חברה" AS name, "קוד ישוב"::int AS code,
         to_date("תאריך התאגדות", 'DD/MM/YYYY') AS dt
  FROM public.append_ica_companies_cc6286ac
  WHERE "תאריך התאגדות" LIKE '%/2025' AND "קוד ישוב" IS NOT NULL
)
```

**Painting a register onto the map** — take the count back to polygons:

```sql
areas AS (
  SELECT over_settlement_code(shem_yishuv) AS code, shem_yishuv AS name,
         ST_AsText(ST_Union(geom)) AS geometry_wkt   -- Union only over ~15 settlements
  FROM idx.govmap_23_d882fbdb_493df16d
  WHERE over_settlement_code(shem_yishuv) IN (SELECT code FROM new_co ORDER BY companies DESC LIMIT 15)
  GROUP BY 1, 2
)
```

`ST_Collect` instead of `ST_Union` scales to all ~1,200 settlements (~5 s) but
emits `GEOMETRYCOLLECTION` when a settlement mixes POLYGON and MULTIPOLYGON —
supported by the map, worth knowing when reading the output.

## Map layers

```sql
-- points + the polygons they fall inside, in one result (the map draws both)
WITH areas AS (
  SELECT shem_yishuv AS muni, geom FROM idx.govmap_23_d882fbdb_493df16d
  WHERE shem_yishuv IN ('ירושלים','באר שבע','חדרה')
)
SELECT 'אזור סטטיסטי' AS סוג, a.muni AS שם, ST_AsText(a.geom) AS geometry_wkt
FROM areas a
UNION ALL
SELECT 'אתר לאומי', h.name || ' (' || a.muni || ')', h.geometry_wkt
FROM idx.govmap_286_0f8ac82b_796b6664 h JOIN areas a ON ST_Intersects(a.geom, h.geom)

-- distance ring, with the rings themselves drawn
SELECT 'טווח 15 ק"מ' AS סוג, NULL AS שם,
       ST_AsText(ST_Boundary(ST_Buffer(p, 15000)::geometry)) AS geometry_wkt FROM tlv

-- nearest neighbour: LATERAL + the <-> operator, not a cross join
SELECT n.name, s.stop_name,
       round(ST_Distance(n.geom::geography, s.geom::geography)::numeric) AS מטר
FROM idx.govmap_286_0f8ac82b_796b6664 n
CROSS JOIN LATERAL (
  SELECT stop_name, geom FROM idx.govmap_20_42de706b_41734d96
  ORDER BY geom <-> n.geom LIMIT 1
) s

-- points in polygon, per capita
SELECT a.shem_yishuv, a.stat11,
       count(sh.*) AS מקלטים,
       round(1000.0 * count(sh.*) / NULLIF(NULLIF(a.pop_total,'')::numeric, 0), 1) AS ל1000,
       ST_AsText(a.geom) AS geometry_wkt
FROM idx.govmap_23_d882fbdb_493df16d a
LEFT JOIN idx.govmap_417_bbd1243d_ece1cf3b sh ON ST_Intersects(a.geom, sh.geom)
WHERE a.shem_yishuv = 'חיפה'
GROUP BY 1, 2, 3, a.geom
```

Useful layers: `govmap_23_…493df16d` אזורים סטטיסטיים (3,063 polygons,
`shem_yishuv`, `pop_total`), `govmap_20_…41734d96` תחנות אוטובוס (35k),
`govmap_96_…9bf332ee` מוסדות ומרפאות (32k, `city_desc`), `govmap_182_…9f1bfd3e`
אתרי עתיקות (30k), `govmap_417_…ece1cf3b` מקלטים (20k),
`govmap_286_…796b6664` אתרים לאומיים (19 points).

## The elections corpus

Per-settlement and per-ballot tables per Knesset, e.g. `append_votes_knesset_1c9517e7`
(K21, April 2019 — column `מספר קלפי`) and `append_votes_knesset_987a6bac`
(K22, September 2019 — column `קלפי`; K23+ use `קלפי` too). Note that **2019 had
two elections** — answer both unless the user picks one.

Party columns are the ballot letters (`מחל`, `פה`, `ג`, `שס`…) and they CHANGE
between elections, so never hardcode a list; see the unpivot below.

**Joining ballots to a place**: `public.append_voting_polls_89252354` (קלפיות,
רשות האוכלוסין) holds street + house + place description per station. The key is
not equality — the register numbers a station (`1010`) and the results split it
into sub-ballots (`101.1 … 101.4`):

```sql
floor(votes."מספר קלפי"::numeric) = polls."סמל קלפי"::numeric / 10
```

324 of 331 Haifa stations matched in K21, so the numbering is stable across
elections — but the register is the CURRENT one, so its ADDRESSES describe today.

**Geocoding a polling station**: the register has no coordinates. The only
practical donor is a municipal institutions layer whose `address` is
`'street house'` — e.g. `idx.govmap_217784_6ad50db7_58154fe8` (מוסדות חינוך
חיפה). Coverage is the story: 49 of 122 distinct Haifa polling addresses. Always
measure and report it:

```sql
count(*) FILTER (WHERE EXISTS (SELECT 1 FROM idx.govmap_217784_… i WHERE i.address = p.addr))
```

**There is no neighbourhood field on a ballot anywhere.** The Haifa
neighbourhoods layer (`govmap_217778_…13cb33c2`) is label POINTS, not polygons,
so a containment test is impossible; distance from the label point is the honest
proxy. Also note the name carries a geresh — `רמת ויז'ניץ` — so `LIKE '%ויזני%'`
finds nothing; use `LIKE 'רמת ויז%ניץ'`.

## Wide tables: unpivoting without naming the columns

A ballot table has ~49 party columns that differ per election. Turn the row into
JSON and iterate its keys — one query then works on every election:

```sql
WITH v AS (
  SELECT "מספר קלפי" AS kalpi, "כשרים"::numeric AS kosher, to_jsonb(t) AS j
  FROM public.append_votes_knesset_1c9517e7 t WHERE "שם ישוב" = 'חיפה'
)
SELECT v.kalpi, kv.key AS מפלגה, kv.value::numeric AS קולות,
       round(100.0 * kv.value::numeric / v.kosher, 1) AS אחוז
FROM v CROSS JOIN LATERAL jsonb_each_text(v.j) AS kv(key, value)
WHERE kv.key NOT IN ('שם ישוב','סמל ישוב','סמל ועדה','ברזל','ריכוז','שופט',
                     'מספר קלפי','קלפי','בזב','מצביעים','פסולים','כשרים','first_seen')
  AND kv.value ~ '^[0-9]+$' AND kv.value::numeric > 0
ORDER BY v.kalpi, קולות DESC
```

The same shape works for any wide source (budget lines, survey batteries).

## Timeout rewrites

| symptom | cause | fix |
|---|---|---|
| `canceling statement due to statement timeout` on a join with a big register | nested loop rescanning the register per key | aggregate the register in a CTE first, join the small result |
| same, but you need row-level output | the planner inlined the CTE | `WITH x AS MATERIALIZED (...)` |
| a date filter on a large table | `to_date` on every row | filter the string shape first (`LIKE '%/2024'`), parse the survivors |
| slow `ST_Union` over a whole layer | Union dissolves geometry | `ST_Collect`, or Union over a `LIMIT`ed subset |
| distances that look absurd | degrees, not metres | cast to `::geography` on both sides |
| `invalid input syntax for type numeric: ""` | empty string is not NULL | `NULLIF(col, '')::numeric` |
| `column "…" does not exist` on a map layer | the layer publishes machine names | look the caption up in `over_column_aliases` |
