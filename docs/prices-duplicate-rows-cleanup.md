# שקיפות מחירים: why the tables grew 1 GB a week, and the cleanup of what is already there

Status 2026-10-15: the scraper fix is committed in GOVSCRAPER (not yet pushed,
push = fleet deploy). The cleanup below is a PLAN; nothing has been run.

## What was happening

The prices source is supposed to publish only changes: one row per price
STATE at a store, opened with `first_seen_date` and closed later by updating
`last_seen_date` in place (OVER hashes every column except `last_seen_date`,
`append_stamp_column`). That contract works. The changelog state carried over
between runs fine as well: the daily rows were not a lost-state problem.

Three source quirks made the scraper close and reopen the same states daily:

| cause | chain | rows/week it produced | mechanism |
|---|---|---|---|
| two files per store scope | קרפור (f240d9c5) | 944k (778k prices, 123k promos, 42k promo items) | 70 of 142 price files are named for store N but carry `StoreId` of another store in their XML header (file `-407-` says 117, verified by download). The engine scoped the diff by header, so both files landed on scope `1|117` and closed each other's ~10k prices every run. Store 407's prices were also filed under store 117. |
| a "full" file that is not | סיטי מרקט (b9dd704c) | 405k (194k prices, 187k promo items) | store 66's PriceFull holds 2–56 items on most days and 37,875 on others. Each small day closed 37k prices (and ~40k chain-wide promo items), the next full day reopened them identically. |
| another chain's file on the site | פז — סופר יודה (bb816aed) | 208k (157k prices) | the listing holds a Freshmarket (7290876100000) store 230 file (29k items) beside its own store 230 (5.5k items); the foreign name carries no sub-chain, both fell to the header's → one scope, flapping daily. |

Together: ~1.56M of the ~2.7M prices-source rows written in the week to
2026-10-14. The remaining ~1.1M/week are real changes across 33 chains
(Shufersal and Super-Pharm alone change 7–9k prices a day each).

## The fix (GOVSCRAPER `govscraper/scrapers/prices/_engine.py`)

1. A store's scope is the file NAME's sub-chain and store; the header is used
   only where the name has none. Published `sub_chain_id`/`store_id` follow.
2. A second file reaching a scope already diffed in the run is skipped with a
   warning. The chain's own chain code (from the file name) sorts first, so it
   wins a collision.
3. A "full" file with fewer than 20% of the rows its scope has open (and 200+
   open) is read as partial: it may open and change rows, closes none, and
   stops the run from closing chain-wide rows (catalogue, promotion items).

Tests: `tests/scrapers/test_prices_changelog.py` (38 pass) runs the engine
over a fake platform for each case.

### What the first run after deploy will do (one-time)

* קרפור: the 70 mis-headed stores are re-homed. Under store 117 (and the
  other 69), the rows that came from the other file are closed; under 407
  (etc.) ~8.5k rows per store open fresh. Expect roughly 600k opened rows plus
  as many in-place closes on that day, then thousands a day.
* פז — סופר יודה: store 230's 29k foreign rows close once; the own 5.5k stay.
* סיטי מרקט: nothing visible unless a small file arrives; then a warning
  `נקרא כחלקי` on the version instead of 37k closes.
* Every chain's local SQLite state is still valid (scope strings did not
  change shape), so no rebuild from OVER is triggered.

Verification after deploy: for קרפור, `rows_added` per `first_seen::date`
should fall from ~120k to a few thousand on the second day after deploy:

```sql
SELECT first_seen::date AS d, count(*) AS rows_added,
       count(*) FILTER (WHERE last_seen_date = '') AS opened,
       count(*) FILTER (WHERE last_seen_date = first_seen_date) AS same_day
FROM append_prices_chain_f240d9c5_2e0a7e7b_ad8971b7
WHERE first_seen >= now() - interval '5 days' GROUP BY 1 ORDER BY 1;
```

## The rows already there

Measured 2026-10-15 on prod (`count(*)` vs `count(DISTINCT md5(all columns
except first_seen_date, last_seen_date, first_seen, row_hash))`):

| table | rows | distinct states | removable | B/row (table+indexes) | ≈ space |
|---|---|---|---|---|---|
| קרפור prices `…f240d9c5_2e0a7e7b_ad8971b7` | 3,790,103 | 1,383,660 | 2,406,443 | 236 | 568 MB |
| קרפור promos `…f240d9c5_2e0a7e7b_67ff632c` | 765,397 | 464,660 | 300,737 | 345 | 104 MB |
| סיטי מרקט prices `…b9dd704c_67414d07_ad8971b7` | 1,235,526 | 742,992 | 492,534 | 248 | 122 MB |
| סיטי מרקט promo items `…b9dd704c_67414d07_527fd157` | 661,892 | 531,167 | 130,725 | 195 | 25 MB |
| סיטי מרקט promos `…b9dd704c_67414d07_67ff632c` | 22,680 | 15,380 | 7,300 | 364 | 3 MB |
| פז — סופר יודה prices `…bb816aed_fca69d99_ad8971b7` | 357,750 | 247,833 | 109,917 | 249 | 27 MB |
| ויקטורי prices `…6d67a910_e500decb_ad8971b7` | 1,000,047 | 837,471 | 162,576 | 236 | 38 MB |
| מחסני השוק prices `…62fb940b_8377b1b0_ad8971b7` | 1,077,492 | 960,337 | 117,155 | 236 | 28 MB |
| **total** | | | **≈3.73M** | | **≈0.9 GB** |

Not worth touching: סופר-פארם (30k of 2.67M), רמי לוי (16k of 1.13M),
שופרסל (6.5k of 2.50M). Those small counts are mostly genuine A→B→A price
flips, which are two different states and must stay two rows.

So the cleanup returns about 0.9 GB of the 196 prices tables' 8.2 GB (the
public schema is 47 GB, the account's DB 59 GB). The weekly saving from the
fix (~0.55 GB/week) is the bigger number; the cleanup is optional and can wait.

### Caveat: interleaved series

In the קרפור and סופר יודה cases the duplicates are two stores' price series
filed under one key, interleaved by day. Compacting each identical state into
one row `[min first_seen_date, max last_seen_date]` gives, for such a key, two
rows that OVERLAP in time (price 3.90 and price 4.50 both "in force" 09-23 to
10-14 at store 117). That is the faithful summary of what the source published
under that header; the post-fix rows separate the series properly (407's
prices under 407). A genuine A→B→A flip would also be collapsed by a
hash-group compaction, which is why the rule below only compacts groups of
3+ identical rows, and only in the tables listed above.

### The procedure (per table, inside one transaction, dataset paused)

Run AFTER the fix is deployed and one quiet day has been verified, with the
dataset's poll paused (or during the day, well away from the 07:00 poll), so
the compaction never races the daily insert.

```sql
-- 1. the state key: every source column except the two date columns
--    (adjust the column list per table; this is the prices table)
CREATE TEMP TABLE grp AS
SELECT ctid AS tid,
       md5(concat_ws('|', chain_id, sub_chain_id, store_id, item_code, item_price,
                     unit_of_measure_price, allow_discount, item_status,
                     price_update_time)) AS k,
       first_seen_date, last_seen_date, first_seen
FROM append_prices_chain_f240d9c5_2e0a7e7b_ad8971b7;

-- 2. groups worth compacting: 3+ identical states
CREATE TEMP TABLE keep AS
SELECT DISTINCT ON (k) k, tid,
       bool_or(last_seen_date = '') OVER (PARTITION BY k) AS any_open,
       max(last_seen_date)          OVER (PARTITION BY k) AS last_max,
       count(*)                     OVER (PARTITION BY k) AS n
FROM grp
ORDER BY k, first_seen_date, first_seen;     -- the earliest row survives

-- 3. date the survivor to the latest sighting; open if any copy is open
UPDATE append_prices_chain_f240d9c5_2e0a7e7b_ad8971b7 t
SET last_seen_date = CASE WHEN keep.any_open THEN '' ELSE keep.last_max END
FROM keep WHERE keep.n >= 3 AND t.ctid = keep.tid;

-- 4. drop the other copies
DELETE FROM append_prices_chain_f240d9c5_2e0a7e7b_ad8971b7 t
USING grp JOIN keep USING (k)
WHERE keep.n >= 3 AND t.ctid = grp.tid AND grp.tid <> keep.tid;
```

`row_hash` needs no recompute: the identity excludes `last_seen_date` and the
survivor keeps its own `first_seen_date`. The set of OPEN states per key is
unchanged (an open copy makes the survivor open), so the scraper's local
state still matches OVER and no rebuild is triggered.

Space comes back only after `VACUUM (FULL)` (exclusive lock, a few minutes for
the 855 MB table) or `pg_repack`; a plain `VACUUM` leaves it for the table to
reuse, which is also fine given the table keeps growing.

Dry run first: replace steps 3–4 with `SELECT count(*) FROM grp JOIN keep
USING (k) WHERE keep.n >= 3 AND grp.tid <> keep.tid` and compare with the
"removable" column above (it will be slightly lower, since groups of 2 stay).
