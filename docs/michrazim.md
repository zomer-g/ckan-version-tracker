# מכרזי רמ"י — the land tenders, across every surface

Added 2026-11-03. The Israel Land Authority's tenders site
(`apps.land.gov.il/MichrazimSite`) is scraped daily by the worker
(`govil-scraper`, source `michrazim`) into ONE tracked dataset,
`b252fc31-29cb-4a17-ac2e-2f22f8fa0878`, whose version carries several tables in
the append DB. This page is how that one dataset reaches the SQL console, the
נדל"ן לעם parcel page, the REST API and the MCP — and the four facts about the
data that each surface states.

## The tables

| role | table (2026-11-03) | rows | one row is |
|---|---|---|---|
| tenders | *not yet in SQL* — see below | ~10.7k | a tender: type, ייעוד, region, settlement + code, dates |
| lots | `…_b252fc31_d849f194` (מתחמים) | 51,752 | a lot of a tender: units, area, winner, winning amount, development costs, threshold, bids |
| lot_bids | `…_b252fc31_abbf6b55` | 302,498 | every bid on every lot |
| tender_bids | `…_b252fc31_6a9219cd` | 28,230 | tender-level bids of registration / lottery tenders |
| parcels | `…_b252fc31_8302b80d` | 76,394 | (tender, lot, גוש, חלקה) |
| plans | `…_b252fc31_ba6004b3` | 84,157 | (tender, lot, תוכנית, מגרש) |
| documents | `…_b252fc31_19163596` | 62,751 | a tender document with its link |

`…` = `append_michrazim_register_33ed4e63`. Nothing in the code names these:
`app/services/michrazim.py` finds each role by its **column signature**
(`SIGNATURES`), the same rule `find_deals_table()` follows, so a re-registered
dataset or a renamed resource does not break anything.

**The tenders table is missing from version 1.** The scraper produces it as the
dataset's primary CSV (`csv_path`) beside the six `csv_tables`; the worker that
ran the first, 34-hour pass predates the fix that declares a primary uploaded
as a file (`over_worker.push_version`, "michrazim's tenders, moc_cellular's
sites"), so the server kept the side tables and dropped the primary. The next
version published by a current worker carries it, and every surface picks it
up on its own (the tables cache lives 5 minutes). Until then, the tender's own
fields (type, ייעוד, dates) are null and `notes` says so; `year` is the
tender-number year either way.

## Four facts every surface states

Measured on the first version.

1. **A lot is (מזהה מכרז, מזהה מתחם).** A lot nobody bid on is re-marketed in a
   later tender under the same lot id — 15,327 of 51,752 lot rows share an id —
   and the earlier rows say "אין הצעות למתחם זה" with amount 0.
2. **סכום זכייה is not always a land price.** In the per-sqm tenders (מחיר
   למשתכן, מחיר מטרה) the bid is the price of a sqm of flat. The won amounts are
   bimodal: 2,330 lots under 30,000 ₪ (median 9,073, threshold median 7,600)
   against 25,670 above (median 585,976). `amount_basis` is `per_sqm_suspected`
   for a won amount under 30,000 on a lot of 2+ units or with a מחיר מקסימום,
   `lot_total` otherwise, NULL when nobody won; `amount_per_unit` is computed for
   `lot_total` only. Once the tenders table is in, `tender_type` is the
   authority and the heuristic is a cross-check.
3. **The amount excludes development costs.** הוצאות פיתוח is paid on top, and
   in places it is larger than the land price (באר שבע, 2015: median land price
   85,556 ₪ per unit, median land + development 635,147). Both are returned per
   unit, and the series gives their sum.
4. **The parcels are as marketed.** 24,192 of the 32,914 distinct גוש/חלקה pairs
   exist in today's cadastre; the rest were re-parcelled, and the flats built on
   them are registered — and sold — under new numbers. Of the 2015+ pairs,
   10,582 of 20,567 have deals in the tax register.

Settlement names are RMI's spellings ("שהם", "פרדיס", "בנימינה-ג.עדה"). 92 of
488 names did not resolve through `over_settlement_code()`; the clear ones were
added to `data/settlement_aliases_manual.json` (applied on every boot), which
leaves the temporary and unnamed places (חירן (זמני), לא ידוע) and the regional
councils (`מ.א. …`, an authority, not a settlement).

## Surfaces

**SQL console / SQL MCP.** The tables are ordinary `/data` tables (field flag
`has_parcel` is set, so the dataset appears under the גוש/חלקה content filter).
Rule **(4ג)** in `app/mcp/sql_server.py` states the join keys, the
`last_seen IS NULL` filter, the direct join to the deals register
(`p."גוש" = d.gush AND p."חלקה" = d.chelka` — both are integer text without
leading zeros), `over_gush_helka()` for the parcel layer, and the per-sqm trap.
Every API/MCP answer carries `console_sql` — the exact query that answered, in
three readable CTE layers (`x` parses, `y` decides `amount_basis`, the outer
SELECT derives per-unit figures) — and `row_url`, the same query opened at /data.

**נדל"ן לעם.** A cube of its own grain, `gush_helka`: `rmi_tender_lots` lists
every lot marketed on the parcel's גוש/חלקה, with the per-sqm warning on the
lots that need it and a link to the tender page. Its hit count rides the same
probe as the spatial cubes. The GovMap tender layers (363, 362) remain as
spatial cubes; they hold ~7k plots, the register 51k lots.

**REST** (`app/api/michrazim.py`): `/api/michrazim/{stats,lots,series,
settlements,tender/{id},parcel/{g}/{h}}`. `/lots` and `/series` share one filter
dependency. The tender-level bidders' ID numbers are published by the source and
kept in SQL, but never returned by the API or the MCP (`PERSONAL_COLUMNS`).

**MCP.** The deals server (`/deals/mcp`) is where price questions go, so the
tenders live there too: `search_tender_lots`, `tender_price_series`,
`get_tender`, `parcel_tenders`, `list_tender_settlements`,
`tender_register_stats`, with five tender rules in its instructions. The
property server (`/nadlan/mcp`) gets `parcel_tenders`.

**Indexes.** `michrazim.ensure_indexes()` runs with the daily deals-index check:
(גוש, חלקה) on parcels, the tender id (and lot) on the rest.

## Researching apartment prices in tenders

The questions this was built for, and the shape of each answer:

* **land price per unit over time** — `tender_price_series` /
  `/api/michrazim/series` with `settlement`, `amount_basis=lot_total` implied by
  `median_amount_per_unit`; always read `lots_priced_per_unit` beside it.
* **the flat price in מחיר למשתכן tenders** — the same series'
  `median_sqm_price` (or `amount_basis=per_sqm_suspected` on `/lots`).
* **land price vs. what the flats later sold for** — `parcel_tenders` (or the
  parcels table) for the גוש/חלקה, then `parcel_deals` / the deals register on
  the same pair, filtered to deals after the tender, with the caveat in fact 4.
* **competition** — `bids`, `lots_no_bids`, and the full bid list per lot from
  `get_tender`.
