# מכרזי רמ"י — the land tenders, across every surface

Added 2026-11-03. The Israel Land Authority's tenders site
(`apps.land.gov.il/MichrazimSite`) is scraped daily by the worker
(`govil-scraper`, source `michrazim`). **One dataset per table** — no dataset
carries tables side by side. This page is how those datasets reach the SQL
console, the נדל"ן לעם parcel page, the REST API and the MCP, and the four facts
about the data that each surface states.

## The datasets

| role | URL that tracks it | rows (v1) | one row is |
|---|---|---|---|
| tenders | `…/MichrazimSite/#/search` (any URL without a route below) — dataset `b252fc31` | ~10.7k | a tender: type, ייעוד, region, settlement + code, dates |
| lots | `…/MichrazimSite/#/lots` | 51,752 | a lot of a tender: units, area, winner, winning amount, development costs, threshold, bids; a polygon for ~1% |
| lot_bids | `…/MichrazimSite/#/lot-bids` | 302,498 | every bid on every lot |
| tender_bids | `…/MichrazimSite/#/tender-bids` | 28,230 | tender-level bids of registration / lottery tenders |
| parcels | `…/MichrazimSite/#/parcels` | 76,394 | (tender, lot, גוש, חלקה) |
| plans | `…/MichrazimSite/#/plans` | 84,157 | (tender, lot, תוכנית, מגרש) |
| documents | `…/MichrazimSite/#/documents` | ~63k | a tender document, with the file archived |

All of them read one on-disk cache of tender reads on the worker machine, and a
tender read in the last 20 hours is not read again (`FRESH_SECONDS`), so the
site is read about once a day however many datasets poll. Only tenders and lots
fetch the map call. The first run of a dataset on a machine without the cache is
a full pass (~half a day; ~a quarter for the corpora without maps).

Nothing in OVER names a physical table: `app/services/michrazim.py` finds each
role by its **column signature** (`SIGNATURES`), restricted to tables of a
dataset's current version (the /data catalog), and takes each role's dataset id
from the catalog (`dataset_of`, `dataset_links`), so answers link to the
versions page of the dataset each part came from.

### The split (2026-11-03) and how to roll it out

Version 1 of `b252fc31` held six tables in one dataset, and not the tenders
table itself (the worker that ran it predated the fix that declares a primary
uploaded as a file). After the split `b252fc31` is the tenders dataset, and
each other table is tracked by its own URL. Order matters, because a version of
`b252fc31` holding the tenders alone takes its six old tables out of the catalog:

1. Deploy the worker (govil-scraper) and OVER.
2. Track the five table URLs above (`#/lots`, `#/lot-bids`, `#/tender-bids`,
   `#/parcels`, `#/plans`); `#/documents` only if not tracked yet.
3. When they have their first versions, set `scraper_config.allow_shrink = true`
   on `b252fc31` for ONE poll: tenders-only is ~10.7k rows against v1's 605k,
   and the shrink guard (50%) would otherwise refuse it, rightly, as a collapse.
   Clear it after that version lands.

While both exist, a table that is its dataset's only table wins over the old
combined one, so the surfaces never answer from the stale copy.

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

**SQL console / SQL MCP.** Each dataset is an ordinary `/data` table (the
lots and parcels datasets carry the `has_parcel` flag, so they appear under the
גוש/חלקה content filter).
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
dependency. The tender-level bidders' ID numbers are public — RMI publishes them —
and are served in full in SQL, the API and the MCP (owner's decision, 2026-11-03;
`BIDDER_ID_COLUMNS`).

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
