# עסקאות נדל"ן — the מיסוי מקרקעין deal register

`/projects/deals` + `/api/deals/*`. One source, one table: 3.84 M reported
real-estate deals from 1998 to today, scraped from nadlan.taxes.gov.il and kept
like any other tracked dataset.

**The table is found, not named.** Its physical name carries the tracked
dataset's id (`…_fd06f5ae`), and the corpus was published as one CSV per
settlement before being merged into one table, so the name has already changed
once. `nadlan_index.find_deals_table()` resolves it by column signature, largest
wins — which is also what makes the resolution self-correcting mid-migration —
and `deals_table()` memoises that for 5 minutes, because every property lookup,
browse and MCP call needs the answer and the finder scans three schemas.

Nothing here is derived. Every other "לעם" project publishes something it
computed; this one publishes one publisher's rows, so the page shows
**מקור ראשוני** instead of the processed banner and every API answer carries
`processed: false`. What the project adds is not data, it is reach: the
publisher only lets you read one גוש at a time behind a form.

The project exists because two of נדל"ן לעם's six tabs were never property
lookups — the gap report below and the quiz built on it — and because the deal
register they argue about had nowhere to be browsed. Split out 2026-09-20;
`/projects/nadlan?tab=gaps` and `?tab=quiz` still resolve, to the new address.

## The three shapes of question

| endpoint | answers |
|---|---|
| `/api/deals/search` | the browse: filtered, sorted, paged |
| `/api/deals/series` | deals and **median** price per year |
| `/api/deals/breakdown` | deals and median price per deal type |

All three take the **identical filter**, built once in `deals_query._where()`.
That is the point: a chart that could describe a different population than the
table beneath it is worse than no chart.

Median, never mean. The register mixes a single flat with the sale of a whole
building, and one such row moves an average by millions.

## Four things the register does, that a query has to know

1. **Every column is text.** Dates are DD/MM/YYYY, amounts are clean integer
   strings (measured over all 3.84 M rows). Numeric casts still go through
   `nullif(col, '')` so a publisher change degrades instead of 500-ing.
2. **`to_date` is only STABLE**, so it can carry neither an expression index nor
   a correct ORDER BY. Everything goes through `nadlan_index.DEAL_SORT_KEY`, the
   immutable `substr` rearrangement into sortable YYYYMMDD, imported by both the
   index declaration and every query so the two cannot drift apart.
3. **The settlement key is the NAME.** 674,340 rows (17.5%) carry an empty
   `settlement_code`; only 2,171 lack a name. `/api/deals/settlements` returns
   the names verbatim, because they are the exact strings `/search` filters on —
   a picker built from that list cannot produce an empty result by spelling.
4. **No gush suffix, no address.** The register reports גוש + חלקה + תת-חלקה
   only, which is why a link to a property goes through נדל"ן לעם and inherits
   its `gp_ambiguous` caveat.

## Two costs that are deliberately bounded

* **`total` is capped at 10,000** and reports `total_capped: true` past it. An
  exact count of a city's whole history is real compute for a number nobody
  reads past the first page.
* **The pick lists are cached 15 minutes.** `/settlements`, `/natures` and
  `/stats` are whole-table aggregates that only move when the scraper appends,
  and they are called on every page load. Same rationale, and the same shape, as
  `nadlan_query.stats()`.

## An unset filter is absent from the SQL

`_where()` emits nothing for a filter the caller did not set, rather than
`$n IS NULL OR settlement = $n`. The second form reads as equivalent and defeats
the index this project adds. There is a test pinning it.

## The MCP resource (`/deals/mcp`)

Added 2026-09-20, on the shared authorization server. This one exists for
free-text questions — "how much did prices move in my town", "where did the
market cool" — which the source cannot answer at all, because it is read one
גוש at a time.

The four rules in `SERVER_INSTRUCTIONS` are the four ways such a question goes
confidently wrong, and each is enforced by the shape of the tools rather than
left to good intentions:

1. **Median, never mean.** `price_series` and `compare_settlements` return
   medians; there is no tool that returns an average.
2. **Filter by deal type.** `compare_settlements` run without `nature` puts a
   `warning` in the RESULT, not only in instructions nobody re-reads: a
   settlement whose mix shifted from flats to plots shows a "price change" that
   is a composition change.
3. **Settlement by name, not code.** `list_settlements` returns the strings the
   filter actually matches.
4. **Show the counts behind a median.** Every aggregate returns `deals`
   alongside the price, and `compare_settlements` enforces a floor in **both**
   years so a place with a handful of sales cannot post a 90% "rise".

| tool | answers |
|---|---|
| `search_deals` | the rows, filtered, sorted, paged, with the /data deep-link |
| `price_series` | deals + median price per year, same filter |
| `compare_settlements` | two years, every settlement, side by side, with the change |
| `list_settlements` / `list_deal_types` | the pick lists, with counts |
| `parcel_deals` | one parcel's history — the same function נדל"ן לעם calls |
| `register_stats` | size, span, and the tracked dataset behind it |

A model that needs to get from an address to a גוש and חלקה is pointed at the
sibling resource, `/nadlan/mcp`, rather than being given a street column it
does not have.

## The gap report: nadlan.gov.il against מיסוי מקרקעין

`/projects/deals?tab=gaps` is not a lookup. It is a written comparison of the
two government sites that publish the same property transactions, added
2026-09-09 from a hand check of ten random parcels read page by page on both
sites, and revised 2026-09-10 after a re-check that also opened the previous-sales
window behind every nadlan.gov.il row.

The headline is that nadlan.gov.il's deals table shows **one row per property**,
the latest sale, and keeps every earlier sale of that property in a
"עסקאות קודמות לנכס" window that opens only on a click. Read as a table it shows
93 sales against the tax authority's 161; opened row by row it shows 145, and 140
of those match the tax authority on date and amount (120 to the shekel, 20 rounded
to the thousand). The first version of the report, written before those windows
were opened, read the same numbers as "one sale per sub-parcel, the last one"
and was wrong. What survives is seven tax-authority sales that appear nowhere on
nadlan.gov.il, amounts that differ, a whole parcel with no page, an area off by a
factor of ten, and the fact that every transaction carries two official amounts
(תמורה מוצהרת and שווי מכירה) of which nadlan.gov.il publishes only one, unmarked.

It matters here because the crosswalk's own consumers hit the same traps:

* Counting rows on nadlan.gov.il, or trusting its "נמצאו N עסקאות", counts
  **properties**, not sales: 52 of the 145 sales sit behind the rows.
* An amount there is the **assessed** value, never the declared one.
* Joining the two registers **by locality name** silently drops rows. Two of the
  ten parcels carry a different locality name on each site, which is the same
  failure mode `over_settlement_code()` exists to prevent.
* A parcel that returns "0 עסקאות", or no page at all, is not evidence that
  nothing was sold there. About a quarter of page loads returned an empty table
  for a parcel that has transactions.

### Where it lives

| piece | path |
|---|---|
| the report | `frontend/src/components/deals/NadlanGaps.tsx` (lazy chunk, ~10 kB gz) |
| figure metadata | `frontend/src/components/deals/nadlanGapsFigures.ts` |
| screenshots | `frontend/public/nadlan-gaps/fig-NN.jpg` + `thumb-NN.jpg` |
| styles | `.ngap-*` block at the end of `frontend/src/index.css` |

Two things about it are deliberate and easy to undo by accident:

1. **The screenshots are static files, not data URIs.** The report arrived as a
   single HTML page with every image inlined in base64 (4 MB and 36 images
   at first, 5.5 MB and 68 after the re-check). Inlined, they
   would sit in the JS bundle and be paid for by every visitor to every page.
2. **The grid shows separate 520px crops.** `loading="lazy"` on the full images
   did not hold them back, the browser fetched every one on tab open, so the grid
   has its own copies: all 68 pictures for 0.75 MB instead of 3.9 MB. The
   full image is fetched only when the lightbox opens.

A third is the `Ltr` helper. A cell like `+132,712 · 18%` is entirely
bidi-neutral, so an RTL paragraph reorders it into `18% · 132,712+` and a
leading minus lands after the digits. Numeric cells that carry a sign, a
separator or two values are wrapped in an LTR isolate; cells that mix Hebrew
with a number isolate only the numeric tail, and units such as מ״ר were moved
into the column header rather than repeated per cell.

## The quiz: "שניים אוחזין בעסקה"

`/projects/deals?tab=quiz` is the gap report as a trivia round. Every question,
every number and every distractor comes from the report next door; nothing in
the bank is invented.

It exists because prose does not stick. A reader who is told that "מחיר העסקה"
on nadlan.gov.il is the assessed value rather than the declared one nods and
forgets; a player who guesses wrong and is told why remembers. So the round is
built to be cheated on: the explanation follows every answer, right or wrong,
and a link to the report sits on screen the whole time.

| piece | path |
|---|---|
| the game | `frontend/src/components/deals/NadlanQuiz.tsx` (lazy chunk, ~7 kB gz) |
| the bank | `frontend/src/components/deals/nadlanQuizQuestions.ts` |
| styles | `.nquiz-*` block at the end of `frontend/src/index.css` |

Rules of the thing, in case it gets extended:

* **The bank is larger than a round**: 38 questions, 25 per round
  (`ROUND_SIZE`). The surplus is what makes a
  second round a different round; drop it and replays become identical.
* **Both the questions and the answers are shuffled.** Without the second
  shuffle, "the longest option" becomes a winning strategy, because the correct
  answer is usually the one that needs a qualifier.
* **Every question carries a `finding`**, the numbered section of the report at
  `?tab=gaps` it is drawn from, and the number is shown with the explanation. A
  player who wants to argue with an answer is told exactly where to go and check.
  `finding: 0` means the appendix or the method section.
* **No timer, no streak, no prompt to come back.** The tab is opt-in and it stays
  that way.
* **The bank follows the report.** A question whose answer the report no longer
  supports is worse than no question. The 2026-09-10 re-check rewrote five whose
  premise was "one sale per sub-parcel", corrected the numbers in four more, and
  added eight about the history window.

The result is a score out of 25, a rank, and a Wordle-style 5×5 grid of 🟩/🟥
that shares as plain text through `navigator.share` where it exists and through
the clipboard elsewhere. Both can be refused by the browser, so the failure is
reported in the UI rather than swallowed.
