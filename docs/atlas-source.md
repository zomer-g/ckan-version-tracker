# אטלס ישראל (atlas.gov.il) as an OVER source

Recon and wiring, 2026-11-01. Entry point: <https://www.atlas.gov.il/>.

## What the site is

An Angular app on its own host (S3 + CloudFront) that draws **GovMap layers**
inside an embedded GovMap map (`govmap.createMap(div, {token: ...})`). Its
content comes from two static files:

| file | what it holds |
|---|---|
| `backend-assets/he/menus/main.json` | the menu: 401 entries; headings, and 293 layers whose `lay` is a GovMap layer id |
| `backend-assets/he/layers.json` | per-layer info (description, legend, links, charts); names one layer the menu does not (227259) |

**290 of the 293 layers are not in GovMap's public catalog.** Like the solar
sub-site (docs/govmap-solar-source.md), they answer only when a call carries the
site's key and the embedding page:

```
x-govmap-token:    5a385fff-… (from main.<hash>.js, never hardcoded)
X-Iframe-Referrer: https://www.atlas.gov.il/
```

With them, the keyed catalog (`catalog?apiToken=<key>&viewerOnly=1`) has all
293, and count / fields / layer-data with inline geometry / SLD work as for any
public layer. The key is bound to atlas.gov.il: a govmap.gov.il referrer is not
what it is issued for.

Atlas opens with a layer switched on from its own `?lay=<id>` parameter, so the
stored source URL is a link that works for readers too:
`https://www.atlas.gov.il/?lay=228527`.

## Size (declared counts, 2026-11-01)

| | layers | features |
|---|---|---|
| all | 293 | 1,820,387 |
| ≤ 1,000 features | 254 | |
| > 100k (heavy path: GeoPackage + GeoParquet, no SQL) | 2 | 233199 נחלים 1,050,154; 235791 586,191 |
| TAMA 1 layers drawn from iplan ArcGIS | 34 | count 0 at GovMap (see below) |

34 layers (231661–231700, "תמ"א 1" groups) are `isDirect` in the keyed catalog:
GovMap only displays them, from
`ags.iplan.gov.il/arcgisiplan/rest/services/PlanningPublic/TAMA_1/MapServer/<n>`.
The worker's existing external-ArcGIS path (`esri_source.py`) scrapes them;
it needed the same TLS cipher floor (`DEFAULT@SECLEVEL=1`, verification kept)
that mavat already uses for that server.

## Wiring

GOVSCRAPER:

* `govmap/embed_auth.py`: `atlas.gov.il` is an embedding host; the referrer is
  the app's root; the key is read from the root-relative bundle (`createMap`
  pattern now tolerates the `onClick` callback before `token:`).
* `govil/legacy_engine.parse_gov_url`, `govmap/scraper.py`, `govil/scraper.py`:
  atlas.gov.il links go to the GovMap engine, never to the gov.il one.
* `govmap/esri_source.py`: SECLEVEL=1 session for `ags.iplan.gov.il` only.

OVER (`source_type` stays `govmap`; storage, versions, index mirror unchanged):

* `app/api/govmap.py`: the bare site expands to every layer the live menu lists
  (cached an hour), titled `<layer> — <menu group> (אטלס ישראל)` because Atlas
  reuses names ("מגרשים" in four local plans); cadence monthly.
* `url_identity` / `dataset_lookup`: an Atlas link's identity is
  `("govmap", <id>)`, and the duplicate lookup considers GovMap datasets on
  either host, so the 3 public layers are not tracked twice.
* Frontend: the home search box and the GovMap request form accept Atlas links.

## Deploy

1. GOVSCRAPER: push master (fleet self-updates). Until then Atlas layers 403.
2. OVER: deploy.
3. Paste `https://www.atlas.gov.il/` on over.org.il, request all layers,
   approve them in the admin queue.
