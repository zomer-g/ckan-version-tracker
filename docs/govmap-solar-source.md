# GovMap solar-potential sub-site as an OVER source

Recon and wiring, 2026-10-16. Entry point:
<https://www.govmap.gov.il/sites/energy/index.html?map=solar_panels>
("פוטנציאל ייצור אנרגיה סולארית בשטחים בנויים", משרד האנרגיה והתשתיות,
processed by the המרכז למיפוי ישראל). The site says its data is current for
2025; the request is a full monthly snapshot regardless, so every month is a
version (a month with no change produces no new version, as usual).

## What the sub-site serves

The page is a small Vue app (`/sites/energy/assets/index-*.js`) with two maps:

| map | what it draws | source |
|---|---|---|
| `dashboard` (default) | choropleths per locality / statistical area / neighbourhood | vector tiles `/backgroundMaps/pbf/energy-solar/...` + JSON aggregates `/apps/data/energy-solar/{impl_rate_jsons,energy_jsons}/...` |
| `solar_panels` | the two feature layers below, inside an embedded GovMap | GovMap layers, via `govmap.createMap(..., {token})` |

`map=solar_panels` is the one asked for. Its layers, from the bundle
(`bo={cmplx_Solar_Poten541:"layer_225680", bldg_Solar541_V2:"layer_225678"}`)
and the keyed catalog:

| layer | caption | features | geometry | fields | catalog updateDate |
|---|---|---|---|---|---|
| **225678** | מבנים בשטחים בנויים (roofs) | **1,035,632** | MultiPolygon | 35 incl. address, land use, roof area, irradiance, est. kW / kWh | 2026-03-16 |
| **225680** | פוטנציאל הצללה סולארית (parking, sports, parks, cemeteries, terminals) | **19,224** | MultiPolygon | 34 incl. use type, coefficient, net area, est. kW / kWh | 2026-06-04 |

Both: `serviceLayerId govmap:layer_<id>`, `idColumn objectid`, server CRS
EPSG:3857, one style each (46216 / 46218), every row carries the remark
"זכויות היוצרים שמורות למרכז למיפוי ישראל, מבוסס על הערכה וניתוחים, לא לשימוש
מסחרי".

## Why the existing GovMap path could not fetch it as-is

Neither layer is in GovMap's public catalog (`/api/layers-catalog/catalog`, 949
layers). Anonymously every endpoint refuses them: `spatial-analysis/layer-data`
403 "You do not have permission to view this layer", `filter/count` and
`filter/fields` return `null`, the SLD fetcher 403 "Layer is not public".

The embedded map runs in GovMap's iframe mode: the sub-site passes an app key
(`api_token=` in the iframe URL, `token:` in `createMap`), and GovMap's own
client then adds two headers to every API call:

```
x-govmap-token:    <the sub-site's key>
X-Iframe-Referrer: <the embedding page>
```

With both, the same public endpoints answer for these layers: count, fields,
layer-data **with inline geometry** (`withoutGeoShape:false`), the SLD, and
`catalog?apiToken=<key>&viewerOnly=1` (1,071 layers, the two above included).
The key alone is refused, and the key is bound to the govmap.gov.il domain: a
referrer of `https://over.org.il/` gets 403, any govmap.gov.il page is accepted.
The worker sends the sub-site page itself. No reCAPTCHA or user login is
involved (GovMap reserves reCAPTCHA for its real-estate endpoints). The key is
not hardcoded anywhere: the worker reads it from the sub-site's bundle on each
run, picking the production branch
(`hostname.includes("govmap.gov.il")&&(n="<key>")`), so a rotated key is
picked up automatically.

## Chosen fetch path

The existing GovMap engine, unchanged in behaviour for every public layer:
WFS is gone, the primary tier is the spatial-analysis enumeration with inline
geometry (`legacy_engine._try_scrape_via_spatial`), which handles both layers.

What was added (GOVSCRAPER):

* `govscraper/scrapers/govmap/embed_auth.py`: sub-site URL to key + keyed
  catalog, cached per site; a per-layer registry.
* `scrape_govmap` registers the layer when its URL is a sub-site URL.
* The header builders of `spatial_client`, `symbology` and `field_dictionary`
  add the two headers for registered layers only; `catalog_fetch` lends the
  registered layers' keyed-catalog entries (caption, extent, idColumn, styles),
  so the caption, the field dictionary and the symbology bundle work like any
  other GovMap layer. A failed public catalog still reads as failed.

What was added (OVER):

* `app/api/govmap.py`: `GOVMAP_SITES`, a hand-listed registry keyed by
  `(site, map)`. Pasting the sub-site URL expands it, like a layer group, to its
  two layers. Each layer is stored as the sub-site page plus `&lay=<id>`:
  `https://www.govmap.gov.il/sites/energy/index.html?map=solar_panels&lay=225678`.
  That URL is what tells the worker to fetch the key; its identity is still
  `("govmap", "225678")`, so duplicates are caught as for any layer.
* Both creation paths (public request, admin add) give these layers the
  sub-site's cadence, **poll_interval 2,592,000 s (30 days)**, and a Hebrew
  title: `<layer caption> (פוטנציאל ייצור אנרגיה סולארית בשטחים בנויים)`.
* Frontend: the home search box recognises the sub-site link and lists its
  layers (`home.govmap_site_hint`).

`source_type` stays `govmap`, so storage, the R2 file versions, the index
mirror and the `.lyrx` conversion are the existing GovMap ones.

## Local first fetch (no deploy, no OVER push)

Run through the worker's own call (`GovILScraper.scrape(url)`) on this machine:

| layer | rows | time | engine | output |
|---|---|---|---|---|
| 225680 | 19,224 (= declared count) | 75 s | spatial-analysis | CSV 21.0 MB, GeoJSON 32.7 MB, fields sidecar; 100% MultiPolygon, `geometry_wkt` in WGS84 lon/lat |
| 225678 | 1,035,632 (= declared count) | 55 min | spatial-analysis | heavy-layer path: GeoPackage 895 MB + GeoParquet 166 MB + fields sidecar (no CSV/GeoJSON above 100k features) |

Symbology for 225680 resolved to 1 SLD; the field dictionary to 34 fields. The
roofs walk saw one transient connection reset, retried by the existing backoff.
A monthly run of both layers is therefore about an hour of worker time.

## Caveat: SQL for the roofs layer

On over.org.il today every GovMap layer above 100k features (20 layers,
largest 2.5M) is `download_only`: the worker publishes a heavy layer as
GeoPackage + GeoParquet instead of the index CSV, and the index mirror (which
puts GovMap rows into SQL) reads only that CSV. So:

* **225680** (19k) gets files on R2 **and** rows in SQL, like the 931
  queryable GovMap layers.
* **225678** (1.04M) gets files on R2 (GeoPackage + GeoParquet, plus the
  symbology/dictionary bundle) and **no SQL table**, like the other 20 heavy
  layers. Bringing heavy GovMap layers into SQL is a separate change to the
  heavy-layer path, not specific to this source.

## Deploy

1. GOVSCRAPER: push master (fleet self-updates). Until then a sub-site layer
   fails with 403s.
2. OVER: deploy (push the sha to git.xhostd.com, then `deploy(sha=...)`).
3. Paste the entry URL on over.org.il, request both layers, approve them in the
   admin queue (storage default `r2` for GovMap; the interval is already 30
   days).
