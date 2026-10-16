# Public API — `gov-versions` v1

A read-only HTTP API for the **גרסאות לעם** project. Use it to discover
which Israeli government datasets are being version-tracked, browse their
version history, and follow links back to the original source on
data.gov.il / gov.il and to the snapshotted mirror on
[odata.org.il](https://www.odata.org.il).

- **Base URL:** `https://<your-deployment>/api/v1`
- **Auth:** none. All endpoints are public.
- **Format:** JSON. UTF-8. No trailing slash on paths.
- **CORS:** allowed by the server config. Calls from the browser are fine.
- **Stability:** the `/api/v1` namespace is the stable public surface.
  Endpoints under `/api/...` (no version prefix) are internal to the SPA
  and may change without notice.

If you find a bug or want a field added, open an issue on the
[GitHub repo](https://github.com/zomer-g/ckan-version-tracker).

---

## Endpoints at a glance

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/v1/datasets` | List datasets, with filters |
| GET | `/api/v1/datasets/{id}` | One dataset by UUID |
| GET | `/api/v1/datasets/{id}/versions` | Version history of a dataset (newest first) |
| GET | `/api/v1/datasets/{id}/versions/latest` | The most recent version of a dataset |
| GET | `/api/v1/datasets/{id}/versions/{number}` | A specific version by its number |
| GET | `/api/v1/tags` | All tags + dataset counts |
| GET | `/api/v1/tags/{id}` | One tag + the datasets under it |
| GET | `/api/v1/organizations` | All organizations + dataset counts |
| GET | `/api/v1/organizations/{id}` | One organization |
| GET | `/api/append/{id}/rows?as_of=…` | Archived rows, optionally as they stood at a moment (see [Row archive](#row-archive-apiappendid-and-row-validity)) |
| GET | `/api/append/{id}/history/files` | Parquet files of the closed rows on R2 (see [Closed history](#closed-history-as-parquet-apiappendidhistoryfiles)) |

Every UUID returned by the API is a stable identifier you can store
client-side. URLs returned in responses (`url`, `versions_url`,
`odata_url`, `download_url`, etc.) are absolute and safe to bookmark.

---

## `GET /api/v1/datasets`

List datasets. Returns a paginated envelope.

### Query parameters

| Name | Type | Default | Description |
|---|---|---|---|
| `organization_id` | UUID | — | Restrict to one organization. |
| `tag_id` | UUID, **repeatable** | — | Restrict to datasets carrying this tag. Repeat the parameter to require **multiple** tags (AND-logic). |
| `tag` | string, **repeatable** | — | Same as `tag_id` but matches by tag NAME (case-insensitive). Mix-and-match with `tag_id` is allowed. |
| `status` | `active` \| `pending` \| `all` | `active` | Default hides rejected and pending submissions. |
| `limit` | integer 1–500 | `100` | Page size. |
| `offset` | integer ≥ 0 | `0` | Page offset. |

### Tag AND-logic

Multiple tag filters are AND-combined. So:

```
GET /api/v1/datasets?tag=מערכת%20האכיפה&tag=הנחיות%20מינהליות
```

returns only datasets that carry **both** `מערכת האכיפה` **and**
`הנחיות מינהליות`. Unknown tag names produce an empty result rather
than an error, so callers can chain filters defensively.

### Response

```json
{
  "total": 42,
  "limit": 100,
  "offset": 0,
  "items": [
    {
      "id": "9d4c6e2a-3f1b-4c9d-9c7e-1234567890ab",
      "title": "פסקי דין מנהליים — תיקי אכיפה",
      "source_type": "ckan",
      "source_url": "https://data.gov.il/he/datasets/justice-ministry/enforcement-rulings",
      "odata_dataset_id": "abcd1234-...",
      "odata_url": "https://www.odata.org.il/dataset/abcd1234-...",
      "organization": {
        "id": "...",
        "name": "justice-ministry",
        "title": "משרד המשפטים",
        "url": "https://example.org/api/v1/organizations/..."
      },
      "tags": [
        { "id": "...", "name": "מערכת האכיפה", "url": "https://example.org/api/v1/tags/..." },
        { "id": "...", "name": "הנחיות מינהליות", "url": "https://example.org/api/v1/tags/..." }
      ],
      "poll_interval": 604800,
      "last_polled_at": "2026-04-25T08:00:00+00:00",
      "last_modified": "2026-04-22T09:14:11.000000",
      "status": "active",
      "version_count": 7,
      "versions_url": "https://example.org/api/v1/datasets/9d4c6e2a-.../versions"
    }
  ]
}
```

### Examples

```bash
# Everything tracked from the Ministry of Health
curl 'https://example.org/api/v1/datasets?organization_id=<UUID>'

# Datasets tagged "transportation" — by name
curl 'https://example.org/api/v1/datasets?tag=%D7%AA%D7%97%D7%91%D7%95%D7%A8%D7%94'

# Datasets tagged BOTH "enforcement" AND "administrative guidelines"
curl 'https://example.org/api/v1/datasets?tag=enforcement&tag=guidelines'

# Same idea using UUIDs
curl 'https://example.org/api/v1/datasets?tag_id=<UUID-A>&tag_id=<UUID-B>'

# Page 2 of 50-item pages, all statuses
curl 'https://example.org/api/v1/datasets?limit=50&offset=50&status=all'
```

---

## `GET /api/v1/datasets/{id}`

Single dataset by UUID. Same shape as one item from the list endpoint.

```bash
curl 'https://example.org/api/v1/datasets/9d4c6e2a-3f1b-4c9d-9c7e-1234567890ab'
```

`404` if the UUID does not exist.

---

## `GET /api/v1/datasets/{id}/versions`

Full version history of a dataset, newest first. Each version describes
what changed and where to download the snapshot data on odata.org.il.

### Response

```json
[
  {
    "id": "...",
    "version_number": 7,
    "detected_at": "2026-04-22T09:14:11+00:00",
    "metadata_modified": "2026-04-22T09:13:55.123456",
    "change_summary": {
      "type": "data_changed",
      "delta": 142,
      "previous_count": 12500,
      "record_count": 12642
    },
    "odata_metadata_resource_id": "9b...c4",
    "odata_metadata_url": "https://www.odata.org.il/dataset/abcd1234-.../resource/9b...c4",
    "resources": [
      {
        "name": "main",
        "storage": "odata",
        "download_url": "https://www.odata.org.il/dataset/abcd1234-.../resource/33...e1/download",
        "odata_resource_id": "33...e1",
        "odata_resource_url": "https://www.odata.org.il/dataset/abcd1234-.../resource/33...e1",
        "format": null
      },
      {
        "name": "_zip",
        "storage": "r2",
        "download_url": "https://pub-63c02556dabd4956af9500eb8fe7198c.r2.dev/datasets/9d4c.../v7/ab12cd34_attachments.zip",
        "odata_resource_id": null,
        "odata_resource_url": null,
        "format": null
      }
    ]
  }
]
```

- `change_summary.type` distinguishes the kind of change. Common values:
  `data_changed`, `resources_added`, `resources_removed`,
  `resources_modified`, `metadata_only`.
- `odata_metadata_url` points at the **CKAN metadata snapshot** captured
  at this version (a JSON blob on odata.org.il). Use it for
  point-in-time metadata queries.
- `resources[]` lists the actual data files for the version. **Always use
  `download_url`** to fetch a file — it is correct regardless of where the
  bytes live. The `storage` field tells you where that is:
  - `"odata"` — legacy CKAN mirror; `odata_resource_id` / `odata_resource_url`
    are populated and `download_url` points at odata.org.il.
  - `"r2"` — independent object store; `download_url` is the object's public
    URL and the two `odata_*` fields are `null`. (Newer versions land in R2;
    older ones remain on ODATA. Both are served transparently.)

> **Send a User-Agent when downloading from the object store.** The R2 public
> domain sits behind a bot filter that rejects the Python standard library's
> default `Python-urllib/3.x` agent with **403**, on every file. `curl`, `wget`,
> `requests`, browsers — and even a request with no User-Agent at all — are
> served normally. This is worth knowing because the shortest possible snippet
> is the one that fails:
>
> ```python
> # 403
> urllib.request.urlopen(resource["download_url"])
>
> # 200
> req = urllib.request.Request(resource["download_url"],
>                              headers={"User-Agent": "my-tool/1.0"})
> urllib.request.urlopen(req)
> ```
>
> Going through `/api/versions/{id}/download/{name}` does not help: it redirects
> to the same domain, and the client re-sends the same agent.
- `name` is either the source's own resource name, or one of the
  underscore keys for files OVER adds alongside the data:

  | `name` | `format` | what it is |
  |---|---|---|
  | `_geojson` | `GeoJSON` | the layer's features, WGS84 (gzipped) |
  | `_gpkg` | `GPKG` | GeoPackage — heavy layers publish this instead of CSV+GeoJSON |
  | `_parquet` | `GeoParquet` | the same features as a columnar analytics file |
  | `_symbology` | `ZIP` | the layer's OGC SLD symbology + its field dictionary (machine name → Hebrew alias) |
  | `_zip`, `_zip_parts` | `ZIP` | files the source published as attachments |

  When one of these holds several files the name carries a 0-based index
  (`_zip_parts_0`, `_zip_parts_1`, …); `_geojson` alone keeps its bare name
  in the single-file case, as it always has.

> **`_symbology` is answered for every version of a layer**, not only the one
> that captured it. A GovMap style belongs to the layer, and the scraper
> re-uploads the bundle only when it changes, so
> `GET /api/versions/{id}/download/_symbology` falls back to the newest bundle
> the dataset has. The `resources` array above still lists only what a version
> literally holds — the fallback lives on the download route.

---

## `GET /api/versions/{version_id}/symbology.lyrx.zip`

The same symbology as an **ArcGIS Pro** bundle: one `.lyrx` (Esri CIM layer
file) per style, converted on the fly from the archived SLD, plus the field
dictionary, the original icons and a README. Works on any version of a GovMap
layer — including every version archived before this existed — under the same
newest-bundle fallback as `_symbology`.

```
GET /api/versions/f4df90c5-.../symbology.lyrx.zip   → application/zip
```

Apply it with *Apply Symbology From Layer*; the `.lyrx` deliberately points at
a placeholder data source, since it carries cartography, not features.

`404` when the dataset has no symbology archived at all, `422` when the bundle
holds no convertible SLD.

`404` if the dataset UUID does not exist. An empty array means the
dataset has been registered but no versions have been detected yet.

---

## `GET /api/v1/datasets/{id}/versions/latest`

The single most recent version of a dataset (the highest `version_number`).
Same object shape as one entry of the versions list above.

```
GET /api/v1/datasets/9d4c6e2a-.../versions/latest
```

`404` if the dataset does not exist, or if it has no versions yet.

---

## `GET /api/v1/datasets/{id}/versions/{number}`

A specific version addressed by its `version_number` (1-based, as shown in
the versions list). Same object shape as one entry of the list.

```
GET /api/v1/datasets/9d4c6e2a-.../versions/3
```

`404` if the dataset does not exist or has no version with that number.

---

## `GET /api/v1/tags`

List every tag with the count of `active`/`pending` datasets carrying
it. Useful for building a tag cloud or an autocomplete.

```json
[
  {
    "id": "...",
    "name": "מערכת האכיפה",
    "description": null,
    "dataset_count": 18,
    "url": "https://example.org/api/v1/tags/..."
  }
]
```

---

## `GET /api/v1/tags/{id}`

One tag, plus the full list of datasets under it (same `DatasetSummary`
shape as `/api/v1/datasets`). The list is ordered newest-first.

```bash
curl 'https://example.org/api/v1/tags/<UUID>'
```

`404` if the tag UUID does not exist.

---

## `GET /api/v1/organizations`

Every organization (ministry, office, sub-unit) along with its dataset
count. `parent_id` lets you reconstruct the hierarchy.

```json
[
  {
    "id": "...",
    "name": "ministry-of-justice",
    "title": "משרד המשפטים",
    "description": null,
    "dataset_count": 24,
    "parent_id": null,
    "url": "https://example.org/api/v1/organizations/..."
  }
]
```

To fetch the datasets of an org, use:

```
GET /api/v1/datasets?organization_id=<id>
```

---

## `GET /api/v1/organizations/{id}`

Single organization. Same shape as one entry from the list.

---

## CSV column conventions for scraped datasets

Datasets sourced from a scraper (`source_type == "scraper"`, e.g. the
gov.il DynamicCollector tracker) follow a small set of reserved column
names in their CSV resources. These are added by the scraper after
attachments are downloaded so that consumers can join a row to its
PDF inside the dataset ZIP without falling back to positional
ordering.

| Column | Type | Description |
|---|---|---|
| `attachment_filename` | string | Exact basename of the PDF file inside the ZIP that belongs to this row. Empty string when the row has no attachment. Multiple attachments are joined with `"; "` (e.g. `"a.pdf; b.pdf"`). |
| `attachment_url` | string | Direct download URL on gov.il for the same file(s). Same `"; "` join convention. |

**Filename guarantees.**

- The value is the **post-dedup basename** as it appears inside the
  ZIP, not the original filename on gov.il. Name collisions are
  resolved with `_1`, `_2`, … suffixes (`foo.pdf`, `foo_1.pdf`).
- For every non-empty `attachment_filename`, a file with exactly that
  basename exists inside the uploaded ZIP under
  `<base_name>/attachments/<basename>`.
- Rows without an attachment have `attachment_filename = ""`.

**How to use.** Resolve a row's PDF by reading the column directly —
no positional fallback needed:

```python
import zipfile

def open_pdf_for_row(zip_path: str, row: dict) -> bytes | None:
    name = row.get("attachment_filename", "")
    if not name:
        return None
    first = name.split("; ", 1)[0]
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if info.filename.endswith("/attachments/" + first):
                return zf.read(info)
    return None
```

**Legacy columns to ignore.** Older snapshots of some scraped
datasets carried columns like `Data.file`, `Data.Document`, or
`Data.attachments` whose value was a literal placeholder string
(`"[1 קבצים]"`). Those columns are no longer produced. New consumers
should rely solely on `attachment_filename` / `attachment_url`.

---

## Row archive: `/api/append/{id}/…` and row validity

Datasets archived row by row (one SQL table per dataset) are read through
`/api/append/{id}/schema`, `/rows`, `/datastore_search`, `/download.csv`,
`/item` and the read-only SQL endpoints. Every row carries two timestamps:

| Column | Meaning |
|---|---|
| `first_seen` | When OVER first stored this exact row state. |
| `last_seen` | `null` while the row is in force. Set to the poll's start time when a **full** pass of the source no longer returned it. A row that comes back identical later is stored again as a new row with its own `first_seen`, so a gap stays visible. |

A row is in force at moment X when `first_seen <= X AND (last_seen IS NULL OR last_seen > X)`.

Only full passes close rows: a resumed scan, a sampled run, one batch of a
multi-batch run, an incremental archive's delta and the historical backfill
never do. A source that publishes only changes (the price-transparency feeds)
opts out, and its rows stay open. A pass that would close more than half of a
table's open rows is refused and logged.

### Parameters (on `/rows`, `/datastore_search`, `/download.csv`)

| Param | Meaning |
|---|---|
| `as_of` | ISO date (`2026-10-01`, read as the **end** of that day, UTC) or timestamp (`2026-10-01T08:30:00+03:00`; no zone = UTC). Returns the rows in force at that moment. |
| `current=true` | Only the rows in force now (`last_seen IS NULL`). |

Neither param: the whole history, exactly as before. Both at once, or an
`as_of` that does not parse: `400`. With either param, `last_seen` is added to
the returned columns; without, it is left out of `columns` (it is still a
normal column in the SQL console). `as_of` / `current` combine with `latest`,
`q`, the per-column filters and `table`; `latest=true&as_of=…` is "the newest
sample of each item among the rows in force then".

`/schema` advertises the feature: `"last_seen_column": "last_seen"` and
`"supports_as_of": true`. The MCP tool `query_dataset_rows` takes the same
`as_of` / `current` arguments.

```bash
# The register as it stood at the end of 1 October 2026
curl "https://over.org.il/api/append/<id>/rows?as_of=2026-10-01&limit=50"
# Only what is in force now, as CSV
curl -o now.csv "https://over.org.il/api/append/<id>/download.csv?current=true"
```

A table OVER has not polled since this shipped has no `last_seen` yet: every
row in it counts as in force, so `current=true` returns all of it and `as_of`
filters on `first_seen` alone.

### Closed history as Parquet: `/api/append/{id}/history/files`

Rows that were closed (`last_seen` set) in a month that has ended are exported
to Parquet files on R2, one closing month per file, split into numbered parts of
up to 500,000 rows:

```
history/<first 8 of the dataset id>/<table>/closed_month=YYYY-MM/part-NNNN.parquet
```

A file is never rewritten; rows closed in that month later arrive as a new part.
Columns: every source column as a string, `first_seen` and `last_seen` as
`timestamp[us, UTC]`, and `row_hash` (the row identity, hex). PostGIS `geom` is
not included; `geometry_wkt`, where the source has it, is. Only tables with at
least 100,000 closed rows are exported.

By default the rows stay in the SQL table too. A dataset whose admin set a
**history window** (`history_window_days`, at least 30) has its rows closed
before that window deleted from SQL, only after the file was read back from R2
and matched against the table. Those rows are then only in the files: until the
query layer reads them (a later phase), `/rows`, `as_of` and `/sql` do not see
them. Check `rows_only_in_r2`.

```json
{
  "dataset_id": "…", "dataset_title": "…", "table": null,
  "parts": 3, "rows_in_r2": 1012345, "rows_only_in_r2": 512345,
  "from": "2026-07-01T00:12:00+00:00", "to": "2026-09-30T23:45:00+00:00",
  "files": [
    {"table": "append_…", "closed_month": "2026-07", "part": 1,
     "public_url": "https://…/history/…/part-0001.parquet",
     "rows": 500000, "bytes": 18234567, "content_hash": "…",
     "first_seen_min": "…", "first_seen_max": "…",
     "last_seen_min": "…", "last_seen_max": "…",
     "deleted_from_sql": true, "missing": false}
  ]
}
```

`?table=` narrows to one table of a multi-resource dataset. `content_hash` is
the md5 of the file's `row_hash` values sorted and joined with commas, so a
download can be checked. `missing: true` means the weekly audit did not find
the object in the bucket. `404` for a dataset with no row archive.

`/schema` carries the same totals for its table as a `history` block:
`{parts, rows_in_r2, rows_only_in_r2, from, to, files_url}` (`null` when the
catalog cannot be read).

```bash
# Every closed row of one month, straight from the file, with DuckDB
duckdb -c "SELECT count(*) FROM read_parquet('https://…/closed_month=2026-07/part-0001.parquet')"
```

---

## Recipes

### "Give me datasets tagged X **and** Y"

```bash
curl 'https://example.org/api/v1/datasets?tag=enforcement&tag=guidelines'
```

The response's `total` field tells you how many matched; `items` holds
the page. Pass `limit` and `offset` for the rest.

### "All version download URLs for a dataset"

```bash
curl 'https://example.org/api/v1/datasets/<id>/versions' \
  | jq '[.[].resources[].download_url]'
```

### "Download every file of the latest version"

```bash
curl 'https://example.org/api/v1/datasets/<id>/versions/latest' \
  | jq -r '.resources[].download_url' \
  | xargs -n1 curl -LO
```

### "Fetch one specific version by number"

```bash
curl 'https://example.org/api/v1/datasets/<id>/versions/3'
```

### "All datasets in one organization that carry a specific tag"

```bash
curl 'https://example.org/api/v1/datasets?organization_id=<ORG>&tag_id=<TAG>'
```

### "Resolve a tag name to its UUID"

```bash
curl 'https://example.org/api/v1/tags' | jq '.[] | select(.name == "תחבורה")'
```

### "Walk a tag from URL"

Every tag and organization in a dataset response includes a `url` field.
Follow it directly — no need to construct anything client-side:

```bash
curl 'https://example.org/api/v1/datasets?limit=1' \
  | jq -r '.items[0].tags[0].url' \
  | xargs curl
```

---

## Errors

Standard HTTP status codes; the body is always
`{"detail": "<message>"}`.

| Status | When |
|---|---|
| `400` | Malformed UUID in path or query. |
| `404` | UUID is well-formed but no such row. |
| `422` | Query param failed validation (e.g. `limit=0`). |
| `5xx` | Bug or upstream outage. Retry with backoff. |

The list endpoints never return `404` — an empty `items: []` is the
correct way to say "no matches".

---

## Rate limits

The public read endpoints under `/api/v1` are not currently
rate-limited, but heavy automated use should set a reasonable
`User-Agent` and back off on `5xx`. If you need a bulk export, please
open an issue first.

---

## Versioning policy

- Adding fields to existing responses is **not** a breaking change.
  Treat unknown fields as forward-compatible.
- Removing or renaming fields, changing types, or changing endpoint
  semantics are breaking changes and will land under a new prefix
  (`/api/v2`, …) with both versions running in parallel during a
  deprecation window.
- The unversioned `/api/...` endpoints (used by the SPA) are not part
  of this contract — do not rely on them.
