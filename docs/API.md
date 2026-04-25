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
| GET | `/api/v1/datasets/{id}/versions` | Version history of a dataset |
| GET | `/api/v1/tags` | All tags + dataset counts |
| GET | `/api/v1/tags/{id}` | One tag + the datasets under it |
| GET | `/api/v1/organizations` | All organizations + dataset counts |
| GET | `/api/v1/organizations/{id}` | One organization |

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
        "odata_resource_id": "33...e1",
        "odata_resource_url": "https://www.odata.org.il/dataset/abcd1234-.../resource/33...e1",
        "download_url": "https://www.odata.org.il/dataset/abcd1234-.../resource/33...e1/download"
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
- `resources[]` lists the actual data files mirrored to ODATA. Use
  `download_url` to fetch the file directly.

`404` if the dataset UUID does not exist. An empty array means the
dataset has been registered but no versions have been detected yet.

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
