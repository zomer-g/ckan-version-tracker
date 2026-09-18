# גרסאות לעם — Worker API Documentation

## Overview

The **גרסאות לעם** (Versions for the People) platform at `https://over.org.il` tracks changes to Israeli government datasets. It supports two data source types:

1. **CKAN** — polls data.gov.il automatically
2. **Scraper** — receives data from an external worker (you)

This document describes the API that the **govil-scraper worker** must implement to integrate with the version tracker.

---

## Architecture

```
┌────────────────────┐         ┌──────────────────────┐
│  גרסאות לעם        │  1. poll│  govil-scraper       │
│  over.org.il       │ <────── │  (your worker)       │
│                    │         │                      │
│  Creates tasks     │  2. push│  Scrapes gov.il      │
│  Stores versions   │ <────── │  Returns data        │
│  Pushes to ODATA   │         │                      │
└────────────────────┘         └──────────────────────┘
```

**Flow:**
1. Admin adds a scraper-type dataset (e.g., a gov.il collector URL)
2. The scheduler creates a **ScrapeTask** with status `pending`
3. Your worker **polls** for tasks → receives the task with the gov.il URL
4. Your worker **scrapes** the data from gov.il
5. Your worker **pushes** the results back to the version tracker
6. The version tracker creates a version and pushes data to odata.org.il

---

## Authentication

All API calls require a **Bearer token** in the `Authorization` header:

```
Authorization: Bearer YOUR_WORKER_API_KEY
```

The API key is shared between the version tracker and your worker. Contact the admin to get your key.

---

## Base URL

```
https://over.org.il/api/worker
```

---

## Endpoints

### 1. Poll for Task

Get the next pending scrape task. Call this on a loop (e.g., every 30 seconds).

```
GET /api/worker/poll
Authorization: Bearer {API_KEY}
```

**Query parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `only_sources` | comma-separated string | No | Source keys this worker can actually run. Omit to accept a task from any source. |

#### Restricting a poll to the sources you can run (`only_sources`)

By default the server hands you the highest-priority pending task from **any**
source. It does not know which engines your build has, and a task cannot be
handed back — `POST /api/worker/fail/{task_id}` (§4) marks it `failed` for good.
So a worker that receives work for a source it cannot run has only bad options:
report a failure on a dataset that was never broken, or scrape it with the wrong
engine.

`only_sources` is how you avoid that. Name the sources you have engines for and
the claim is restricted to them:

```
GET /api/worker/poll?only_sources=govmap,munidata,jda
Authorization: Bearer {API_KEY}
```

**The keys.** A source key is the name OVER already uses for an upstream site:

* for a source registered from a manifest, it is the manifest `id` — which is
  also the `scraper_config["kind"]` you dispatch your engine on, and the
  `<key>-scraper-` prefix of that source's dataset ids. If your engine table is
  keyed by `kind`, its keys are exactly the keys to send.
* for a built-in source, it is the `source_type`: `govmap`, `ckan`, `cbs`, …

There is no separate capability vocabulary to maintain — one name per source.

**Unknown keys are rejected.** A key the server does not recognise gets a `400`
naming it, together with the keys that *are* known:

```json
{
  "detail": "unknown source key(s) in only_sources: govmpa. Known keys: avodata, cbs, ckan, ..."
}
```

This is deliberate. A misspelled key matches no dataset, so ignoring it would
leave you polling a queue that merely *looks* permanently empty — indefinitely,
and indistinguishable from "no work for me right now". An empty value
(`?only_sources=`) is a `400` for the same reason: it can only mean a worker
that meant to name its engines and sent none, and both readings of it
("everything" / "nothing") are wrong without saying so. At most 64 keys.

Keys are matched **exactly** — not case-folded, since a key is also an engine
name. Whitespace around a key is trimmed and duplicates are ignored, so
`"govmap, munidata"` is fine.

**Notes:**

* **Omitting the parameter keeps the old behaviour exactly**: an unrestricted
  claim from any source. Nothing needs to change in an existing worker.
* This says nothing about *priority*. Within the sources you declare, the queue
  order is unchanged: highest priority band first, oldest first inside a band.
* It does not override a per-source worker cap. If a source you declared is
  already at its cap, you get nothing from it — the cap is the server's decision
  about how hard to push one upstream.
* Declaring a source with nothing pending answers `204`, which is normal. `204`
  now means "nothing for *you*" rather than "nothing at all".

**Response 200** — Task available:
```json
{
  "task_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "tracked_dataset_id": "11223344-5566-7788-99aa-bbccddeeff00",
  "source_url": "https://www.gov.il/he/departments/dynamiccollectors/menifa",
  "scraper_config": {
    "download_files": true,
    "max_pages": null
  },
  "callback_url": "/api/worker/push-version"
}
```

**Response 204** — No tasks available (empty body).

**Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `task_id` | UUID string | Unique task ID — use for progress/fail reports |
| `tracked_dataset_id` | UUID string | The dataset being tracked — pass back in push-version |
| `source_url` | string | The gov.il collector URL to scrape |
| `scraper_config` | object | Configuration for this scrape |
| `scraper_config.download_files` | boolean | Whether to download PDF/file attachments |
| `scraper_config.max_pages` | int or null | Max pages to scrape (null = all) |

---

### 2. Push Version (Main Endpoint)

After scraping, push the results here. This creates a new version in the tracker and pushes the data to odata.org.il.

```
POST /api/worker/push-version
Authorization: Bearer {API_KEY}
Content-Type: application/json
```

**Request body:**
```json
{
  "tracked_dataset_id": "11223344-5566-7788-99aa-bbccddeeff00",
  "metadata_modified": "2026-04-12T10:30:00Z",
  "resources": [
    {
      "name": "הנחיות היועץ המשפטי לממשלה",
      "format": "CSV",
      "records": [
        {
          "title": "הנחיה מס' 1.0001",
          "date": "2026-01-15",
          "category": "משפט פלילי",
          "description": "הנחיה בנושא...",
          "attachment_filename": "הנחיה_1.0001.pdf",
          "attachment_url": "https://www.gov.il/BlobFolder/generalpage/guidelines/he/1.0001.pdf"
        },
        {
          "title": "הנחיה מס' 1.0002",
          "date": "2026-02-20",
          "category": "משפט אזרחי",
          "description": "הנחיה בנושא...",
          "attachment_filename": "הנחיה_1.0002.pdf",
          "attachment_url": "https://www.gov.il/BlobFolder/generalpage/guidelines/he/1.0002.pdf"
        }
      ],
      "fields": [
        {"id": "title", "type": "text"},
        {"id": "date", "type": "text"},
        {"id": "category", "type": "text"},
        {"id": "description", "type": "text"},
        {"id": "attachment_filename", "type": "text"},
        {"id": "attachment_url", "type": "text"}
      ],
      "row_count": 450
    }
  ],
  "attachments": [
    {
      "name": "הנחיה_1.0001.pdf",
      "url": "https://www.gov.il/BlobFolder/generalpage/guidelines/he/1.0001.pdf",
      "size": 245000
    },
    {
      "name": "הנחיה_1.0002.pdf",
      "url": "https://www.gov.il/BlobFolder/generalpage/guidelines/he/1.0002.pdf",
      "size": 180000
    }
  ],
  "scrape_metadata": {
    "source_url": "https://www.gov.il/he/departments/dynamiccollectors/menifa",
    "scrape_duration_seconds": 45,
    "total_items": 450,
    "total_files": 12,
    "scraper_version": "1.0.0"
  }
}
```

**Request fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `tracked_dataset_id` | UUID string | **Yes** | From the poll response |
| `metadata_modified` | ISO 8601 string | **Yes** | Timestamp of this scrape. Use `datetime.now().isoformat()`. The system uses this to detect changes — if the same timestamp is sent twice, the second push is skipped. |
| `resources` | array | **Yes** | Tabular data resources (see below) |
| `attachments` | array | No | File attachments (PDFs, docs) — metadata only, not uploaded |
| `scrape_metadata` | object | No | Free-form metadata about the scrape (for debugging/auditing) |

**Resource object:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **Yes** | Human-readable name (Hebrew OK) |
| `format` | string | No | Default: "CSV". The format of the data. |
| `records` | array of objects | **Yes** | The actual data rows. Each object is a row with column→value pairs. |
| `fields` | array of objects | **Yes** | Column definitions. Each has `id` (column name) and `type` ("text", "integer", "numeric", "date"). |
| `row_count` | integer | **Yes** | Total number of records |

**Attachment object:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | **Yes** | Filename. **Must match exactly the basename used inside the ZIP and the value of `attachment_filename` in the corresponding CSV record** (after dedup, e.g. `foo_1.pdf` if a name collision was resolved). |
| `url` | string | **Yes** | Direct download URL on gov.il |
| `size` | integer | No | File size in bytes |

#### Linking CSV rows to PDF attachments

Every record in `records[]` may carry two reserved string fields that
describe its attachment(s):

| Column | Type | Description |
|---|---|---|
| `attachment_filename` | string | Exact basename of the PDF inside the ZIP. Empty string when the row has no attachment. Multiple attachments are joined with `"; "` (e.g. `"a.pdf; b.pdf"`). |
| `attachment_url` | string | Direct download URL on gov.il for the same file(s). Same `"; "` join convention. |

**Why these exist.** Consumers of a tracked dataset routinely need to
join a CSV row to its specific PDF inside the ZIP — for example to
diff PDF text across versions, to render a row's source document, or
to validate the data. Without a deterministic key, consumers fall
back to positional ordering (i-th row ↔ i-th file in
`zf.infolist()`), which silently drifts whenever the producer's
internal ordering differs from CSV `_id` ordering. `attachment_filename`
is that deterministic key.

**Producer guarantees.**

- The value of `attachment_filename` is the **post-dedup basename**
  inside the ZIP, not the original filename on gov.il. If two
  attachments would have produced the same filename, the second one
  is suffixed (`foo.pdf`, `foo_1.pdf`) and that suffixed name is what
  appears in the CSV.
- For every non-empty `attachment_filename`, a corresponding entry
  exists in the top-level `attachments[]` array with `name` equal to
  the basename, and a file with that exact basename exists inside the
  uploaded ZIP under `<base_name>/attachments/<basename>`.
- Rows with no PDF have `attachment_filename = ""`. Don't treat this
  as an error.

**Consumer recipe** (Python):

```python
import zipfile, csv, io

def open_pdf_for_row(zip_path: str, row: dict) -> bytes | None:
    name = row.get("attachment_filename", "")
    if not name:
        return None
    # Multiple files per row are "; "-joined.
    first = name.split("; ", 1)[0]
    with zipfile.ZipFile(zip_path) as zf:
        # Files live under <base>/attachments/ — find by suffix.
        for info in zf.infolist():
            if info.filename.endswith("/attachments/" + first):
                return zf.read(info)
    return None
```

**What you should NOT do.** The producer used to emit columns
called `Data.file`, `Data.Document`, `Data.attachments` whose value
was the literal string `"[1 קבצים]"` — a count, not a filename. Those
columns are no longer produced. If your consumer code reads them,
migrate to `attachment_filename` instead. If you fall back to
positional matching when `attachment_filename` is missing, log a
warning — for any new scrape produced by the current worker, the
column must be populated whenever attachments exist.

**Response 200:**
```json
{
  "version_id": "aaaabbbb-cccc-dddd-eeee-ffffffffffff",
  "version_number": 5,
  "odata_resource_ids": ["res-id-on-odata"],
  "message": "Version 5 created with 450 records"
}
```

**Response when no change:**
```json
{
  "message": "No change detected",
  "version_number": 4
}
```

**Important notes:**
- `metadata_modified` is used for deduplication. Use a new timestamp for each scrape. If the data hasn't changed, send the same timestamp — the system will skip it.
- `records` must be flat dictionaries — no nested objects.
- `fields` type values: `"text"` (default), `"integer"`, `"numeric"`, `"date"`, `"boolean"`.
- Maximum payload size: ~50MB (for very large datasets, paginate or summarize).

---

### 3. Report Progress

Report progress while scraping. Optional but recommended for long-running tasks.

```
POST /api/worker/progress/{task_id}
Authorization: Bearer {API_KEY}
Content-Type: application/json
```

**Request body:**
```json
{
  "phase": "scraping",
  "current": 120,
  "total": 450,
  "percentage": 27,
  "message": "Scraping page 6/19..."
}
```

| Field | Type | Description |
|-------|------|-------------|
| `phase` | string | Current phase: `"initializing"`, `"scraping"`, `"downloading_files"`, `"exporting"` |
| `current` | int | Current item number |
| `total` | int | Total expected items |
| `percentage` | int | 0-100 |
| `message` | string | Human-readable status message (Hebrew OK) |

**Response:** `{"status": "ok"}`

---

### 4. Report Failure

If the scrape fails, report it so the task isn't stuck as "running".

```
POST /api/worker/fail/{task_id}
Authorization: Bearer {API_KEY}
Content-Type: application/json
```

**Request body:**
```json
{
  "error": "Cloudflare blocked after 3 retries",
  "phase": "scraping"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `error` | string | **Required.** Error message. |
| `phase` | string | Which phase failed. |

**Response:** `{"status": "failed"}`

---

## Rate Limits

| Endpoint | Limit |
|----------|-------|
| `GET /poll` | 60/minute |
| `POST /push-version` | 30/minute |
| `POST /progress/{id}` | 120/minute |
| `POST /fail/{id}` | 30/minute |

---

## Worker Implementation Example (Python)

```python
import time
import requests
from datetime import datetime

SERVER = "https://over.org.il"
API_KEY = "your-worker-api-key"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}
POLL_INTERVAL = 30  # seconds


def poll():
    """Get next task from the server."""
    resp = requests.get(f"{SERVER}/api/worker/poll", headers=HEADERS)
    if resp.status_code == 204:
        return None  # No tasks
    resp.raise_for_status()
    return resp.json()


def report_progress(task_id, phase, current, total, message):
    """Report scraping progress."""
    requests.post(
        f"{SERVER}/api/worker/progress/{task_id}",
        json={
            "phase": phase,
            "current": current,
            "total": total,
            "percentage": int(current / total * 100) if total else 0,
            "message": message,
        },
        headers=HEADERS,
    )


def push_version(tracked_dataset_id, records, fields, attachments, source_url):
    """Push scraped data as a new version."""
    resp = requests.post(
        f"{SERVER}/api/worker/push-version",
        json={
            "tracked_dataset_id": tracked_dataset_id,
            "metadata_modified": datetime.now().isoformat(),
            "resources": [
                {
                    "name": "scraped_data",
                    "format": "CSV",
                    "records": records,
                    "fields": fields,
                    "row_count": len(records),
                }
            ],
            "attachments": attachments,
            "scrape_metadata": {
                "source_url": source_url,
                "scrape_duration_seconds": 0,
                "total_items": len(records),
                "total_files": len(attachments),
            },
        },
        headers=HEADERS,
    )
    resp.raise_for_status()
    return resp.json()


def report_failure(task_id, error, phase="scraping"):
    """Report task failure."""
    requests.post(
        f"{SERVER}/api/worker/fail/{task_id}",
        json={"error": error, "phase": phase},
        headers=HEADERS,
    )


def run_worker():
    """Main worker loop."""
    print("Worker started. Polling for tasks...")
    while True:
        task = poll()
        if not task:
            time.sleep(POLL_INTERVAL)
            continue

        task_id = task["task_id"]
        source_url = task["source_url"]
        config = task["scraper_config"]
        print(f"Got task {task_id}: scrape {source_url}")

        try:
            # ===== YOUR SCRAPING LOGIC HERE =====
            # Use your existing GovILScraper to scrape the URL
            # Convert the result to records + fields format
            
            # Example with your existing scraper:
            # from scraper_engine import GovILScraper
            # scraper = GovILScraper()
            # result = scraper.scrape(source_url, download_files=config.get("download_files", False))
            #
            # # Each item already carries `attachment_filename` and
            # # `attachment_url` — the canonical post-dedup keys for
            # # joining a row to its PDF inside the ZIP. They are
            # # populated by govscraper.io.attachments.inject_attachment_columns
            # # AFTER files are downloaded (so the dedup-suffix logic
            # # has run), so make sure your worker downloads the
            # # attachments BEFORE building the records payload.
            # records = result.items  # list of dicts (incl. attachment_filename / attachment_url)
            # fields = [{"id": col, "type": "text"} for col in result.columns]
            # attachments = [{"name": f.filename, "url": f.url, "size": f.size} for f in result.files]
            # # Invariant: for every record where attachment_filename != "",
            # # the same basename appears as `name` in `attachments`.

            records = [...]  # your scraped data
            fields = [...]   # column definitions
            attachments = [] # PDF/file metadata

            # Push to version tracker
            result = push_version(
                tracked_dataset_id=task["tracked_dataset_id"],
                records=records,
                fields=fields,
                attachments=attachments,
                source_url=source_url,
            )
            print(f"Version created: {result['message']}")

        except Exception as e:
            print(f"Task failed: {e}")
            report_failure(task_id, str(e))

        time.sleep(5)  # Brief pause before next poll


if __name__ == "__main__":
    run_worker()
```

---

## Field Type Reference

When defining `fields`, use these CKAN DataStore types:

| Type | Python equivalent | Example values |
|------|------------------|----------------|
| `text` | str | `"שלום"`, `"hello"` |
| `integer` | int | `42`, `0`, `-5` |
| `numeric` | float | `3.14`, `100.0` |
| `date` | str (YYYY-MM-DD) | `"2026-04-12"` |
| `boolean` | bool | `true`, `false` |
| `json` | dict/list | `{"nested": "data"}` |

**Default:** If unsure, use `"text"` for everything — it always works.

---

## Error Codes

| Status | Meaning |
|--------|---------|
| 200 | Success |
| 204 | No tasks available (for poll) |
| 400 | Bad request (invalid JSON, missing fields, unknown `only_sources` key) |
| 401 | Missing Authorization header |
| 403 | Invalid API key |
| 404 | Task or dataset not found |
| 409 | Push rejected — see below |
| 429 | Rate limit exceeded |
| 500 | Server error |

A `409` from `push-version` means the push was refused before anything was
written. Two cases:

- `staged_csv_missing` — a `neon-csv` reference whose staged file is gone
  (a restart or a deploy emptied `/tmp`). Upload it again, then push again.
- *"No running task for this dataset"* — the dataset has no task in `running`
  state. This is the stale-push guard: a version is built from whatever the
  worker sends, so a worker whose task was cancelled or reassigned could
  otherwise land a junk version (and, for an archive source, a poisoned
  checkpoint with it). There is at most one active task per dataset, so a
  legitimate in-flight push always satisfies it.

---

## Datasets published from outside the fleet

Not every source has a worker in the poll → scrape → push loop. Some are
produced elsewhere and handed to OVER directly with the worker key, one push
per run. For those the loop is not just unused but harmful: a task is queued on
every `poll_interval` for work nobody in the fleet can do, gets claimed by
whichever worker asks first, and comes back *"no engine for kind=…"*. And the
stale-push guard above can never be satisfied, because nothing ever moves a
task to `running` — so every push is rejected with `409`.

Such a dataset declares itself once, at registration, in its `scraper_config`:

```json
{"kind": "your_source_id", "push_mode": "external"}
```

With that set, OVER queues no scrape tasks for the dataset (a manual "דגום"
does nothing either — there is nobody to hand the work to), and `push-version`
skips the running-task precondition. Nothing else changes: uploads, versioning,
`csv_resource_ids`, append mode and NEON archiving all behave exactly as they
do for a fleet-scraped dataset.

The value is matched exactly — only the literal string `"external"` exempts a
dataset, so a typo leaves the normal guard in place rather than silently
disabling it.

---

## Testing

To test your integration:

1. Ask admin to create a scraper-type dataset
2. Poll for a task — you should receive it
3. Push a small test version (5-10 records)
4. Check on `over.org.il` that the version appears
5. Check on `odata.org.il` that the data is queryable

---

## Contact

- **Project:** https://github.com/zomer-g/ckan-version-tracker
- **Site:** https://over.org.il
- **Admin:** Gai Zomer
