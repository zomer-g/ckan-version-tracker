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
          "description": "הנחיה בנושא..."
        },
        {
          "title": "הנחיה מס' 1.0002",
          "date": "2026-02-20",
          "category": "משפט אזרחי",
          "description": "הנחיה בנושא..."
        }
      ],
      "fields": [
        {"id": "title", "type": "text"},
        {"id": "date", "type": "text"},
        {"id": "category", "type": "text"},
        {"id": "description", "type": "text"}
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
| `name` | string | **Yes** | Filename |
| `url` | string | **Yes** | Direct download URL on gov.il |
| `size` | integer | No | File size in bytes |

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
            # records = result.items  # list of dicts
            # fields = [{"id": col, "type": "text"} for col in result.columns]
            # attachments = [{"name": f.filename, "url": f.url, "size": f.size} for f in result.files]

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
| 400 | Bad request (invalid JSON, missing fields) |
| 401 | Missing Authorization header |
| 403 | Invalid API key |
| 404 | Task or dataset not found |
| 429 | Rate limit exceeded |
| 500 | Server error |

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
