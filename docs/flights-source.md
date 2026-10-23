# מאגר הטיסות (flydata) ב־גרסאות לעם — תיעוד מלא של איסוף ובניית הנתונים

מסמך זה מתאר את **כל** מה שרץ בצד גרסאות לעם (OVER, over.org.il) כדי לאסוף,
לפרק ולבנות את ארכיון הטיסות: אילו פונקציות רצות, מה מפעיל אותן, מה מגדיר
אותן, איפה הנתונים נשמרים ואיך אפשר לקרוא אותם.

**כל הקוד נמצא בתוך ריפו OVER** (`ckan-version-tracker`). אין לזה שום חלק ב־
GOVSCRAPER (הוורקר החיצוני) ואין אף תוסף דפדפן מעורב — נבדק ואומת: אין בריפו
הוורקר שום התייחסות ל־flydata או לטיסות. הפולינג רץ בתהליך ה־FastAPI של OVER
עצמו, דרך APScheduler.

עדכני ל־2026-08-11.

---

## 1. המקור

| שדה | ערך |
|---|---|
| פורטל | data.gov.il (CKAN) |
| ארגון | `airport_authority` — רשות שדות התעופה |
| שם המאגר (`ckan_name`) | `flydata` |
| UUID של המאגר במקור | `3dfc6b2a-bc1d-4770-8d5f-457ce50a73b3` |
| Resource מנוטר | `e83f763b-b7d7-479e-b172-ae981ddc6de5` (יחיד, CSV, `datastore_active=true`) |
| כתובת מקור | https://data.gov.il/he/datasets/airport_authority/flydata/e83f763b-b7d7-479e-b172-ae981ddc6de5 |
| קצב רענון במקור | ~15 דקות |
| גודל בכל רגע נתון | ~2,000–2,200 שורות, 17 עמודות |

**האופי של המקור הוא לוח טיסות חי, לא ארכיון.** כל טיסה מופיעה על הלוח, משנה
מצב לאורך חייה (מתוכננת → מתעדכנת → נחתה) ואז נעלמת מהלוח כשהזמן שלה חולף.
כלומר: מי שקורא רק את המקור רואה חלון של יומיים; ההיסטוריה לא קיימת בשום מקום
אצל המפרסם. הארכיון של OVER הוא מה שיוצר את ההיסטוריה.

### עמודות המקור (נשמרות בשמן המקורי)

`CHOPER`, `CHFLTN`, `CHOPERD`, `CHSTOL`, `CHPTOL`, `CHAORD`, `CHLOC1`,
`CHLOC1D`, `CHLOC1TH`, `CHLOC1T`, `CHLOC1CH`, `CHLOCCT`, `CHTERM`, `CHCINT`,
`CHCKZN`, `CHRMINE`, `CHRMINH`

(שמות השדות הם של רשות שדות התעופה; OVER לא משנה, לא ממפה ולא מתרגם אותם.)
הזהות האמיתית של טיסה היא צירוף `CHOPER|CHFLTN|CHSTOL|CHAORD` — אין עמודה
מזהה יחידה. `CHSTOL` הוא datetime מלא (כולל תאריך), `CHAORD` הוא A/D
(נחיתה/המראה). טיסות code-share חולקות את אותו `CHSTOL`.

### אילוץ טכני מכריע: הורדת הקובץ חסומה

הורדת הקובץ הישירה
`e.data.gov.il/…/download/datafile.csv` מוגנת מאחורי Google-OAuth/IAP — היא
מחזירה 302 ל־HTML של התחברות, לא CSV. **לכן מסלול "הורד קובץ ופרסר" בלתי
אפשרי כאן**, וכל האיסוף עובר דרך ה־API של ה־datastore
(`/api/3/action/datastore_search`) שאינו חסום. זה הנימוק המרכזי לארכיטקטורה
שמתוארת בהמשך.

---

## 2. מה בעצם נשמר — הסמנטיקה

הארכיון הוא **היסטוריית מצבים מלאה**, לא event log ולא snapshot יומי.

* הזהות של שורה = **hash של כל השורה** (`row_hash`, SHA-256 מעל כל 17 ערכי
  העמודות). אין `append_key`.
* משמעות: כל **מצב שונה** של אותה טיסה נשמר כשורה נפרדת. טיסה שהתעדכנה
  (למשל שינוי דלפק, שינוי שער, זמן בפועל, סטטוס) תופיע כמה פעמים — פעם לכל
  ניסוח שונה של השורה. מצב שחוזר על עצמו בין דגימות **לא** נשמר שוב.
* לכל שורה נוספת עמודת `first_seen` — הרגע שבו OVER ראה את המצב הזה לראשונה.
  זו למעשה חותמת הזמן של האירוע.
* אין מחיקות: טיסה שירדה מהלוח נשארת בארכיון לנצח.

זו הייתה החלטה מפורשת (2026-06-26) — לתעד היסטוריית סטטוס מלאה ולא רק "אילו
טיסות היו".

---

## 3. הזרימה מקצה לקצה — מה רץ, לפי הסדר

```
APScheduler (כל 900 שניות)
  └─ poll_dataset(dataset_id)                     app/worker/poll_job.py
       ├─ conditional_archiver.try_conditional_archive()   → FALLBACK (מדלג)
       ├─ ckan_client.package_show("flydata")      app/services/ckan_client.py
       ├─ ckan_client.datastore_info(resource_id)
       ├─ ניתוב → _poll_large_dataset()            app/worker/poll_job.py:1260
       │    └─ delta_archiver.archive_via_datastore_streaming()
       │         └─ delta_archiver._archive_streaming_to_db()   ← המסלול בפועל
       │              ├─ append_store.ensure_table()
       │              ├─ _stream_datastore_pages()  ← datastore_search בדפים
       │              ├─ append_store.append_rows() ← INSERT … ON CONFLICT
       │              ├─ העלאת snapshot CSV מלא ל־R2
       │              └─ יצירת VersionIndex
       └─ commit
```

### 3.1 מה מפעיל את הפולינג

`app/worker/scheduler.py`

* `init_scheduler()` — נטען בעליית האפליקציה, טוען כל TrackedDataset פעיל
  ורושם לו job.
* `add_poll_job(dataset_id, interval_seconds, last_polled_at)` — רושם
  `IntervalTrigger(seconds=900)` עם `id="poll_<dataset_id>"`,
  `max_instances=1`, `misfire_grace_time=60`.
  * הטריגר מעוגן ל־`last_polled_at + interval` ולא ל־"now + interval". זה
    קריטי על Render: כל דיפלוי מריץ מחדש את `init_scheduler`, ו־
    `IntervalTrigger` רגיל היה מאפס את הספירה. למאגרים בקצב שבועי זה היה אומר
    "לעולם לא לרוץ". במקרה של הטיסות (15 דק') זה פחות קריטי אך אותו מנגנון.
* הפעלה ידנית: `POST /api/v1/datasets/{id}/poll` (כפתור "דגום" בממשק הניהול)
  קורא לאותה `poll_dataset` עם `force=True`.

### 3.2 מגבלות בטיחות סביב הפולינג

`app/worker/poll_job.py`

* `_active_polls: set[str]` — נעילת single-flight לכל מאגר. אף פעם לא שני
  סריקות במקביל של אותו מאגר.
* `_POLL_SEMAPHORE = asyncio.Semaphore(settings.poll_max_concurrency)` —
  ברירת מחדל 2. תקרה גלובלית על מספר הפולים הכבדים המקבילים, כדי לא להפיל את
  ה־dyno של 512MB.

### 3.3 `conditional_archiver` — למה הוא מדלג

`app/services/conditional_archiver.py:_try()` — הארכיון "הזול" שרץ ראשון
ומנסה להימנע מהורדה מלאה. עבור מאגר הטיסות הוא יוצא מיד ב־`Result.FALLBACK`
בסעיף 0b:

```python
from app.services.storage_client import dataset_archives_neon
if dataset_archives_neon(ds):
    return Result.FALLBACK
```

הסיבה: השורות של המאגר חיות ב־append DB, שהארכיון המותנה לא יודע עליו כלום.
`NO_CHANGE` כאן היה מונע לנצח את הזרמת השורות ל־NEON.

### 3.4 הניתוב בתוך `poll_dataset`

`app/worker/poll_job.py:384-428` — אחרי שנבחר ה־resource היחיד המנוטר:

```python
ds_info = await ckan_client.datastore_info(target_rid)   # total + fields
appendonly_datastore = (ds.storage_mode == "append_only"
                        and ds_info is not None
                        and bool(resource_to_check.get("datastore_active")))
neon_datastore = (dataset_archives_neon(ds)
                  and ds_info is not None
                  and bool(resource_to_check.get("datastore_active")))
if ds_info is not None and (total_rows >= settings.large_dataset_threshold
                            or appendonly_datastore or neon_datastore):
    blocked_resources.remember(ds, [])
    await _poll_large_dataset(...)
    return
```

מאגר הטיסות נכנס דרך **`neon_datastore`**: הוא קטן (~2k שורות, הרבה מתחת
לסף 50,000) ו־`storage_mode` שלו הוא `full_snapshot` — מה שמנתב אותו הוא
תוכנית האחסון (`archive_neon`), לא הגודל ולא `append_only`.

`blocked_resources.remember(ds, [])` — מתעד ש"לא נחסמו קבצים", אחרת ההיוריסטיקה
"מעולם לא נבדק" הייתה כופה פוליטה יקרה בכל טיק לנצח.

### 3.5 המסלול הפעיל — `_archive_streaming_to_db`

`app/services/delta_archiver.py:157`

זו הפונקציה שעושה את העבודה בפועל בכל דגימה:

1. **`append_key = scraper_config.get("append_key")`** → `None` → `keyless=True`.
2. **`table = append_store.table_name(ds)`** → `append_flydata_<8 תווים מה־UUID>`.
3. **`append_store.ensure_table(table, source_cols, key_col=None, keyless=True)`**
   * יוצר את הטבלה אם אין (כל העמודות `text`, פלוס
     `first_seen timestamptz NOT NULL DEFAULT now()` ופלוס `row_hash text`).
   * מטפל ב־schema drift: עמודה חדשה במקור מתווספת ב־`ALTER TABLE … ADD COLUMN
     IF NOT EXISTS`.
   * מוודא `CREATE UNIQUE INDEX IF NOT EXISTS … (row_hash)` — **זה** מנגנון
     ה־dedup.
4. **`start_offset = scraper_config.get("seed_offset") or 0`** — נקודת המשך אם
   ריצה קודמת נהרגה באמצע.
5. **`_stream_datastore_pages(rid, start_offset)`** — גנרטור אסינכרוני שקורא
   `GET {data_gov_il_url}/api/3/action/datastore_search?resource_id=…&limit=…&offset=…`
   דף אחרי דף, מסיר את עמודת `_id` הסינתטית, ומחזיר `(next_offset, page)`.
   עוצר כשדף חוזר ריק.
   * `_fetch_datastore_page()` — retry עם backoff אקספוננציאלי מוגבל:
     `PAGE_MAX_RETRIES = 6`, `PAGE_TIMEOUT_SECONDS = 120.0`, backoff
     `min(30, 2**attempt)`. 4xx לא מנוסה מחדש (לא יסתדר מעצמו); timeout /
     transport / 5xx כן.
   * במקרה של הטיסות: ~2,200 שורות בגודל דף 10,000 ⇒ **דף אחד בלבד** לכל
     דגימה (`pages_processed: 1` ב־change_summary).
6. **`pending.extend(batch)`** ואז flush כל `PENDING_FLUSH_THRESHOLD = 5000`
   שורות (ובסוף הסריקה). ה־flush קורא ל־
   **`append_store.append_rows(table, source_cols, pending, key_col=None, keyless=True)`**:
   * `chunk_size_for(num_cols, keyless)` — מחשב כמה שורות ל־INSERT כך שמספר
     ה־bind params יישאר מתחת ל־`_MAX_PARAMS = 30000`.
   * `build_insert(...)` — בונה `INSERT … VALUES (…),(…) ON CONFLICT ("row_hash")
     DO NOTHING`, כולל dedup בתוך אותו chunk (כדי ששאילתה אחת לא תישא פעמיים
     את אותו hash — Postgres היה נופל על זה).
   * `first_seen` נכתב כ־`now()` inline — כלומר רק שורות שבאמת נכנסו מקבלות
     חותמת; שורה שכבר קיימת שומרת את ה־`first_seen` המקורי שלה. זו בדיוק
     הסמנטיקה של "מתי ראינו את זה לראשונה".
   * מספר השורות שנכנסו נקרא מתוך תג הפקודה (`"INSERT 0 42"`).
7. **checkpoint** — אחרי כל flush מוצלח נשמר `scraper_config.seed_offset`
   ב־DB של OVER, ורק אחרי שהשורות כבר נמצאות ב־NEON. הריגה באמצע ממשיכה
   מהנקודה הזו (ובכל מקרה ON CONFLICT הופך דף חוזר ל־no-op זול).
8. **dual-write ל־R2** — במקביל להזרמה, כל שורה נכתבת ל־CSV זמני על הדיסק
   (`utf-8-sig`, DictWriter), ובסוף הסריקה הקובץ מועלה ל־R2 תחת מפתח
   `datasets/<dataset_id>/v<version>/<rand8>_<filename>.csv`
   (`storage_client.build_key`). זהו snapshot **מלא** של הלוח באותו רגע — לא
   רק השורות החדשות. פועל רק בסריקה מלאה (`start_offset == 0`).
   * כישלון בהעלאה ל־R2 **לא** מבטל את השורות שכבר נכנסו ל־NEON — הוא רק
     נרשם בלוג והגרסה נשמרת בלי קובץ.
9. **ניקוי ה־checkpoint** — `seed_offset` נמחק מ־`scraper_config` בסיום סריקה
   מלאה, כדי שהדגימה הבאה תתחיל מחדש מאפס.
10. **`VersionIndex`** — נרשמת גרסה חדשה עם:
    ```json
    {"type": "append_db", "rows_added": 47, "rows_total": 139694,
     "key": "_hash", "pages_processed": 1, "start_offset": 0,
     "r2_snapshot": true, "resources_added": [], "resources_removed": [],
     "resources_modified": []}
    ```
    ו־`resource_mappings = {"_resource_ids": [rid], "append_table": "<table>",
    "<rid>": "r2:<key>", "_names": {...}}`.
11. `ds.last_polled_at`, `ds.last_modified`, `ds.last_error = None` ואז commit.

### 3.6 מסלול ה־seen-set הישן (לא בשימוש היום — לתיעוד היסטורי)

`archive_via_datastore_streaming` פותחת ב־

```python
if append_store.is_configured():
    return await _archive_streaming_to_db(...)
```

כל מה שאחרי זה בפונקציה — `compute_new_rows_windowed`, `_appendonly_seen_gen`,
`seen_window_versions=192`, הכתיבה ל־ODATA datastore — הוא **fallback לעולם שבו
אין `APPEND_DATABASE_URL`**. בפרודקשן יש append DB, ולכן הענף הזה לא רץ.
הוא נשאר בקוד כי הוא המסלול היחיד כשאין Postgres ייעודי, והוא מתועד כאן כדי
שלא יטעה מי שקורא את הקוד: **החלון (window) של 192 גרסאות כבר לא רלוונטי** —
ה־dedup הוא UNIQUE index במסד, לא רשימה שנסחבת ב־JSONB.

(הרקע: הרשימה `_appendonly_seen` נשמרה ב־`resource_mappings` והייתה נכתבת
מחדש בכל דגימה. ב־96 דגימות ביום זה write amplification ריבועי. המעבר ל־NEON
ביטל את הבעיה מהשורש.)

---

## 4. הקונפיגורציה — מה מגדיר את ההתנהגות

### 4.1 קונפיגורציה ברמת המאגר (עמודות `tracked_datasets` ב־DB של OVER)

| שדה | ערך בפועל | מה הוא עושה |
|---|---|---|
| `ckan_id` / `ckan_name` | `flydata` | מה נשלח ל־`package_show` |
| `resource_ids` | `["e83f763b-b7d7-479e-b172-ae981ddc6de5"]` | ה־resource היחיד המנוטר; קובע שהניתוב "single resource" נתפס |
| `poll_interval` | `900` (שניות) | תדירות ה־APScheduler job |
| `status` / `is_active` | `active` / `true` | נרשם ב־`init_scheduler` |
| `storage_mode` | `full_snapshot` | **לא** `append_only` — הניתוב מגיע מתוכנית האחסון |
| `odata_dataset_id` | `e493970e-…` | מראה ODATA היסטורי; לא בשימוש במסלול הנוכחי |
| `scraper_config.storage_backend` | `"r2"` | יעד קובצי ה־snapshot |
| `scraper_config.archive_neon` | `true` | מה שמפעיל את `dataset_archives_neon()` ואת כל מסלול ההזרמה |
| `scraper_config.append_key` | **לא מוגדר** | היעדרו = dedup לפי `row_hash` (היסטוריית מצבים מלאה) |
| `scraper_config.seed_offset` | זמני | checkpoint להמשכיות; נמחק בסיום סריקה |
| `scraper_config.ckan_dataset_uuid` | `3dfc6b2a-…` | נלכד אוטומטית בכל פוליטה, מאפשר resolve הפוך מ־UUID של data.gov.il |

הצירוף `storage_backend="r2"` + `archive_neon=true` הוא תוכנית האחסון
**`r2+neon`** בבורר של ממשק הניהול: קובץ CSV מלא לכל גרסה ב־R2 **וגם** טבלה
שאילתתית ב־NEON.

הפונקציות שקוראות את זה — `app/services/storage_client.py`:

```python
def dataset_storage_target(ds) -> str:      # 'odata' | 'r2' | 'local' | 'neon'
    sc = getattr(ds, "scraper_config", None) or {}
    if sc.get("upload_mode") == "local_only": return "local"
    return sc.get("storage_backend") or settings.storage_backend

def dataset_archives_neon(ds) -> bool:
    sc = getattr(ds, "scraper_config", None) or {}
    return sc.get("storage_backend") == "neon" or bool(sc.get("archive_neon"))

def dataset_stores_files(ds) -> bool:
    return dataset_storage_target(ds) != "neon"
```

### 4.2 קונפיגורציה גלובלית (`app/config.py`, משתני סביבה)

| הגדרה | ברירת מחדל | תפקיד במסלול הטיסות |
|---|---|---|
| `data_gov_il_url` | `https://data.gov.il` | בסיס ה־CKAN API |
| `datastore_page_size` | `10000` | שורות לכל `datastore_search` — הצריכה הדומיננטית של RAM |
| `large_dataset_threshold` | `50000` | סף "מאגר גדול". הטיסות **מתחת** לסף — הן מנותבות בכל זאת דרך `archive_neon` |
| `append_database_url` | (סוד) | ה־Postgres (Neon) הייעודי. קיומו הוא מה שמפעיל את `_archive_streaming_to_db` |
| `append_readonly_database_url` | (סוד) | תפקיד SELECT-only לקונסולות ה־SQL הציבוריות |
| `storage_backend` | `"r2"` | ברירת מחדל גלובלית ליעד קבצים |
| `poll_max_concurrency` | `2` | תקרת פולים כבדים מקבילים |
| `min_poll_interval` | `300` | הרצפה שמתחתיה אי אפשר להגדיר `poll_interval` |
| `conditional_archive_enabled` | on | הארכיון המותנה — מדלג כאן (סעיף 3.3) |

### 4.3 קבועים בקוד

| קבוע | ערך | מיקום |
|---|---|---|
| `PENDING_FLUSH_THRESHOLD` | 5000 | `delta_archiver.py:65` |
| `PAGE_TIMEOUT_SECONDS` | 120.0 | `delta_archiver.py:72` |
| `PAGE_MAX_RETRIES` | 6 | `delta_archiver.py:73` |
| `_MAX_PARAMS` | 30000 | `append_store.py:57` |
| `_MAX_IDENT_BYTES` | 63 | `append_store.py:60` |

### 4.4 רישום ראשוני של המאגר

`app/services/append_registrar.py` — `APPEND_DATASET_SPECS` מחזיק spec
אידמפוטנטי לרישום המאגר (`poll_interval=900`, `seen_window_versions=192`).
מופעל דרך:

* CLI: `python scripts/register_append_datasets.py [--poll]`
* או `POST /api/admin/register-append-datasets` (מאומת, מממשק הניהול)

הערה: ה־spec הזה משקף את התכנון המקורי (`append_only` + חלון seen). המצב
בפועל בפרודקשן שונה — `full_snapshot` + `r2+neon`, ראו סעיף 4.1. הרישום הוא
נקודת ההתחלה, ותוכנית האחסון נערכה אחר כך מממשק הניהול.

---

## 5. איפה הנתונים יושבים

### 5.1 NEON (הארכיון השאילתתי)

Postgres ייעודי (`APPEND_DATABASE_URL`), schema `public`. שם הטבלה נגזר
דטרמיניסטית ב־`append_store.table_name(ds)`:

```python
base = re.sub(r"[^a-z0-9_]+", "_", ds.ckan_name.lower()).strip("_")
sid  = str(ds.id).replace("-", "")[:8]
return f"append_{base}"[:54] + f"_{sid}"
```

מצב חי (2026-08-11):

| טבלה | שורות | `first_seen` מוקדם ביותר | אחרון |
|---|---:|---|---|
| `append_flydata_31c812a6` | 139,694 | 2026-06-25 22:33 UTC | 2026-08-11 07:21 UTC |
| `append_flydata_2c9bf587` | 138,972 | 2026-06-26 07:54 UTC | 2026-08-11 07:22 UTC |

סכימת כל טבלה: 17 עמודות המקור (כולן `text`) + `first_seen timestamptz` +
`row_hash text` (מוסתרת מהקונסולות ומה־API — ראו `_HIDDEN_COLS`).
כל ערך נשמר כטקסט — עמידות ארכיונית על פני טיפוסים; הצרכן ממיר בשאילתה.

### 5.2 Cloudflare R2 (ה־snapshot לכל גרסה)

קובץ CSV אחד לכל גרסה, מכיל את **כל** הלוח באותו רגע (~2,200 שורות), לא רק
את החדשות. דוגמה מגרסה 6693:

```
https://pub-63c02556dabd4956af9500eb8fe7198c.r2.dev/datasets/31c812a6-9b0c-4f32-8317-e5f268c28f60/v6693/5f34448b_file.csv
```

(שם הקובץ יוצא `file.csv` כי שם ה־resource במקור עברי, ו־`_safe_filename`
מנקה תווים לא־ASCII מהמפתח, שהוא גם segment ב־URL ציבורי.)

### 5.3 ה־DB של OVER עצמו

`tracked_datasets` (ההגדרה) ו־`version_index` (שורה לכל דגימה — מספר גרסה,
`detected_at`, `metadata_modified`, `change_summary`, `resource_mappings`).
זהו ה־DB הראשי, **נפרד פיזית** מ־append DB (ראו `append-db-separation-guard`).

---

## 6. איך צורכים את זה

### 6.1 API ציבורי

`app/api/append.py`, prefix `/api/append`:

| endpoint | מה מחזיר |
|---|---|
| `GET /api/append/{dataset_id}/schema` | טבלה, סה"כ שורות, רשימת עמודות, מפתח |
| `GET /api/append/{dataset_id}/schema.txt` | DDL להעתקה ל־AI |
| `GET /api/append/{dataset_id}/rows?limit=&offset=&sort=&order=&q=` | שורות, עם סינון per-column |
| `GET /api/append/{dataset_id}/download.csv` | CSV מוזרם של הטבלה (המסוננת) |
| `POST /api/append/{dataset_id}/sql` | SELECT קריאה-בלבד על הטבלה |
| `GET /api/append/{dataset_id}/datastore_search` | תאימות CKAN־style |

דוגמה חיה:
```bash
curl "https://www.over.org.il/api/append/31c812a6-9b0c-4f32-8317-e5f268c28f60/schema"
```

היסטוריית הגרסאות והקבצים:
```bash
curl "https://www.over.org.il/api/v1/datasets/31c812a6-9b0c-4f32-8317-e5f268c28f60/versions?limit=5"
```

כל endpoint ציבורי מוגן ב־rate limit (`app/rate_limit.py`) ובתקציב בייטים
לכל IP (`api-data-budget`).

### 6.2 קונסולת ה־SQL המרכזית

`/data` באתר, ו־`POST /api/tables/sql` בקוד (`app/api/tables.py`). רץ על
ה־role של קריאה בלבד עם `search_path = public, knesset, idx, odata, ocal,
extensions` ו־statement timeout של 30 שניות. הוולידציה
(`append_store.validate_readonly_sql`) מתירה רק `SELECT`/`WITH`.

```sql
SELECT "CHOPER", "CHFLTN", "CHSTOL", "CHAORD", first_seen
FROM append_flydata_31c812a6
WHERE "CHAORD" = 'D'
ORDER BY first_seen DESC
LIMIT 100;
```

### 6.3 עמודי האתר

* https://www.over.org.il/versions/31c812a6-9b0c-4f32-8317-e5f268c28f60
* https://www.over.org.il/versions/2c9bf587-3652-464c-9e91-2a76303302c8

---

## 7. הקבצים שמרכיבים את הפיצ'ר

| קובץ | תפקיד |
|---|---|
| `app/worker/scheduler.py` | רישום ה־job כל 900 שניות (`init_scheduler`, `add_poll_job`) |
| `app/worker/poll_job.py` | `poll_dataset` — התזמור; `_poll_large_dataset` — הניתוב למסלול ההזרמה |
| `app/services/delta_archiver.py` | **הליבה** — `_archive_streaming_to_db`, `_stream_datastore_pages`, `_fetch_datastore_page`, `_fields_for_odata` |
| `app/services/append_store.py` | שכבת ה־Postgres — `table_name`, `ensure_table`, `build_insert`, `append_rows`, `row_hash`, `table_count`, וכל קריאת ה־API/SQL |
| `app/services/ckan_client.py` | `package_show`, `datastore_info`, ולידציית SSRF |
| `app/services/storage_client.py` | `dataset_archives_neon`, `dataset_storage_target`, `dataset_stores_files`, `build_key`, `mark`, העלאה ל־R2 |
| `app/services/conditional_archiver.py` | הארכיון המותנה — הפטור בסעיף 0b |
| `app/services/append_registrar.py` | ה־spec לרישום אידמפוטנטי |
| `scripts/register_append_datasets.py` | עטיפת CLI לרישום |
| `app/api/append.py` | ה־API הציבורי מעל הארכיון |
| `app/api/tables.py` | קונסולת ה־SQL של `/data` |
| `app/config.py` | כל ההגדרות הגלובליות |
| `app/models/tracked_dataset.py`, `app/models/version_index.py` | המודלים |

### בדיקות

| קובץ | מה מכוסה |
|---|---|
| `tests/test_append_store.py` | `row_hash` (order-independent, none-safe), `build_insert` keyless מכוון ל־`row_hash` ומבצע dedup בתוך chunk, `chunk_size_for` שומר על תקרת הפרמטרים, `table_name` יציב/ייחודי/בגבול, ולידציית SQL קריאה־בלבד |
| `tests/test_first_seen_stamp.py` | `first_seen` לא מפריע לזהות ה־hash ולא לזהות ה־keyed; התנהגות החלון (מסלול ה־fallback) |
| `tests/test_delta_multi_neon.py` | המסלול המרובה־resources (לא רלוונטי לטיסות, אותה תשתית) |

---

## 8. שני ממצאים תפעוליים שכדאי להכיר

### 8.1 שני מאגרים עוקבים אחרי אותו מקור במקביל

זה לא באג בקוד — זו כפילות ברישום. שתי רשומות `TrackedDataset` מצביעות על
אותו `resource_id` בדיוק:

| | `31c812a6-…` | `2c9bf587-…` |
|---|---|---|
| כותרת | "מאגר טיסות" | "מאגר טיסות — מאגר טיסות" |
| `ckan_id` | `3dfc6b2a-…` (UUID) | `flydata` (slug) |
| נרשם | ~2026-06-25 22:33 | ~2026-06-26 07:54 |
| גרסאות | 6,693 | 4,307 |
| שורות | 139,694 | 138,972 |

**איך זה קרה:** שני מסלולי הרישום בודקים "האם המאגר כבר קיים?" לפי `ckan_id`,
אבל ממלאים את השדה בשתי מוסכמות שונות — `auto_discovery._onboard` (וכן רישום
ידני שבו הודבק ה־UUID) שם את **ה־UUID** של החבילה, ו־`append_registrar` שם את
**ה־slug**. הרשומה הראשונה נוצרה תחת ה־UUID; למחרת רץ ה־registrar, חיפש
`ckan_id == "flydata"`, לא מצא — ויצר שנייה. `package_show` של CKAN מקבל גם
UUID וגם slug, ולכן שתיהן דוגמות בהצלחה מאז ושום דבר לא התלונן.

**העלות:** פי 2 קריאות ל־data.gov.il (192 במקום 96 ליום), פי 2 אחסון NEON,
ופי 2 אובייקטים ב־R2 (~5,800 קבצים בחודש במקום ~2,900).

**אבל הארכיונים אינם עותקים זהים.** מדידת החפיפה בפועל (2026-08-11):

| | סה"כ | רק ב־A | רק ב־B |
|---|---:|---:|---:|
| כל התקופה | A 139,757 / B 139,035 | 1,266 | 544 |
| 7 ימים אחרונים | ~24,150 כל אחת | 90 | 116 |

הפער הכולל מוסבר ברובו ביתרון של ~9.4 שעות שהיה ל־A. אבל בשבוע האחרון הפער
**סימטרי וממשיך**: השתיים דוגמות בהפרש של כדקה, הלוח משתנה בין הדגימות, וכל
אחת תופסת ~100 מצבים שהשנייה החמיצה.

המסקנה החשובה: הכפילות מתפקדת בטעות כניסוי ביקורת, והיא מודדת **אובדן דגימה**.
אם שתי דגימות בהפרש של דקה כבר מפספסות ~0.4% אחת מהשנייה, דוגם יחיד בהכרח
מפספס מצבים בין הטיקים של 15 הדקות שלו — והפער האמיתי גדול בהרבה, כי הפרש של
דקה מכסה רק פרוסה צרה מהחלון. **אף אחד משני הארכיונים אינו שלם, והאיחוד שלהם
שלם יותר מכל אחד לחוד.**

**החלטה (2026-08-11): משאירים את המצב כמו שהוא.** אין למחוק אף אחת מהשתיים —
מחיקה תאבד 544 מצבים (או 1,266). מי שיבחר בכל זאת לאחד בעתיד חייב למזג קודם
בשאילתה שמשמרת את `first_seen` המקורי (`INSERT … SELECT … ON CONFLICT
(row_hash) DO NOTHING`, ולא `now()`), ו־`31c812a6` היא המועמדת הטבעית לשרוד.

### 8.2 למה בכלל נוצרת גרסה כל 15 דקות

`storage_mode` הוא `full_snapshot`, כלומר `is_append=False`, כלומר קיצורי
הדרך של "המטא-דאטה לא זזה" **כן** חלים. הם לא מפילים את הפולינג רק מפני
ש־data.gov.il באמת מעדכן את `metadata_modified` בכל רענון של הלוח (למשל
`2026-08-11T07:16:15` לגרסה 6693). אם המקור יפסיק לעדכן את השדה הזה, המאגר
יפסיק לייצר גרסאות — למרות ש־`archive_neon` מוגדר. זו נקודת שבירה שכדאי
לזכור: העברת המאגר ל־`storage_mode="append_only"` תנתק את התלות הזו.

---

## 9. תקציר בפסקה אחת (למי שמבקש את "מה עושים")

כל 15 דקות APScheduler מפעיל את `poll_dataset` על מאגר הטיסות. הפוליטה
קוראת את המטא-דאטה מ־data.gov.il, מזהה שהמאגר מוגדר לארכיון NEON, ומנתבת
ל־`delta_archiver._archive_streaming_to_db`. שם היא קוראת את כל לוח הטיסות
דרך `datastore_search` (לא דרך הורדת הקובץ — היא חסומה מאחורי OAuth),
מחשבת SHA-256 של כל שורה שלמה, ומכניסה ל־Postgres ייעודי ב־
`INSERT … ON CONFLICT (row_hash) DO NOTHING`. שורות שכבר נראו נדחות בשקט;
שורות חדשות מקבלות `first_seen = now()`. במקביל כל הלוח נכתב כ־CSV ומועלה
ל־R2 כ־snapshot של אותה גרסה. לבסוף נרשמת `VersionIndex` עם כמה שורות
נוספו ומה הסך הכל. התוצאה: היסטוריית מצבים מלאה של כל טיסה — משהו שלא קיים
בשום מקום אצל המפרסם, שהלוח שלו הוא חלון של יומיים.
