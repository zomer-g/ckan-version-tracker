# שלב 1: תוקף שורה (`last_seen`) ו-API של "המאגר בתאריך X"

חלק מ-[history-storage-strategy.md](history-storage-strategy.md). נכתב 2026-10-15
כתוכנית ביצוע לסשן נפרד. סדר העבודה מחייב: **להגדיר את הבדיקות, להריץ אותן לפני
השינוי, לשנות, להריץ שוב, ואין רגרסיה.**

## 1. המטרה

היום טבלת append יודעת מתי שורה נראתה לראשונה (`first_seen`) ולא מתי הפסיקה
להיות בתוקף. לכן אי אפשר לענות "מה המצב הנוכחי" לטבלה בלי מפתח פריט, ואי אפשר
להפריד הווה מהיסטוריה (וזה מה ששלב 2 צריך כדי להוציא היסטוריה ל-Parquet).

בסוף השלב:

1. לכל טבלת append יש עמודה `last_seen timestamptz NULL`. NULL = השורה בתוקף.
2. poll **מלא** שלא הביא שורה פתוחה סוגר אותה (`last_seen = זמן ה-poll`).
3. שורה שנסגרה וחזרה זהה נפתחת כ**שורה חדשה** (מרווח תוקף חדש), בלי לאבד את
   הפער. זה מושג באינדקס ייחודי **חלקי**: `UNIQUE (row_hash) WHERE last_seen IS NULL`.
4. API: `GET /api/append/{id}/rows?as_of=<timestamp>` ו-`datastore_search?as_of=`
   מחזירים את השורות שהיו בתוקף באותו רגע; `?current=true` מחזיר רק פתוחות.
   `/schema` מפרסם `last_seen_column` ו-`supports_as_of`.
5. הקונסולה ב-/data לא משתנה: `last_seen` היא עוד עמודה שאפשר לסנן עליה.

## 2. מודל הנתונים

```
first_seen   timestamptz NOT NULL   מתי OVER שמר את השורה (קיים)
last_seen    timestamptz NULL       מתי OVER ראה לאחרונה poll מלא בלי השורה. NULL = בתוקף
row_hash     uuid / text            זהות התוכן (קיים; בלי עמודת ה-stamp)
```

"בתוקף בתאריך X": `first_seen <= X AND (last_seen IS NULL OR last_seen > X)`.

**אינדקסים.** היום: `UNIQUE (row_hash)` (או `UNIQUE (<append_key>)` לטבלאות
ממופתחות). אחרי: `UNIQUE (row_hash) WHERE last_seen IS NULL`, ואותו דבר למפתח.
ה-INSERT משתמש ב-`ON CONFLICT (row_hash) WHERE last_seen IS NULL`, ש-Postgres
מסיק מהאינדקס החלקי. שורה סגורה לא מתנגשת, ולכן חזרה של תוכן זהה פותחת מרווח
חדש. אינדקס רגיל (לא ייחודי) על `last_seen` אינו נחוץ בשלב זה; הסינון
`last_seen IS NULL` נהנה מהאינדקס החלקי עצמו.

**מי סוגר שורות.** רק מעבר מלא על המקור:

| נתיב כתיבה | קובץ | מתי זה מעבר מלא |
|---|---|---|
| CKAN datastore streaming | `delta_archiver._archive_streaming_to_db` | `start_offset == 0` והלולאה הסתיימה בלי חריגה |
| CKAN multi-resource | `delta_archiver.archive_multi_via_datastore_streaming` | אותו תנאי, לכל טבלה בנפרד |
| scraper push (inline CSV) | `worker.py` סביב `_neon_load_from_csv` | `not body.partial_run`, לכל טבלה שהגרסה הזו טענה |
| scraper push (קובץ גדול) | `worker.py` סביב שורה 1270 (`_neon_stream_load_file`) | כנ"ל |
| backfill היסטורי | `r2_backfill.py` | **לעולם לא** סוגר (הוא משחזר עבר, לא מודד הווה) |
| דגימה חלקית (sampling run) | worker, `partial_run=True` | **לעולם לא** |

**opt-out לפי מקור.** מקור שמפרסם רק שינויים (שקיפות מחירים: כל שורה היא מצב עם
`last_seen_date` משלו) לא מביא את כל השורות הפתוחות בכל poll, ולכן אסור לסגור
לפיו. דגל חדש `scraper_config["append_closes_rows"]` (ברירת מחדל `True`), שמניפסט
המחירים ב-GOVSCRAPER מגדיר `False` ב-`default_config`. הסשן שמבצע את השלב
מוסיף את השורה למניפסט ומתאם עם הסשן של המחירים אם הוא עדיין פתוח.

**איך הסגירה מחושבת.** ה-loader כבר מחשב את זהות כל שורה בפייתון (`row_hash`)
לפני ה-INSERT. במהלך מעבר מלא הוא כותב את הזהויות של המעבר לטבלה זמנית **לא**
TEMP (כי כל flush מקבל connection אחר מה-pool): `_loader.pass_<token>` כ-UNLOGGED,
עמודה אחת, ב-COPY לכל flush. בסיום המעבר:

```sql
UPDATE <t> SET last_seen = $run_ts
WHERE last_seen IS NULL AND row_hash NOT IN (SELECT ident FROM _loader.pass_<token>)
```

בקבוצות של 50k לפי ctid (כמו ensure_content_diff), ואז DROP של טבלת המעבר.
לטבלאות `capture_changes` (מרשם הרכב, `append_diff`) ה-staging כבר קיים לכל
batch; מצטברים ממנו לאותה טבלת מעבר. טבלת מעבר שנשארה אחרי קריסה נמחקת
בבוט (prefix scan על `_loader`).

**זמן הסגירה** הוא זמן תחילת ה-poll (`run_ts`), אחד לכל המעבר, לא `now()` לכל
batch, כדי ש"בתאריך X" יהיה עקבי.

## 3. מה משתנה בקוד (רשימה סגורה)

1. `app/services/append_store.py`
   - `ensure_table` / `ensure_content_diff`: `ADD COLUMN IF NOT EXISTS last_seen timestamptz`;
     האינדקס הייחודי נבנה חלקי. **מיגרציית אינדקס לטבלאות קיימות** היא job
     נפרד ומתחדש (כמו `hash_compaction.py`): לכל טבלה `CREATE UNIQUE INDEX
     CONCURRENTLY <t>_open_uq ON t (row_hash) WHERE last_seen IS NULL`, ואז
     `DROP INDEX` הישן. עד שטבלה הומרה, ה-INSERT שלה ממשיך להשתמש בסעיף הישן
     (`ON CONFLICT (row_hash) DO NOTHING`), לפי בדיקה של האינדקס הקיים, בדיוק
     כמו ש-`hash_column_is_uuid` שואל את הטבלה על טיפוס העמודה.
   - `build_insert`: פרמטר `partial_open: bool` שמוסיף `WHERE last_seen IS NULL`
     לסעיף ה-conflict. ה-DO UPDATE של ה-stamp נשאר.
   - `append_diff`: אותו סעיף.
   - חדש: `begin_pass(table) -> PassTracker` עם `add(idents)` ו-`close_missing(run_ts)`,
     ו-`sweep_abandoned_passes()` לבוט.
   - `latest_source`: ללא שינוי. חדש: `validity_where(as_of | current)` שמחזיר
     תנאי SQL, בשימוש `query` ו-`datastore_search`.
2. `app/services/delta_archiver.py`: שני ה-loaders פותחים PassTracker כש-
   `start_offset == 0` ו-`append_closes_rows`, מזינים אותו בכל flush, וסוגרים
   בסיום מוצלח בלבד. חריגה באמצע = לא סוגרים כלום (הטבלה נשארת כמו היום).
3. `app/api/worker.py`: אותו דבר לשני נתיבי הטעינה, בתנאי `not body.partial_run`.
4. `app/api/append.py`: `as_of` / `current` ב-`/rows`, `/datastore_search`,
   `/download.csv`; `/schema` מפרסם `last_seen_column`, `supports_as_of`.
   `as_of` מנותח כ-ISO timestamp או תאריך (תאריך = סוף היום, UTC).
5. `app/mcp` (השרת של גרסאות לעם): `query_dataset_rows` מקבל `as_of`.
6. `frontend`: בעמוד המאגר, תצוגת הטבלה מקבלת בורר "מצב נוכחי / בתאריך / כל ההיסטוריה"
   (ברירת מחדל: כל ההיסטוריה, כמו היום, כדי לא לשנות התנהגות). טאב נפח SQL
   מציג את התקדמות מיגרציית האינדקס (כמו דחיסת row_hash).
7. `docs/API.md`: הפרמטרים החדשים.

**לא בשלב הזה:** הוצאת שורות מ-SQL, Parquet, DuckDB, חלון שמירה. אלה שלבים 2 ו-3.

## 4. סדרת הבדיקות

### 4.1 בסיס (להריץ לפני כל שינוי, ולשמור את הפלט)

```bash
python -m pytest -p no:cacheprovider -q tests
```

```bash
cd frontend && npx tsc --noEmit -p .
```

**בסיס שנמדד 2026-10-15 על master 293103c:** `2279 passed, 6 skipped` ב-7:47 דקות;
tsc נקי. כל ריצה אחרי השינוי חייבת להחזיר לפחות את אותם 2,279 ירוקים.

אין Postgres מקומי ואין docker על המכונה הזו, ולכן כל בדיקת מסד היא
יחידה טהורה (בוני SQL) או אימות על prod לפי פרוטוקול 4.3. לא להוסיף תלות
ב-Postgres חי לסוויטה.

### 4.2 בדיקות חדשות (לכתוב **לפני** הקוד, לראות אותן נכשלות, ואז לממש)

`tests/test_append_validity.py`:

1. `build_insert(partial_open=True)` מייצר `ON CONFLICT ("row_hash") WHERE "last_seen" IS NULL DO NOTHING`; עם stamp: `… DO UPDATE SET <stamp> = EXCLUDED.<stamp>`; `partial_open=False` משאיר את הסעיף הקיים אות באות (רגרסיה).
2. לטבלה ממופתחת (`key_col`) אותו דבר על המפתח.
3. `validity_where(as_of=…)` מחזיר `first_seen <= $n AND (last_seen IS NULL OR last_seen > $n)` עם פרמטר אחד; `current=True` מחזיר `last_seen IS NULL`; שניהם יחד = שגיאה 400 בשכבת ה-API.
4. `as_of` כתאריך בלי שעה מתפרש כ-23:59:59.999 UTC של אותו יום; מחרוזת לא חוקית = 400.
5. `PassTracker.close_sql()` מייצר את ה-UPDATE בקבוצות עם `run_ts` אחד, ו-`NOT IN` על טבלת המעבר ולא על רשימת ערכים.
6. החלטת הסגירה: `should_close(ds, start_offset, partial_run)` מחזיר False כש-`append_closes_rows` הוא False, כשהמעבר חלקי, כש-`start_offset > 0`, וכש-`partial_run`; True רק כשכולם מתקיימים. בדיקה לכל זרוע.
7. r2_backfill לעולם לא פותח PassTracker (בדיקה על החתימה/קריאה, עם monkeypatch).
8. `tests/test_append_sample_stamp.py` ו-`tests/test_append_store.py` הקיימים עוברים ללא שינוי (הם מגינים על זהות השורה וה-stamp).
9. API: `/schema` מכיל `last_seen_column: "last_seen"` ו-`supports_as_of: true` (בדיקת מבנה התשובה עם monkeypatch על `append_store`, כמו ב-`tests/test_append_api_multi.py`).
10. מיגרציית האינדקס: `index_migration.plan(existing_indexes)` מחזיר טבלאות שעדיין עם אינדקס מלא; טבלה שכבר הומרה לא מופיעה (אידמפוטנטי).

### 4.3 אימות על prod (אחרי deploy, לפני שמדליקים לכולם)

הסגירה נכנסת מאחורי דגל גלובלי `APPEND_CLOSE_ROWS_ENABLED` (ברירת מחדל `false`
בדיפלוי הראשון) ודגל לכל מאגר (`append_closes_rows`). סדר:

1. deploy עם הדגל הגלובלי כבוי. מיגרציית האינדקס רצה מהאדמין, קטנות קודם.
   אימות: `SELECT indexdef FROM pg_indexes WHERE indexname LIKE '%open_uq'` גדל,
   אין כפילויות של row_hash (`SELECT row_hash, count(*) … HAVING count(*)>1` על
   3 טבלאות שהומרו = 0 שורות), וה-poll השוטף ממשיך להכניס שורות (לוג).
2. מדליקים את הדגל הגלובלי, ומפעילים poll ידני על מאגר קטן ויציב (למשל
   מאגר CKAN של כמה אלפי שורות, לא מחירים ולא רכב). אימות: אחרי ה-poll
   `count(*) FILTER (WHERE last_seen IS NULL)` שווה למספר השורות שהמקור מחזיר
   היום (`total` מה-datastore), ו-`count(*)` לא ירד.
3. בדיקת פתיחה מחדש: מאגר שידוע שמשתנה (bus_speed או flydata): אחרי שני polls,
   `SELECT count(*) FROM t WHERE last_seen IS NOT NULL` > 0 ואין row_hash שמופיע
   פעמיים עם `last_seen IS NULL`.
4. `as_of`: על אותו מאגר, `/rows?as_of=<לפני ה-poll>` מחזיר את מספר השורות
   שהיו פתוחות אז; `?current=true` מחזיר את המספר של עכשיו; בלי פרמטרים אותו
   total כמו לפני השלב (רגרסיה).
5. רק אז: הדגל לכל המאגרים. 24 שעות מעקב ב-"לוג משימות" ובלוג xhostd על
   `close_missing` (זמן ריצה, שורות שנסגרו לכל מאגר). מאגר שסגר יותר מ-50%
   משורותיו ב-poll אחד נרשם כאזהרה ולא סוגר (אותה רוח כמו shrink guard):
   סף `APPEND_CLOSE_MAX_FRACTION=0.5`, עם `allow_shrink` שמבטל אותו.

### 4.4 סיום השלב

- כל הבדיקות של 4.1 עוברות באותם מספרים כמו הבסיס, ובדיקות 4.2 עוברות.
- פרוטוקול 4.3 בוצע ותועד ב-`docs/history-phase1-plan.md` בסעיף "תוצאות".
- המיגרציה הושלמה על כל הטבלאות (0 ממתינות בטאב נפח SQL).
- דיפלוי ל-prod דורש אישור המשתמש (ראו memory: over-xhostd-deploy-path,
  three-lineage-masters). קומיט לפי pathspec בלבד.

## 5. סיכונים

- **CREATE INDEX CONCURRENTLY** לא רץ בתוך טרנזקציה; asyncpg מחוץ לטרנזקציה מתאים.
  על טבלאות ענק (shape 1.1M שורות עם גיאומטריה) זה דקות; הכול resumable.
- **מקור שמשמיט שורות באקראי** (gov.il מחזיר עמוד חסר) יסגור שורות בטעות;
  הסף של 50% + הפתיחה מחדש האוטומטית בפול הבא (מרווח חדש) מגבילים את הנזק,
  והפער נשאר מתועד. זה עדיף על המצב הנוכחי שבו היעלמות אינה מתועדת כלל.
- **טבלת המעבר** על 6.4M זהויות (רכב) היא כ-100 MB UNLOGGED, נמחקת בסיום.
- **זיכרון**: לא מצטבר כלום בפייתון מעבר ל-batch הנוכחי.

## 6. תוצאות

**2026-10-15, הסשן המבצע (worktree `claude/history-phase1-last-seen-5c71b1`).**

### 6.1 קומיטים

| ריפו | SHA | תוכן |
|---|---|---|
| OVER | `cd756e7` | כל סעיף 3 + `tests/test_append_validity.py` (51 בדיקות) |
| GOVSCRAPER | `b334fed` (master מקומי, **לא נדחף**) | `append_closes_rows: False` במניפסט המחירים |

לא בוצע deploy לשום ריפו. ב-GOVSCRAPER push ל-origin/master הוא deploy לצי (self-update), ולכן הוא ממתין לאישור.

### 6.2 בסיס לפני השינוי

הבסיס הראשון ב-worktree יצא `4 failed, 2256 passed, 11 skipped`. ארבעת הכישלונות סביבתיים:
ל-worktree אין `frontend/dist` ואין `frontend/node_modules` (3 בדיקות SEO), ואין
`../GOV scraper` לידו (בדיקת המניפסטים). חוברו שלושתם כ-junction מה-checkout הראשי
(בלי לגעת בקוד), והבסיס נמדד מחדש:

```
python -m pytest -p no:cacheprovider -q tests
2262 passed, 9 skipped, 391 warnings in 473.84s (0:07:53)
npx tsc --noEmit -p .   → נקי (exit 0)
```

**הפער מול 2,279 של סעיף 4.1 מוסבר ואינו רגרסיה:** הבסיס של התוכנית נמדד ב-checkout
הראשי, שבו יש עריכות לא-מקומטות של סשן אחר (`tests/test_append_store.py`,
`tests/test_deep_search.py`, 141 שורות, 9 פונקציות בדיקה חדשות). ה-worktree יושב על
העץ המקומט `dc62c67`, שבו הבסיס הוא 2,262/9, כולו ירוק. זה המספר שההשוואה נעשית מולו.

### 6.3 הבדיקות החדשות לפני הקוד

`tests/test_append_validity.py` נכתב לפני כל שינוי קוד והורץ: `38 failed, 1 passed, 6 errors`
(העובר היחיד הוא בדיקת הרגרסיה של `build_insert` הממופתח בלי `partial_open`, שאמור
לעבור גם לפני). אחרי המימוש נוספו 6 בדיקות חיווט ל-loaders (סה"כ 51).

### 6.4 אחרי השינוי

```
python -m pytest -p no:cacheprovider -q tests
2313 passed, 9 skipped, 437 warnings in 553.80s (0:09:13)
npx tsc --noEmit -p .   → נקי (exit 0)
```

2,313 = 2,262 של הבסיס + 51 החדשות. אין רגרסיה. בדרך נמצאה ותוקנה דליפת state
בבדיקה חדשה (monkeypatch על המופע של `storage_client` השאיר bound method שהסתיר את
ה-patch ברמת המחלקה של `test_symbology_download.py`); התיקון בבדיקה, לא בקוד.

GOVSCRAPER אחרי השורה במניפסט: `tests/scrapers` 1563 passed. הסוויטה המלאה: 8 כישלונות,
כולם `tests/web/test_xhostd_deploy.py::TestDbCopy` (סקריפט העתקת DB), לא קשורים למניפסט.

### 6.5 סטיות מהתוכנית ותוספות

1. **NOT EXISTS במקום NOT IN** בסגירה (בדיקה 5). NOT IN על subquery גדול מ-work_mem
   נופל לסריקה חוזרת לכל שורה (6.4M זהויות ברכב), ו-NULL אחד בטבלת המעבר הופך את
   התנאי ל-NULL ולא נסגר כלום. הבדיקה מוודאת anti-join על טבלת המעבר ולא רשימת ערכים.
2. **תנאי הסגירה בנתיב ה-worker רחב מ-`not partial_run`:** גם `more_batches`,
   `consolidate_dedup_key` (כל אצווה של ריצה מרובת אצוות, כולל האחרונה, נושאת רק את
   השורות שלה) ו-`skip_version` חוסמים. `should_close` חוסם גם `scraper_config.archive`
   (ארכיון אינקרמנטלי כמו JDA וממ"מ שולח רק את החדש). טבלה שמוזנת מכמה משאבים באותו
   push (`neon_single_table`) נסגרת רק אם כל המשאבים שלה נטענו inline בהצלחה.
3. **אין סגירה בטבלה שעוד לא הומרה** (`begin_pass` מחזיר None): סגירה מול אינדקס מלא
   הייתה חוסמת חזרה של אותו תוכן.
4. **`last_seen` לא ב-`columns` של ברירת המחדל** (ולא ב-`iter_csv_open`, שה-worker
   בונה ממנו cache ומגבב את העמודות שלו). מופיע בשורות רק כשמבקשים `as_of`/`current`.
   לכן כל endpoint עונה אות באות כמו קודם בלי הפרמטרים.
5. **מקור שעמודה שלו נקראת `last_seen`** נשאר עם האינדקס המלא ובלי תוקף.
6. סף 50% (`APPEND_CLOSE_MAX_FRACTION`, עם `allow_shrink`) מומש כבר עכשיו, ולא רק
   כפרוטוקול. שימו לב: במניפסט המחירים `allow_shrink: True`, אבל המחירים ממילא לא
   סוגרים (`append_closes_rows: False`).
7. שם האינדקס החלקי הוא `<t>_open_uq`, אבל טבלה עם שם ארוך מקבלת שם מגובב
   (`_index_name`). לכן שאילתת האימות של 4.3 צריכה לחפש לפי ה-predicate:
   `SELECT count(*) FROM pg_indexes WHERE indexdef LIKE '%WHERE (last_seen IS NULL)%'`.
8. מיגרציית האינדקס נוגעת רק באינדקסים ש-append_store יצר (`<t>_uq`, `<t>_hash_uq`);
   אינדקס `url` של מאגר ה-CBS נשאר כמו שהוא.

### 6.6 מה נשאר למשתמש לאשר (פרוטוקול 4.3, לא בוצע)

1. **deploy של OVER** (`cd756e7` אחרי merge ל-master) לפי over-xhostd-deploy-path:
   push ל-git.xhostd.com ואז `deploy(sha=…)`. `APPEND_CLOSE_ROWS_ENABLED` לא מוגדר =
   false. אחרי הבוט: עמודת `last_seen` נוספת לכל טבלה בפול הראשון שלה (catalog בלבד).
2. **מיגרציית האינדקס** מטאב "נפח SQL" באדמין ("אינדקס חלקי לתוקף שורה", "המר עכשיו").
   אימות: `SELECT count(*) FROM pg_indexes WHERE indexdef LIKE '%WHERE (last_seen IS NULL)%'`
   גדל; על 3 טבלאות שהומרו `SELECT row_hash, count(*) FROM t WHERE last_seen IS NULL GROUP BY 1 HAVING count(*)>1`
   מחזיר 0; ה-poll השוטף ממשיך להכניס שורות. סיום: 0 ממתינות בטאב.
3. **push של GOVSCRAPER `b334fed`** (deploy לצי) לפני הדלקת הדגל הגלובלי, כדי שהמחירים
   יקבלו את ה-opt-out דרך סנכרון המניפסט.
4. הדלקת `APPEND_CLOSE_ROWS_ENABLED=true` ו-poll ידני על מאגר CKAN קטן ויציב; אימות
   `count(*) FILTER (WHERE last_seen IS NULL)` מול `total` של ה-datastore, ו-`count(*)`
   לא ירד. ה-version מקבל `rows_closed` / `close_skipped` ב-change_summary.
5. פתיחה מחדש על bus_speed או flydata אחרי שני polls; `as_of` / `current` מול המספרים;
   בלי פרמטרים אותו total כמו לפני (רגרסיה).
6. 24 שעות מעקב בלוג xhostd על `append close:` (זמן, שורות שנסגרו, סירובי 50%).

סעיף 4.4 ("המיגרציה הושלמה על כל הטבלאות", "פרוטוקול 4.3 בוצע") עדיין פתוח.

### 6.7 פרוטוקול 4.3 על prod (2026-10-15, הסשן המתכנן)

| צעד | תוצאה |
|---|---|
| 1. deploy עם הדגל כבוי | `9bc8fc9` (xhostd) עלה 20:51 UTC. GOVSCRAPER `b334fed` נדחף (opt-out המחירים). |
| 2. מיגרציית האינדקס | 21:06 עד 21:25 UTC, 1,546 מתוך 1,546 טבלאות, 0 כשלונות, 0 busy. SQL: 1,546 אינדקסים חלקיים, 0 מלאים, 0 INVALID, 1,546 טבלאות עם `last_seen`; בדיקת כפילויות על vehicles / bus_speed / accidents / 3 הראשונות = 0. המסד 58.6 → 57 GB. polls המשיכו להכניס שורות בזמן ההמרה. |
| 3. דגל גלובלי | `APPEND_CLOSE_ROWS_ENABLED=true` (app-level), פריסה חוזרת 21:28 UTC. |
| 4. poll מלא על מאגר קטן | תאונות דרכים לפי רשות מוניציפלית (6fb0e203, 2 טבלאות: 1,174 + 31 שורות). הניסיון הראשון (21:32) נכשל בפתיחת pass: **xhostd מסרב ל-UNLOGGED** ("UNLOGGED tables and sequences are not supported on xhost"); הנפילה הייתה בטוחה (0 נסגרו, גרסה v6 נרשמה). תיקון `775a032` (טבלת מעבר רגילה), פרוס כ-`2251add` 21:41. הניסיון השני (21:42): pass של 1,174 ו-31, "closed 0 of 1174 open rows", v7 נרשמה, `_loader` נקי מטבלאות pass. `count(*)` לא ירד, פתוחות = שורות המקור. |
| 4. API | `/rows` 1,174 (כמו לפני, בלי `last_seen` בשורות); `current=true` 1,174 עם `last_seen`; `as_of=2026-09-01` 0 (לפני הטעינה הראשונה); `as_of=2026-10-01` 1,174; `as_of` לא חוקי 400; `as_of`+`current` 400. `/schema` מפרסם `supports_as_of`. |
| 5. פתיחה מחדש | טרם נצפתה: דורש מאגר שמשתנה בין שני polls. ייבדק במעקב 24 השעות. |
| 6. מעקב 24h | פתוח. ממצא ראשון: **flydata** (לוח הטיסות, poll כל 15 דק') מסורב בסף 50% בכל poll ("would close 336,357 of 338,587 open rows"), כלומר הלוח החי מחזיק רק ~2k שורות מתוך 338k שבארכיון. ההחלטה למשתמש: `allow_shrink` (סגירה חד-פעמית של ~336k ואז סגירות קטנות, ו-`current=true` יענה "מה על הלוח עכשיו") או `append_closes_rows=false`. |

**לקח:** אסור UNLOGGED על xhostd; `index_mirror` משתמש בו ל-staging ועובד, כנראה כי הסירוב תלוי בסכימה או בהקשר, ולכן לא שונה שם.

**עדכון 21:56 UTC:** המשתמש בחר `allow_shrink` לשני מאגרי flydata; ב-poll הבא נסגרו 336,401 ו-336,176 שורות ב-5.1 ו-5.3 שניות (pass של 2,227 שורות חיות). צעד 5 (פתיחה מחדש) ייבדק עליהם במעקב 24 השעות.
