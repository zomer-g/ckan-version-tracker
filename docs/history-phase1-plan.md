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

(הסשן המבצע ממלא: תאריך, SHA, פלט הבדיקות לפני/אחרי, תוצאות 4.3.)
