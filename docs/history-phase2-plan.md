# שלב 2: ייצוא שורות סגורות ל-Parquet על R2, קטלוג וחלון שמירה

חלק מ-[history-storage-strategy.md](history-storage-strategy.md). ממשיך את
[history-phase1-plan.md](history-phase1-plan.md), שחי ב-prod מ-2026-10-15:
לכל שורה יש `first_seen` ו-`last_seen` (NULL = בתוקף), האינדקס הייחודי הוא חלקי
(`WHERE last_seen IS NULL`), ו-poll מלא סוגר שורות שלא חזרו. סדר העבודה מחייב:
**להגדיר את הבדיקות, להריץ אותן לפני השינוי, לשנות, להריץ שוב, ואין רגרסיה.**

## 1. המטרה

שורות שנסגרו לפני זמן רב הן ההיסטוריה שמעמיסה על SQL. השלב הזה מוציא אותן
ל-Parquet על R2, בקבצים חודשיים בלתי משתנים, אחרי אימות, ומוחק אותן מ-SQL.
ההווה (שורות פתוחות) וההיסטוריה הקרובה נשארים ב-SQL, ושום שאילתה על ההווה
לא משתנה.

**מה השלב לא עושה:** הוא לא נותן למשתמשים לשאול את הקר. זה שלב 3 (DuckDB).
לכן בשלב הזה המחיקה מ-SQL מתבצעת **רק** למאגרים שהאדמין הפעיל להם חלון
שמירה במפורש, וברירת המחדל לכל מאגר היא "ייצוא בלי מחיקה" (הקבצים נבנים,
SQL לא נוגע). כך שלב 2 מתקיים כולו בלי לאבד נגישות, ושלב 3 מסיר את המגבלה.

בסוף השלב:

1. קטלוג `app.history_parts`: שורה לכל קובץ Parquet שנכתב (מאגר, טבלה, חודש
   סגירה, מפתח R2, שורות, hash, טווח `first_seen`/`last_seen`, גודל, זמן).
2. job יומי `export_closed_rows` שבוחר, כותב, מאמת, רושם, ומוחק (רק במאגרים
   עם חלון).
3. ציר חמישי בתכנית האחסון: `history_window_days` לכל מאגר (NULL = אין מחיקה,
   רק ייצוא), בבורר של האדמין לצד `storage_target`.
4. API ציבורי לרשימת הקבצים: `GET /api/append/{id}/history/files` עם URL
   ציבורי לכל קובץ, ו-`/schema` שמפרסם `history_parts` (כמה חלקים, טווח
   תאריכים, האם יש שורות שאינן ב-SQL).
5. טאב נפח SQL מציג לכל מאגר כמה מהשורות שלו בקר, ומה החלון.

## 2. מודל הנתונים

### 2.1 הקובץ

```
history/<dsid8>/<table>/closed_month=YYYY-MM/part-<NNNN>.parquet
```

- עמודות: כל עמודות המקור כ-string (כמו `parquet_export.py`), ועוד
  `first_seen` (timestamp[us, UTC]), `last_seen` (timestamp[us, UTC]),
  `row_hash` (string, ההקסה של ה-uuid). לא `geom`: PostGIS geometry לא נכנס
  ל-Parquet ב-string; `geometry_wkt`, אם קיים, נכנס כטקסט.
- `closed_month` = חודש `last_seen` ב-UTC. חודש = יחידת קובץ. טבלה שסוגרת
  מיליוני שורות בחודש (flydata) מקבלת כמה `part-NNNN` של עד 500k שורות.
- zstd + dictionary, row group של 100k.
- קובץ שנכתב **אינו משתנה לעולם**. חודש שכבר יוצא ונסגרו בו שורות נוספות
  מאוחר יותר (בגלל חלון ארוך או poll מאוחר) מקבל `part` חדש, לא כתיבה מחדש.

### 2.2 הקטלוג (alembic 072, סכימה `app`)

```
history_parts
  id              uuid PK
  dataset_id      uuid FK tracked_datasets ON DELETE RESTRICT   -- מאגר שנמחק לא מאבד קטלוג בשקט
  table_name      text NOT NULL
  closed_month    date NOT NULL       -- היום הראשון בחודש
  part            int  NOT NULL
  r2_key          text NOT NULL UNIQUE
  rows            bigint NOT NULL
  bytes           bigint NOT NULL
  content_hash    text NOT NULL       -- md5 של ה-row_hash-ים ממוינים, ראו 3.3
  first_seen_min  timestamptz, first_seen_max timestamptz
  last_seen_min   timestamptz, last_seen_max timestamptz
  deleted_from_sql boolean NOT NULL DEFAULT false
  deleted_at      timestamptz
  created_at      timestamptz NOT NULL DEFAULT now()
  UNIQUE (table_name, closed_month, part)
```

`deleted_from_sql=false` הוא המצב אחרי ייצוא בלי מחיקה (ברירת המחדל). רק
`true` אומר שהשורות קיימות אך ורק ב-R2.

### 2.3 החלון

`scraper_config["history_window_days"]`: מספר ימים, או חסר. חסר = ייצוא בלבד.
ערך = שורות עם `last_seen < now() - window` נמחקות מ-SQL אחרי ייצוא מאומת.
מינימום מותר: 30. טבלה מתחת ל-`HISTORY_MIN_ROWS` (ברירת מחדל 100,000 שורות
סגורות) לא מיוצאת כלל: לא שווה את המורכבות.

מקורות שסוגרים הרבה (flydata, וב-עתיד המחירים אם יופעלו) הם המועמדים
הראשונים לחלון; מאגרים ש-`append_closes_rows=false` לא סוגרים ולכן אין להם
מה לייצא.

## 3. התהליך (job יומי, 03:30 UTC, `max_instances=1`, resumable)

### 3.1 בחירה

לכל מאגר פעיל עם טבלאות NEON (דרך `append_store.tables_from_mappings` על
הגרסה האחרונה, כמו `parquet_export.eligible_tables`): לכל טבלה,
`SELECT date_trunc('month', last_seen) AS m, count(*) FROM t WHERE last_seen IS NOT NULL
AND last_seen < <cutoff> GROUP BY 1`, כאשר cutoff = תחילת החודש הנוכחי (חודש
שעדיין רץ לא מיוצא, כך שקובץ חודשי הוא שלם ברגע שנכתב) **וגם** `now() - window`
כשיש חלון. חודש שכבר קיים בקטלוג מיוצא רק אם יש בו שורות שלא כוסו
(`last_seen > last_seen_max` של ה-part האחרון), וזה פותח `part` חדש.

### 3.2 כתיבה

כמו `build_table_parquet`: cursor צד-שרת, batch של 50k, קובץ זמני בדיסק,
`upload_object`. השאילתה ממוינת `ORDER BY last_seen, row_hash` כדי שהחלוקה
ל-parts תהיה דטרמיניסטית ושה-hash בסעיף 3.3 יהיה ניתן לשחזור.

### 3.3 אימות (לפני כל מחיקה, ובלי קשר אם תהיה מחיקה)

1. `content_hash` מחושב **בשני הצדדים** באופן בלתי תלוי: ב-SQL
   `md5(string_agg(row_hash::text, ',' ORDER BY row_hash))` על אותו תנאי
   בחירה, ובפייתון מה-Parquet שהועלה: מורידים אותו חזרה מ-R2 (לא מהקובץ
   הזמני), קוראים את עמודת `row_hash`, ממיינים, md5. חייבים להיות שווים, וגם
   `rows` חייב להיות שווה ל-`count(*)` של התנאי.
2. רק אז שורת קטלוג (`deleted_from_sql=false`).
3. **מחיקה** (רק כשיש חלון, ורק אחרי שורת קטלוג): `DELETE FROM t WHERE ctid IN
   (SELECT ctid FROM t WHERE last_seen IS NOT NULL AND last_seen >= <month start>
   AND last_seen < <month end> AND row_hash IN (<ה-hash-ים של הקובץ, ב-batches
   של 10k דרך טבלת temp>) LIMIT 50000)` בלולאה; בסיום `deleted_from_sql=true`,
   `deleted_at`. אין DELETE לפי טווח תאריכים בלבד: מוחקים בדיוק את מה שבקובץ.
4. `VACUUM (ANALYZE)` על הטבלה אחרי מחיקה (מחוץ לטרנזקציה). לא `VACUUM FULL`:
   המקום חוזר למסד לשימוש חוזר, וזה מה שעוצר את הגידול; שחרור לדיסק של
   המערכת הוא עניין נפרד ואינו מטרת השלב.

### 3.4 כשלים

- כשל בהעלאה / באימות: הקובץ הזמני נמחק, אובייקט R2 חלקי נמחק
  (`delete_object`), אין שורת קטלוג, הטבלה לא נגעה. הניסיון הבא מחר.
- קריסה באמצע DELETE: שורות שכבר נמחקו נמצאות בקובץ (הוא מאומת), השורות
  שנותרו יימחקו בריצה הבאה כי התנאי זהה ו-`deleted_from_sql` עדיין false.
  מחיקה היא אידמפוטנטית.
- `history_parts` ללא אובייקט ב-R2 (נמחק ידנית): `GET /history/files` מסמן
  `missing=true`; job שבועי `audit_history_parts` עושה HEAD לכל מפתח ומדווח
  בטאב נפח SQL. לא מוחק מהקטלוג לבד.

## 4. מה משתנה בקוד (רשימה סגורה)

1. `alembic/versions/072_history_parts.py`: הטבלה מסעיף 2.2 בסכימה `app`.
2. `app/models/history_part.py` + רישום ב-`app/models/__init__.py`.
3. `app/services/history_export.py` (חדש): `plan(db) -> list[Job]`,
   `export_month(table, month, cutoff) -> Part`, `verify(part)`, `delete_exported(part)`,
   `run()` (in-process state כמו `index_migration.py`: running/current/done/failed),
   `content_hash_sql(table, month_start, month_end, cutoff)` ו-`content_hash_of_parquet(path)`
   כפונקציות טהורות/בדיקות. קורא ל-`storage_client` בלבד לגישה ל-R2.
4. `app/worker/scheduler.py`: job יומי + job שבועי (audit), שניהם
   `max_instances=1`, מאחורי `HISTORY_EXPORT_ENABLED` (ברירת מחדל `false`
   בדיפלוי הראשון).
5. `app/config.py`: `history_export_enabled`, `history_min_rows` (100000),
   `history_part_rows` (500000), `history_export_hour_utc` (3).
6. `app/api/datasets.py`: `history_window_days` ב-`UpdateRequest` (אימות:
   None או ≥30) ו-`DatasetResponse`; נכתב ל-`scraper_config`.
7. `app/api/admin_storage.py`: `GET /history` (מצב ה-job, הקטלוג מסוכם לפי מאגר:
   parts, rows_in_r2, rows_deleted, bytes), `POST /history/run`, `POST /history/stop`,
   `GET /history/audit`.
8. `app/api/append.py`: `GET /{dataset_id}/history/files` (רשימה ציבורית מהקטלוג,
   עם `public_url` לכל מפתח, מסוננת לפי `table`), ו-`/schema` מקבל
   `history: {parts, rows_in_r2, rows_only_in_r2, from, to}`.
9. `app/services/archive_state.py`: `fidelity` מקבל ערך `tiered` כשלמאגר יש
   part עם `deleted_from_sql=true`.
10. frontend: `StorageUsagePanel` מקבל כרטיס "היסטוריה ב-R2" (מצב, הפעלה,
    סיכום לפי מאגר); `AdminPage` בורר `history_window_days` ליד יעד האחסון;
    `AppendArchivePage` מציג קישור "קבצי היסטוריה" כשיש parts.
11. `docs/API.md`: הנתיב החדש והשדות ב-`/schema`.

**לא בשלב הזה:** DuckDB, מתג "כולל היסטוריה", `as_of` על קר (שלב 3). העברת
מאגרים קרים לגמרי כמו תחזית הגשם (שלב 4) כן אפשר להתחיל כאן אם הם עומדים
בתנאי "שורות סגורות", אבל תחזית הגשם לא סגורה (היא פשוט לא נפולת), ולכן היא
נשארת לשלב 4.

## 5. סדרת הבדיקות

### 5.1 בסיס (לפני כל שינוי, לשמור את הפלט)

```bash
python -m pytest -p no:cacheprovider -q tests
```

```bash
cd frontend && npx tsc --noEmit -p .
```

הבסיס הנוכחי על master (2026-10-15, אחרי שלב 1): 2,316 עוברות, 6 מדולגות, tsc נקי
(ב-worktree בלי junctions ל-`frontend/dist` ו-`../GOV scraper` המספר נמוך ב-3-4
בדיקות סביבתיות; למדוד שוב ב-worktree לפני השינוי ולהשוות מולו).

אין Postgres מקומי ואין docker: בדיקות המסד הן יחידה טהורה על בוני SQL ועל
הלוגיקה, ועל Parquet אמיתי (pyarrow מותקן) בקבצים זמניים. R2 מדומה ב-monkeypatch
על `storage_client` (upload/download/delete/public_url).

### 5.2 בדיקות חדשות (לכתוב לפני הקוד, לראות אותן נכשלות)

`tests/test_history_export.py`:

1. `plan`: טבלה עם שורות סגורות בחודשים 2026-07/08/09 וחודש נוכחי 10 מייצרת
   עבודות ל-07/08/09 בלבד; עם חלון 60 יום (היום 2026-10-16) רק 07 ו-08 מסומנות
   `delete=True`; חודש שכבר בקטלוג עם `last_seen_max` שמכסה הכול מדולג; עם
   שורות חדשות אחריו, part חדש עם מספר עוקב.
2. טבלה עם פחות מ-`history_min_rows` שורות סגורות לא מתוכננת.
3. מאגר עם `append_closes_rows=false` לא מתוכנן (אין לו שורות סגורות, אבל
   הבדיקה מוודאת שהמסלול לא תלוי בזה).
4. `content_hash_sql` מייצר md5 על `string_agg(row_hash::text ORDER BY row_hash)`
   עם אותם תנאי חודש וה-cutoff; `content_hash_of_parquet` על קובץ Parquet אמיתי
   שנכתב בבדיקה מחזיר את אותו ערך שמחושב בפייתון מאותם hash-ים ממוינים
   (אותו אלגוריתם בשני הצדדים, מוכח על קובץ).
5. `export_month` כותב Parquet אמיתי לקובץ זמני מתוך iterator של שורות:
   סכימה = עמודות המקור כ-string + `first_seen`/`last_seen` כ-timestamp +
   `row_hash` כ-string; חלוקה ל-parts כל 500k (בבדיקה עם `history_part_rows=3`).
6. `verify`: אי התאמה ב-hash או ב-rows מחזירה כשל, **ואז** `delete_object` נקרא
   על המפתח, ואין שורת קטלוג (monkeypatch על storage_client).
7. סדר הפעולות: `delete_exported` מסרב לרוץ בלי שורת קטלוג עם `deleted_from_sql=false`,
   ומסרב כשאין `history_window_days`.
8. `delete_sql` מוחק לפי חודש **וגם** לפי `row_hash` בקבוצות, אף פעם לא לפי
   טווח תאריכים בלבד; `VACUUM (ANALYZE)` אחרי, מחוץ לטרנזקציה.
9. אידמפוטנטיות: הרצה שנייה על חודש עם `deleted_from_sql=true` לא כותבת ולא
   מוחקת דבר.
10. API `/history/files`: רשימה מהקטלוג עם `public_url`, סינון לפי `table`,
    404 למאגר בלי NEON (monkeypatch כמו `test_append_api_multi.py`); `/schema`
    מכיל `history` עם `rows_only_in_r2` = סכום rows של parts עם `deleted_from_sql`.
11. `UpdateRequest.history_window_days`: 29 → 400, 30 → נכתב, null → נמחק
    מה-config.
12. `archive_state`: `tiered` רק כשיש part מחוק, לא כשיש part לא מחוק.
13. הבדיקות הקיימות של שלב 1 (`tests/test_append_validity.py`) ושל
    `parquet_export` עוברות ללא שינוי.

### 5.3 אימות על prod (אחרי deploy, הדגל כבוי)

1. deploy עם `HISTORY_EXPORT_ENABLED` לא מוגדר. alembic 072 רץ בבוט. אימות:
   `app.history_parts` קיימת וריקה (דרך `GET /api/admin/storage/history`).
2. הפעלה ידנית (`POST /history/run`) כשלשום מאגר אין חלון: מייצא בלי למחוק.
   המועמדים כיום: שני מאגרי flydata (~336k שורות סגורות כל אחד, חודש 2026-10,
   **שלא מיוצא עד שיסתיים**). לכן לבדיקה הראשונה: להקטין זמנית
   `history_min_rows` אם צריך, או להמתין ל-2026-11-01. עדיף: בדיקה על טבלה
   שהסגירות שלה כבר בחודש סגור. אם אין כזו עד הדיפלוי, הפעלה ראשונה ב-1 בנובמבר.
3. אחרי ייצוא: `GET /api/append/{flydata}/history/files` מחזיר את הקבצים; הורדת
   קובץ אחד ב-DuckDB/pandas מקומית: `rows` שווה לקטלוג, `content_hash` משוחזר.
   `SELECT count(*)` ב-SQL לא השתנה (אין מחיקה).
4. חלון: `history_window_days=30` על מאגר flydata אחד בלבד. ריצה ידנית. אימות:
   `count(*)` ירד בדיוק ב-`rows` של ה-parts שסומנו `deleted_from_sql`, שורות
   פתוחות ללא שינוי, `/rows?current=true` אותו total, `pg_total_relation_size`
   לא גדל, ואחרי VACUUM `n_dead_tup` קטן.
5. הדלקת הדגל (job יומי) + 7 ימי מעקב בטאב נפח SQL: גודל המסד לא עולה בגלל
   flydata; אין כשלי אימות בלוג (`history export:`).

### 5.4 סיום השלב

- 5.1 באותם מספרים כמו הבסיס, 5.2 ירוק, 5.3 בוצע ותועד בסעיף 7.
- דיפלוי ל-prod דורש אישור המשתמש. קומיט לפי pathspec. בלי UNLOGGED (xhostd
  מסרב). שמות עמודות עבריים עוברים ל-Parquet כמו שהם.

## 6. סיכונים

- **מחיקה של שורות שלא בקובץ**: נמנעת בבנייה (מחיקה לפי row_hash מתוך הקובץ
  המאומת, לא לפי תאריך).
- **R2 כנקודת כשל יחידה לקר**: `rows_only_in_r2` מוצג בגלוי, audit שבועי,
  והגיבוי החודשי של המסד (pg_dump) אינו מכסה את R2; לכן בשלב 2 אין מחיקה
  בלי חלון מפורש, וברירת המחדל שומרת הכול ב-SQL.
- **`string_agg` על מיליוני hash-ים** ב-SQL: 336k × 37 תווים = 12 MB מחרוזת,
  סביר; ל-5M שורות (חודש רע של מחירים) זה 185 MB, ולכן ה-hash מחושב **לכל
  part** (עד 500k שורות) ולא לכל החודש.
- **קובץ Parquet פגום שעבר אימות**: האימות קורא את הקובץ מ-R2 חזרה, לא את
  הזמני, ולכן מה שאומת הוא מה שיש.

## 7. תוצאות

(הסשן המבצע ממלא: תאריך, SHA, פלט הבדיקות לפני/אחרי, סטיות, מה נשאר לאשר.)
