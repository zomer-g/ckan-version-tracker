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

### 6.1 בדיקת שעון מדומה (`as_of` על הריצה הידנית)

**למה.** הסגירות התחילו רק ב-2026-10-15, ולכן כל שורה סגורה ב-prod היא
`closed_month=2026-10`; עם השעון האמיתי שום דבר לא מיוצא לפני 2026-11-03
(ה-cutoff + 3 ימי חסד). כדי להריץ את הצינור כולו היום על נתוני אמת,
`POST /api/admin/storage/history/run` מקבל `as_of` (תאריך או timestamp ISO,
UTC): התכנון (`plan`) וסף המחיקה (`delete_exported`) רואים את השעון המדומה,
`export_month` לא זקוק לשעון (ה-job נושא את החודש). ערך בעבר נדחה ב-409 (השעון
זז רק קדימה), ערך לא תקין ב-422; השעון המדומה מוצג ב-`GET /history` תחת
`job.as_of`. הריצה האמיתית ב-3 בנובמבר משלימה את אוקטובר עם part N+1
(`after=covered`), כך שייצוא מוקדם אינו פוגע בשלמות החודש.

**מה קורה ב-flydata 31c812a6 עם `as_of=2026-11-20`:** export_cutoff = 2026-11-01,
delete_cutoff (חלון 30 יום) = 2026-10-21. ~336k שורות שנסגרו ב-2026-10-15 מיוצאות
ל-part אחד (מתחת ל-500k), מאומתות ב-hash, ואז נמחקות מ-SQL (last_seen < 10-21);
~2.2k השורות הפתוחות לא נוגעות. שורות שייסגרו בין הריצה ל-3.11 יישארו ב-SQL
וייצאו ב-part 2 בריצה האמיתית.

**פרוטוקול (על prod, רק אחרי אישור המשתמש ו-deploy של הקומיט; אין להריץ מסשן
שלא קיבל אישור).** `$ID` = המזהה המלא של 31c812a6 מהקטלוג
(`GET /api/admin/storage/history` → `datasets[].dataset_id`, או טבלת המאגרים באדמין).

1. **בסיס.** `GET /api/admin/storage/history`: לשמור את `job`, `totals`, ואת שורת
   המאגר (`parts`, `rows_in_r2`, `rows_only_in_r2`). ב-SQL (קונסולת /data, או
   `POST /api/append/$ID/sql`), לכל טבלת append של המאגר:
   ```sql
   SELECT count(*) AS total,
          count(*) FILTER (WHERE last_seen IS NULL) AS open_rows,
          count(*) FILTER (WHERE last_seen < '2026-10-21') AS closed_before_cutoff,
          count(*) FILTER (WHERE last_seen >= '2026-10-21') AS closed_inside_window
   FROM "<table>";
   ```
   לשמור גם `pg_total_relation_size('<table>')` ו-`n_dead_tup` מ-`pg_stat_user_tables`.
2. **ריצה.** `POST /api/admin/storage/history/run?dataset_id=$ID&as_of=2026-11-20`.
   תשובה צפויה: `{"started": true, "as_of": "2026-11-20T23:59:59.999000+00:00"}`.
   (409 עם "as_of must be in the future" = התאריך כבר עבר; לבחור תאריך עתידי אחר,
   הכלל היחיד הוא שהוא אחרי 2026-11-03 כדי שאוקטובר ייחשב סגור.)
3. **מעקב.** `GET /api/admin/storage/history` עד `job.running = false`. צפוי:
   `planned` = מספר טבלאות ה-append של המאגר, `done = planned`, `failed = []`,
   `exported_parts = planned`, `exported_rows` ≈ 336k לטבלה, `deleted_rows =
   exported_rows` (כל השורות הסגורות ישנות מה-cutoff), `as_of` כמו בצעד 2.
   בלוג xhostd: `history export: <table> 2026-10 part 1 ...` ואז `... deleted N rows (complete)`.
4. **קטלוג.** באותה תשובה, שורת המאגר: `parts = planned`, `rows_in_r2 = exported_rows`,
   `rows_only_in_r2 = deleted_rows`, כל part עם `closed_month = 2026-10-01`,
   `part = 1`, `deleted_from_sql = true`, `last_seen_min` ≥ 2026-10-15,
   `last_seen_max` < 2026-10-21.
5. **R2.** `GET /api/admin/storage/history/audit` → `missing = []`, `checked` ≥ מספר
   ה-parts. המפתחות: `history/31c812a6/<table>/closed_month=2026-10/part-0001.parquet`.
6. **SQL אחרי.** השאילתה מצעד 1 שוב: `closed_before_cutoff = 0`, `open_rows` ו-
   `closed_inside_window` ללא שינוי, `total` ירד בדיוק ב-`deleted_rows`;
   `pg_total_relation_size` לא גדל, `n_dead_tup` קטן (VACUUM רץ אחרי המחיקה).
   `GET /api/append/$ID/rows?current=true&limit=1` מחזיר את אותו `total` כמו לפני.
7. **קבצים ציבוריים.** `GET /api/append/$ID/history/files` מונה את ה-parts עם
   `public_url`; הורדת part אחד: `rows` כמו בקטלוג, ו-md5 של ערכי `row_hash`
   ממוינים ומופרדים בפסיק שווה ל-`content_hash`.
8. **קריאה חמה+קרה.** `POST /api/append/$ID/sql` עם
   `{"sql": "SELECT count(*) FROM \"<table>\"", "include_history": true}` מחזיר
   `engine: "duckdb"` ואת ה-`total` של צעד 1 (לפני המחיקה); בלי `include_history`
   את ה-`total` של צעד 6.
9. **as_of.** `GET /api/append/$ID/rows?as_of=2026-10-16&limit=5` מנותב ל-DuckDB
   (`engine: "duckdb"` בתשובה, או `history_note`) ומחזיר שורות שהיו בתוקף ב-16.10,
   כולל כאלה שכבר אינן ב-SQL; `as_of=2026-10-17` (אחרי המחיקה, בתוך החלון) נשאר
   ב-Postgres.
10. **רישום.** לכתוב את התוצאות בסעיף 7 (זמני ריצה, גדלים, סטיות), ולהשאיר את
    `HISTORY_EXPORT_ENABLED` כפי שהיה; הריצה האמיתית ב-3.11 תוסיף part 2 לאותו חודש.

**ביטול.** אם משהו בצעדים 4 עד 9 לא תואם: `POST /api/admin/storage/history/rehydrate/$ID`
(שלב 4) מחזיר את השורות המחוקות מ-R2 ל-SQL; את הקטלוג ואת הקבצים לא מוחקים ידנית.

## 7. תוצאות

**2026-10-16, הסשן המבצע (worktree `claude/history-phase2-parquet-3dcb16`).**
לא בוצע deploy ולא הורץ פרוטוקול 5.3.

### 7.1 קומיטים

| ריפו | SHA | תוכן |
|---|---|---|
| OVER | `57dacde` | כל סעיף 4 + `tests/test_history_export.py` (55 בדיקות) |
| OVER | (הקומיט של הסעיף הזה) | סעיף 7 |

### 7.2 בסיס לפני השינוי

ב-worktree חוברו `frontend/dist` ו-`frontend/node_modules` כ-junction מה-checkout הראשי
(`../GOV scraper` כבר קיים ליד ה-worktrees), ולכן הבסיס הוא המספר המלא:

```
python -m pytest -p no:cacheprovider -q tests
2316 passed, 6 skipped, 406 warnings in 653.96s (0:10:53)
npx tsc --noEmit -p .   → נקי (exit 0)
```

זהה לבסיס של master בסעיף 5.1, בלי כישלונות.

### 7.3 הבדיקות החדשות לפני הקוד

`tests/test_history_export.py` נכתב לפני כל שינוי קוד. מול מודול ריק:
`34 failed, 21 errors, 0 passed` (55 בדיקות). אחרי המימוש נמצא באג אחד בנתוני הבדיקה
עצמה (שני parts בקטלוג המדומה קיבלו אותו `r2_key`), תוקן בבדיקה.

### 7.4 אחרי השינוי

2,371 = 2,316 של הבסיס + 55 החדשות, 0 כישלונות. בריצה המלאה הראשונה אחרי המימוש
נכשלה בדיקה קיימת אחת (`test_app_schema::test_every_model_without_a_schema_is_a_known_app_table`):
מודל חדש חייב להופיע ב-`APP_TABLES`. תוקן בקוד (סעיף 7.5.14), והריצה שלמטה היא על
העץ הסופי:

```
python -m pytest -p no:cacheprovider -q tests
2371 passed, 6 skipped, 401 warnings in 527.33s (0:08:47)
npx tsc --noEmit -p .   → נקי (exit 0)
```

### 7.5 סטיות מהתוכנית ותוספות

1. **המחיקה לפי זוגות `(ident, last_seen)` מתוך הקובץ, ב-`unnest` של שני מערכים**
   (10k זוגות לפקודה), ולא טבלת temp + לולאת ctid. שתי סיבות: (א) xhostd מסרב
   ל-UNLOGGED וטבלת staging היא DDL על connection מה-pool; (ב) זוג מחמיר יותר מ-`row_hash`
   לבד: תוכן שנסגר, נפתח ונסגר שוב באותו חודש הוא שתי שורות עם אותו hash, ומחיקה לפי
   hash בלבד הייתה מוחקת גם את השורה שאינה בקובץ. בדיקה מוכיחה זאת (שורה "stray").
   התנאי עדיין חסום בחודש, ומוסיף `last_seen < now() - window`.
2. **ה-hash ב-SQL ממוין `ORDER BY ident::text COLLATE "C"`**, כדי שהמיון יהיה זהה
   למיון של פייתון בכל collation של המסד.
3. **ה-hash ב-SQL מחושב לכל part על הטווח `(last_seen, ident)` שהקובץ עצמו מחזיק**
   (השורה הראשונה והאחרונה שלו, כפי שנקראו חזרה מ-R2), ולא ב-OFFSET. כך קובץ שחסרה
   בו שורה באמצע נכשל בספירה. כל ה-parts של חודש נרשמים יחד בטרנזקציה אחת, או
   שכל האובייקטים שהועלו לחודש נמחקים: הקטלוג לא מחזיק חודש שנחתך באמצע חותמת
   סגירה אחת (pass אחד סוגר 336k שורות באותו `last_seen`).
4. **cutoff עם 3 ימי חסד** (`GRACE_DAYS`): חודש מיוצא רק מ-3 בחודש שאחריו. `last_seen`
   של pass הוא זמן ההתחלה שלו, ו-pass ארוך (רכב) שהתחיל ב-31 עוד סוגר ב-1 בבוקר.
5. **החלון:** `delete=True` לחודש שמתחיל לפני `now() - window` (כמו בדיקה 1: 07 ו-08
   ב-60 יום), אבל נמחקות רק שורות עם `last_seen < now() - window` (סעיף 2.3). part
   מסומן `deleted_from_sql=true` רק כשכל השורות שלו מחוץ לחלון; עד אז הריצה היומית
   ממשיכה אותו (מתוכנן כ"מחיקה בלבד", בלי כתיבה מחדש). בתקופת הביניים
   `rows_only_in_r2` מציג פחות מהאמת (עד חודש), לעולם לא יותר.
6. **סינון מוקדם מ-pg_stats** (`reltuples × (1 − null_frac)` של `last_seen`): בלעדיו
   הבחירה היומית הייתה סורקת את כל 1,546 הטבלאות (כ-57 GB) כל לילה. טבלה שכבר
   בקטלוג נסרקת תמיד; טבלה בלי סטטיסטיקה נסרקת אם גודלה מעל הסף.
7. **טבלאות ממופתחות:** ה-ident הוא העמודה של האינדקס הייחודי החלקי (`row_hash` או
   מפתח המאגר). עמודת `row_hash` ב-Parquet קיימת רק כשה-ident הוא `row_hash`.
   טבלה בלי אינדקס חלקי לא מיוצאת.
8. **לפני כל מחיקה הקובץ מורד שוב מ-R2** ונבדק מול `content_hash` ו-`rows` של הקטלוג
   (גם ביום אחר מהייצוא); אי התאמה = `DeleteRefused`, הטבלה לא נוגעת.
9. **מאגרים `hidden` לא מיוצאים** (רק `status='active'`): הקבצים ציבוריים. ה-endpoint
   הציבורי מחזיר 404 למאגר מוסתר (`require_visible`), למאגר לא ידוע, ולמאגר בלי ארכיון
   שורות שאין לו parts. הוא קורא רק את מסד האפליקציה, לא את מסד ה-append.
10. **`missing`** מגיע ממצב ה-audit בזיכרון התהליך: מתאפס בפריסה עד ה-audit הבא
    (ראשון 05:00 UTC) או כפתור "בדוק קבצים" בטאב.
11. **`history` ב-`/schema` הוא `null`** כשהקטלוג לא נקרא (כך הבדיקות הקיימות שקוראות
    עם `db=None` עוברות בלי שינוי), ו-`files_url` מצביע לרשימה המסוננת לטבלה.
12. הבורר באדמין מוצג רק לתוכניות עם `neon`. `PATCH` מבחין בין שדה חסר (לא נוגע)
    ל-`null` (מוחק מה-config) דרך `model_fields_set`. ערך מתחת ל-30 שהגיע ל-config
    בדרך אחרת (`scraper_config_merge`) נקרא כאין חלון.
13. `history_window_days` בתשובות ה-API הוא הערך שה-job יפעיל בפועל (מספר שלם ≥ 30
    או null), לא הערך הגולמי מה-config: ערך זבל שהגיע דרך `scraper_config_merge` היה
    מפיל את מודל התשובה ב-500.
14. `history_parts` נוסף ל-`APP_TABLES` ב-`app/database.py` (בדיקת הבוט שטבלאות
    האפליקציה לא ב-`public`), ו-`tests/test_app_schema.py` מכיר אותה כטבלה שנולדה
    ב-`app` אחרי 066 (כמו 068). זה השינוי היחיד בבדיקה קיימת, והוא הרחבה של רשימה.
15. `tiered` נשאל בשאילתה מקובצת אחת לעמוד האדמין ולעמוד המאגר הציבורי, ולא נופל
    כשהטבלה חסרה. צ'יפ חדש ב-`ArchiveChips`: "טבלה לתשאול + היסטוריה בקבצים".

### 7.6 מה נשאר למשתמש לאשר (פרוטוקול 5.3, לא בוצע)

1. **deploy של OVER** לפי over-xhostd-deploy-path / three-lineage-masters: cherry-pick
   של הקומיטים על master של xhostd (worktree זמני), push ל-git.xhostd.com, ואז
   `deploy(sha=…)`, עם `HISTORY_EXPORT_ENABLED` לא מוגדר. alembic 072 רץ בבוט.
   אימות: `GET /api/admin/storage/history` מחזיר `totals.parts = 0`, `enabled: false`,
   `available: true` (אם `available: false`, חסר pyarrow או R2 בסביבה).
2. **ריצה ידנית בלי חלון:** בטאב "נפח SQL", כרטיס "היסטוריה ב-R2", "ייצא עכשיו" (או
   `POST /api/admin/storage/history/run`). בגלל ה-cutoff וה-grace, אוקטובר 2026 לא
   מיוצא לפני **2026-11-03**; ריצה לפני כן תתכנן 0 עבודות אם אין טבלה עם 100k שורות
   סגורות בחודש שהסתיים (flydata נסגר כולו ב-2026-10-15). לבדיקה מוקדמת יותר אפשר
   להוריד זמנית `HISTORY_MIN_ROWS`, אבל זה לא מקדים את אוקטובר.
3. **אחרי הייצוא:** `GET /api/append/{flydata}/history/files` מחזיר קבצים; הורדת part אחד
   ובדיקה מקומית: `rows` שווה לקטלוג, ו-md5 של `row_hash` ממוינים מופרדים בפסיק שווה
   ל-`content_hash`. `SELECT count(*)` בטבלה לא השתנה.
4. **חלון:** `PATCH /api/datasets/{flydata}` עם `{"history_window_days": 30}` על מאגר
   flydata אחד (או הבורר "היסטוריה ב-SQL" באדמין), ריצה ידנית. אימות: `count(*)` ירד
   בדיוק בסכום `rows` של ה-parts שסומנו `deleted_from_sql` (ושל השורות שמחוץ לחלון
   ב-part חלקי, ראו 7.5.5), שורות פתוחות ללא שינוי, `/rows?current=true` אותו total,
   `pg_total_relation_size` לא גדל, ו-`n_dead_tup` קטן אחרי ה-VACUUM.
5. **הדלקת הדגל** `HISTORY_EXPORT_ENABLED=true` + 7 ימי מעקב בטאב ובלוג xhostd
   (`history export:`), כולל audit ראשון ביום ראשון.

### 7.7 פריסה (2026-10-16, הסשן המתכנן)

xhostd `6b78cce` (GitHub `acf236e`) עלה 09:51 UTC; בלוג: `Running upgrade 071 -> 072` ללא שגיאות; `HISTORY_EXPORT_ENABLED` לא מוגדר ולכן ה-job היומי וה-audit לא נרשמו בסקדולר (0 אזכורים). צעדים 2 ו-3 של 5.3: ריצה ידנית לא תתכנן דבר לפני 2026-11-03 (תקופת החסד של 3 ימים אחרי סוף החודש; כל סגירות flydata הן מאוקטובר), ולכן הן נדחות ל-3 בנובמבר. צעד 4 (חלון 30 יום על flydata אחד) וצעד 5 (הדלקת הדגל) ממתינים לאישור.

### 7.8 בדיקת השעון המדומה על prod (2026-10-17, הסשן המתכנן)

פרוס 67e3bfc (xhostd; GitHub 28111a6; מקומי 14d0856 + 80d1bd6, כולל תיקון ה-union
של שלב 4 סעיף 6.9). הפרוטוקול של 6.1 רץ על מאגר הטיסות 31c812a6
(`append_flydata_31c812a6`) עם `as_of=2026-11-20`, הדגל היומי נשאר כבוי.

| צעד | תוצאה |
|---|---|
| 1 בסיס | 341,677 שורות: 2,191 פתוחות, 339,486 סגורות לפני 21.10, 0 בתוך החלון; 150.4 MB; `current` = 2,191 |
| 2 ריצה | `{"started": true, "as_of": "2026-11-20T23:59:59.999000+00:00"}` |
| 3 מעקב | 16:48:17 עד 16:48:38 UTC (21 שניות): planned 1, done 1, exported_parts 1, exported_rows 339,486, deleted_rows 339,486, failed [] |
| 4 קטלוג | part 1, closed_month 2026-10, deleted_from_sql true, last_seen 15.10 21:56 עד 17.10 16:34, 13.2 MB |
| 5 R2 | audit: 182 נבדקו, 0 חסרים; המפתח `history/31c812a6/append_flydata_31c812a6/closed_month=2026-10/part-0001.parquet` |
| 6 SQL אחרי | 2,191 שורות (ירידה של בדיוק 339,486), 0 סגורות, 82.9 MB (מ-150.4), n_dead_tup 0; `current` עדיין 2,191 |
| 7 קובץ ציבורי | הורדה מ-r2.dev, md5 של row_hash ממוינים = `5e1787ff1e759abdf91876b4a20d4ee7`, זהה לקטלוג, 339,486 שורות |
| 8 חם+קר | `count(*)` עם include_history: 341,677 דרך duckdb ב-1.9 שניות; בלי: 2,191 |
| 9 as_of | `as_of=2026-10-16`: duckdb, 2,166 שורות בתוקף; `as_of=2026-10-17T12:00Z` גם duckdb (ה-part מכיל שורות שהיו בתוקף אז, נסגרו ב-16:34), התנהגות נכונה |

סטייה יחידה מהצפי: צעד 9 ציפה ש-as_of בתוך החלון יישאר ב-Postgres; הניתוב
לקר נכון כי ה-part המחוק כולל שורות שנסגרו אחרי אותה נקודת זמן. הריצה האמיתית
ב-3.11 תוסיף part 2 לאוקטובר עם השורות שייסגרו מעכשיו ועד סוף החודש.

### 7.9 הדגל היומי הודלק (2026-10-17 16:58 UTC)

`HISTORY_EXPORT_ENABLED=true` על prod עם redeploy של 67e3bfc. בלוג האתחול:
"Scheduler started with 3583 jobs" מול 3581 בקונטיינר הקודם, כלומר שני ה-jobs
נרשמו: הייצוא היומי ב-03:30 UTC (`history_export`) והאודיט השבועי ביום ראשון
05:00 UTC (`history_audit`). עם השעון האמיתי הייצוא לא יעשה דבר עד 3.11
(אוקטובר אינו סגור), ואז יוסיף part 2 לאוקטובר של מאגר הטיסות 31c812a6 ו-part 1
למאגר 2c9bf587, וימחק לפי חלון 30 הימים. מעקב: הכרטיס בטאב "נפח SQL" ו-`GET
/api/admin/storage/history` (job.finished_at, failed).
