# שלב 3: מנוע שאילתות לקר (DuckDB) ומתג "כולל היסטוריה"

חלק מ-[history-storage-strategy.md](history-storage-strategy.md). ממשיך את
[history-phase1-plan.md](history-phase1-plan.md) (תוקף שורה, חי) ואת
[history-phase2-plan.md](history-phase2-plan.md) (ייצוא שורות סגורות ל-Parquet
על R2 + קטלוג `app.history_parts`, פרוס עם הדגל כבוי; הייצוא הראשון ב-2026-11-03).
סדר העבודה מחייב: **להגדיר את הבדיקות, להריץ אותן לפני השינוי, לשנות, להריץ
שוב, ואין רגרסיה.**

## 1. המטרה

שלב 2 מוציא שורות סגורות מ-SQL לקבצי Parquet, אבל רק למאגר שהאדמין נתן לו
חלון שמירה, בדיוק כי עד השלב הזה אין למשתמש דרך לשאול את הקר. שלב 3 נותן
את הדרך: אותה שאילתת SQL, עם מתג "כולל היסטוריה", רצה על DuckDB שמאחד את
הטבלה החמה ב-Postgres עם קבצי ההיסטוריה ב-R2. בסוף השלב אפשר להפעיל חלון
שמירה על כל מאגר בלי לאבד נגישות.

בסוף השלב:

1. שירות `history_query`: DuckDB בתוך תהליך האפליקציה, מוגבל בזיכרון, בזמן
   ובמקביליות, שקורא Parquet ישירות מ-R2 ושורות חמות מ-Postgres.
2. **תצוגת היסטוריה** לכל טבלה: `UNION ALL` של הטבלה החמה (כל מה שב-SQL,
   פתוח וסגור) ושל חלקי ה-Parquet **שנמחקו מ-SQL** (`deleted_from_sql=true`
   בלבד; חלק שעדיין ב-SQL נמצא בשני המקומות ואסור לספור אותו פעמיים).
3. `POST /api/append/{id}/sql` ו-`POST /api/tables/sql` (קונסולת /data) מקבלים
   `include_history: true`; `/rows` ו-`/datastore_search` עם `as_of` ישן
   מהחלון עוברים לבד למנוע ההיסטוריה; כלי MCP מקבילים.
4. הקונסולה ב-/data ועמוד הארכיון של מאגר מקבלים מתג "כולל היסטוריה", שמופיע
   רק כשיש למאגר שורות שאינן ב-SQL (`history.rows_only_in_r2 > 0`), עם ציון
   שהשאילתה איטית יותר ורצה על DuckDB.
5. דגל גלובלי `HISTORY_QUERY_ENABLED` (ברירת מחדל `false` בדיפלוי הראשון).

## 2. המנוע

**DuckDB בתהליך** (`duckdb` מ-PyPI, גרסה 1.5.x, נבדק מקומית עם 1.5.1).
DuckDB סינכרוני, ולכן כל שאילתה רצה ב-`asyncio.to_thread` על חיבור שנפתח
לשאילתה (לא חיבור גלובלי משותף: קל יותר לבודד זיכרון ולבטל).

הגדרות לכל חיבור:

| הגדרה | ערך | למה |
|---|---|---|
| `memory_limit` | `HISTORY_QUERY_MEMORY_MB` = 1536 | לאפליקציה 8 GB, p95 בשימוש 6 GB |
| `threads` | 2 (מעבר ה-footers לפני השאילתה: 8, phase 4 סעיף 6.9) | לא לדחוק את ה-event loop |
| `temp_directory` | תיקייה זמנית בדיסק | spill במקום OOM |
| timeout | `HISTORY_QUERY_TIMEOUT_S` = 30, אכיפה ב-`conn.interrupt()` מה-event loop | כמו הקונסולה |
| מקביליות | semaphore 1, תור של עד 3 ממתינים, מעבר לזה 429 "המנוע עסוק" | שאילתה אחת בזיכרון בכל רגע |
| תוצאה | עד 1,000 שורות, אותו מבנה כמו `run_readonly_sql` (`columns`, `fields`, `rows`, `truncated`, `row_count`) ועוד `engine: "duckdb"`, `cold_parts`, `elapsed_ms` | הפרונט לא משתנה |

**גישה ל-R2:** הרחבת `httpfs` של DuckDB עם פרטי S3 של R2 (`R2_ENDPOINT`,
`R2_ACCESS_KEY`, `R2_SECRET_KEY`, `R2_REGION`, `R2_BUCKET`, ראו `app/config.py`
שורות 90 עד 95; `S3_*` שמורים ב-xhostd, לכן תמיד `R2_*`). לא דרך ה-URL הציבורי
(`r2.dev` חוסם לקוחות שאינם דפדפן, ראו docs/r2-custom-domain.md).
`read_parquet([...keys...], union_by_name=true)`: חלקים של טבלה אחת יכולים
להיכתב עם עמודות שהתווספו מאוחר יותר.

**גישה ל-Postgres (השורות החמות):** הרחבת `postgres` של DuckDB
(`ATTACH 'dsn' AS hot (TYPE postgres, READ_ONLY)`) עם ה-DSN של ה-**role
הקריאה בלבד** (`DATABASE_URL_READONLY`, אותו role של הקונסולה הציבורית), כך
שהמנוע לא יכול לראות יותר ממה שהקונסולה רואה. אם ההרחבה אינה זמינה בזמן
ריצה (xhostd בלי גישה ל-extensions.duckdb.org), נפילה בטוחה: השורות החמות
נמשכות ב-asyncpg כ-Arrow ונרשמות כ-view, עם תקרה של
`HISTORY_QUERY_HOT_MAX_ROWS` = 2,000,000; מעבר לזה השאילתה מסורבת עם הסבר.
ההרחבות מותקנות פעם אחת בבוט (`INSTALL httpfs; INSTALL postgres`) ונטענות
לכל חיבור; כשל התקנה נרשם ו-`is_available()` מחזיר את הסיבה.

**בניית התצוגה** (`build_history_view(table, parts, hot_cols)`): פונקציה
טהורה שמחזירה את ה-SQL:

```sql
CREATE VIEW "<table>" AS
SELECT <cols> FROM hot.public."<table>"
UNION ALL
SELECT <cols> FROM read_parquet(['s3://bucket/history/.../part-0000.parquet', ...], union_by_name=true)
```

העמודות: עמודות המקור + `first_seen`, `last_seen`, `row_hash`; עמודה שחסרה
בצד אחד מתמלאת `NULL::<type>`; `geom` (PostGIS) לא נכלל, `geometry_wkt` כן.
טיפוסים: כל עמודת מקור `VARCHAR`, `first_seen`/`last_seen` `TIMESTAMPTZ`.

**אילו טבלאות:** `extract_table_refs(sql, known_tables)` (טהורה) מוצאת בשאילתה
שמות טבלאות מוכרים (עם או בלי מרכאות, עם או בלי `public.`); לכל אחת עם חלקים
מחוקים נבנית תצוגה; טבלה בלי חלקים מחוקים נקראת ישירות מ-`hot` (אותה תוצאה
כמו ב-Postgres, רק איטית יותר, ולכן המתג מוצע רק כשיש קר).

**דיאלקט:** DuckDB קרוב ל-Postgres אבל לא זהה. `validate_readonly_sql` הקיים
חל כמו שהוא (SELECT יחיד, בלי `;`). `sql_error_hint` מקבל מיפוי של שגיאות
DuckDB נפוצות: `~` (regex) → `regexp_matches`, פונקציות PostGIS → "לא זמין
בהיסטוריה", `::uuid` על hex → השוואה כטקסט, `ILIKE` עובד.

**`as_of` על הקר:** `/rows` ו-`/datastore_search` עם `as_of` מנתבים ל-DuckDB
כשה-`as_of` קודם ל-`last_seen_min` של החלק המחוק הישן ביותר **או** כשיש
לטבלה שורות שאינן ב-SQL; אחרת נשארים ב-Postgres (מהיר). אותו תנאי תוקף
(`first_seen <= X AND (last_seen IS NULL OR last_seen > X)`) על התצוגה.

## 3. מה משתנה בקוד (רשימה סגורה)

1. `requirements.txt`: `duckdb==1.5.1` (עם pin), כמו `pyarrow`.
2. `app/services/history_query.py` (חדש): `is_available()`, `ensure_extensions()`
   (בוט), `extract_table_refs()`, `build_history_view()`, `cold_parts_for(table)`
   (מהקטלוג, `deleted_from_sql=true` בלבד), `run(sql, *, tables, as_of=None,
   current=False, timeout_s, max_rows)` עם ה-semaphore וה-interrupt,
   `duckdb_error_hint()`.
3. `app/config.py`: `history_query_enabled`, `history_query_memory_mb` (1536),
   `history_query_timeout_s` (30), `history_query_hot_max_rows` (2,000,000),
   `history_query_queue` (3).
4. `app/main.py`: `ensure_extensions()` בבוט כשהדגל דולק (לא חוסם).
5. `app/api/append.py`: `SqlBody.include_history`; `/sql` מנתב ל-`history_query.run`
   כשביקשו ויש למאגר חלקים מחוקים (אחרת מריץ כרגיל ומוסיף `engine: "postgres"`
   ו-`history_note`); `/rows` ו-`/datastore_search` עם `as_of`/`current`
   מנתבים לפי הכלל בסעיף 2; `/schema.history` מקבל `queryable_via: "duckdb"`.
6. `app/api/tables.py`: `SqlBody.include_history` ב-`POST /sql`; טבלאות מזוהות
   עם `extract_table_refs` מול `data_catalog`.
7. `app/mcp/sql_server.py` ו-`app/mcp/server.py` (`query_dataset_rows`): פרמטר
   `include_history` / `as_of`, עם תיאור הכלי שמסביר מתי להשתמש.
8. `app/api/admin_storage.py`: `GET /history/query-status` (זמינות, הרחבות,
   מצב ה-semaphore, סטטיסטיקה: שאילתות, timeouts, 429) ו-`POST
   /history/query-smoke` שמריץ `SELECT count(*)` על קובץ Parquet קיים ב-R2
   (ה-mirrors של `parquet/<table>.parquet` מ-`parquet_export`) ועל טבלה חמה אחת
   דרך `hot`: בדיקת קצה-לקצה שאינה תלויה בקיום חלקי היסטוריה.
9. frontend: `DataSqlPage` ו-`AppendArchivePage` מקבלים מתג "כולל היסטוריה"
   (מוצג רק כש-`rows_only_in_r2 > 0`, או תמיד בקונסולה הכללית עם הסבר),
   ותג "DuckDB · N חלקים · X ms" בתוצאה; `StorageUsagePanel` מציג את
   `query-status`.
10. `docs/API.md`: הפרמטרים והדיאלקט.

**לא בשלב הזה:** הורדת Parquet מאוחד לכל ההיסטוריה (קיים דרך `history/files`),
העברת מאגרים קרים לגמרי (שלב 4), שינוי במדיניות החלונות.

## 4. סדרת הבדיקות

### 4.1 בסיס (לפני כל שינוי, לשמור את הפלט)

```bash
python -m pytest -p no:cacheprovider -q tests
```

```bash
cd frontend && npx tsc --noEmit -p . && npx vitest run
```

בסיס על master `154300b` (2026-10-17): 2,371 עוברות, 6 מדולגות; tsc נקי;
vitest 13 עוברות. ב-worktree בלי junctions ל-`frontend/dist` ו-`../GOV scraper`
המספר נמוך ב-3 עד 4 בדיקות סביבתיות; למדוד שוב ב-worktree ולהשוות מולו.

אין Postgres מקומי. **DuckDB כן זמין מקומית** (1.5.1), ולכן בדיקות המנוע רצות
על DuckDB אמיתי: קבצי Parquet שנכתבים בבדיקה (pyarrow) ו"טבלה חמה" שהיא טבלת
DuckDB בזיכרון במקום Postgres (ה-builder מקבל את שם המקור כפרמטר). R2 מדומה
בקבצים מקומיים (`read_parquet` על נתיב); `httpfs` ו-`postgres` לא נבדקים
מקומית אלא ב-4.3.

### 4.2 בדיקות חדשות (לכתוב לפני הקוד, לראות אותן נכשלות)

`tests/test_history_query.py`:

1. `extract_table_refs`: מוצא `append_x_abcd1234`, `"append_x_abcd1234"`,
   `public.append_x_abcd1234`; לא מוצא מחרוזת דומה בתוך ליטרל או הערה; מחזיר
   רק שמות שבקטלוג.
2. `build_history_view`: UNION ALL עם אותן עמודות בשני הצדדים, `NULL::VARCHAR`
   לעמודה שחסרה בצד אחד, `geom` מושמט, `read_parquet([...], union_by_name=true)`
   עם רשימת המפתחות בסדר `closed_month, part`; בלי חלקים → אין view (הטבלה
   נקראת ישירות).
3. `cold_parts_for` מחזיר רק `deleted_from_sql=true` (monkeypatch על הקטלוג).
4. **מנוע אמיתי:** שני חלקי Parquet (חודשים שונים, השני עם עמודה נוספת) +
   טבלה חמה עם 3 שורות: `SELECT count(*)` על התצוגה = סכום; `GROUP BY`
   על העמודה שנוספה נותן NULL לישנות; `as_of` באמצע מחזיר בדיוק את השורות
   שהיו בתוקף; `current=true` רק פתוחות.
5. timeout: שאילתה אינסופית (`FROM range(1e12)` עם סכימה) מופסקת תוך
   `timeout_s=1` ומחזירה שגיאה ברורה, והחיבור משוחרר.
6. מקביליות: 4 שאילתות במקביל עם semaphore 1 ותור 3: הרביעית מקבלת 429-שקול
   (חריגה ייעודית), השאר מסתיימות.
7. `memory_limit` ו-`threads` מוגדרים על החיבור (קריאה חוזרת של
   `current_setting`).
8. `duckdb_error_hint`: שגיאת regex `~` מקבלת רמז `regexp_matches`; שגיאת
   `ST_` מקבלת רמז "לא זמין בהיסטוריה"; שגיאה לא מוכרת מוחזרת כפי שהיא.
9. ניתוב: `should_use_history(as_of, parts, rows_only_in_r2)`: True כש-as_of
   ישן מהחלק המחוק הישן ביותר או כשיש שורות רק ב-R2, False אחרת, ותמיד False
   כשהדגל כבוי (ואז `as_of` ישן מחזיר את מה שיש ב-SQL עם `history_note`).
10. API (monkeypatch כמו `tests/test_append_api_multi.py`): `/sql` עם
    `include_history` ובלי חלקים מחוקים מחזיר `engine: "postgres"` ו-`history_note`;
    עם חלקים מנתב ל-`history_query.run` עם שמות הטבלאות הנכונים; `/schema.history`
    מכיל `queryable_via`.
11. `tables.sql` עם `include_history` מעביר ל-`extract_table_refs` את הקטלוג
    וקורא למנוע רק כשלפחות טבלה אחת מהשאילתה קרה.
12. הקיימות: `tests/test_append_validity.py`, `tests/test_history_export.py`
    ו-`tests/test_append_store.py` עוברות ללא שינוי.

### 4.3 אימות על prod (אחרי deploy, הדגל כבוי)

1. deploy עם `HISTORY_QUERY_ENABLED` לא מוגדר. `GET /api/admin/storage/history/query-status`
   מדווח `available: false` עם הסיבה "disabled". זיכרון האפליקציה לא עלה.
2. הדלקת הדגל + פריסה חוזרת. `query-status`: ההרחבות `httpfs` ו-`postgres`
   מותקנות; אם `postgres` נכשלה, המצב מדווח "hot via arrow" והמשך הפרוטוקול
   תקף עם התקרה.
3. `POST /history/query-smoke`: `count(*)` על `parquet/append_taxes_nadlan_full_f41fb496_fd06f5ae.parquet`
   (mirror קיים, 3.84M שורות) חוזר שווה ל-`count(*)` ב-Postgres; זמן ו-RSS
   נרשמים; p95 זיכרון האפליקציה בטאב xhostd לא חוצה 7 GB.
4. בקונסולה: שאילתה עם "כולל היסטוריה" על מאגר **בלי** חלקים מחוקים מחזירה
   `engine: "postgres"` עם הערה, אותה תוצאה כמו בלי המתג (רגרסיה).
5. **הבדיקה האמיתית של הקר אפשרית רק אחרי 2026-11-03** (הייצוא הראשון של
   שלב 2) ואחרי שלב 4 של פרוטוקול שלב 2 (חלון על flydata אחד): אז
   `SELECT count(*)` עם המתג על `append_flydata_31c812a6` = שורות ב-SQL +
   שורות בחלקים המחוקים; `as_of=2026-10-10` מחזיר את מספר הטיסות שהיו על
   הלוח אז; בלי המתג רק מה שב-SQL עם `history_note`.
6. 7 ימים: `query-status` ללא timeouts חוזרים, 429 נדירים, זיכרון יציב.

### 4.4 סיום השלב

- 4.1 באותם מספרים כמו הבסיס, 4.2 ירוק, 4.3 צעדים 1 עד 4 בוצעו ותועדו בסעיף 6;
  צעד 5 מתועד כממתין לנובמבר.
- דיפלוי ל-prod דורש אישור המשתמש. קומיט לפי pathspec. בלי UNLOGGED. תזכורת
  לדיפלוי: cherry-pick על xhostd ו-origin מתנגש ב-`app/config.py` בגלל CRLF,
  פותרים עם `checkout --ours` והכנסת הבלוק עם ה-newline של הקובץ.

## 5. סיכונים

- **זיכרון**: `memory_limit` + spill לדיסק + שאילתה אחת בכל רגע. אם בכל זאת
  ה-RSS גדל, הגדר `HISTORY_QUERY_MEMORY_MB` נמוך יותר; מעבר לזה: channel
  נפרד ב-xhostd למנוע ההיסטוריה (לא בשלב הזה).
- **הרחבות DuckDB בבוט** דורשות רשת ל-extensions.duckdb.org; כשל = המנוע
  לא זמין, האתר לא מושפע (דגל).
- **אבטחה**: DuckDB עם `httpfs` יכול לקרוא כל URL. לכן `enable_external_access`
  מוגבל: `SET allowed_paths`/`allowed_directories` ו-`disabled_filesystems`
  לפי הנתמך בגרסה, ובכל מקרה ה-SQL עובר `validate_readonly_sql` וה-view-ים
  נבנים רק מהקטלוג; `ATTACH`/`COPY`/`INSTALL` ברשימת הסירוב (קיים ב-`_SQL_DENY`,
  להוסיף `attach`, `install`, `load`, `export`, `import`, `pragma`, `set`).
- **פער דיאלקט**: שאילתה שעובדת ב-Postgres ונכשלת ב-DuckDB. הרמזים מכסים
  את הנפוצים; השאר מוחזרים כשגיאה עם הסבר שהמנוע שונה.
- **כפילויות**: רק חלקים `deleted_from_sql=true` נכנסים לתצוגה. בדיקה 3
  מגינה על הכלל.

## 6. תוצאות

**2026-10-17, הסשן המבצע (worktree `claude/history-phase3-duckdb-1712a7`).**
לא בוצע deploy, לא בוצע push, ולא הורץ פרוטוקול 4.3.

### 6.1 קומיטים

| ריפו | SHA | תוכן |
|---|---|---|
| OVER | `c79efcd` | כל סעיף 3 + `tests/test_history_query.py` (78 בדיקות) |
| OVER | (הקומיט של הסעיף הזה) | סעיף 6 |

### 6.2 בסיס לפני השינוי

ב-worktree חובר `frontend/dist` כ-junction מה-checkout הראשי (`../GOV scraper` כבר קיים ליד
ה-worktrees). `frontend/node_modules` של ה-checkout הראשי חסר את `vitest` (tsc נכשל שם על
`geoFilters.test.ts`, סביבתי), ולכן ב-worktree הותקן `npm ci` משלו (לא בקומיט).

```
python -m pytest -p no:cacheprovider -q tests
2387 passed, 6 skipped, 392 warnings in 469.70s (0:07:49)
npx tsc --noEmit -p .   → נקי (exit 0)
npx vitest run          → 13 passed
```

2,387 ולא 2,371 כמו בסעיף 4.1: הקומיטים שאחרי `154300b` (govmap) הוסיפו בדיקות. בלי כישלונות.

### 6.3 הבדיקות החדשות לפני הקוד

`tests/test_history_query.py` נכתב לפני הקוד. מול מודול ריק (`app/services/history_query.py`
עם docstring בלבד): `32 failed, 46 errors, 0 passed` (78 בדיקות). אחרי המימוש תוקנה בבדיקה
עצמה ציפייה אחת (תנאי התוקף נבדק עם שם העמודה במרכאות, כמו שאר הבדיקה).

### 6.4 אחרי השינוי

```
python -m pytest -p no:cacheprovider -q tests
2465 passed, 6 skipped, 399 warnings in 462.69s (0:07:42)
npx tsc --noEmit -p .   → נקי (exit 0)
npx vitest run          → 13 passed
```

### 6.5 סטיות מהתוכנית ותוספות

1. **רשימת הסירוב המורחבת חלה רק על מסלול DuckDB** (`history_query.validate_sql`), לא על
   `_SQL_DENY` הגלובלי. `"export"`/`"import"` הם שמות עמודה סבירים, והרחבת הרשימה הגלובלית
   הייתה משנה את התנהגות הקונסולות הקיימות (בדיקה מוכיחה ש-`SELECT "export"` עדיין עובר
   ב-Postgres). במסלול DuckDB: המילים attach/detach/install/load/export/import/pragma/set/
   reset/checkpoint/use/copy (מחוץ למרכאות כפולות), וכל פונקציה שקוראת קובץ, URL, הגדרה או
   סוד (`read_*`, `*_scan`, `parquet_*`, `postgres_*`, `glob`, `query`, `current_setting`,
   `getenv`, `duckdb_*`, `pragma_*`...).
2. **הגבלת הגישה החיצונית:** אחרי בניית ה-view-ים `allowed_paths` = מפתחות החלקים בדיוק,
   `allowed_directories` = תיקיית ה-spill, `enable_external_access=false`,
   `lock_configuration=true`. נבדק על DuckDB אמיתי שקריאת קובץ אחר נחסמת. פרטי R2 ו-Postgres
   נכנסים כ-`TEMPORARY SECRET` (DuckDB מסתיר אותם), לא כ-`SET` ולא במחרוזת ה-ATTACH.
3. **ה-DSN של הצד החם הוא `APPEND_READONLY_DATABASE_URL`** (השם האמיתי בקוד; התוכנית כתבה
   `DATABASE_URL_READONLY`).
4. **מצב הצד החם נקבע בבוט:** `ensure_extensions` מתקין httpfs ו-postgres ומריץ ATTACH +
   נעילה + שאילתה אחת על `hot.information_schema`, בדיוק כמו שאילתת משתמש. הצליח →
   `hot_mode=attach`; נכשל → `arrow` (asyncpg דרך ה-pool הקריאה בלבד, עם התנאי של as_of
   כבר ב-Postgres, ותקרה HISTORY_QUERY_HOT_MAX_ROWS). כך גם השאלה הלא-בדוקה "האם סריקת
   postgres עובדת אחרי `enable_external_access=false`" נענית בפרוד ולא שוברת את המנוע.
5. **כלל הניתוב של as_of מחמיר מהנוסח:** עוברים ל-DuckDB רק כשקיים חלק מחוק שה-`last_seen_max`
   שלו מאוחר מ-as_of (כלומר שורה שהייתה בתוקף אז נמצאת רק ב-R2). הנוסח "או כשיש שורות רק
   ב-R2" היה מעביר כל as_of של טבלה עם חלק מחוק כלשהו, גם כשה-SQL לבדו מדויק. `current`
   לעולם לא עובר. בדיקה 9 מקודדת את הכלל.
6. **התור:** 1 רצה + עד 3 ממתינות (סעיף 2), החמישית מקבלת 429. נוסח בדיקה 4.2.6 ("הרביעית")
   מתאים לתור 2, ולכן נבדקים שני המקרים.
7. **תוקף בתוך ה-view:** as_of/current מצמצמים את ה-view עצמו (literal מוקלד, view לא מקבל
   פרמטרים), ולכן גם SQL חופשי וגם /rows רואים רק את השורות שבתוקף.
8. **TIMESTAMPTZ חוזר כטקסט** (המרה ל-Python דורשת pytz, שאינו ב-requirements; מסלול
   Postgres ממילא מחזיר מחרוזת).
9. **`duckdb` נטען בעצלות** (ב-import הראשון של שימוש), כדי שעם הדגל כבוי הספרייה לא נטענת
   לזיכרון (צעד 4.3.1).
10. **`dialect_note`:** ב-DuckDB `~` הוא התאמה מלאה ולא חלקית, הבדל שקט בלי שגיאה. שאילתה
    עם `~` מקבלת הערה בתוצאה. רמזי שגיאה: `~*`/`!~`, PostGIS/geometry, uuid/INT128, `to_char`,
    טבלה לא קיימת, עמודה לא קיימת.
11. **`/api/append/{id}/sql` רואה רק את טבלאות המאגר**; טבלה של מאגר אחר מחזירה שגיאת DuckDB
    עם רמז לקונסולה הכללית. הקונסולה (`/api/tables/sql` ו-`run_sql` ב-MCP) רואה כל טבלה
    מהקטלוג שהשאילתה מזכירה (גם knesset/idx, כ-view ישיר על `hot`), והקטלוג נקרא רק כשביקשו
    `include_history`.
12. **`/rows` ו-`/datastore_search` על המנוע** (`rows_page` / `datastore_page`): אותם פילטרים,
    q, sort, latest ועימוד, total ועמוד בשאילתה אחת. `/download.csv` נשאר על SQL בלבד (מתועד
    ב-API.md). `/datastore_search_sql` ללא שינוי.
13. **`query-smoke` מקבל `table` חובה** (הכרטיס באדמין ממלא מראש את
    `append_taxes_nadlan_full_f41fb496_fd06f5ae`). במצב arrow הספירה החמה נעשית ב-pool הקריאה
    בלבד (טעינת 3.84M שורות היא בדיוק מה שהתקרה אוסרת). מדווח RSS לפני/אחרי מ-`/proc`.
14. **פער ידוע (משלב 2, 7.5.5):** part שנמחק חלקית (`deleted_from_sql` עדיין false) לא נכנס
    ל-view, ולכן השורות שכבר נמחקו ממנו לא נראות במנוע עד שה-part מסומן, עד חודש. זה אותו
    פער שבו `rows_only_in_r2` מציג פחות מהאמת. תיקון אפשרי בשלב הבא: anti-join על
    `(ident, last_seen)` מול החם.
15. **בדיקה קיימת תלוית סדר (לא מהשינוי):** `test_append_sql_service_token::test_step_three_*`
    נכשלת כשהיא רצה אחרי `test_append_api_multi.py` או `test_append_validity.py` בלבד (500
    במקום 401, `JWT_SECRET_KEY` נקבע ב-setdefault רק אם `app.config` עוד לא נטען). שוחזר על
    HEAD נקי; בריצה המלאה היא עוברת.
16. **frontend:** המתג בקונסולה /data מוצג תמיד עם הסבר; בעמוד הארכיון רק כש-`rows_only_in_r2 > 0`
    וגם `queryable_via == "duckdb"`. התג: "DuckDB · N חלקים · X ms", וההערות מתחתיו. כרטיס
    "מנוע ההיסטוריה (DuckDB)" ב-StorageUsagePanel עם המצב ובדיקת הקצה-לקצה. לא נבדק בדפדפן
    (אין backend מקומי עם מסד); tsc ו-vitest נקיים.
17. **לא נבדק מקומית** (כמתוכנן): httpfs, postgres extension, ה-secrets, ו-ATTACH אחרי נעילה.
    כל אלה נבדקים ב-4.3 צעדים 2-3.

### 6.6 מה נשאר למשתמש לאשר (פרוטוקול 4.3, לא בוצע)

1. **deploy של OVER** לפי over-xhostd-deploy-path / three-lineage-masters (cherry-pick על master של
   xhostd ושל origin ב-worktree זמני, `app/config.py` מתנגש ב-CRLF: `checkout --ours` והכנסת
   הבלוק עם ה-newline של הקובץ), push ל-git.xhostd.com, `deploy(sha=…)`, עם
   `HISTORY_QUERY_ENABLED` לא מוגדר. ה-build מתקין `duckdb==1.5.1`. אימות:
   `GET /api/admin/storage/history/query-status` → `available: false`, `reason: "disabled"`;
   זיכרון האפליקציה בטאב xhostd לא עלה.
2. **`HISTORY_QUERY_ENABLED=true`** + פריסה חוזרת. `query-status`: `extensions.httpfs = "ok"`,
   `extensions.postgres = "ok"`, `hot_mode = "attach"` (או `"arrow"`, ואז שאר הפרוטוקול תקף עם
   התקרה), `available: true`. כשל ב-httpfs = המנוע לא זמין, האתר לא מושפע; הסיבה בשדה.
3. **בדיקת קצה-לקצה:** בכרטיס "מנוע ההיסטוריה" או `POST /api/admin/storage/history/query-smoke?table=append_taxes_nadlan_full_f41fb496_fd06f5ae`:
   `parquet_rows == hot_rows` (3.84M), `equal: true`; לרשום `parquet_ms`, `hot_ms`, RSS לפני/אחרי;
   p95 זיכרון בטאב xhostd לא חוצה 7 GB.
4. **רגרסיה בקונסולה:** ב-/data עם "כולל היסטוריה" על טבלה בלי חלקים מחוקים: `engine: "postgres"`
   עם `history_note`, אותה תוצאה כמו בלי המתג.
5. **אחרי 2026-11-03** (הייצוא הראשון) ואחרי חלון על flydata אחד (שלב 2 צעד 4):
   `SELECT count(*) FROM append_flydata_31c812a6` עם המתג = שורות ב-SQL + שורות בחלקים
   המחוקים; `/rows?as_of=2026-10-10` מחזיר `engine: "duckdb"` ואת מספר הטיסות שהיו על הלוח;
   עם הדגל כבוי, אותו as_of מחזיר את מה שב-SQL עם `history_note`.
6. **7 ימים:** `query-status.stats` בלי timeouts חוזרים, `busy` נדיר, זיכרון יציב.

### 6.7 פרוטוקול 4.3 על prod (2026-10-17, הסשן המתכנן)

| צעד | תוצאה |
|---|---|
| 1. deploy עם הדגל כבוי | xhostd `5c999a6` (GitHub `101c039`) עלה 07:45 UTC. `query-status`: `enabled:false`, `available:false`, `reason:"disabled"`. זיכרון האפליקציה 5.2 GB (p95 6.29), ללא שינוי. |
| 2. הדגל דולק | `HISTORY_QUERY_ENABLED=true`, פריסה חוזרת 07:50 UTC. `query-status`: DuckDB 1.5.1, `httpfs: ok`, `postgres: ok`, `hot_mode: attach` (הרחבת postgres עובדת גם אחרי הנעילה), `available:true`. |
| 3. בדיקת עשן | `query-smoke?table=append_taxes_nadlan_full_f41fb496_fd06f5ae`: Parquet 3,844,200 שורות ב-566 ms, חם (attach) 3,844,200 ב-537 ms, `equal:true`; RSS 887 → 837 MB. |
| 4. רגרסיה בקונסולה | `/api/append/6fb0e203/sql` עם ובלי `include_history`: אותה תוצאה (1,174), עם המתג `engine: postgres` והערה שאין לטבלה שורות שרק בהיסטוריה. |
| 5. הקר האמיתי | ממתין ל-2026-11-03 (ייצוא ראשון של שלב 2) ולחלון על flydata אחד. |

הערה: הקריאות לאדמין בוצעו דרך תוסף הכרום בסשן של המשתמש (fetch עם הטוקן של ה-SPA), כי טוקן אדמין פג וקליקים ישירים של המשתמש בדפדפן שלו לא יצאו.
