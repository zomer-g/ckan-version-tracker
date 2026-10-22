# אימות קצה לקצה: היסטוריה ל-Parquet (שלבים 1 עד 4 + הצמצום של 20.10), prod, 2026-10-22

אימות עצמאי ולקריאה בלבד. לא שונה שום מצב ב-prod: לא הועבר מאגר לקר, לא בוצע rehydrate,
לא הורץ ייצוא, לא הורץ compact, לא שונו משתני סביבה ולא נפרס דבר. הכלים: בקשות GET לאדמין
(`/api/admin/storage/...`, עם JWT ששמור רק בפקודות ה-shell), ה-SQL הציבורי לקריאה בלבד
(`POST /api/append/31c812a6-.../sql`), `GET` ציבוריים, הורדת ארבעה חלקי Parquet מה-URL הציבורי של R2,
לוג הריצה של xhostd (`get_logs`, קריאה בלבד), והבדיקות המקומיות. זמני המדידה: 05:20 עד 06:00 UTC.

## סיכום

| | מספר |
|---|---|
| בדיקות | 30 |
| עברו | 25 |
| סטיות | 3 (כולן בחומרה נמוכה) |
| תצפיות (לא סטייה מהתוכנית) | 2 (7a ו-7b, ממצא אחד: גידול המסד) |

**אין סטייה שפוגעת בנכונות ההיסטוריה.** הקטלוג, R2, ה-hash, המנוע, הניתוב של `as_of`, המאגרים
הקרים, המחירים והתזמון מתאימים לציפייה. הסטיות: בדיקה אחת שנכשלת לסירוגין בהרצה המלאה (תזמון,
לא קשורה להיסטוריה), וטבלה אחת (`append_cbs_index_cb500000`) שאין בה `last_seen` ואינדקס חלקי כי
היא נכתבת מחוץ ל-`append_store`.

מצב prod בזמן הבדיקה: xhostd sha `5f1f9d2` (לא `6e7b21b` כפי שנמסר; ההפרש בין השניים הוא 5 שורות
ב-`docs/history-phase4-plan.md` בלבד), התהליך עלה מחדש ב-2026-10-22 04:16:48 UTC (ראו סעיף 8).

## 1. בדיקות מקומיות (worktree על `a999164`)

| # | בדיקה | צפוי | נמדד | תוצאה |
|---|---|---|---|---|
| 1a | `pytest tests -q --ignore tests/test_seo.py --deselect tests/test_registry_identity.py` | הכול ירוק | 2,492 עברו, 6 דולגו, 16 הוסרו, **1 נכשלה**: `tests/test_worker_fleet.py::test_fleet_lists_what_each_machine_is_doing` (`assert w["busy-box"]["offline"] is False`). הקובץ לבדו: 21/21 עוברות | סטייה, נמוכה |
| 1b | `npx tsc -b` ב-frontend (עם junction ל-`node_modules` של ה-checkout הראשי) | נקי | יציאה 0 | עבר |
| 1c | `npx vitest run` | ירוק | 13/13 | עבר |

**סטייה 1a:** הבדיקה בודקת ש-worker "busy" אינו offline, וזה תלוי בזמן שעבר מאז ה-heartbeat
המדומה. ההרצה המלאה ארכה 13:46 דקות, והבדיקה עוברת כשהיא רצה לבד. כלומר flaky לפי תזמון, לא
קשורה לקוד ההיסטוריה. חומרה: נמוכה.

## 2. אינווריאנטים ב-SQL (pg_catalog דרך ה-endpoint הציבורי)

| # | בדיקה | צפוי | נמדד | תוצאה |
|---|---|---|---|---|
| 2a | עמודת `last_seen` בכל טבלת `append_*` ב-public | 0 חסרות | 1,570 טבלאות, **1 חסרה**: `append_cbs_index_cb500000` | סטייה, נמוכה |
| 2b | אינדקס UNIQUE חלקי עם `last_seen IS NULL` (כמו `classify_unique_indexes`: predicate מתוך ההגדרה, לא מהשם) | 0 חסרים | **1 חסר**, אותה טבלה. יש לה רק `UNIQUE (url)` | סטייה, נמוכה |
| 2c | מחירים: 0 שורות עם `last_seen IS NOT NULL` | 0 ב-196 טבלאות | 197 טבלאות `append_prices_*` (196 `prices_chain` + טבלת האינדקס `append_prices_index_*`), 33,901,638 שורות לפי reltuples, סריקה מלאה ב-8 אצוות: **0 סגורות** | עבר |
| 2d | מחירים: 0 `row_hash` כפול פתוח | 0 | בכל 197 הטבלאות יש אינדקס UNIQUE חלקי על `row_hash` עם `indisvalid=true`, ולכן כפילות פתוחה בלתי אפשרית. בדיקה ישירה על קרפור (`f240d9c5_2e0a7e7b_ad8971b7`, הטבלה שהייתה בה 7,445 כפילויות): `count(*) - count(DISTINCT row_hash)` = 0 | עבר |
| 2e | 12 + 60 הטבלאות הקרות ריקות וקטנות | ריקות, זעירות | 72/72 נמצאו, `count(*)` = 0 בכולן, `reltuples` = 0, הגדולה 16,384 בתים, סה"כ 1,179,648 בתים | עבר |
| 2f | flydata `31c812a6`: אין שורה סגורה שכבר יוצאה | ראו הערה | 2,090 פתוחות, 12,200 סגורות. `min(last_seen)` של הסגורות = 2026-10-17 16:49:33, אחרי `last_seen_max` של החלק היחיד (16:34:31). 8,592 מהן עם `last_seen` לפני 2026-10-21 | עבר (בהגדרה המתוקנת) |

**הערה ל-2f:** הציפייה שנמסרה ("אין סגורות לפני 2026-10-21") מחמירה מדי. הריצה עם השעון המדומה
(17.10, `as_of=2026-11-20`) ייצאה ומחקה את מה שהיה סגור **באותו רגע**. שורות שנסגרו אחריה
(17.10 16:49 עד היום) עוד לא יוצאו: הן ייכנסו לחלק של אוקטובר בריצה האמיתית ב-3.11, וימחקו כשיעברו
את חלון 30 הימים. הבדיקה הנכונה היא שאף שורה סגורה ב-SQL אינה בטווח החלק שנמחק, והיא עוברת.

**סטייה 2a/2b:** `append_cbs_index_cb500000` נכתבת על ידי `app/services/cbs_neon.py`
(`CBS_APPEND_TABLE`, upsert לפי `url` ממראה של `cbs_index`), ולא דרך
`append_store.ensure_table`, שמוסיף את `last_seen` ואת האינדקס החלקי. ההשלכה: למאגר הזה אין
`as_of`/`current` ולא ייצוא היסטוריה. נראה כמו מבנה מכוון (אינדקס תוכן שמתעדכן במקום), אבל הוא
לא מתועד כחריג באף אחד ממסמכי השלבים. לא נבדק מה `/schema` של המאגר מחזיר עבור `supports_as_of`.
חומרה: נמוכה.

## 3. הקטלוג מול R2

מקור: `GET /api/admin/storage/history`, `GET /api/admin/storage/history/audit`,
`GET /api/append/{id}/history/files` לכל 12 המאגרים בקטלוג.

| # | בדיקה | צפוי | נמדד | תוצאה |
|---|---|---|---|---|
| 3a | כל שורה ב-`history_parts` קיימת ב-R2 | `missing = []` | audit (ran_at 05:24:01 UTC): `checked` 216, `missing []` | עבר |
| 3b | `rows_in_r2` לכל מאגר = סכום החלקים | שוויון | בכל 12 המאגרים מספר החלקים והשורות ב-`/history/files` שווה לקטלוג. סה"כ 216 חלקים, 76,050,935 שורות, 2,114,149,174 בתים, זהה ל-`totals`. `rows_only_in_r2` = 76,046,373 = סכום `rows_deleted` | עבר |
| 3c | מאגרים קרים: `history_tier=cold`, רק חלקים `full` | 9 מאגרים | 9/9 עם `history_tier: cold`, ו-214 החלקים שלהם כולם תחת `full_month=`, כולם `deleted_from_sql: true` | עבר |
| 3d | flydata `31c812a6`: חלק `closed` אחד, נמחק מ-SQL | 1 | חלק 1, `closed_month=2026-10`, 339,486 שורות, `deleted_from_sql: true`, `history_tier: hot`, חלון 30 | עבר |
| 3e | חישוב מחדש של `content_hash` (md5 של `row_hash` ממוינים ומחוברים בפסיק) ושל מספר השורות | שווים לקטלוג | 4 חלקים, ראו טבלה | עבר |

| חלק | סוג | שורות (קובץ / קטלוג) | hash מחושב | hash בקטלוג |
|---|---|---|---|---|
| `31c812a6/append_flydata_31c812a6/closed_month=2026-10/part-0001` | closed | 339,486 / 339,486 | `5e1787ff1e759abdf91876b4a20d4ee7` | זהה |
| `486c4e43/append_maflasmultiannual_486c4e43/full_month=2026-10/part-0001` | full | 160,243 / 160,243 | `6132d1da89d67b7d8b24c1b4481758c3` | זהה |
| `d8629820/append_mavat_doclinks_8c511bb0_d8629820/full_month=2026-10/part-0001` | full | 415,420 / 415,420 | `ec12b64e1fb2ed9a9e5f16a238d70018` | זהה |
| `2d893026/append_reshev_bitul_sofi_2d893026/full_month=2026-10/part-0001` | full | 500,000 / 500,000 | `078c542c8957209afe60bf9ccffb7e2a` | זהה |

בחלק ה-closed כל השורות עם `last_seen` (0 NULL); בשלושת חלקי ה-full כל השורות עם `last_seen` NULL,
כצפוי למאגר שהועבר שלם כשהיה פתוח.

**הערות:**
- `kind` של חלק אינו מתפרסם ב-`/history/files` ולא ב-GET האדמין. 3c אומת לפי קידומת המפתח
  (`full_month=` מול `closed_month=`), שנגזרת מאותו `kind` בקוד הכתיבה.
- במאגר `4f92c758` (מצפן השירותים, `hot`) יש חלק `full` עם `deleted_from_sql: false`, 4,562 שורות.
  זה שריד מתועד של בדיקת ה-rehydrate (תוכנית שלב 4, 6.8 צעד 6), לא סטייה.
- `2c9bf587` (מאגר הטיסות השני, חלון 30): 0 חלקים, 351,461 שורות סגורות ב-SQL מ-15.10. צפוי:
  הייצוא הראשון האמיתי שלו ב-3.11.

## 4. המנוע (DuckDB) והניתוב

| # | בדיקה | צפוי | נמדד | תוצאה |
|---|---|---|---|---|
| 4a | `GET /history/query-status` | available, httpfs ok, postgres ok, attach, threads 2, metadata_threads 8 | `enabled: true`, `available: true`, DuckDB 1.5.1, `httpfs: ok`, `postgres: ok` (checked_at 04:16:54), `hot_mode: attach`, `threads: 2`, `metadata_threads: 8`, `timeout_s: 30`, `memory_mb: 1536`, `queue: 3` | עבר |
| 4b | 9 מאגרים קרים: `count(*)` עם ובלי `include_history` = `rows_in_r2` | שוויון, engine duckdb בשני המקרים | 9/9 שווים בשתי הקריאות, `engine: duckdb`, `cold_parts` = מספר החלקים בקטלוג. הגשם 62,228,520 ב-6.1 / 5.1 שניות; השאר מתחת ל-1.1 שניות | עבר |
| 4c | `rain_forecast_stations` ב-`POST /api/tables/sql` עם `include_history` ו-token | 62,228,520 בתוך 30 שניות | 62,228,520, `engine: duckdb`, `cold_parts: 180`, 9.8 שניות (קר), 4.8 שניות (שני). בלי המתג: 0 + `history_note` שמפנה ל"כולל היסטוריה" | עבר |
| 4d | `GET /rows?as_of=2026-10-16` על flydata | engine duckdb | `engine: duckdb`, `cold_parts: 1`, total 2,166, 1.8 שניות | עבר |
| 4e | `as_of=2026-10-17T20:00:00Z` | נשאר ב-Postgres | אין שדה `engine`, total 2,252 | עבר |
| 4f | `current=true` = השורות הפתוחות | 2,090 | total 2,090, שווה ל-`count(*) WHERE last_seen IS NULL`; בלי פילטר 14,290 = 2,090 + 12,200 | עבר |

**הערה:** `stats` ב-query-status מראה 0 שאילתות, למרות כ-25 שאילתות DuckDB שהרצתי. המונים
והמצב הם בזיכרון של תהליך העובד שענה, ויש כמה עובדים. לכן אין להסיק מהם דבר.

## 5. מתזמן ודגלים

| # | בדיקה | צפוי | נמדד | תוצאה |
|---|---|---|---|---|
| 5a | מצב העבודה ב-`GET /history` | `as_of: null`, לא רץ | `job.running: false`, `as_of: null`, `failed: []`; `tier_job.running: false`; reopen ו-candidates job לא רצים | עבר |
| 5b | הייצוא היומי רשום ורץ | cron 03:30 UTC | לוג הריצה: `Added job "run"` ו-`Added job "audit_history_parts"` בעליות ב-20.10 18:57 וב-22.10 04:16. הריצות: 21.10 03:30:30 ו-22.10 03:30:33, שתיהן `finished, 0/0 jobs, 0 parts, 0 rows exported, 0 deleted, 0 failed`. `hour_utc: 3`, `min_rows: 100000`, `part_rows: 500000` | עבר |
| 5c | השפעת `HISTORY_EXPORT_ENABLED` ו-`HISTORY_QUERY_ENABLED` | עבודה רשומה, מנוע זמין | `enabled: true`, `available: true` בשני הצדדים; בלוג: `history query: httpfs=ok postgres=ok hot=attach` בכל עלייה | עבר |

**הערה:** בלוק `audit` ב-`GET /history` הראה `ran_at: null` מיד אחרי ש-`GET /history/audit` החזיר
`ran_at` 05:24:01: אותה תופעה של מצב לכל עובד בנפרד, ידועה.

## 6. תיעוד ציבורי

| # | בדיקה | צפוי | נמדד | תוצאה |
|---|---|---|---|---|
| 6a | עמוד /api החי (`index-Dzx-ePrt.js`) | as_of, current, include_history, /history/files, שדות ההיסטוריה ב-/schema | כל המונחים קיימים: `as_of` (18), `current=true` (3), `include_history` (7), `history/files` (2), `last_seen_column` (2), `supports_as_of` (2), `history_tier` (4), `rows_only_in_r2`, `files_url` | עבר |
| 6b | `docs/API.md` | אותו דבר | כל המונחים, כולל `{parts, rows_in_r2, rows_only_in_r2, from, to, files_url, queryable_via}` וסעיף `history_tier` | עבר |
| 6c | `GET /api/append/31c812a6-.../schema` מול התיעוד | אותם שדות | `last_seen_column: "last_seen"`, `supports_as_of: true`, `history: {parts 1, rows_in_r2 339486, rows_only_in_r2 339486, from, to, files_url, queryable_via: "duckdb"}`, `history_tier: "hot"` | עבר |

**הערה קטנה ל-6c:** תיאור `/schema` ב-`frontend/src/pages/ApiPage.tsx` מונה את שדות `history` בלי
`queryable_via` (שמופיע ב-`docs/API.md` ובתשובה). לא סטייה מהותית.

## 7. תוצאות הצמצום של 20.10

| # | בדיקה | צפוי | נמדד | תוצאה |
|---|---|---|---|---|
| 7a | גודל המסד | 55.20 GB אחרי 20.10 | `pg_database_size` 56,055,600,831 (56.06 GB) ב-05:44; דוח האחסון 56.04 GB ב-05:24 | תצפית |
| 7b | סך public | 44.6 GB אחרי 6.10 | 45.27 GB (pg_class) / 45.26 GB (דוח האחסון) | תצפית (חלק מ-7a) |
| 7c | `app.api_access_log` | 0.4 עד 0.6 GB בשמירה של 7 ימים | 515,784,704 בתים (0.52 GB), כ-994k שורות לפי האומדן, ו-`purge_api_access_log` רשום כ-cron לילי. `GET /access-log/compact` מחזיר רק null (ראו "לא אומת") | עבר |
| 7d | הטבלאות הקרות אחרי TRUNCATE | זעירות | 1.18 MB ל-72 טבלאות (2e) | עבר |

**תצפית 7a:** המסד גדל ב-0.85 GB תוך כיום וחצי מאז המדידה של 20.10, ומזה כ-0.66 GB ב-public. לא
בדקתי מה גדל. סביר שזו צמיחה רגילה (מקורות פעילים, 351k שורות סגורות שמצטברות ב-flydata
`2c9bf587` עד 3.11, לוג הגישה שחזר לגודל המאוזן), ולא חזרה של נפח שפונה. כדאי להשוות שוב אחרי
הריצה של 3.11. חומרה: מידע בלבד.

## 8. דברים שראיתי מחוץ לתחום (לא נספרו)

- **הפעלה מחדש ב-22.10:** תהליך האפליקציה נכבה בצורה מסודרת ב-04:13:38 UTC (`Shutting down`), ואז
  נכשל בעלייה חמש פעמים בין 04:16:01 ל-04:16:43 עם `ConnectionRefusedError` מול ה-DB, ועלה ב-04:16:48
  (מונה ההפעלות: 6). נראה כמו הפעלה מחדש של המארח או של ה-DB בצד xhostd. המשמעות להיסטוריה:
  כל המצב שבזיכרון (תוצאות compact, audit, מונים) התאפס.
- `knesset_db sync tick failed` כל 3 דקות מאז לפחות 03:36: `knesset.gov.il/OdataV4` מפנה ל-
  `maintenance-page-geo` (303). תחזוקה בצד הכנסת, לא קשור.
- `Connection pool is full ... r2.cloudflarestorage.com` (אזהרת urllib3) ב-04:12. אזהרה בלבד.

## 9. מה לא אומת ולמה

| פריט | סיבה |
|---|---|
| `before`/`after` של compact לוג הגישה (20.10) | `GET /access-log/compact` שומר את התוצאה בזיכרון, והתהליך עלה מחדש ב-22.10 04:16. כל השדות null. נסמכתי על הגודל הנוכחי (7c) ועל המספרים שבתוכנית (6.10) |
| `kind` של כל חלק בקטלוג | השדה לא מוחזר מאף GET; אומת לפי קידומת `r2_key` בלבד (3c) |
| query-smoke | `POST`, מחוץ לגבולות הבדיקה (ובכל מקרה עובד רק על טבלאות עם מראה Parquet) |
| `include_history` דרך MCP | לא נבדק בסשן הזה |
| הייצוא האמיתי הראשון (3.11) ומחיקה לפי החלון של `2c9bf587` | עוד לא קרה |
| טבלת `app.history_parts` ישירות ב-SQL | הסכימה `app` לא קריאה לתפקיד הציבורי; כל הבדיקות עברו דרך ה-API |
| התאמה מדויקת של מספר בדיקות ה-pytest לבסיס של שלב 4 (2,525) | ההרצה כאן היא על master עם בדיקות 6.10, עם `--deselect` ו-`--ignore`; המספרים לא מושווים אחד לאחד |
| `supports_as_of` במאגר ה-CBS | לא נבדק (2a/2b) |
