# שלב 4: תיקון סגירות שגויות, מאגרים קרים לגמרי, וחלונות למקורות דברנים

חלק מ-[history-storage-strategy.md](history-storage-strategy.md). ממשיך את
[שלב 1](history-phase1-plan.md) (תוקף שורה, חי), [שלב 2](history-phase2-plan.md)
(ייצוא לקר, הדגל כבוי עד 2026-11-03) ו-[שלב 3](history-phase3-plan.md) (מנוע DuckDB,
חי מ-2026-10-17). סדר העבודה מחייב: **להגדיר את הבדיקות, להריץ אותן לפני השינוי,
לשנות, להריץ שוב, ואין רגרסיה.**

## 0. ממצא דחוף שנמדד 2026-10-17, ושלב 4 מתחיל בו

**מקור שקיפות המחירים סוגר שורות בטעות מאז שהדגל `APPEND_CLOSE_ROWS_ENABLED`
הודלק (2026-10-15 21:28 UTC).** ה-opt-out `append_closes_rows: False` נוסף ב-GOVSCRAPER
`b334fed` ל-`default_config` של המניפסט, אבל `default_config` נכנס ל-`scraper_config`
רק כשמאגר **נוצר** (`app/services/source_registry.py:468`), ו-33 מאגרי המחירים
(196 טבלאות) נוצרו לפני כן. לכן `should_close` רואה `True`, המקור מפרסם רק שינויים,
כל push הוא "מעבר מלא" בעיני ה-loader, ו-`allow_shrink: True` במניפסט עוקף את סף
ה-50%.

| טבלה | סגורות | סה"כ | מתי |
|---|---|---|---|
| קרפור, מחירים (f240d9c5_2e0a7e7b_ad8971b7) | 4,446,985 | 4,455,384 | 3.68M ב-15.10, 743k ב-16.10, 27k ב-17.10 |
| 5faadb05 מחירים | 2,695,395 | 2,710,014 | |
| 8e92d86c מחירים | 2,515,267 | 2,524,524 | |
| b9dd704c מחירים | 1,280,606 | 1,349,079 | |

בקרפור יש גם 7,445 `row_hash` שמופיעים פעמיים (שורה סגורה + שורה פתוחה חדשה עם
אותו תוכן): זו תוצאת הסגירה השגויה ואחריה חזרה של אותו מצב ב-push הבא.

**השלכות:** `current=true` ו-`as_of` על המחירים שגויים; ב-3.11 שלב 2 היה מייצא
מיליוני שורות "סגורות" מאוקטובר לקבצי Parquet (בלי מחיקה, כי אין חלון, אבל מיותר
ומטעה). שום שורה לא אבדה: `last_seen` הוא שדה שמתווסף, והתיקון הפיך.

**התיקון (צעד 0 של השלב, לפני כל דבר אחר):**
1. **המניפסט מנצח גם במאגר קיים**: `should_close` (ואיתו `stamp_column`, שכבר
   עושה זאת ל-sampling) קורא את `append_closes_rows` מה-`default_config` של
   המניפסט דרך `source_registry` כשהמפתח חסר ב-`scraper_config`. אותו עיקרון
   שמתועד ב-`sampling_runs.sampling_spec`: "המניפסט מנצח כשיש כזה".
2. **כלי תיקון באדמין**: `POST /api/admin/storage/closures/reopen?source=prices&dry_run=1`
   לכל טבלאות מקור: (א) מוחק את הכפילות: לכל `row_hash` עם שורה סגורה ושורה
   פתוחה, מוחק את הפתוחה (החדשה) ומשאיר את הסגורה עם ה-`first_seen` המקורי;
   (ב) `UPDATE ... SET last_seen = NULL WHERE last_seen IS NOT NULL` בקבוצות של 50k
   לפי ctid; (ג) `VACUUM (ANALYZE)`. `dry_run` מחזיר ספירות בלבד. מוגבל למקורות
   שה-`append_closes_rows` האפקטיבי שלהם הוא False (אין מה "לפתוח מחדש" במקור
   שסוגר כדין). רושם ב-activity log.
3. **הגנה שלישית:** `close_guard` מסרב גם כשה-pass קטן מ-`HISTORY_CLOSE_MIN_PASS_FRACTION`
   (ברירת מחדל 0.2) מהשורות הפתוחות, **גם עם `allow_shrink`**: מעבר "מלא" שמביא
   פחות מחמישית מהשורות הפתוחות הוא כמעט תמיד מקור שמפרסם רק שינויים, לא מחיקה
   המונית. `allow_shrink` נשאר רלוונטי לסף ה-50% (lo לוח הטיסות הביא 2,227 מתוך
   338k, כלומר 0.7%: לכן הסף החדש מוחל רק כש-`append_closes_rows` לא נקבע במפורש
   ב-`scraper_config` או במניפסט; flydata מקבל `append_closes_rows: true` מפורש).
4. אימות ב-prod אחרי התיקון: `count(*) FILTER (WHERE last_seen IS NOT NULL)` = 0
   בכל 196 הטבלאות, אין `row_hash` כפול פתוח, וה-push הבא של כל רשת לא סוגר
   (`change_summary.closed` נעדר או `close_skipped` עם הסיבה).

## 1. המטרה של שאר השלב

אחרי שלושת השלבים הקודמים, הנפח שעוד יושב ב-SQL בלי סיבה הוא משני סוגים:

- **מאגרים קרים לגמרי**: טבלאות שאיש לא שואל ולא מתעדכנות. הדוגמה הגדולה: תחזית
  הגשם (ds d3d20a89, 60 טבלאות, 62M שורות, 8.9 GB אחרי האינדקסים החלקיים), שנזרעה
  פעם אחת. שורותיהן **פתוחות** (לא נסגרו, כי המקור לא נפולת), ולכן שלב 2 לא נוגע בהן.
- **מקורות דברנים**: שורות שנסגרות בקצב גבוה (לוח הטיסות, מרשם הרכב כשתיקוני כתיב
  פותחים שורות חדשות, והמחירים אחרי התיקון, אם יוחלט שהם סוגרים). שלב 2 מטפל
  בהן, אבל רק למאגר שקיבל חלון ידנית.

בסוף השלב:

1. **העברה מלאה לקר** (`history_tier: "cold"`): כל שורות הטבלה, פתוחות וסגורות,
   יוצאות ל-Parquet כחלק מסוג `full`, מאומתות, נמחקות, והטבלה ב-SQL נשארת ריקה
   עם אותה סכימה (כדי ש-`list_tables`, האוטוקומפליט ו-`/schema` ימשיכו לעבוד).
   כל קריאה למאגר כזה מנותבת למנוע ההיסטוריה גם בלי המתג.
2. **החזרה ל-SQL** (`rehydrate`): הפעולה ההפוכה, מאומתת, שרצה לבד לפני poll חדש
   של מאגר קר (אחרת ה-loader היה מכפיל שורות פתוחות).
3. **כלי המלצה ומדיניות חלונות**: טאב נפח SQL מציג לכל מאגר כמה שורות סגורות
   מעל 90 יום ומה ייחסך, עם החלת חלון בלחיצה ובאצווה; רשימת "מועמדים לקר"
   לפי `version_index` (לא נשאל ב-`api_access_log` ולא התעדכן N ימים) עם העברה
   בלחיצה.
4. **סגירת הפער משלב 2/3**: חלק שנמחק חלקית (`deleted_from_sql=false` אבל חלק
   משורותיו כבר נמחקו) נכנס לתצוגת ההיסטוריה עם anti-join על `(ident, last_seen)`
   מול החם, כך ששום שורה לא נעלמת לחודש.

## 2. מודל הנתונים והכללים

- `app.history_parts.kind`: `'closed'` (ברירת המחדל, שלב 2) או `'full'`. חלק `full`
  מכיל גם שורות עם `last_seen IS NULL`, ו-`closed_month` שלו הוא חודש ההעברה.
- `scraper_config.history_tier`: חסר/`"hot"` או `"cold"`. נקבע רק על ידי האדמין.
- **תנאי להעברה לקר**: המאגר במצב `paused` או עם `poll_interval` מעל 30 יום,
  **או** שהאדמין מאשר במפורש שהמאגר יוחזר ל-SQL לפני ה-poll הבא (ה-poll עצמו
  מפעיל `rehydrate` ומחכה לו). שני המסלולים מתועדים בכרטיס.
- **תצוגת ההיסטוריה למאגר קר**: רק החלק `full` (והחלקים `closed` שנמחקו, אם יש),
  בלי הצד החם (ריק). `as_of`/`current` על אותה תצוגה. `rows_only_in_r2` = כל
  השורות.
- **תחזית הגשם**: ה-view `rain_forecast_stations` (`union_views.py`) נשאר ב-Postgres
  על 60 טבלאות ריקות ומחזיר 0; המנוע מקבל view מאוחד באותו שם מעל 60 החלקים
  `full` (העמודה `station` נגזרת מהשם כמו היום). השאילתות הקיימות בעמוד /data
  שמפנות אליו מקבלות את ההערה "דורש כולל היסטוריה".
- **חלונות**: `history_window_days` נשאר ידני לכל מאגר, אין ברירת מחדל גלובלית.
  כלי ההמלצה מציג ומחיל, לא מחליט. אצווה: `PATCH /api/admin/storage/history/windows`
  עם רשימת `(dataset_id, days)`.

## 3. מה משתנה בקוד (רשימה סגורה)

1. `app/services/append_store.py`: `should_close` קורא את המניפסט דרך
   `source_registry` כשהמפתח חסר; `close_guard` מקבל `min_pass_fraction` עם הכלל
   מסעיף 0.3; `reopen_closures(table, *, dry_run)` (כפילויות + NULL + VACUUM).
2. `app/api/admin_storage.py`: `POST /closures/reopen` (לפי `source` או `dataset_id`,
   `dry_run`), `GET /history/candidates` (מועמדים לקר + המלצות חלון),
   `PATCH /history/windows`, `POST /history/cold/{dataset_id}` ו-`POST /history/rehydrate/{dataset_id}`
   (שניהם in-process state כמו `history_export.run`).
3. `alembic/versions/073_history_parts_kind.py`: עמודת `kind` עם ברירת מחדל `'closed'`.
4. `app/services/history_export.py`: `export_full(dataset, table)` (אותו writer, אותו
   אימות, `kind='full'`, מחיקה של **כל** השורות לפי זוגות `(ident, last_seen)` עם
   NULL-safe), `rehydrate(dataset, table)` (קריאת החלקים מ-R2, COPY לטבלה, אימות
   ספירה ו-hash מול הקטלוג, ואז סימון החלקים `deleted_from_sql=false` ומחיקת
   `history_tier`), ו-`plan` שמדלג על מאגר `cold`.
5. `app/services/history_query.py`: `cold_parts_for` מחזיר גם חלקים `full`, ואת
   החלקים `closed` שנמחקו חלקית עם anti-join; `build_history_view` ללא צד חם
   למאגר `cold`; view מאוחד לתחזית הגשם.
6. `app/api/append.py` ו-`app/api/tables.py`: מאגר `cold` מנותב למנוע בלי המתג,
   `/schema` מפרסם `history_tier`; `app/worker/poll_job.py` ו-`app/api/worker.py`:
   לפני טעינה למאגר `cold`, `rehydrate` חובה (או סירוב עם הודעה כשהאדמין לא
   אישר).
7. `app/services/archive_state.py`: `fidelity = "cold"` למאגר `cold`.
8. frontend: בטאב נפח SQL כרטיס "סגירות שגויות" (dry-run + תיקון), כרטיס
   "מועמדים לקר" ו"המלצות חלון" עם החלה בלחיצה ובאצווה; בעמוד המאגר תג "קר
   (R2)" והסבר; בכרטיס המאגר באדמין כפתור "העבר לקר" / "החזר ל-SQL".
9. `docs/API.md`.

**לא בשלב הזה:** שינוי ברירת המחדל של חלונות; מחיקת טבלאות מ-SQL (הן נשארות
ריקות); מקורות חדשים.

## 4. סדרת הבדיקות

### 4.1 בסיס (לפני כל שינוי, לשמור את הפלט)

```bash
python -m pytest -p no:cacheprovider -q tests
```

```bash
cd frontend && npx tsc --noEmit -p . && npx vitest run
```

בסיס על master `d32445a` (2026-10-17): 2,465 עוברות, 6 מדולגות; tsc נקי; vitest 13.
למדוד שוב ב-worktree (עם junctions ל-`frontend/dist` ו-`frontend/node_modules`
כמו הסשנים הקודמים) ולהשוות מולו.

אין Postgres מקומי. DuckDB זמין (1.5.1). בדיקות המסד הן בוני SQL טהורים ו-monkeypatch;
המנוע נבדק על DuckDB אמיתי עם Parquet בקבצים זמניים.

### 4.2 בדיקות חדשות (לכתוב לפני הקוד, לראות אותן נכשלות)

`tests/test_history_phase4.py`:

1. `should_close`: מאגר בלי `append_closes_rows` ב-`scraper_config` שהמניפסט שלו
   אומר False → False; מאגר שהמניפסט אומר True והקונפיג False → False (הקונפיג
   של המאגר מנצח כשהוא קיים); בלי מניפסט → True כמו היום.
2. `close_guard` עם `min_pass_fraction`: pass של 1% מהפתוחות ו-`allow_shrink` →
   מסורב כשאין הצהרה מפורשת; עם `append_closes_rows: true` מפורש → עובר (flydata).
3. `reopen_closures` SQL: מחיקת הכפילות שומרת את השורה עם `first_seen` המוקדם;
   ה-UPDATE בקבוצות לפי ctid; `dry_run` לא מייצר UPDATE/DELETE; מקור עם
   `append_closes_rows` אפקטיבי True מסורב.
4. `export_full`: חלק `kind='full'` מכיל שורות עם `last_seen` NULL; האימות זהה לשלב
   2; המחיקה מתאימה NULL (`IS NOT DISTINCT FROM`), לא `=`.
5. `rehydrate`: קורא חלקים `full` + `closed` מחוקים, טוען, מאמת ספירה ו-hash מול
   הקטלוג, ורק אז מסמן `deleted_from_sql=false` ומסיר `history_tier`; כשל באימות
   = לא נוגע בקטלוג ומנקה את הטעינה.
6. תנאי ההעברה: מאגר `active` עם poll של 7 ימים בלי אישור מפורש → מסורב.
7. `plan` של שלב 2 מדלג על מאגר `cold`.
8. מנוע: תצוגה למאגר `cold` בלי צד חם; `rain_forecast_stations` מאוחד מעל 60 חלקים
   עם `station`; anti-join לחלק שנמחק חלקית (DuckDB אמיתי: 2 שורות שנמחקו מהחם
   מתוך חלק של 5 מופיעות פעם אחת בדיוק).
9. ניתוב: מאגר `cold` עובר למנוע בלי המתג; `/schema.history_tier`.
10. poll על מאגר `cold`: בלי אישור → 409 עם הודעה; עם אישור → `rehydrate` נקרא לפני
    הטעינה (monkeypatch).
11. `history/candidates`: מאגר שלא התעדכן 180 יום ולא נשאל ב-`api_access_log` 90 יום
    מופיע; מאגר עם גישה אתמול לא; המלצת חלון מחשבת שורות סגורות מעל 90 יום
    ו-bytes משוערים.
12. `PATCH /history/windows`: מאמת (None או ≥30) לכל פריט, אטומי.
13. `archive_state`: `cold`.
14. הקיימות של שלבים 1 עד 3 עוברות ללא שינוי.

### 4.3 אימות על prod

**צעד 0 (ראשון, דחוף):**
1. deploy. `POST /closures/reopen?source=prices&dry_run=1`: ספירות לכל 196 הטבלאות
   (צפוי: כ-20M+ שורות סגורות, כ-7.4k כפילויות בקרפור).
2. הפעלה אמיתית. אימות SQL: 0 שורות סגורות ב-196 הטבלאות, 0 `row_hash` כפול פתוח,
   ספירת השורות ירדה בדיוק במספר הכפילויות.
3. ה-push הבא של קרפור (07:00 שעון ישראל): `change_summary` בלי `closed`, או
   `close_skipped` עם "opted out"; `count(*) FILTER (WHERE last_seen IS NOT NULL)`
   נשאר 0.

**שאר השלב (הדגלים הקיימים נשארים כפי שהם):**
4. `history/candidates`: תחזית הגשם ברשימה; המאגר מועבר ל-`paused` (אם אינו), ואז
   `POST /history/cold/{d3d20a89}`: 60 חלקים `full`, 62.2M שורות, אימות עובר, הטבלאות
   ריקות, `pg_database_size` יורד אחרי VACUUM (צפוי כ-8 GB פחות שימוש פנימי; שטח
   הדיסק של המערכת חוזר רק אחרי VACUUM FULL, שלא רץ).
5. `/data`: `SELECT count(*) FROM rain_forecast_stations` עם ובלי המתג (בלי: 0 והערה;
   עם: 62.2M דרך המנוע), ושאילתת תחנה אחת.
6. `rehydrate` על **מאגר קטן** אחד שהועבר לקר לצורך הבדיקה (לא הגשם): ספירה ו-hash
   חוזרים זהים, `history_tier` נמחק, poll אחרי זה עובד.
7. המלצות חלון: החלת 30 יום על שני מאגרי flydata (שלב 2 צעד 4) דרך ה-PATCH; הייצוא
   הראשון ב-3.11 מאמת את הכול יחד עם שלב 2 ושלב 3.

### 4.4 סיום השלב

- 4.1 באותם מספרים כמו הבסיס, 4.2 ירוק, 4.3 צעדים 0 עד 6 בוצעו ותועדו בסעיף 6.
- דיפלוי ל-prod דורש אישור המשתמש; צעד 0 מופעל רק באישור מפורש אחרי ה-dry-run.
  קומיט לפי pathspec. בלי UNLOGGED. `app/config.py` מתנגש ב-CRLF בעת cherry-pick
  לריפואים המרוחקים.

## 5. סיכונים

- **תיקון הסגירות מוחק שורות** (הכפילויות): רק את השורה הפתוחה החדשה מבין שתיים
  עם אותו `row_hash`, והיא עותק מדויק של הסגורה. ה-dry-run מציג את המספרים קודם.
- **העברה לקר של מאגר פעיל** תכפיל שורות ב-poll הבא: לכן התנאי בסעיף 2 ו-rehydrate
  חובה לפני טעינה.
- **הגשם**: 62M שורות ייצוא = כשעה-שעתיים ו-60 חלקים; רץ כ-job in-process שניתן
  לעצור ולחדש (כל טבלה היא יחידה).
- **הפער של חלקים חלקיים** נסגר ב-anti-join, שמייקר מעט את שאילתות ההיסטוריה
  על טבלאות עם חלק כזה; זמני, עד שהחלק מסומן.

## 6. תוצאות

**2026-10-17, הסשן המבצע (worktree `claude/history-phase4-cold-8bfd8f`).**
לא בוצע deploy, לא בוצע push, ולא הורץ פרוטוקול 4.3.

### 6.1 קומיטים

| ריפו | SHA | תוכן |
|---|---|---|
| OVER | `8d5c599` | סעיף 0 בלבד: `closes_rows_setting` / `should_close` מהמניפסט, `close_guard` עם `min_pass_fraction`, SQL ופונקציית `reopen_closures`, `POST/GET /closures/reopen`, `declared=` בשלושת נתיבי הסגירה |
| OVER | (הקומיט שאחריו) | שאר סעיף 3 + `tests/test_history_phase4.py` (60 בדיקות) + סעיף 6 |

סעיף 0 קומט בנפרד כדי שהסשן המתכנן יוכל לפרוס אותו לבד (הוא דחוף); שאר השלב בקומיט נפרד על אותו ענף.

### 6.2 בסיס לפני השינוי

`frontend/dist` חובר כ-junction מה-checkout הראשי ו-`frontend/node_modules` מה-worktree של שלב 3 (שבו `vitest` מותקן).

```
python -m pytest -p no:cacheprovider -q tests
2465 passed, 6 skipped, 400 warnings in 449.92s (0:07:29)
npx tsc --noEmit -p .   → נקי
npx vitest run          → 13 passed
```

זהה לבסיס של סעיף 4.1.

### 6.3 הבדיקות החדשות לפני הקוד

`tests/test_history_phase4.py` נכתב לפני כל שינוי קוד: `45 failed, 13 errors, 2 passed` (60 בדיקות). שתי העוברות
הן בדיקות רגרסיה שאמורות לעבור גם לפני (קונפיג המאגר מנצח; חלק שלא נמחק לא נכנס לתצוגה). תיקונים בבדיקה
עצמה אחרי שנכתבה: (א) ה-fake connection זיהה UPDATE של פתיחה לפי המילה `NULL`, שמופיעה גם ב-SQL של העברת
ה-stamp (`IS NOT NULL`), ולכן זוהה לפי `SET "last_seen" = NULL`; (ב) "חלק שנמחק חלקית" מוגדר לפי חלון (6.5.5), ולכן
ה-fixture מקבל `window_days` במקום `deleted_at`.

### 6.4 אחרי השינוי

```
python -m pytest -p no:cacheprovider -q tests
2525 passed, 6 skipped, 392 warnings in 630.97s (0:10:30)
npx tsc --noEmit -p .   → נקי
npx vitest run          → 13 passed
```

2,525 = 2,465 של הבסיס + 60 החדשות. ריצה חוזרת על העץ הסופי (אחרי ה-endpoints של האדמין וה-boot hook) החזירה
`2522 passed, 3 failed`: שלוש בדיקות SEO, כי `frontend/dist` של ה-checkout הראשי (שה-junction הצביע אליו) התרוקן
ב-14:26 מחוץ לסשן הזה. אחרי `npx vite build` מקומי ב-worktree (בלי junction): `tests/test_seo.py` 30 passed, כלומר
2,525 עוברות על העץ הסופי, וה-build עצמו עובר עם קוד הפרונט החדש. שינוי אחד בבדיקה קיימת: `test_history_export::test_the_model_matches_the_plan`
מונה את עמודות המודל, ו-`kind` (alembic 073) נוספה לרשימה. זה הרחבה של רשימה, כמו `APP_TABLES` בשלב 2.

### 6.5 סטיות מהתוכנית ותוספות

1. **כלי הפתיחה מחדש רץ כ-job ברקע** (גם `dry_run`): 196 טבלאות ו-20M שורות לא נכנסים לבקשת HTTP. `POST` מחזיר
   `started`, ו-`GET /closures/reopen` מחזיר את הספירות לכל טבלה (`closed`, `duplicates`, ואחרי ריצה אמיתית
   `deleted`, `reopened`, `remaining`) ו-`totals`. `dry_run` הוא ברירת המחדל.
2. **הכפילות מוגדרת לכל זהות שיש לה שורה סגורה**, לא רק לזוג סגורה+פתוחה: נשמרת השורה עם `first_seen` המוקדם
   ונמחקות שאר השורות של אותה זהות. אחרת זהות שנסגרה פעמיים (סגורה, חזרה, נסגרה) הייתה מתנגשת באינדקס החלקי
   כששתיהן נפתחות. בדיקה על DuckDB אמיתי מוכיחה את שני המקרים.
3. **ה-stamp עובר לשורה שנשארת**: אם לעותק החדש יש `last_seen_date` (המצב הסתיים), השורה המקורית מקבלת את ה-stamp
   הלא-ריק החדש ביותר של הקבוצה, כדי שהמחיקה לא תאבד את תאריך הסיום.
4. **ה-UPDATE של הפתיחה מדלג על זהות שכבר פתוחה** (push שנחת בזמן הריצה), ועד 3 סבבים של ניקוי+פתיחה. מה שנשאר
   סגור מדווח ב-`remaining`.
5. **"חלק שנמחק חלקית" נגזר מהחלון ולא מעמודה חדשה**: חלק `closed` שלא סומן, למאגר עם חלון, שה-`last_seen_min`
   שלו ישן מהחלון. ה-anti-join נכון תמיד (מוסיף רק שורות שחסרות ב-SQL), כך שהתנאי רק מחליט מתי לשלם עליו. אין
   צורך לשנות את `delete_exported` ואין עמודה נוספת מעבר ל-`kind`.
6. **הקונסולה ב-/data לא מנתבת מאגר קר בלי המתג**, לפי 4.3 צעד 5 ("בלי: 0 והערה"); הנוסח של 3.6 ("מנותב למנוע
   בלי המתג") מומש ב-`/api/append/{id}/rows|datastore_search|sql`. ההערה בקונסולה נשענת על סט בזיכרון
   (`history_query.cold_tables()`, נטען בבוט ואחרי כל העברה), כך ששאילתה רגילה עדיין לא קוראת את הקטלוג.
7. **מאגר קר מסומן `history_tier=cold` לפני הייצוא**, כך ששום poll לא טוען לטבלה שמתרוקנת. טבלה שנכשלה נשארת חמה
   ונקראת כחמה: המנוע מחליט לכל טבלה לפי הקטלוג (חלק `full` מחוק = בלי צד חם), לא לפי הדגל של המאגר.
8. **כל טבלה מועברת בטרנזקציה אחת עם `LOCK ... SHARE ROW EXCLUSIVE`**: קריאה, כתיבה ל-R2, קריאה חוזרת, אימות
   מול SQL, רישום בקטלוג ומחיקה לפי זוגות, ורק אחרי commit סימון `deleted_from_sql=true`. קריסה בין ה-commit לסימון
   מתוקנת בהרצה הבאה (חלקים רשומים + טבלה ריקה = סימון בלבד). האימות לטבלה שלמה (ספירה ו-md5 של כל הזהויות),
   לא לכל part, כי מיון `(last_seen, ident)` של שלב 2 לא מתאים לשורות עם `last_seen` NULL.
9. **טבלה עם PostGIS או עמודת מקור לא-טקסט מסורבת לקר**: הקבצים לא נושאים גאומטריה ושומרים מחרוזות, וההחזרה
   ל-SQL לא הייתה משחזרת אותן.
10. **מפתח קובץ `full_month=YYYY-MM`** במקום `closed_month=`, כדי שלא יתנגש בחלק סגור של אותו חודש.
11. **שער ה-poll**: `history_export.cold_poll_gate` רץ ב-`poll_job._poll_dataset` (מדלג עם לוג, כדי לא ליצור משימה
    שתיגמר ב-409) וב-`worker.push_version` (409 עם הסבר בעברית), שניהם לפני כל טעינה. עם אישור מפורש
    (`history_rehydrate_on_poll`) ההחזרה ל-SQL רצה שם ומחכים לה.
12. **מועמדים לקר**: גישה נמדדת ב-`api_access_log.target` (מזהה המאגר בנתיבי `/api/append/{id}/…`); שאילתות
    בקונסולה הכללית לא נספרות כגישה. "שורות סגורות מעל 90 יום" נספרות רק לטבלה שהערכת `pg_stats` שלה מעל 10k.
13. **`DatasetResponse.history_tier`** נוסף (לכפתור "העבר לקר" / "החזר ל-SQL" בכרטיס המאגר), ו-`fidelity="cold"`
    גובר על `tiered`. צ'יפ חדש "קר (R2)".
14. **לא נבדק מקומית** (אין Postgres): `PgTable` (LOCK, cursor, COPY עם המרת uuid, DELETE ... USING unnest),
    `candidates` מול pg_class, ו-`union_branches` מול מסד האפליקציה. כל אלה נבדקים בצעדים 4-6 של 4.3. הפרונט
    לא נבדק בדפדפן (אין backend מקומי); tsc ו-vitest נקיים.
15. **MCP `run_sql`** לא קיבל את ההערה על טבלה קרה (לא ברשימה הסגורה של סעיף 3); `include_history` שם ממשיך לעבוד
    כמו בשלב 3 ומנתב גם טבלה קרה.

### 6.6 מה נשאר לאשר (פרוטוקול 4.3, לא בוצע)

**צעד 0 (קומיט `8d5c599` לבד אפשרי):**
1. deploy. `POST /api/admin/storage/closures/reopen?source=prices&dry_run=1`, ואז `GET` על אותו נתיב עד
   `running:false`: `totals.closed` (צפוי כ-20M+), `totals.duplicates` (קרפור כ-7.4k), `refused` ריק או מוסבר.
2. באישור מפורש: `POST ...?source=prices&dry_run=0` (או "פתח מחדש" בכרטיס "סגירות שגויות"). אימות SQL: 0 סגורות
   ב-196 הטבלאות, 0 `row_hash` כפול פתוח, `count(*)` ירד בדיוק ב-`totals.deleted`, `remaining` 0 בכל טבלה.
3. ה-push הבא של קרפור: `change_summary` בלי `closed`, או `close_skipped`; הסגורות נשארות 0.

**שאר השלב (אחרי deploy של הקומיט השני; alembic 073 רץ בבוט):**
4. `GET /history/candidates` (כרטיס "מועמדים לקר", "חשב"): תחזית הגשם ברשימה. השהיית המאגר, ואז
   `POST /history/cold/d3d20a89-5c41-4b49-a7a8-cc1c73e070f3`. מעקב ב-`GET /history` (`tier_job`): 60 טבלאות,
   כ-62.2M שורות, 0 כשלונות; הטבלאות ריקות; `pg_database_size` אחרי VACUUM.
5. `/data`: `SELECT count(*) FROM rain_forecast_stations` בלי המתג (0 + `history_note`) ועם המתג (62.2M, `engine: duckdb`),
   ושאילתת תחנה אחת.
6. `rehydrate` על מאגר קטן שהועבר לקר לבדיקה: `POST /history/rehydrate/{id}`; ספירה ו-hash זהים, `history_tier` נמחק,
   poll אחר כך עובד.
7. חלון 30 יום לשני מאגרי flydata דרך `PATCH /history/windows` (`{"items":[{"dataset_id":…,"days":30},…]}`).


### 6.7 צעד 0 על prod (2026-10-17, הסשן המתכנן)

| צעד | תוצאה |
|---|---|
| deploy | xhostd `ffa44ca` (GitHub `a62081e`, צעד 0 בלבד) עלה 11:32 UTC. |
| flydata | `append_closes_rows: true` נכתב ל-scraper_config של שני מאגרי flydata (PATCH 200), כדי שסף ה-20% לא יעצור את הסגירה המוצהרת שלהם. |
| dry-run | 34 מאגרים (33 רשתות + עמוד הרגולטור), 197 טבלאות: 32,767,344 שורות סגורות, 175,613 עותקים כפולים; 2 טבלאות של מאגר ac68e67a דולגו ("no partial dedup index", אין להן אינדקס כפילויות כלל ולכן גם לא סגירות). |
| ריצה אמיתית | 11:4x עד 11:59 UTC: 32,694,680 שורות נפתחו מחדש, 175,613 עותקים נמחקו, 0 נשארו סגורות, 0 כשלונות (מלבד שתי הטבלאות הנ"ל). |
| אימות SQL | על כל 196 הטבלאות (query_to_xml): 0 שורות עם `last_seen IS NOT NULL`, 0 `row_hash` כפול בין שורות פתוחות. |
| ה-push הבא | ייבדק אחרי 07:00 שעון ישראל ב-18.10: `change_summary` בלי `closed`. |

### 6.8 צעדים 4 עד 7 על prod (2026-10-17, הסשן המתכנן)

| צעד | תוצאה |
|---|---|
| deploy | xhostd `e5f92f0` (GitHub `29e7396`) עלה 12:18 UTC, `Running upgrade 072 -> 073`. |
| 4. מועמדים + גשם לקר | `candidates` ריק (הגשם קיבל גרסה ב-2.10, לא "ישן" לפי 180 יום; אף מאגר עדיין בלי שורות סגורות מעל 90 יום). ההעברה הופעלה ישירות (poll של 90 יום עומד בתנאי): 60 טבלאות, 62,228,520 שורות, 180 חלקים `full`, 1.50 GB ב-R2 (מול 8.9 GB ב-SQL), 0 כשלונות, כ-25 דקות. SQL: 60 טבלאות ריקות, `rain_forecast_stations` = 0, המסד 61 → 58 GB. |
| 5. קונסולה | בלי המתג: 0 + `history_note` שמסביר שהטבלה בשכבה הקרה. עם המתג על **טבלה אחת** (3 חלקים): 1,037,142 שורות ב-2.3 שניות (`engine: duckdb`). עם המתג על ה-view המאוחד (180 קבצים): **timeout 30 שניות**, גם ל-`count(*)` וגם ל-`GROUP BY station`. ממצא לטיפול: איחוד של 180 קבצים ב-httpfs חורג מהתקציב; פתרון אפשרי: ספירות מקובצות מהקטלוג ל-`count(*)`, timeout גבוה יותר לאדמין, או איחוד החלקים של מאגר קר לקובץ אחד לטבלה. |
| 6. rehydrate | מצפן השירותים הממשלתיים (4f92c758, 4,562 שורות): cold ב-שניות (חלק `full` אחד), rehydrate: 4,562 שורות חזרו, `history_tier: hot`, החלק נשאר בקטלוג עם `deleted_from_sql=false`, `rows_only_in_r2: 0`. |
| 7. חלונות | `PATCH /history/windows`: 30 יום לשני מאגרי flydata (200). הייצוא הראשון ב-3.11. |

### 6.9 פער הביצועים של ה-view המאוחד (2026-10-17, סשן נפרד)

**המדידה** (DuckDB 1.5.1, 2 threads, מהמחשב המקומי מול החלקים האמיתיים ב-R2; ה-latency
ל-R2 דומה לזו של prod: טבלה אחת של 3 חלקים ענתה שם ב-2.3 שניות, כאן ב-2.2):

| מה | זמן |
|---|---|
| footer של קובץ אחד (HEAD + GET אחד לכל קובץ, `EXPLAIN ANALYZE`) | 0.2–0.35 שניות |
| `DESCRIBE read_parquet` של 180 קבצים, `union_by_name` | 35 שניות |
| `count(*)` של 180 קבצים בקריאה אחת, חיבור חדש | 26 שניות |
| אותו דבר ב-8 threads | 6.6–8 שניות |
| 60 קבצים (קובץ אחד לטבלה) ב-2 threads | 8.9 שניות |
| `parquet_metadata_cache` לבד: מעבר שני על 180 | 14 שניות (ה-HEAD נשאר) |
| `parquet_metadata_cache` + `enable_http_metadata_cache`: מעבר שני ואילך | 0.04–0.3 שניות |
| ה-pipeline של המנוע כמו שהיה (`_execute` על 60 טבלאות + view), `count(*)` | **177 שניות** |
| אותו pipeline אחרי התיקון, `count(*)` | **7.8 שניות** (מהן 7.0 מעבר ה-footers) |
| אחרי התיקון, `GROUP BY station` | **9.7 שניות** |

הזמן כולו הוא latency של HTTP לכל קובץ, ולא נתונים: `count(*)` ו-`GROUP BY station`
(התווית היא ליטרל לכל ענף) לא קוראים אף עמודה. המנוע קרא את 180 ה-footers כחמש פעמים
בכל שאילתה, סדרתית, טבלה-טבלה: `DESCRIBE read_parquet` לכל טבלה ב-`_prepare_views`,
`DESCRIBE` של כל תצוגה, של ה-view המאוחד, של השאילתה העטופה, והשאילתה עצמה; בלי cache
כל bind חוזר ל-R2. threads=2 הגביל גם את המקביליות של קריאת ה-footers בתוך קריאה אחת.
ה-casts בתצוגה לא משתתפים (count בלי WHERE הוא מטא-דאטה בלבד).

**התיקון** (`app/services/history_query.py`, הקטן ביותר מבין האפשרויות ב-6.8):
1. `SET parquet_metadata_cache = true` ו-`SET enable_http_metadata_cache = true` על החיבור
   (חיבור חדש לכל שאילתה, כך שה-cache לא שורד מעבר לחלקים שראה).
2. `_warm_metadata`: לפני התצוגה הראשונה, מעבר אחד על כל חלק של כל טבלה שהשאילתה נוגעת בה
   (`parquet_file_metadata([...])`) ב-`METADATA_THREADS` = 8; מיד אחריו `threads` חוזר ל-2,
   לפני `_lock`. המעבר הוא המתנות HTTP ולא CPU, ו-`memory_limit` חל עליו כרגיל.
   השאילתה עצמה רצה ב-2 threads כמו קודם; timeout ו-queue לא השתנו.
3. `status()` מדווח `threads` ו-`metadata_threads`.

לא שונה: גודל החלקים (`HISTORY_PART_ROWS`), הקטלוג, קבצי ה-Parquet, הפרונט. בדיקות:
`test_parquet_and_http_metadata_are_cached_for_the_connection`,
`test_warm_metadata_reads_every_part_then_restores_two_threads` (test_history_query.py),
`test_the_union_view_warms_every_part_once_before_any_bind` (test_history_phase4.py).

**מה נשאר**: (א) deploy ואימות בקונסולה עם המתג על `rain_forecast_stations`: `count(*)` ו-
`GROUP BY station` צפויים ב-8 עד 10 שניות, ו-`cold_parts: 180`. (ב) שאילתה שקוראת עמודה
על כל 62M השורות (למשל `WHERE pr <> ''`) עדיין מורידה את העמודה מ-R2: 84 שניות ב-2
threads, מעבר לתקציב; מי שצריך אותה יסנן לפי station או שנה. (ג) אם 7 השניות של מעבר
ה-footers יכבידו: חלקים גדולים יותר לייצוא `full` (קובץ אחד לטבלה = 60 במקום 180, כ-2.5
שניות ב-8 threads) דורשים rehydrate וייצוא מחדש של הגשם, ולא בוצעו.

### 6.10 צמצום נפח מיידי (2026-10-20, הסשן המתכנן)

המסד לא ירד אחרי שלב 4 כי VACUUM רגיל מחזיר רק דפי heap מהסוף ולעולם לא דפי
אינדקס: 60 טבלאות תחזית הגשם היו ריקות אבל האינדקסים שלהן החזיקו 1.96 GB.
לוג הגישה ל-API (`app.api_access_log`) הגיע ל-2.98 GB (5.5M שורות, בעיקר 429 של
ה-progress של צי העובדים), ושמונה מאגרים סטטיים ללא גאומטריה (לא התעדכנו 60+ יום,
נדגמים כל 90 יום) ישבו ב-SQL עם 2.9 GB.

**קוד (xhostd 7e3f27d, GitHub 7da30f6, מקומי ae3f739):** `export_full` עושה
TRUNCATE לטבלה שהתרוקנה (במקום VACUUM), `POST /api/admin/storage/history/compact`
עושה זאת לכל טבלה קרה שכבר ריקה, ו-`POST /api/admin/storage/access-log/compact`
מריץ purge לפי השמירה ואז VACUUM FULL ללוג (רקע, GET לתוצאה). הדגל
`API_ACCESS_LOG_RETENTION_DAYS=30`. 133 בדיקות עוברות (tests/test_history_compact.py חדש).

| פעולה | תוצאה |
|---|---|
| compact לטבלאות הקרות | 60/60 טבלאות TRUNCATE, 0 דילוגים, 0 כשלונות, 1.965 GB שוחררו ב-24 שניות |
| לוג הגישה | purge 0 שורות (הלוג צעיר מ-30 יום), VACUUM FULL: 2.98 → 2.48 GB ב-45 שניות |
| מעבר לקר: 14951fe8 ישויות התכנית | 1 טבלה, 2,781,819 שורות, 6 parts |
| abb8cecc היסטוריית כלי רכב | 2 טבלאות, 7,571,060 שורות, 16 parts, 10 דקות |
| 2d893026 / 7d259333 / cc732b17 כלי רכב שירדו מהכביש | 1,195,056 / 670,293 / 499,791 שורות |
| d8629820 קישורי מסמכים, 486c4e43 מפל"ס, 96d8639c גמל-נט | 415,420 / 160,243 / 184,685 שורות (גמל-נט 4 טבלאות) |
| כל 12 הטבלאות אחרי | 0 MB ב-SQL (TRUNCATE מהקוד החדש), 0 כשלונות |
| הקטלוג | 216 parts, 76,050,935 שורות ב-R2, 2.11 GB |
| המסד | 62.98 → 57.30 GB (pg_size_pretty: 59 → 53 GB); public 49.7 → 44.6 GB |

**מה לא השתנה:** שכבות המפה הסטטיות (shape 3.27, mavat_landuse 1.64, רמ"י
שימושי קרקע 1.11, נחלים 1.05, גושים 0.58 GB) נשארו חמות לפי החלטת המשתמש: מעבר
לקר היה מאבד את עמודת geom (רק geometry_wkt עובר ל-Parquet) ואיתה את /features,
תצוגת המפה ותוסף QGIS. לוג הגישה יתייצב סביב 2.5 GB עם שמירה של 30 יום; קיצור
ל-7 יום או סינון ה-429 של העובדים מהלוג היו משחררים עוד כ-2 GB.
