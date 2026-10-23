# יציאה מ-Neon ל-xhostd — מפת כל הפרויקטים וסדר המעבר

תוכנית מעבר. נכתבה 1.9.2026, נבדקה מחדש מול שני הצדדים בו־זמנית:
Neon API + חיבור `psql` חי ל-Postgres של xhostd. כל מספר כאן נמדד, לא נלקח מתיעוד.

> **הערה על מסמך קודם.** `xhostd/05-render-exit-map.md` §0 קובע את העיקרון ההפוך —
> "xhostd = Compute בלבד, Neon = כל הנתונים", ואף מתכנן להעביר 5 מסדי Render *לתוך* Neon.
> המטרה שלך התהפכה: לצאת מ-Neon כי הוא יקר. שני המסמכים סותרים זה את זה,
> וזה כאן הוא העדכני. יש לעדכן את 05 בהתאם.

---

## 1. שלוש תגליות שקובעות הכל

### א. ל-Postgres של xhostd אין PostGIS. הוכח, לא שוער.

התחברתי ל-Postgres של xhostd (`masham` — היחיד עם External DB access פתוח) ובדקתי:

```
PostgreSQL 18.4 (Debian) — אותה גרסה כמו Neon
pg_available_extensions → 46 בדיוק. contrib סטנדרטי בלבד.
CREATE EXTENSION postgis;  →  ERROR: extension "postgis" is not available
```

זמינים: `pg_trgm` · `pgcrypto` · `unaccent` · `postgres_fdw` · `hstore` · `citext` ·
`ltree` · `uuid-ossp` · `btree_gin` · `btree_gist` · `pg_stat_statements` · `tablefunc`
ועוד. הדבר היחיד שמריח מרחבי הוא `earthdistance` — מרחק על גבי כדור, לא גיאומטריה.

**המשמעות:** `over-datastore-archive` — 37% מהחשבון — **לא יכול לעבור כמו שהוא.**
הוא מריץ PostGIS 3.6.0, ועליו יושבים `over_re_parcels`, `over_re_addresses`,
`over_parcel_at()`, `over_parcels_near()` וכל שכבות ממ"ג.

### ב. גם `CREATE ROLE` חסום

```
CREATE ROLE probe_ro_tmp NOLOGIN;  →  ERROR: permission denied to create role
```

התפקיד ש-xhostd מזריק אינו superuser ואינו `rolcreaterole`. לכן
`APPEND_READONLY_DATABASE_URL` — התפקיד המצומצם שמריץ את הקונסולות הציבוריות
ב-`/data` (ראה `scripts/create_append_readonly_role.sql` ו-`tests/test_readonly_role.py`) —
**לא ניתן לשחזור ב-xhostd.** אכיפת ה-read-only הייתה נסוגה לשכבת האפליקציה,
וזו נסיגה אבטחתית. `CREATE SCHEMA` ו-`CREATE EXTENSION` (מהרשימה הזמינה) — מותרים.

### ג. `masham` כבר עבר. אתה משלם על עותק מת.

השוואה שורה-מול-שורה בין Neon לבין xhostd:

| | Neon `masham` | xhostd `masham` |
|---|---|---|
| `app.import_runs` | 516 | **516** |
| הרצה אחרונה | `2026-08-21 09:14:05.940124+00` | **זהה לחלוטין** |
| `app.sources` / `app.users` / `app.imported_tables` | 50 / 2 / 111 | **50 / 2 / 111** |
| סכימת `data` | 113 טבלאות | **113 טבלאות, אותם גדלים** |

הנתונים זהים, ואיש לא כתב לאף אחד מהצדדים מאז 21.8 — 11 יום.
ה-compute ב-Neon "פעיל" רק כי שירות ה-Render הישן עדיין מחזיק pool פתוח.

**זו לא משימת מעבר. זו משימת פירוק.**

> **תיקון ל-122 CU-השעות.** בדיקת ה-endpoints מגלה ש-`suspend_timeout_seconds: 0`
> ב-Neon פירושו **ברירת המחדל (5 דקות)**, לא "לעולם לא להירדם". ה-endpoint של
> `masham` אכן נרדם — `current_state: idle`. כלומר 122 CU-השעות נצברו בעיקר
> בתקופת הבנייה והייבוא (14–21.8), והן **אינן קצב ריצה חוזר**.
> קדימה, `masham` ב-Neon עולה בעיקר **אחסון** (4.46 GiB). עדיין שווה למחוק —
> אבל זה לא חיסכון של 12.4% לחודש.

---

## 2. המפה המלאה — כל שישה הפרויקטים

| פרויקט (Neon) | אחסון | CU-שעות | % מהחשבון | extensions | האפליקציה רצה ב… | יכול לעבור? |
|---|---|---|---|---|---|---|
| `ckan-version-tracker` <br><small>quiet-tooth · us-east-1</small> | 425 MB<br><small>1.4 GB לוגי</small> | 370.8 | **37.7%** | `pg_trgm` ✅ | Render (OVER) | ✅ טכנית — אך אחרי ה-compute |
| `over-datastore-archive` <br><small>hidden-forest · us-east-1</small> | 58.0 GiB<br><small>2,460 טבלאות</small> | 366.5 | **37.3%** | **PostGIS 3.6** ❌ | Render (OVER) | ⛔ **חסום** |
| `masham` <br><small>green-bird · eu-central-1</small> | 4.46 GiB | 122.1 | **12.4%** | ללא ✅ | **xhostd — כבר עם הנתונים** | ✅ **כבר עבר** |
| `court-downloader` <br><small>tiny-leaf · eu-central-1</small> | 4.9 MB<br><small>טבלה אחת</small> | 66.5 | **6.8%** | ללא ✅ | Render (docker) | ✅ קל |
| `Shomrim` <br><small>winter-thunder · eu-central-1</small> | 28 MB<br><small>10 טבלאות</small> | 51.9 | **5.3%** | ללא ✅ | Render (node) | ✅ קל |
| `creteus` <br><small>ancient-paper · eu-central-1</small> | 7 MB<br><small>46 טבלאות</small> | 5.1 | **0.5%** | ללא ✅ | xhostd (קורא ל-Neon) | ✅ טריוויאלי |
| **סה"כ** | **64.1 GiB** | **982.9** | **100%** | | | |

שים לב ליחס: `court-downloader` ו-`Shomrim` הם יחד **12.1% מהחשבון על 33 מגה-בייט**.
העלות ב-Neon כמעט לא קשורה לגודל — היא קשורה לזה שה-compute ער.

**מצב ה-endpoints ברגע הבדיקה** (1.9.2026, 21:30 UTC) — זה מה שמפריד בין עלות
היסטורית לקצב ריצה חוזר:

| endpoint | מצב | רץ ברציפות מאז |
|---|---|---|
| `quiet-tooth` · ckan-version-tracker | **active** | **20.8 — 11 יום ללא הפוגה** |
| `hidden-forest` · over-datastore-archive | **active** | 31.8 18:54 |
| `green-bird` · masham | **idle** | נרדם ב-31.8 21:28 ✅ |

`quiet-tooth` הוא החשוד האמיתי: 11 ימים רצופים בלי ולו חלון הירדמות אחד.

---

## 3. ההמלצה: במה להתחיל

### הראשון — `masham`. וזו לא הגירה, זו סגירה.

| | |
|---|---|
| **חיסכון** | **אחסון בעיקר** — 4.46 GiB. ה-compute שלו כבר נרדם (ראה התיקון ב-§1ג); 122.1 CU-השעות היו תקופת הבנייה, לא קצב חוזר |
| **עבודת העתקה** | **אפס.** הנתונים כבר שם ואומתו זהים |
| **סיכון** | הנמוך ביותר — האפליקציה כבר רצה על xhostd Postgres בפועל |
| **זמן** | חצי יום, רובו אימות |

**למה זה עדיין הראשון הנכון, גם אחרי התיקון:** האימות שנדרש כאן — השוואת מספר
טבלאות, `count(*)` לכל טבלה, ו-checksum — הוא בדיוק סקריפט הפריטי שנצטרך בכל
מעבר אחר. הצעד הזול והבטוח ביותר בחשבון הוא גם הצעד שמייצר את כלי האימות לשאר,
ומנקה כפילות שאחרת תבלבל כל ספירה עתידית. הוא כבר לא ה*חיסכון* הגדול — הוא ה*התחלה* הנכונה.

**הצעדים:** אימות פריטי מלא → לוודא ששירות ה-Render של masham אכן מיותר ולכבות אותו →
`pg_dump` של Neon `masham` ל-R2 → המתנה → מחיקת הפרויקט ב-Neon.

### השני — `creteus`. חזרה גנרלית אמיתית על המכניקה.

7 מגה-בייט, 46 טבלאות, והאפליקציה **כבר** רצה על xhostd — היא פשוט קוראת ל-Neon
דרך `NEON_DATABASE_URL`. לכן המעבר הוא בדיוק הרצף שנצטרך בגדולים, במיניאטורה:

```
pg_dump neon → psql xhostd → למחוק NEON_DATABASE_URL → deploy → אימות
rollback = להחזיר את המשתנה ולעשות deploy
```

חוסך 0.5% בלבד — אבל מוכיח את כל הרצף מקצה לקצה תוך פחות משעה, בלי סיכון.
עושים אותו **ביום שבו עושים את masham**, לא כגל נפרד.

### השלישי — `court-downloader` + `Shomrim`. יחס העלות-לבייט הטוב בחשבון.

12.1% מהחשבון על 33 MB. חסם יחיד: שתי האפליקציות עדיין ב-Render, ולכן **אין להזיז
את ה-DB לפני האפליקציה** — אחרת נוצרת קפיצה בין ספקים בכל שאילתה.
שתיהן ממילא בגל 2 של מפת היציאה מ-Render, אז זה מתלכד.

### הרביעי — `ckan-version-tracker`. גדול, קל טכנית, וכבול.

425 MB ורק `pg_trgm` — טכנית מהמעברים הקלים בחשבון, ומחזיר 37.7%.
מה שכובל אותו: הענף `production` מדווח **1.008 TB של data transfer**.
כל עוד OVER רץ ב-Render, כל התעבורה הזו תחצה את האינטרנט הפתוח אל `db.xhostd.com`,
לתוך מכסת bandwidth לא מתועדת ולתוך ה-statement timeout של 10 שניות ב-`/data`.
לכן: **רק אחרי שה-compute של OVER עובר ל-xhostd.**

*(אילוץ ההפרדה ב-`app/config.py:682` דווקא לא נשבר כאן: אם ה-append DB נשאר ב-Neon
וה-app DB עובר ל-xhostd, אלה עדיין שני מסדים פיזיים נפרדים.)*

### החמישי — `over-datastore-archive`. חסום. ארבע דרכים.

| דרך | מה זה אומר | הערכה |
|---|---|---|
| **א. להשאיר אותו ב-Neon ולתקן את ה-compute** | הוא ער 97% מהחודש ב-0.5 CU. `docs/neon-compute-idle-prompt.md` כבר מאבחן: ג'ובים כל 30 שניות/3 דקות מול חלון הירדמות של 5 דקות | **הכי משתלם.** יישור פאזה של הג'ובים יכול להוריד 366 CU-שעות למאה ומשהו — חיסכון גדול יותר מ-masham, court-downloader ו-Shomrim **יחד**, בלי להעביר בייט אחד |
| **ב. לבקש מ-xhostd להוסיף PostGIS** | `submit_feedback` — הערוץ המתועד | זול לשאול, לא בשליטתנו |
| **ג. Postgres+PostGIS מנוהל עצמית** | קונטיינר עם `postgis/postgis` על שרת אחר | ה-Blob של xhostd אינו דיסק; דורש יעד שלישי |
| **ד. לוותר על PostGIS** | לא ריאלי — 1.1M חלקות, 622k כתובות, ההצלבה כולה | ❌ |

**ההמלצה: א׳, ובמקביל ב׳.** הפרויקט היחיד שלא יכול לעבור הוא גם הפרויקט שבו
תיקון תצורה מחזיר את מרבית העלות.

---

## 4. סדר הביצוע, וכמה כל שלב מחזיר

| גל | מה | חיסכון | מצטבר | חסם |
|---|---|---|---|---|
| **1** | `masham` — אימות + פירוק | אחסון 4.46 GiB | — | — |
| **1** | `creteus` — חזרה גנרלית | 0.5% | — | — |
| **2** | **יישור פאזה על `quiet-tooth` + `hidden-forest`** | **עד ~60%** *(אומדן)* | ~60% | ללא מעבר נתונים כלל |
| **3** | `court-downloader` + `Shomrim` | 12.1% | ~72% | האפליקציות ל-xhostd קודם |
| **4** | `ckan-version-tracker` | שארית | | ה-compute של OVER קודם |
| **5** | `over-datastore-archive` | שארית | | **PostGIS** |

הסדר התהפך אחרי התיקון ב-§1ג: **גל 2 הוא כעת המנוף הגדול ביותר בחשבון**,
והוא שינוי תצורה בקוד שלך — לא תלוי ב-xhostd, לא תלוי ב-PostGIS, ולא מזיז בייט.

גל 1 הוא היום. גל 2 הוא שינוי תצורה בלבד ואינו תלוי ב-xhostd כלל.
סגירת חשבון Neon לגמרי תלויה בגל 5, שהוא כרגע חסום.

---

## 5. מה נשאר לא ידוע

| שאלה | למה זה חשוב | איך נדע |
|---|---|---|
| מכסת אחסון ל-Postgres ב-xhostd | `masham` מחזיק 3 GB בלי בעיה; 58 GB לא נבדק | `submit_feedback` |
| מכסת bandwidth / egress | 1 TB בחודש על ה-app DB לבדו | `submit_feedback` |
| גיבוי מעבר ל-snapshots | 3 pre-deploy + nightly לשבוע | `submit_feedback` |
| PostGIS בעתיד | פותח או סוגר את גל 5 | `submit_feedback` |

ארבע השאלות הן פנייה אחת ל-xhostd. היא **דורשת את אישורך** — זו הודעה לצד שלישי.

---

## 6. מי עושה מה

**רק אתה:** לכבות את שירות ה-Render של masham · לאשר מחיקת פרויקט Neon ·
לאשר את הפנייה ל-xhostd · להדליק External DB access לכל אפליקציה חדשה (protected action) ·
להכריע אם ה-compute של OVER עובר.

**הסוכן:** סקריפט האימות · ה-dump-ים ל-R2 · יצירת אפליקציות, env ו-deploy ·
החלפת משתנים · יישור הג'ובים בסקדיולר · הניטור.

---

## נספח — עובדות שנמדדו על Postgres של xhostd

```
version           PostgreSQL 18.4 (Debian 18.4-1.pgdg13+1)      -- Neon: 18 גם כן
max_connections   500
shared_buffers    1 GB
statement_timeout 0
role              לא superuser · אין createdb · אין createrole · אין bypassrls
extensions        46 זמינות (contrib בלבד) · PostGIS איננו
CREATE SCHEMA     מותר
CREATE EXTENSION  מותר, מתוך ה-46
CREATE ROLE       חסום
cross-database    חסום — כל ערוץ הוא גבול
```
