# מסמך העברה — מצב ההגירה ל-xhostd

> **מיועד להדבקה לסשן חדש.** המסמך עומד בפני עצמו: הוא לא מניח שום ידע קודם.
> נכתב 2.9.2026 בערב. **לפני שאתה פועל — אמת את המצב מול ה-API החי**, כי
> אוטומציות אחרות ממשיכות לרוץ והמסמך הזה הוא תצלום רגע.

---

## 0. מה קורה כאן, בשני משפטים

יש **שני מעברים** שרצים במקביל על אותם שירותים, ובנקודה אחת הם סותרים:

- **יציאה מ-Render** (מאז 26.8) — ה-**compute** עובר ל-xhostd.
- **יציאה מ-Neon** (מאז 1.9) — ה-**נתונים** עוברים ל-Postgres של xhostd, כי
  חשבון Neon הוא ~98% compute ולא אחסון.

`xhostd/05-render-exit-map.md` עדיין מתאר את הכיוון ההפוך ("Neon = כל הנתונים")
**ולא עודכן — אל תסתמך עליו.** הכיוון התקף: compute ל-xhostd, ונתונים איתו,
למעט מה שחסום.

---

## 1. שלוש עובדות פלטפורמה שנמדדו ולא שוערו

| | |
|---|---|
| **אין PostGIS ב-xhostd** | 46 extensions, contrib סטנדרטי בלבד. `CREATE EXTENSION postgis` → `is not available`. **זה מה שחוסם את OVER.** נשלחה בקשת פיצ'ר (feedback `25df3127`). |
| **`CREATE ROLE` חסום** | התפקיד המוזרק אינו superuser. אי-אפשר לשחזר תפקיד read-only ייעודי. `CREATE SCHEMA` ו-`CREATE EXTENSION` — מותרים. |
| **מכסות** | Postgres **60 GiB (אכיפה רכה)** · Blob **150 GiB (אכיפה קשיחה)** · egress **ללא מגבלה, לא מחויב**. נמצאות ב-`get_account_overview` תחת `plan_headroom`. |

Postgres של xhostd הוא **18.4** — אותה גרסה כמו Neon.

---

## 2. איפה כל דבר עומד עכשיו

### עברו במלואם — compute + data ב-xhostd

| שירות | הערה |
|---|---|
| `zg-crm` | אין לו שירות ב-Render בכלל |
| `court_downloader` | Render **מושהה**. DB פנימי, R2 חיצוני (Cloudflare) |
| `shomrim-feed` | Render **מושהה**. ingest פעיל |
| `creteus` | רץ גם ב-xhostd וגם ב-Render, שניהם על מסד xhostd אחד |

### עברו, ממתינים ל-DNS בלבד

| שירות | דומיין | מצב |
|---|---|---|
| `kerem-navot` | navotvinyard.co.il | ✅ רץ ומאומת מול Render · Render **חי** |
| `plugat-tzav` | pluga.org.il | ✅ רץ · Turso משותף · Render **חי** |

### פרויקטי Neon

| פרויקט | גודל | מצב |
|---|---|---|
| `tiny-leaf-11038500` | 36 MB | **מוכן למחיקה** — אפס חיבורים |
| `winter-thunder-16178491` | 62 MB | **מוכן למחיקה** — אפס חיבורים |
| `ancient-paper-65839348` | 39 MB | **מוכן למחיקה** — אפס חיבורים |
| `green-bird-01948630` | 4.5 GB | masham. חסום — ראה §4 |
| `quiet-tooth-91355739` | 1.4 GB | OVER app DB. **לא לגעת** |
| `hidden-forest-76634201` | 58 GB | OVER append DB. **לא לגעת** — PostGIS |

**מחיקת פרויקט Neon היא של המשתמש בלבד.** אל תבצע אותה.

---

## 3. המשימה הפעילה — השלמת מעבר הדומיינים

ארבעה דומיינים כבר נוספו ב-xhostd ב-2.9 ומחכים ל-DNS. **הרשומות:**

```
kerem-navot  (prod)
  apex   A      navotvinyard.co.il          159.195.201.188
         TXT    _xhost.navotvinyard.co.il   xhost-verify-2d7f1b08f21034ede33494016b27e5ef
  www    CNAME  www.navotvinyard.co.il      kerem-navot-zomerg.xhostd.app
         TXT    _xhost.www.navotvinyard.co.il  xhost-verify-e2d4c8ca52b77186906a81d9f2c3161f

plugat-tzav  (prod)
  apex   A      pluga.org.il                159.195.201.188
         TXT    _xhost.pluga.org.il         xhost-verify-5d2587937ca107df01a9f4a5049242b7
  www    CNAME  www.pluga.org.il            plugat-tzav-zomerg.xhostd.app
         TXT    _xhost.www.pluga.org.il     xhost-verify-20ad2e605e2308b8d7b3fa6a162fa9ce
```

xhostd מציע CNAME גם ל-apex, אבל רוב הרשמים — כולל internic — לא מאפשרים
CNAME על apex. **להשתמש ב-A record עם `159.195.201.188` ל-apex, ו-CNAME ל-www.**

### הרצף אחרי שה-DNS מתפשט

1. `verify_custom_domain` לכל ארבעת הדומיינים.
2. **`kerem-navot` — סנכרון דלתא.** האתר חי ומקבל הזמנות ותשלומים ל-Render עד
   הרגע הזה, אז ההעתק ב-xhostd נסחף. להשהות את שירות ה-Render, להריץ
   `scripts/pg_resync_data.py`, לאמת ב-`scripts/verify_db_parity.py`, ורק אז לכבות.
   *(`plugat-tzav` לא צריך את זה — שני הצדדים על אותו Turso.)*
3. להחזיר את משתני הכתובת מהכתובת הזמנית של xhostd לדומיין הקבוע:
   - `kerem-navot`: `PUBLIC_BASE_URL` ו-`GOOGLE_REDIRECT_URI` → `https://www.navotvinyard.co.il...`
   - `plugat-tzav`: `AUTH_URL` → `https://pluga.org.il`
   ואז deploy. **אחרי זה לא נדרש שינוי ב-Google** — ה-redirect URI-ים המקוריים חוזרים לתוקף.
4. `RUN_HEALTH_SCHEDULER` ב-`kerem-navot` מוגדר `0` כדי שלא ירוץ כפול מול Render.
   **להחזיר ל-`1`** אחרי שכיביתי את Render.
5. לכבות את שירותי ה-Render של השניים — **רק** אחרי אימות שהדומיין עובד.

---

## 4. חסמים פתוחים

| # | חסם | פרטים |
|---|---|---|
| 1 | **PostGIS** | חוסם את שני מסדי OVER (59.4 GB). ממתין לתשובת xhostd. המכסה של 60 GiB **אינה** החסם — 58 GB נכנסים. |
| 2 | **smart-dms 180GB** | חורג מ-150 GiB של Blob. צריך R2 (כבר קיים אצל המשתמש) או B2 + שכתוב שכבת האחסון. |
| 3 | **Redis** | אין ב-xhostd. חוסם את `ocal-api`/`ocal-worker`, שממילא מושהים. |
| 4 | **masham** | Render ב-alembic `010`, xhostd ב-`011` — הפניית Render למסד xhostd **תפיל** את ה-migrate. ובנוסף 4GB קבצי מקור על דיסק Render שאין ב-xhostd. **אבל `app/services/storage_client.py` כבר כולל backend מלא ל-S3** שנבחר אוטומטית כשיש `S3_BUCKET`, והפריסה זהה בשני ה-backends — כלומר זו העתקת בייטים ל-Blob ולא refactor. זה החסם הכי קל לפתיחה, והוא זה שמשחרר את `green-bird` למחיקה. |

**שעון:** `creteus` ב-Render הוא היחיד שנשאר מצביע ל-`db.xhostd.com` עם טוקן
`get_credentials` שפג **30.9.2026**. שאר השירותים רצים בתוך xhostd עם DSN פנימי
ולהם אין תפוגה. או להעביר גם אותו, או לחדש טוקן לפני התאריך.

---

## 5. מלכודות שכבר עלו — אל תיפול בהן שוב

1. **SSL מול Postgres הפנימי.** ה-`DATABASE_URL` ש-xhostd מזריק מצביע לרשת פרטית
   שלא מדברת TLS. קוד שכופה SSL לפי `NODE_ENV==="production"` נופל על
   `The server does not support SSL connections`. **להחליט לפי המארח, לא לפי הסביבה.**
   תוקן ב-creteus (`server/db.js`) וב-kerem-navot (`server/lib/env.ts` → `dbSsl`).
   ב-shomrim-feed היה כבר מתג `PGSSL=disable`.
2. **`SELECT *` מול עמודה `GENERATED`.** `COPY ... FROM STDIN` לא מקבל עמודות
   generated, אבל `SELECT *` מחזיר אותן → `row field count is N, expected N-1`.
   לרשום עמודות במפורש ולסנן `attgenerated <> ''`.
3. **CSV מול bytea.** להעביר נתונים ב-`FORMAT BINARY`, לא CSV.
4. **`install.sh` רץ בלי env.** כל דבר שצריך DB או `NEXT_PUBLIC_*` בזמן build
   ייכשל או ייצרב ריק. להעביר ל-`launch.sh`, או לקבוע fallback בקוד.
5. **`pg_dump` חייב להיות ≥ גרסת השרת.** Debian נותן 17, Neon/xhostd הם 18 →
   להתקין `postgresql-client-18` מ-PGDG ב-`install.sh`.
6. **סקדיולרים בתהליך רצים פעמיים** כששני עותקים חיים על מסד אחד. לכבות באחד.
7. **תמיד לעצור כתיבות לפני החלפה.** בהעברה של court/shomrim נוספו 7 ג'ובים
   ו-165 פריטים בין ההעתקה לחלון — בלי השהיה הם היו אובדים.

---

## 6. כלים שכבר קיימים בריפו

| קובץ | מה הוא עושה |
|---|---|
| `scripts/pg_export.py` | ייצוא מסד: CSV לכל טבלה + DDL/רצפים ל-`schema.json` |
| `scripts/verify_db_parity.py` | השוואת שני מסדים: עמודות, `count(*)`, checksum בלתי-תלוי-סדר |
| `scripts/pg_resync_data.py` | סנכרון דלתא: truncate + COPY בינארי, עם התרת FK בסבבים |

הרצה: משתני סביבה `A_DSN`/`B_DSN` (parity) או `SRC_DSN`/`DST_DSN` + `APPLY=1` (resync).

**תבנית ההעתקה שהוכחה:** להריץ `pg_dump <source> | psql "$DATABASE_URL"`
**בתוך הקונטיינר** של xhostd, מגודר על "היעד ריק" כדי שיהיה idempotent.
היעד מקומי ורק רגל אחת חוצה את האינטרנט. זו המכונה שתידרש ל-58 הג'יגה של OVER.

---

## 7. גישה

- **xhostd**: MCP מחובר. `get_credentials` מנפיק טוקן ל-git + Postgres + API (30 יום).
- **Neon**: MCP מחובר.
- **Render**: אין MCP. יש מפתח API ב-`C:\Users\zomer\CLAUDE CODE\Render advisor\.env`
  (`RENDER_API_KEY`), ו-audit יומי באותה תיקייה.
- **ערכי env קודמים של Render** גובו לפני כל שינוי — ראה
  `scratchpad/render_rollback/*.json` בסשן המקורי; אם אינם, לשלוף מחדש מה-API.

---

## 8. כללי עבודה שסוכמו עם המשתמש

- **מחיקת נתונים בלתי הפיכה** (פרויקט Neon, מסד Render) — **המשתמש בלבד**.
- **רכישת שירות בתשלום** — לאשר מולו מראש עם הסכום.
- **שינוי DNS** — המשתמש. הסוכן מספק את הרשומות.
- לגבות לפני כל `DROP`/`TRUNCATE`, ולאמת ב-checksum אחרי כל העתקה.
- לדווח מה לא אומת, ולא רק מה שעבד.

---

## 9. תיעוד מלא

- `docs/xhostd-db-migration/README.md` — מפת הפרויקטים והחסמים
- `docs/xhostd-db-migration/findings-2026-09-01.md` — כל הממצאים, האימותים והתקלות
- `docs/xhostd-db-migration/parity-*.txt` — פלט האימות הגולמי
- `docs/neon-compute-idle-prompt.md` — האבחון של ה-compute הער ב-Neon
