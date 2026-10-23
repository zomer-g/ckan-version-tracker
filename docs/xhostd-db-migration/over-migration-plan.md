# גרסאות לעם (OVER) — תוכנית מעבר מ-Render+Neon ל-xhostd

נכתבה 2.9.2026. כל מספר כאן **נמדד היום** מול שלושת הצדדים — Neon API,
מסד ה-append החי, ו-`get_account_overview` של xhostd — ולא נלקח מתיעוד.
הקבצים ב-Cloudflare R2 **נשארים במקומם** ואינם חלק מהמעבר.

המסמך הזה הוא הגל האחרון של `README.md` שבתיקייה הזו (שם: "הרביעי" ו"החמישי").
הוא מניח שקראת אותו ואת `findings-2026-09-01.md`.

---

## 0. מה השתנה מאז 1.9, ומה זה מפיל

`README.md` §1א קבע — במדידה, לא בהשערה — ש**ל-Postgres של xhostd אין PostGIS**,
ולכן `over-datastore-archive` (58 GB, 37% מחשבון Neon) **חסום למעבר**. כל סדר
הגלים שם נגזר מהחסם הזה.

נמסר לך שזה השתנה. אם זה נכון, החסם היחיד שהחזיק את OVER ב-Neon נפל, והמעבר
המלא — compute **ו**נתונים — הופך אפשרי. **התוכנית כאן מותנית באימות של העובדה
הזו**, ולא הצלחתי לאמת אותה בסשן הזה: `get_credentials` נחסם ע"י מסנן ההרשאות,
ותיעוד xhostd (`llms-full.txt`) אינו מזכיר PostGIS, `CREATE EXTENSION` או
`CREATE ROLE` כלל. זו בדיקה של שתי שאילתות — ראה גל 0.

---

## 1. המלאי — מה בדיוק עובר

### השירות ב-Render

| | |
|---|---|
| שירות | `ckan-version-tracker`, type web, env python |
| plan | **standard (2 GB)** — לא starter. נבחר במדידה: טעינת מרשם החלקות (1,097,775 שורות) שיאה ~670MB |
| דיסק מתמיד | **אין.** כל הכתיבה לקבצים היא `tempfile` בלבד (18 מופעים, כולם ephemeral) |
| build | `pip install` → `npm install && npm run build` → **`alembic upgrade head`** |
| start | `uvicorn app.main:app --port $PORT --proxy-headers --forwarded-allow-ips=...` |
| סקדיולר | **בתוך התהליך** (APScheduler), 18 ג'ובים, מ-30 שניות עד 12 שעות |
| env | 102 שדות ב-`Settings`; ~20 מהם סודות/`sync:false` |

### שני מסדי הנתונים ב-Neon

| | app DB — `ckan-version-tracker` | append DB — `over-datastore-archive` |
|---|---|---|
| פרויקט | `quiet-tooth-91355739` · us-east-1 | `hidden-forest-76634201` · us-east-1 |
| Postgres | **17** | **18.6** |
| גודל (Neon synthetic) | 1.51 GB | 62.44 GB |
| גודל אמיתי (`pg_database_size`) | — | **58 GB** |
| טבלאות | — | **2,545** ב-9 סכימות |
| extensions | `pg_trgm` | **`postgis` 3.6.0** (בסכימה `extensions`), `postgres_fdw`, `pg_trgm`/`pgcrypto`/`unaccent` (בסכימה `ocal`) |
| active_time החודש | 154,632 שנ' | 152,112 שנ' |
| מה יושב בו | `api_users` (טוקנים), `users`, כל טבלאות הניהול | כל מה שהקונסולות הציבוריות קוראות |

פירוט ה-58 GB לפי סכימה (הערכת `relpages`, נמדד היום):

| סכימה | טבלאות | GiB |
|---|---|---|
| `public` | 1,129 | **38.8** |
| `pg_toast` | — | 10.9 |
| `idx` | 920 | 2.3 |
| `ocal` | 11 | 1.8 |
| `pilot_idx` | 310 | **0.9** ← מועמד למחיקה |
| `odata` | 40 | 0.9 |
| `ocoi` | 15 | 0.6 |
| `knesset` | 50 | 0.5 |
| `idx_backup` | 1 | ~0 ← מועמד למחיקה |

### חשבון xhostd — מה יש בו היום

| מדד | ערך | אכיפה |
|---|---|---|
| plan | **pro** | |
| CPU | 3 soft cores, burst ×2 · **בשימוש 0.24%** | |
| זיכרון | **3 GiB לכל החשבון** · בשימוש 626 MB (p95 619 MB) על 7 אפליקציות | |
| אחסון DB | **61,440 MB** · בשימוש 3.35 GB · פנוי 61.07 GB | **רך** — הקונסולה מזהירה, שום דבר לא נחסם |
| אחסון אובייקטים | 150 GiB · בשימוש 9.9 MB | **נאכף** (507 על העלאה חורגת) |
| egress | ללא מגבלה, **לא מחויב** | ללא |
| ערוצים | 7 מתוך 75 | |
| image | עד 12 GiB · snapshots 7 ימים + 3 לפני deploy | |

**שים לב לשורה השלישית.** מסד ה-append לבדו (58 GiB) גדול מכל התקציב הפנוי
(61.07 GB ≈ 56.9 GiB), ועם מסד ה-app הוא חוצה גם את התקרה הכוללת. האכיפה רכה,
אבל "התקרה כרגע לא נאכפת" אינה תוכנית. זו הכרעה #3 בגל 0.

**ושים לב לשורה החמישית.** ה-egress חינמי וללא תקרה — כלומר החשש ב-`README.md`
§3 מ-1.008 TB של תעבורה שתחצה את האינטרנט **בטל**. זה משנה את סדר הגלים.

---

## 2. הבעיה הארכיטקטונית האחת שאין לה פתרון ברירת מחדל

`app/config.py:682` + `app/main.py:103` אוכפים אינווריאנט אבטחה: **`DATABASE_URL`
ו-`APPEND_DATABASE_URL` חייבים להיות שני מסדים פיזיים שונים** (host+port+dbname).
אחרת הקונסולות הציבוריות של SQL — שרצות מול ה-append DB — יכולות לקרוא את
`api_users` ואת הטוקנים שבו. אם הם מתלכדים, **האפליקציה מסרבת לעלות**.

ב-xhostd, אפליקציה אחת = מסד אחד. `CREATE DATABASE` חסום לתפקיד שהפלטפורמה
מזריקה, וכל ערוץ הוא גבול. כלומר שני מסדים = **שתי אפליקציות**, ואפליקציה אחת
מגיעה למסד של השנייה רק דרך `db.xhostd.com` החיצוני — עם טוקן מ-`get_credentials`
ש**פג אחרי 30 יום**. זו בדיוק המלכודת שנרשמה ב-`findings` §ג ונשלחה עליה שאלה
ל-xhostd (feedback `c2d08389`) שעדיין פתוחה.

### שלוש דרכים, ומה כל אחת עולה

| | א׳ — שתי אפליקציות | ב׳ — מסד אחד + תפקיד מוגבל | ג׳ — compute בלבד |
|---|---|---|---|
| `over` (app DB) | פנימי, מוזרק | פנימי | Neon |
| `over-append` | אפליקציית-סרק שמחזיקה את המסד; OVER מגיע דרך `db.xhostd.com` | אותו מסד, סכימות נפרדות | Neon |
| האינווריאנט | **נשמר כלשונו** | נשען על `CREATE ROLE` + שינוי בקוד השומר | נשמר |
| תלוי ב… | טוקן ארוך-טווח (הכרעה #4) | `CREATE ROLE` ב-xhostd (הכרעה #2) | כלום |
| אחסון | 60 GiB לחשבון — **אותה תקרה** | אותה תקרה | לא נוגע |
| latency | קפיצה חיצונית לכל שאילתת `/data` | פנימי, הכי מהיר | קפיצה חיצונית **לכל בקשה** |
| סיכון | בינוני | גבוה — מנמיך אינווריאנט מתועד | **נמוך** |

התקרה של 60 GiB היא **לחשבון**, לא לאפליקציה, ולכן פיצול לשתי אפליקציות אינו
קונה אחסון. הכרעה #3 נדרשת בכל מסלול שמזיז את ה-58 GB.

**ההמלצה:** לפצל את המעבר לשניים ולעשות את ג׳ **קודם**, בלי קשר לבחירה בין א׳ ל-ב׳.
העברת ה-compute היא החצי ההפיך, היא לא תלויה באף אחת מארבע ההכרעות, והיא מוציאה
אותך מ-Render מיד. העברת ה-58 GB היא החצי הקשה, והיא נשארת החלטה נפרדת שנפתחת
רק אחרי שגל 0 מחזיר תשובות. ג׳ הוא גם המסלול שמודד בפועל את ה-latency שקובעת אם
א׳ בכלל שווה את עצמו.

---

## 3. גל 0 — ארבע הכרעות. חוסם את גלים 4–6, לא את 1–3

| # | שאלה | איך בודקים | מה תלוי בה |
|---|---|---|---|
| 1 | **PostGIS קיים?** | `SELECT * FROM pg_available_extensions WHERE name LIKE 'postgis%'` ואז `CREATE EXTENSION postgis SCHEMA extensions` על מסד בדיקה | כל העברת ה-append DB. גרסה ≥3.0 מספיקה — OVER משתמש ב-`ST_AsText`/`ST_AsGeoJSON`/GiST/`&&`/`ST_DWithin` בלבד |
| 2 | **`CREATE ROLE` מותר?** | `CREATE ROLE probe_ro NOLOGIN` | האם `APPEND_READONLY_DATABASE_URL` שורד. אם לא — הקונסולות הציבוריות יורדות לאכיפה ברמת האפליקציה בלבד (נסיגה אבטחתית מודעת, כמו ב-masham) |
| 3 | **התקרה 60 GiB ניתנת להעלאה?** | `submit_feedback` | האם 58 GB בכלל נכנסים |
| 4 | **יש credential ארוך-טווח ל-`db.xhostd.com`, או גישה פנימית בין אפליקציות?** | `submit_feedback` — המשך של `c2d08389` | הבחירה בין א׳ ל-ב׳ |

שאלות 3 ו-4 הן פנייה אחת ל-xhostd. **היא דורשת את אישורך** — זו הודעה לצד שלישי.
שאלות 1 ו-2 הן שתי שאילתות שאני יכול להריץ ברגע שיהיה credential.

---

## 4. גל 1 — שינויי קוד. על branch, תואמי-לאחור, Render ממשיך לעבוד

| # | שינוי | קובץ | למה |
|---|---|---|---|
| 1 | `install.sh` — `pip install -r requirements.txt` + `cd frontend && npm ci && npm run build` | חדש | build ב-xhostd רץ כ-root **בלי env**. הפרונטאנד לא קורא `import.meta.env` בשום מקום, אז אין ערכי build לצרוב |
| 2 | `launch.sh` — `alembic upgrade head` ואז `exec uvicorn ... --port "$XHOST_HTTP_PORT"` | חדש | ב-xhostd **אין DB בזמן build**. המיגרציה חייבת לעבור מ-build ל-boot |
| 3 | פורט: `${XHOST_HTTP_PORT:-${PORT:-8000}}` | `launch.sh` | תואם לשני הצדדים |
| 4 | `--forwarded-allow-ips` — להוסיף את טווח ה-ingress של xhostd | `launch.sh` | היום מפורט "10/8, 172.16/12, 192.168/16, loopback". ה-DB הפנימי של xhostd נמדד ב-`10.200.2.2`, כלומר RFC1918 — סביר שה-ingress גם. **לאמת בלוג, לא להניח.** `"*"` אסור: הוא מחזיר בדיוק את הזיוף ש-`app/client_ip.py` סוגר |
| 5 | שרשרת DSN: `NEON_DATABASE_URL || DATABASE_URL` ובאותו אופן ל-append | `app/config.py` | `DATABASE_URL` הוא **reserved** ב-xhostd — הפלטפורמה מזריקה אליו Postgres ריק משלה ו-`set_env` דוחה כתיבה אליו |
| 6 | SSL לפי **המארח**, לא לפי "האם זה ייצור" | `app/database.py`, `append_store.get_pool` | זו המלכודת שהפילה גם את creteus וגם את kerem-navot: ה-Postgres הפנימי של xhostd **אינו מדבר SSL**, ו-Neon מחייב אותו. הכלל: SSL ל-`*.neon.tech`/`*.render.com`/`db.xhostd.com`, בלי SSL לכתובת פרטית |
| 7 | **מתג `MAINTENANCE_MODE`** — חוסם את הסקדיולר ומחזיר 503 מכל endpoint שכותב (`/api/worker/*`, admin) | `app/config.py`, `app/worker/scheduler.py`, middleware | **אין כזה היום.** בלעדיו אין דרך לעצור כתיבות בחלון ההחלפה, וזה בדיוק הכשל ש-`findings` §ג תיעד: `court` צבר 7 ג'ובים ו-`shomrim` 165 פריטים בין ההעתקה לחלון. בלי המתג, הדלתא הולכת לאיבוד |
| 8 | `min_size` של pool ה-append | `app/services/append_store.py:131` | `min_size=0` קיים כדי לאפשר scale-to-zero ב-Neon. ב-xhostd המסד תמיד ער — 0 רק קונה latency בכל חיבור ראשון |
| 9 | לקומיט את `scripts/pg_export.py`, `verify_db_parity.py`, `pg_resync_data.py` | git | שלושתם **untracked** היום. אלה כלי המעבר — הם חייבים להיות בהיסטוריה לפני שמסתמכים עליהם |

שינויים 1–8 אינם מפילים את Render: כולם fallback-ים על הערך הקיים.

---

## 5. גל 2 — compute ל-xhostd, הנתונים נשארים ב-Neon

```
create_app(name="over", template="app")
git push <branch>:master
set_env  NEON_DATABASE_URL / NEON_APPEND_DATABASE_URL  → ה-DSN של Neon (secret)
set_env  JWT_SECRET_KEY · GOOGLE_CLIENT_ID/SECRET · WORKER_API_KEY · ODATA_API_KEY
set_env  S3_ENDPOINT/BUCKET/ACCESS_KEY/SECRET_KEY/PUBLIC_BASE_URL   ← R2, ללא שינוי
set_env  MCP_SERVICE_TOKEN · TAGIT_MCP_TOKEN · CONNECTOR_API_KEY · DEEPSEEK/ANTHROPIC
set_env  APP_BASE_URL = https://over-zomerg.xhostd.app   (זמנית)
set_env  MALLOC_ARENA_MAX=2 · MAINTENANCE_MODE=true      ← עולה שקט
deploy(ref="master")
```

**`MAINTENANCE_MODE=true` ב-deploy הראשון היא לא זהירות יתר.** בלעדיו יעלה
מופע שני של הסקדיולר מול אותם מסדי Neon בזמן ש-Render עדיין רץ — ייבוא כפול,
poll כפול, ותורי עבודה שנמשכים פעמיים. זו המלכודת "שני instances על אותו Neon"
מה-playbook, ועל OVER היא יקרה במיוחד.

מה שנבדק בגל הזה, על `over-zomerg.xhostd.app` בלבד, בלי לגעת ב-DNS:

1. `deploy success`, `/` מחזיר 200, ה-SPA נטען, `/assets` מוגש עם ה-cache header.
2. **latency ל-Neon** — זה המדד שקובע אם ג׳ יכול להישאר לאורך זמן ואם א׳ שווה
   את עצמו. להשוות זמן תגובה של `/api/datasets` ושל שאילתת `/data` בין Render
   ל-xhostd. שני מסדי Neon ב-us-east-1; אזור xhostd לא ידוע ויימדד כאן.
3. זיכרון — 2.6 GB פנויים בחשבון; OVER לבדו שיאו ~670MB. לוודא ב-`get_app_stats`
   שהשיא לא מטפס אחרי `refresh_dataset_sizes` ואחרי טעינת החלקות.
4. `app/client_ip.py` — לאמת שה-ingress של xhostd מוסיף XFF מכתובת פרטית
   ושהזהות שמגיעה מ-Cloudflare עדיין נגזרת נכון.
5. OAuth: `redirect_uri` — **רק אתה יכול** להוסיף
   `https://over-zomerg.xhostd.app/auth/google/callback` ב-Google Cloud Console.
6. MCP (`/mcp`) עם `MCP_SERVICE_TOKEN`, ה-Looker connector, ו-`/api/deep-search`.

**rollback בגל הזה = לא לעשות כלום.** Render חי, ה-DNS לא זז.

---

## 6. גל 3 — DNS. הרגע שבו over.org.il עובר

1. `add_custom_domain("over.org.il")` → xhostd מחזיר CNAME/A + TXT `_xhost`.
2. **אתה** מגדיר אותן ב-Cloudflare. להשאיר את הענן **כתום** — `app/client_ip.py`
   מאמת `CF-Connecting-IP` מול טווחי Cloudflare, וגרייז-קלאוד ישבור את
   ה-rate-limit ואת תקציב הנתונים. SSL mode: Full (strict).
3. להעביר `MAINTENANCE_MODE=false` ב-xhostd ו-**true** ב-Render (או להשהות את
   השירות) — **באותו חלון**, אחרת שני סקדיולרים.
4. `APP_BASE_URL=https://over.org.il`, redirect URI חוזר לדומיין הקנוני.
5. צי ה-scraper (`govil-scraper`) עוקב אחרי `over.org.il` — **לא נדרש שינוי אצלו**.
   אותו דבר ל-MCP של claude.ai ול-connector של Looker: כולם קשורים לדומיין.
6. `files.over.org.il` — **לא נוגעים**. R2 נשאר כפי שהוא.

מעקב 72 שעות לפני שממשיכים. `get_runtime_log` + `get_app_health`.

---

## 7. גל 4 — הרזיית ה-append DB (לפני שמזיזים בייט)

מטרה: לרדת מ-58 GiB לרווח סביר מתחת לתקרה, ולקצר את זמן ההעתקה.

| מועמד | GiB | מה לבדוק לפני |
|---|---|---|
| סכימת `pilot_idx` (310 טבלאות) | 0.9 | שריד מ-`docs/neon-index-pilot/`. לוודא שאין `search_path`/קוד שנוגע בה |
| `idx_backup` | ~0 | שם מפורש של גיבוי |
| כפילות `idx` ↔ `public.append_*` | לא נמדד | ידוע ומתועד: מראת האינדקס נתנה לסקרייפרים מסוג `archive_neon` עותק `idx` שני, מתבדר, של שורות שכבר יושבות ב-`public.append_*` |
| foreign server `ocal_srv` | 0 | `postgres_fdw` עם **0 foreign tables** — שריד מהתקופה שבה ocal היה במסד נפרד. למחוק, ולא לשחזר ב-xhostd |
| bloat | לא נמדד | `pg_toast` לבדו 10.9 GiB. שחזור טרי מ-dump מהדק ממילא — הגודל בפועל ב-xhostd עשוי לצאת נמוך משמעותית מ-58 |

**כל מחיקה כאן היא בלתי הפיכה ודורשת את אישורך פר-פריט.**

---

## 8. גל 5 — העתקת הנתונים

התבנית מוכחת (`findings` §ג), אבל **לא בסדר גודל הזה**. שלושה הבדלים מ-1.9:

1. **אי אפשר לשמור dump על דיסק.** ה-image מוגבל ל-12 GiB ו-`/tmp` הוא חלק ממנו.
   ההעתקה חייבת להיות `pg_dump | psql` בזרימה, בלי קובץ ביניים. זה גם מוציא מהמשחק
   את `pg_restore --jobs`, שדורש פורמט directory.
2. **הריצה ארוכה מחלון ה-health.** `/` חייב להחזיר 2xx תוך 120 שנ'. לכן ההעתקה
   **לא** יכולה לחסום את ה-boot כמו ב-court/shomrim. אפליקציית טעינה זמנית
   (`over-append`) מעלה שרת טריוויאלי **קודם**, ורק אז מריצה את ההעתקה ברקע עם
   `/status` שמדווח התקדמות פר-סכימה.
3. **מקביליות לפי סכימה.** `public` לבדה 38.8 GiB ו-1,129 טבלאות; זרם יחיד עליה
   ייקח שעות. לפצל: `knesset`, `ocoi`, `odata`, `ocal`, `idx` בזרמים נפרדים,
   ואת `public` לקבוצות טבלאות לפי `--table`.

לפני הכל, על היעד: `CREATE SCHEMA extensions; CREATE EXTENSION postgis SCHEMA extensions;`
— לפני שחזור כל טבלה שיש בה `geom`, אחרת הטיפוס לא קיים והשחזור נופל.
`postgresql-client-18` **מ-PGDG** ולא מ-Debian (המקור הוא PG 18.6).

אימות: `scripts/verify_db_parity.py` — עמודות, `count(*)`, ו-checksum בלתי-תלוי-סדר.
על 2,545 טבלאות זו ריצה ארוכה; להריץ פר-סכימה.

**שתי מלכודות שכבר עלו בדם** (`findings` §ד) ומופיעות גם כאן: עמודות `GENERATED`
מחייבות רשימת עמודות מפורשת, ו-`bytea` מחייב `FORMAT BINARY`. שתיהן כבר מתוקנות
ב-`pg_resync_data.py`.

---

## 9. גל 6 — חלון ההחלפה

הסדר שמונע אובדן כתיבות, ואין לו קיצור דרך:

1. `MAINTENANCE_MODE=true` + deploy. הסקדיולר עוצר, endpoint-ים שכותבים מחזירים 503.
2. לאמת שאין כותב: `SELECT count(*) FROM pg_stat_activity WHERE state='active'` בשני הצדדים.
3. `scripts/pg_resync_data.py` — סנכרון דלתא של מה שנצבר מאז ההעתקה.
4. `verify_db_parity.py` — הפעם חייב לחזור IDENTICAL.
5. להחליף `NEON_DATABASE_URL`/`NEON_APPEND_DATABASE_URL` ב-DSN של xhostd.
6. deploy, `MAINTENANCE_MODE=false`, אימות מלא.

**rollback:** להחזיר את שני המשתנים ל-DSN של Neon ולעשות deploy. פרויקטי Neon
נשארים חיים עד שתאשר מחיקה — זו רשת הביטחון היחידה בשלב הזה.

---

## 10. גל 7 — פירוק

בסדר הזה, ולא לפניו: 72 שעות יציבות → השהיית שירות Render → שבוע → מחיקת
`quiet-tooth-91355739` ו-`hidden-forest-76634201` ב-Neon → מחיקת `NEON_SOURCE_DSN`
ו-`NEON_*` מכל האפליקציות → עדכון `README.md` בתיקייה הזו ו-`xhostd/05-render-exit-map.md`.

עם שני אלה, **חשבון Neon נסגר לגמרי**: ארבעת האחרים כבר עברו (§ד+ה ב-findings),
ו-`masham` הוא העותק המת שנשאר לסגור.

---

## 11. מה חוסך

| | היום | אחרי |
|---|---|---|
| Render `standard` | ~$25/חודש | 0 |
| Neon — שני הפרויקטים של OVER | **75% מחשבון Neon** (370.8 + 366.5 מתוך 982.9 CU-שעות) | 0 |
| Neon — כלל החשבון | ~$56/חודש (98% compute) | **0 — החשבון נסגר** |
| xhostd | plan pro, כבר משולם | אותו plan |

מעבר לחיסכון: `statement_timeout` על xhostd הוא 0 ו-`max_connections` הוא 500,
לעומת חלון ההירדמות של Neon שהג'ובים של OVER מעולם לא הצליחו להיכנס אליו
(`quiet-tooth` ער **11 יום ברצף**). הבעיה ש-`findings` §ב אבחנה — סקירה כל 30
שניות מול חלון של 5 דקות — פשוט מפסיקה להיות בעיה כשאין מה להעיר.

---

## 12. מי עושה מה

**רק אתה:** לאשר את הפנייה ל-xhostd (הכרעות 3+4) · להדליק External DB access ·
להוסיף redirect URI ב-Google Cloud Console · להזיז DNS ב-Cloudflare · לאשר כל
מחיקה בגל 4 · לאשר מחיקת פרויקטי Neon · להשהות את Render.

**הסוכן:** שאילתות האימות בגל 0 · כל שינויי הקוד בגל 1 · `create_app`/`set_env`/
`deploy`/הלוגים · המדידות בגל 2 · אפליקציית הטעינה וההעתקה · `verify_db_parity` ·
הסנכרון בחלון · הניטור.

---

## 13. הסיכונים, מדורגים

| סיכון | חומרה | הפחתה |
|---|---|---|
| PostGIS עדיין אינו זמין | **חוסם הכל מגל 4** | גל 0, שאילתה אחת. אם אין — עוצרים אחרי גל 3 והנתונים נשארים ב-Neon |
| התקרה 60 GiB לא תועלה | **חוסם את ה-append DB** | הכרעה #3. אכיפה רכה, אבל לא לבנות עליה |
| `CREATE ROLE` חסום | נסיגה אבטחתית בקונסולות הציבוריות | להישאר על שני מסדים (א׳), שם ההפרדה פיזית ולא תלויה בתפקיד |
| טוקן `db.xhostd.com` פג ב-30 יום | נפילת ייצור בתאריך ידוע | הכרעה #4. עד שתיענה — **לא להפנות ייצור ל-DSN חיצוני** |
| latency Neon↔xhostd בגל 2 | חוויית משתמש | נמדד לפני DNS. אם גרוע — או מאיצים לגל 5, או חוזרים ל-Render |
| שני סקדיולרים על אותם מסדים | ייבוא כפול, נזק לנתונים | `MAINTENANCE_MODE` בגל 1 היא הדרישה שמונעת את זה |
| זיכרון 3 GiB לכל החשבון | OOM על OVER או על שכניו | 626MB בשימוש היום; לנטר `get_app_stats` בגל 2 לפני DNS |
| דלתא שהולכת לאיבוד בחלון | אובדן נתונים שקט | הסדר בגל 6, בדיוק כפי שנכשל ב-court/shomrim ותוקן |

---

## 14. הצעד הבא

גל 0, שאלות 1 ו-2 — שתי שאילתות SQL. הן דורשות `get_credentials`, שנחסם בסשן הזה.
ברגע שיהיה credential, התשובה עליהן קובעת אם התוכנית הזו רצה עד הסוף או נעצרת
אחרי גל 3 עם OVER על xhostd והנתונים עדיין ב-Neon — וגם זה, לבדו, מוציא אותך
מ-Render.
