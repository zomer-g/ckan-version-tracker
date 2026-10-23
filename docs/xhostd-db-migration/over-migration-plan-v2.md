# גרסאות לעם (OVER) — תוכנית מעבר ל-xhostd, גרסה 2 (מסמך ביצוע)

נכתב 2.9.2026 אחרי סקירה של `over-migration-plan.md` (v1). המסמך הזה **מחליף** את v1
כמסמך הביצוע. v1 ו-`findings-2026-09-01.md` נשארים כרקע: המלאי, המדידות והלקחים
מארבעת השירותים שכבר עברו נכונים ואינם חוזרים כאן.

המסמך כתוב לסשן שיבצע אותו. כל גל מסתיים ב**קריטריון קבלה** שאפשר להדפיס ללוג,
ובכל גל מסומן מה רק המשתמש יכול לעשות. אין להתקדם לגל הבא לפני שקריטריון הקבלה
של הגל הנוכחי התקיים ונרשם ב-`progress.md` (ראה §14).

---

## 0. מה השתנה מ-v1, בשורה לכל שינוי

| # | v1 | v2 | למה |
|---|---|---|---|
| 1 | גל 0 חסום על `get_credentials` | גל 0 רץ דרך **אפליקציית בדיקה** שמדפיסה ללוג | `create_app`+`deploy` בסמכות הסוכן; אין צורך ב-credential |
| 2 | הכרעה 2 = "האם `CREATE ROLE` מותר" | הכרעה 2 = "האם `GRANT` לתפקיד `DATABASE_URL_READONLY` המובנה עובד על סכימות שאינן `public`" | xhostd מזריק תפקיד קריאה-בלבד לכל מסד (llms-full.txt §DB) |
| 3 | שלוש טופולוגיות א׳/ב׳/ג׳ | **ב׳ נמחקה.** ברירת המחדל: א׳ הפוכה (append **מקומי** ב-`over`, app DB באפליקציה שנייה) | התפקיד המובנה קורא כל טבלה ב-`public` אוטומטית, ולכן מסד משותף חושף את `api_users` בלי שום טעות קונפיגורציה |
| 4 | טוקן `db.xhostd.com` פג ב-30 יום = חסם | **ג'וב חידוש credential** באפליקציה + סיסמה נפתרת בזמן חיבור | `POST /credentials` הוא API; הפגיעה הופכת לג'וב מתוזמן |
| 5 | `pg_toast` 10.9 GiB "מתהדק בשחזור" | TOAST הוא נתונים, לא bloat. bloat נמדד ב-`n_dead_tup` | אל תבנו שוליים על זה |
| 6 | `MAINTENANCE_MODE` אחד | שני מתגים: `SCHEDULER_ENABLED` ו-`MAINTENANCE_MODE` | גל 2 רוצה למדוד ג'ובים ובאותו זמן לא להריץ סקדיולר כפול |
| 7 | 58 GB נגד תקרה 60 GiB | תקרה מבוקשת לפי **קצב גידול לשנה** | המסד גדל בעשרות GB בחודשיים (ראה §3) |
| 8 | דלתא ב-`pg_resync_data.py` על הכל | סיווג טבלאות **קר/חם** ו**מקור-אמת/נגזר**; דלתא רק לחם-מקור-אמת | truncate+COPY על 2,545 טבלאות בחלון = להעתיק 58 GB מחדש |
| 9 | ההעתקה הגדולה מיד אחרי גל 0 | **חזרה גנרלית** על סכימה אחת עם geom, ואז **מסד ה-app (1.5 GB)** קודם | שלושה סיכונים הופכים למספרים; נוהל החלון נבדק על 1.5 GB לפני 58 |
| 10 | Neon נמחק אחרי שבוע | לפני המחיקה: **dump קר ל-R2** | snapshot של xhostd נשמר 7 ימים; egress חינמי |
| 11 | עצירה אחרי גל 3 = תוצאה סבירה | עצירה אחרי גל 3 משאירה ~$56 מ-$81 בחודש. מסלול מקביל: **הרדמת ה-computes של Neon** | 98% מחשבון Neon הוא compute שער בגלל ג'ובים כל 30 שניות |

---

## 1. עובדות שאומתו בסקירה (אל תבדוק שוב)

- `app/main.py:87` `_guard_db_separation()` + `app/config.py:683` `append_db_shares_main_db()`: מסרבים לעלות אם `DATABASE_URL` ו-`APPEND_DATABASE_URL` הם אותו (host, port, dbname).
- אין `MAINTENANCE_MODE` ואין `SCHEDULER_ENABLED` בקוד. הסקדיולר מתחיל ב-`app/worker/scheduler.py:543` דרך `init_scheduler()` ב-lifespan של `app/main.py`.
- `app/database.py:29`: SSL נגזר מהמחרוזת `"neon"` ב-URL. `append_store.get_pool()` (`app/services/append_store.py:127`) תמיד שולח `ssl=ctx`. שניהם ייפלו מול ה-Postgres הפנימי של xhostd, שאינו מדבר SSL.
- `append_store.get_pool()` הוא `min_size=0, max_size=5, command_timeout=180`.
- `render.yaml` מריץ `uvicorn ... --port $PORT --proxy-headers --forwarded-allow-ips="127.0.0.1,::1,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16"`.
- `APPEND_READONLY_DATABASE_URL` מחובר ב-`append_store.py:145` ו-`index_mirror.py:325`. אם לא מוגדר: fallback ל-pool הכתיבה עם אזהרה חד-פעמית.
- ב-`over_worker.py` בריפו GOVSCRAPER **לא נמצאה** לוגיקת retry/backoff על 5xx (grep ריק). זה ממצא שלילי מ-grep, לא מקריאה מלאה. גל 1 פריט 11 מאמת.
- xhostd (llms-full.txt, נשלף 2.9): כל ערוץ מקבל `DATABASE_URL` **ו-`DATABASE_URL_READONLY`** (תפקיד שני, `SELECT` על כל טבלה ב-`public`, בלי כתיבה; "data the read-only role must not see goes in a schema the app creates"). שניהם reserved ב-`set_env`. `POST /credentials` (bearer) מנפיק credential ל-30 יום עם `scopes` אופציונלי. יש טוקן סטטי `xh_...` ל-headless. `launch.sh` רץ כ-`app` (uid 10001), כותב רק ל-`/app`, `$HOME`, `/tmp`. `/` חייב 2xx תוך 120 שניות, או `$XHOST_READY_FILE`. `install.sh` רץ כ-root בלי env. Postgres של xhostd הוא **18** (ההעתקות של creteus רצו "שני הצדדים על PG 18").
- מסד ה-append ב-Neon: PG 18.6, 58 GB (`pg_database_size`), 2,545 טבלאות ב-9 סכימות, PostGIS 3.6.0 בסכימה `extensions`, `postgres_fdw` עם 0 foreign tables, `pg_trgm`/`pgcrypto`/`unaccent` בסכימה `ocal`. מסד ה-app: PG 17, 1.51 GB, `pg_trgm`.
- הצי (`govil-scraper`), ה-MCP של claude.ai וה-connector של Looker קשורים לדומיין `over.org.il` ולא לכתובת Render.
- `over.org.il` (אפקס) עושה 301 ל-`www.over.org.il` ב-Cloudflare. `files.over.org.il` הוא R2 ולא זז.

## 2. עובדות שעדיין לא אומתו (גל 0 עונה עליהן)

1. PostGIS זמין ב-Postgres של xhostd (v1 מדד ב-1.9 שלא; נמסר שהשתנה).
2. `CREATE ROLE` / `CREATE SCHEMA` / `CREATE EXTENSION` מותרים לתפקיד המוזרק.
3. `GRANT USAGE/SELECT` לתפקיד של `DATABASE_URL_READONLY` על סכימה שאינה `public` עובד, ו-`ALTER DEFAULT PRIVILEGES` נתפס.
4. הגדרות Postgres בפועל: `shared_buffers`, `work_mem`, `maintenance_work_mem`, `max_connections`, `statement_timeout`, `effective_cache_size`.
5. האם ה-Postgres של הערוץ נספר בתוך 3 GiB הזיכרון ו-3 הליבות של החשבון.
6. מה עולה snapshot `pre_deploy` של מסד 58 GB (זמן deploy, ואם הוא נספר ב-`database_storage`). OVER עושה deploy לעיתים קרובות; שלושה snapshots של 58 GB הם 174 GB.
7. האם ל-Neon (תוכנית Launch) יש חיוב egress על 58 GB יוצאים.
8. קצב הגידול של מסד ה-append. בזיכרון: 15.7 GB ב-23.7.2026, 58 GB ב-2.9.2026. ייתכן שחלק מההפרש הוא שיטת מדידה; להוציא סדרת `pg_database_size` יומית מה-Neon API (או מ-`consumption_history`) לפני שמבקשים תקרה.
9. הבדל בין 0.9 GiB (v1, relpages) ל-5.1 GB (findings §ו) לגודל `pilot_idx`. למדוד `pg_total_relation_size` על הסכימה.
10. מה קורה ב-`over_worker.py` כשהשרת מחזיר 503 על העלאת תוצאה.

---

## 3. הטופולוגיה הנבחרת

**ברירת מחדל: א׳-הפוכה.**

| אפליקציה ב-xhostd | מה בה | מסד |
|---|---|---|
| `over` | ה-web + הסקדיולר. **מסד ה-append (58 GB)** יושב ב-`DATABASE_URL` המוזרק שלה | מקומי. הקונסולות הציבוריות רצות על `DATABASE_URL_READONLY` המוזרק, אחרי `GRANT` על שאר הסכימות |
| `over-core` | שרת-סרק שמחזיק את **מסד ה-app (1.5 GB, `api_users`, `users`, ניהול)** | `over` מגיע אליו דרך `db.xhostd.com` עם credential מתחדש |

למה הפוך מ-v1: העומס הכבד (COPY של מיליוני שורות ב-sync, SQL ציבורי חופשי על 38 GB)
נשאר מקומי ומקבל תפקיד קריאה-בלבד שהפלטפורמה מתחזקת. הקפיצה החיצונית נופלת על
מסד ה-app, שכל שאילתה בו קטנה. האינווריאנט של `_guard_db_separation` נשמר כלשונו:
שני מסדים פיזיים, וה-secrets נמצאים במסד ש**אינו** זה של הקונסולה.

**תנאי הביטול (חוזרים ל-א׳ של v1, append באפליקציית-סרק):** אם גל 0 מראה
ש-snapshot של מסד גדול יקר (שאלה 6), או אם ה-latency של `db.xhostd.com` שנמדד
בגל 6 גדול מ-5 ms חציוני לשאילתה. במקרה כזה הקונסולות הציבוריות תלויות ב-`CREATE ROLE`
(שאלה 2), ואם גם הוא חסום: אכיפה ברמת האפליקציה בלבד, מתועדת כנסיגה.

**אם xhostd עונה שיש גישה פנימית בין אפליקציות או credential קבוע:** משתמשים בזה
וג'וב החידוש (גל 1 פריט 8) נשאר בקוד כ-no-op.

---

## 4. גל 0 — בדיקות. הסוכן מבצע היום, בלי credential

### 4א. אפליקציית בדיקה `over-probe`

`create_app(name="over-probe", template="app")`, ואז שני קבצים:

`install.sh`
```sh
#!/bin/sh
set -eu
apt-get update -qq && apt-get install -y -qq postgresql-client >/dev/null
```

`launch.sh`
```sh
#!/bin/sh
set -u
RO_ROLE=$(python3 -c "import os,urllib.parse as u;print(u.urlsplit(os.environ.get('DATABASE_URL_READONLY','')).username or '')")
echo "PROBE ro_role=$RO_ROLE"
psql "$DATABASE_URL" -v ON_ERROR_STOP=0 -v ro="$RO_ROLE" -e <<'SQL'
select version();
select current_user, session_user, rolsuper, rolcreaterole, rolcreatedb from pg_roles where rolname = current_user;
select name, default_version, installed_version from pg_available_extensions
 where name in ('postgis','postgis_topology','pg_trgm','pgcrypto','unaccent','postgres_fdw','pg_stat_statements') order by 1;
select name, setting, unit from pg_settings where name in
 ('shared_buffers','work_mem','maintenance_work_mem','max_connections','statement_timeout',
  'effective_cache_size','max_parallel_workers_per_gather','max_wal_size') order by 1;
create schema if not exists extensions;
create extension if not exists postgis schema extensions;
select extensions.postgis_full_version();
create extension if not exists pg_trgm;
create extension if not exists pgcrypto;
create extension if not exists unaccent;
create schema if not exists probe_s;
create table if not exists probe_s.t(id int primary key, g extensions.geometry(Point,4326));
insert into probe_s.t values (1, extensions.ST_SetSRID(extensions.ST_MakePoint(34.78,32.08),4326)) on conflict do nothing;
create index if not exists t_g_gist on probe_s.t using gist (g);
grant usage on schema probe_s to :"ro";
grant select on all tables in schema probe_s to :"ro";
alter default privileges in schema probe_s grant select on tables to :"ro";
grant usage on schema extensions to :"ro";
create role probe_ro nologin;
select rolname from pg_roles where rolname in ('probe_ro') or rolname = :'ro';
set session_replication_role = replica;
SQL
echo "PROBE readonly-side:"
psql "$DATABASE_URL_READONLY" -v ON_ERROR_STOP=0 -e \
  -c "set statement_timeout='5s'; select count(*) from probe_s.t;" \
  -c "select extensions.ST_AsText(g) from probe_s.t;" \
  -c "create table probe_s.should_fail(x int);" \
  -c "insert into probe_s.t values (2, null);"
echo "PROBE done"
exec python3 -m http.server "$XHOST_HTTP_PORT" --bind 0.0.0.0
```

`sed -i 's/\r$//'` על שניהם, `git update-index --chmod=+x`, push, `deploy`, ואז
`get_runtime_log` עם `grep -A200 PROBE`. הלוג הוא התשובה לשאלות 1 עד 4 ב-§2.
בסוף: `delete_app("over-probe")` (הסוכן, אחרי שהלוג נשמר ל-`probe-<תאריך>.txt` בתיקייה הזו).

### 4ב. פנייה אחת ל-xhostd (`submit_feedback`) — **דורשת אישור המשתמש לפני שליחה**

טיוטה לאישור, ארבע שאלות:
1. תקרת אחסון Postgres: יש לנו מסד של 58 GB שגדל ב-X GB לחודש (למלא מ-§2 שאלה 8). מה המסלול לתקרה של 120 GiB?
2. גישה בין אפליקציות של אותו חשבון למסד: יש credential שאינו פג, או כתובת פנימית? (המשך `c2d08389`.)
3. snapshot `pre_deploy` ו-`nightly` של מסד 58 GB: כמה זמן, והאם נספר ב-`database_storage`?
4. האם ה-Postgres של הערוץ נספר בתוך `memory`/`cpu` של החשבון ב-`plan_headroom`?

### 4ג. מדידות ב-Neon (הסוכן, קריאה בלבד)

```sql
-- על מסד ה-append
select schemaname, pg_size_pretty(sum(pg_total_relation_size(schemaname||'.'||quote_ident(tablename))))
from pg_tables group by 1 order by 2 desc;                      -- מכריע 0.9 מול 5.1 של pilot_idx
select sum(n_dead_tup) dead, sum(n_live_tup) live from pg_stat_user_tables;   -- bloat אמיתי
```
ו-`GET /projects/hidden-forest-76634201/consumption_history` (Neon API) לסדרת הגודל היומית.
לבדוק בתוכנית Launch של Neon אם יש חיוב data transfer.

### קריטריון קבלה של גל 0
קובץ `probe-<תאריך>.txt` בתיקייה הזו עם הלוג המלא, ושורה ב-`progress.md` לכל אחת
מ-10 השאלות ב-§2: **כן / לא / נשלח ל-xhostd ב-<תאריך>**. אם PostGIS = לא: גלים 1 עד 3
ומסלול ג׳ (§11) ממשיכים, גלים 5 ואילך נעצרים.

---

## 5. גל 1 — קוד. branch `xhostd-migration`, תואם-לאחור, Render ממשיך לעבוד

| # | שינוי | קובץ | פרטים |
|---|---|---|---|
| 1 | `install.sh` | חדש | `uv pip install --system --no-cache -r requirements.txt` ואז `cd frontend && npm ci && npm run build`. אין ערכי build לצרוב (הפרונטאנד לא קורא `import.meta.env`). |
| 2 | `launch.sh` | חדש | מפעיל uvicorn **קודם** ברקע, ממתין ל-`/healthz` מקומי, יוצר `$XHOST_READY_FILE`, ואז `alembic upgrade head`, ואז `wait`. אם המיגרציה נכשלת: `kill` ל-uvicorn ויציאה עם קוד שגיאה, כדי שה-deploy ייכשל וה-גרסה הקודמת תמשיך. זה מוריד את המיגרציה מחלון 120 השניות. |
| 3 | פורט | `launch.sh` | `${XHOST_HTTP_PORT:-${PORT:-8000}}` |
| 4 | `--forwarded-allow-ips` | `launch.sh` | הערך הקיים + טווח ה-ingress של xhostd **כפי שנמדד בלוג של גל 2** (ה-DB הפנימי נמדד ב-`10.200.2.2`). לעולם לא `*`. |
| 5 | שרשרת DSN | `app/config.py` | `Settings.effective_database_url` = `OVER_DATABASE_URL or DATABASE_URL`; `effective_append_database_url` = `OVER_APPEND_DATABASE_URL or APPEND_DATABASE_URL`; אותו דבר ל-readonly. `_guard_db_separation` משווה את ה**אפקטיביים**. השם `OVER_*` ולא `NEON_*`: אחרי גל 8 הערך יצביע ל-xhostd. |
| 6 | SSL לפי מארח | `app/database.py`, `append_store._dsn_from`/`get_pool`/`get_readonly_pool` | פונקציה אחת `db_ssl_for(host)`: SSL ל-`*.neon.tech`, `*.render.com`, `db.xhostd.com`; בלי SSL ל-RFC1918/loopback/שם ללא נקודה. גם להסיר את `statement_cache_size=0` רק אם המארח אינו pooler של Neon. |
| 7 | שני מתגים | `app/config.py`, `app/main.py`, `app/worker/scheduler.py`, middleware חדש | `SCHEDULER_ENABLED` (ברירת מחדל `true`): `false` = `init_scheduler()` לא נקרא, אבל endpoint-ים של admin לטריגר ג'וב בודד עובדים. `MAINTENANCE_MODE` (ברירת מחדל `false`): `true` = הסקדיולר לא רץ **וגם** כל `POST/PUT/DELETE` תחת `/api/worker/*`, `/api/admin/*`, `/api/datasets/*/poll` מחזיר 503 עם `Retry-After: 300`; קריאה ממשיכה. `/healthz` מחזיר את מצב שני המתגים ב-JSON. |
| 8 | credential מתחדש | `app/services/xhost_credential.py` חדש, ג'וב בסקדיולר | אם `XHOST_API_TOKEN` ו-`XHOST_DB_NAME` מוגדרים: כל 7 ימים `POST /credentials` (bearer = הטוקן הסטטי, `scopes` מצומצם למסד בלבד אם ה-API מקבל) ושמירת `{token, expires_at}` בזיכרון + ב-`$HOME/.xhost_db_cred.json`. הסיסמה **נפתרת בזמן חיבור**: asyncpg מקבל `password=callable`; ב-SQLAlchemy: `@event.listens_for(engine.sync_engine, "do_connect")` שמזריק `cparams["password"]`. אין צורך לבנות engine מחדש. אם החידוש נכשל 3 פעמים ברצף: לוג CRITICAL ו-`activity_log`. |
| 9 | `min_size` | `append_store.py:131` | `APPEND_POOL_MIN_SIZE` (ברירת מחדל 0). ב-xhostd יוגדר 1. |
| 10 | `GRANT` בקוד | `index_mirror.ensure_schema`, `scripts/create_append_readonly_role.sql` | הסקריפט מקבל שם תפקיד קיים במקום ליצור אותו (`-v ro_role=<שם מ-DATABASE_URL_READONLY>` ובלי `CREATE ROLE` כשהוא קיים). כל סכימה ש-OVER יוצר (`idx`, `knesset`, `odata`, `ocal`, `ocoi`, `extensions`) מקבלת `GRANT USAGE` + `ALTER DEFAULT PRIVILEGES` לתפקיד הזה. |
| 11 | worker מול 503 | ריפו GOVSCRAPER, `over_worker.py` | לקרוא את הקוד במלואו. אם אין retry על 5xx בהעלאת תוצאה: להוסיף backoff אקספוננציאלי עד 6 שעות עם שמירת התוצאה בדיסק המקומי בין ניסיונות. **בלי זה גל 8 מאבד סריקות.** |
| 11ב | **R2 מול ה-S3 המוזרק** | `app/config.py:63-71` | OVER קורא `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `S3_PUBLIC_BASE_URL`. xhostd **שומר** `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION` (ועוד `S3_ACCESS_KEY_ID`/`S3_SECRET_ACCESS_KEY`) ומזריק אליהם את ה-Blob הפנימי שלו; `set_env` ידחה אותם. בלי תיקון, OVER על xhostd יכתוב ל-Blob של xhostd במקום ל-R2 **בשקט**. התיקון: `R2_ENDPOINT`/`R2_BUCKET`/`R2_REGION`/`R2_ACCESS_KEY`/`R2_SECRET_KEY`/`R2_PUBLIC_BASE_URL` עם **עדיפות** על `S3_*` כשהם מוגדרים; Render ממשיך עם `S3_*`. v1 טעה כאן ("משתני R2 עוברים כמו שהם"). |
| 12 | דומיינים | `app/config.py` `cors_origins`, `APP_BASE_URL` | לוודא ש-`www.over.org.il` ו-`over.org.il` שניהם מטופלים (ה-301 היום ב-Cloudflare; אם הוא כלל של CF הוא נשאר, אם הוא של Render הוא נעלם בגל 3). |
| 13 | לקומיט | git | `scripts/pg_export.py`, `scripts/verify_db_parity.py`, `scripts/pg_resync_data.py` (untracked היום) + `scripts/db_classify.py` החדש (§7). |
| 14 | בדיקות | `tests/` | `test_maintenance_mode.py` (503 על כתיבה, 200 על קריאה, סקדיולר לא מותחל), `test_db_ssl_for.py`, `test_xhost_credential.py` (callable מחזיר את הטוקן החדש אחרי חידוש). |

### קריטריון קבלה של גל 1
`pytest` ירוק; ה-branch נפרס ל-Render **preview** או רץ מקומית מול Neon עם ערכי ברירת
מחדל ומתנהג זהה ל-master; `MAINTENANCE_MODE=true` מקומית מחזיר 503 על `POST /api/worker/...`.

---

## 6. גל 2 — compute על xhostd, נתונים ב-Neon. rollback = לא לעשות כלום

```
create_app(name="over", template="app")
git push <branch>:master
set_env OVER_DATABASE_URL         = DSN של Neon app DB        (secret)
set_env OVER_APPEND_DATABASE_URL  = DSN של Neon append DB     (secret)
set_env OVER_APPEND_READONLY_DATABASE_URL = DSN של over_readonly ב-Neon (secret)
set_env <כל הסודות מ-Render Environment>   ← המשתמש מספק את הערכים
set_env R2_ENDPOINT / R2_BUCKET / R2_REGION / R2_ACCESS_KEY / R2_SECRET_KEY / R2_PUBLIC_BASE_URL  ← ערכי R2 מ-Render (גל 1 פריט 11ב). לא S3_*.
                                     אימות בגל 2: העלאת קובץ קטן דרך admin ובדיקה שהוא מופיע ב-files.over.org.il ולא ב-Blob של xhostd
set_env APP_BASE_URL = https://over-zomerg.xhostd.app
set_env SCHEDULER_ENABLED=false · MAINTENANCE_MODE=false · MALLOC_ARENA_MAX=2 · APPEND_POOL_MIN_SIZE=1
deploy(ref="master")
```

מדידות, כולן על הכתובת הזמנית, בלי DNS:

| # | מדידה | איך | סף |
|---|---|---|---|
| 1 | deploy + SPA | `curl -o /dev/null -w %{http_code}` על `/`, `/assets/...`, `/healthz` | 200 |
| 2 | latency ל-Neon | 20 קריאות ל-`/api/datasets` ול-`POST /api/tables/sql` עם שאילתה קבועה, מ-Render ומ-xhostd, חציון | לרשום. אם xhostd איטי פי 3 מ-Render: לא להזיז DNS לפני גל 8 |
| 3 | טווח ה-ingress | `get_runtime_log` עם `grep X-Forwarded-For` אחרי בקשה עם `X-Forwarded-For: 1.2.3.4` | להשלים פריט 4 בגל 1 ולעשות deploy נוסף |
| 4 | גזירת IP | בקשה עם `CF-Connecting-IP` מזויף ישירות לכתובת xhostd; `/api/...` צריך לראות את ה-peer ולא את הזיוף | כמו ב-Render |
| 5 | זיכרון | `get_app_stats` לפני ואחרי הפעלה ידנית של `refresh_dataset_sizes` ושל טעינת `over_re_parcels` דרך admin endpoint (זה למה `SCHEDULER_ENABLED=false` ולא `MAINTENANCE_MODE`) | שיא < 1.2 GB, `oom_kills=0` |
| 6 | OAuth | `curl -i /auth/google/start` → 302 עם `redirect_uri` הזמני | **המשתמש** מוסיף אותו ב-Google Cloud Console |
| 7 | MCP, Looker, deep-search | `/mcp` עם `MCP_SERVICE_TOKEN`; `/api/connector` עם מפתח; `/api/deep-search` | 200 |

### קריטריון קבלה של גל 2
טבלת המדידות ב-`progress.md`; `oom_kills=0`, `restart_count=0` אחרי 24 שעות.

---

## 7. גל 3 — DNS. over.org.il עובר

1. `add_custom_domain` ל-**שני** השמות: `over.org.il` ו-`www.over.org.il`. הסוכן מחזיר CNAME/A + TXT `_xhost` לכל אחד.
2. **המשתמש** ב-Cloudflare: קודם רשומות **DNS-only (אפור)** עד ש-`verify_custom_domain` מחזיר תעודה, ואז **כתום**. הענן הכתום חובה בסופו של דבר (`app/client_ip.py` מאמת מול טווחי Cloudflare). SSL mode: Full (strict). לשמור/ליצור כלל redirect אפקס→www ב-CF.
3. באותו חלון: ב-xhostd `SCHEDULER_ENABLED=true` + deploy; ב-Render `MAINTENANCE_MODE=true` + deploy (או השהיה). **לא** להשאיר שני סקדיולרים חיים יותר מדקות.
4. `APP_BASE_URL=https://www.over.org.il`, redirect URI חוזר לדומיין הקנוני.
5. הצי, MCP, Looker: אין שינוי. `files.over.org.il`: אין שינוי.

### קריטריון קבלה של גל 3
72 שעות: `get_app_health` נקי, `activity_log` מראה polls שהסתיימו, `oom_kills=0`. שירות Render במצב `MAINTENANCE_MODE=true` ולא נמחק.

**נקודת עצירה מותרת.** אם PostGIS = לא, או שהתקרה לא תועלה: OVER נשאר כאן. אז חובה
לבצע את §11 (מסלול ג׳), אחרת החשבון של Neon לא יורד.

---

## 8. גל 4 — סיווג והרזיה (בלי להזיז בייט)

### 8א. סיווג כל 2,545 הטבלאות: `scripts/db_classify.py` (חדש)

מריץ על מסד ה-append (קריאה בלבד) ומפיק `classify-<תאריך>.csv` עם עמודות:
`schema, table, total_bytes, n_tup_ins+upd+del (מאז stats reset), last_autoanalyze, class, origin`.

| `class` | כלל | טיפול בגל 7-8 |
|---|---|---|
| **cold** | 0 שינויים ב-30 יום | מועתק פעם אחת בזמן שהאתר חי. **אין דלתא.** |
| **hot-derived** | משתנה, אבל נבנה מחדש על ידי ג'וב מ-R2/ODATA (`idx.*` דרך `index_mirror`, `knesset.*` דרך `knesset_db_sync`) | מועתק **עם** טבלת ה-checkpoint שלו (`idx._sync_state`); הג'וב הבא משלים לבד. **אין דלתא.** |
| **hot-source** | משתנה ואין לו מקור חיצוני שמשחזר אותו (`ocal.*` מהסקרייפר החי, טבלאות `append_only` ב-`public` כמו מרשם רכב והחלטות ממשלה, `odata.*`, `ocoi.*`) | מועתק בפס הקר **ומסונכרן דלתא** בחלון. |
| **drop** | `pilot_idx.*`, `idx_backup.*`, foreign server `ocal_srv` | נמחק לפני ההעתקה, פריט-פריט, באישור. |

`origin` ממולא מהמטא-דאטה של OVER (מסד ה-app: `datasets.storage_plan`, `versions`) כדי לדעת
לאיזה מאגר כל `append_*` שייך. הפלט הוא **הקלט של גל 7 ו-8**; בלעדיו לא מתחילים.

### 8ב. מחיקות (המשתמש מאשר כל שורה)
`DROP SCHEMA pilot_idx CASCADE`, `DROP SCHEMA idx_backup CASCADE`, `DROP SERVER ocal_srv CASCADE`,
ואחריהן `pg_database_size` חדש. לרשום ב-`progress.md` את הגודל לפני/אחרי.

### קריטריון קבלה של גל 4
`classify-<תאריך>.csv` בתיקייה, וסכום הבייטים לפי `class` ב-`progress.md`. הסכום של
`hot-source` הוא **גודל החלון** בגל 8; אם הוא מעל 5 GB, לחפש טבלאות שאפשר להעביר ל-cold
(למשל להשהות מאגר `append_only` ספציפי לזמן ההעתקה).

---

## 9. גל 5 — חזרה גנרלית: `idx` בלבד לאפליקציה זמנית

`create_app(name="over-rehearsal", template="app")`. `install.sh` מתקין `postgresql-client-18`
**מ-PGDG** (המקור הוא 18.6). `launch.sh` מעלה `http.server` קודם, ואז ברקע:

```sh
psql "$DATABASE_URL" -c "create schema if not exists extensions; create extension if not exists postgis schema extensions;"
time pg_dump "$NEON_SOURCE_DSN" --schema=idx --no-owner --no-privileges -Fp | psql "$DATABASE_URL" -v ON_ERROR_STOP=1 2>&1 | tail -5
psql "$DATABASE_URL" -c "select pg_size_pretty(pg_database_size(current_database()))"
```

מה נמדד, וכל מספר נרשם:

| מדד | למה |
|---|---|
| MB/s של הזרם `pg_dump \| psql` | ETA ל-58 GB: `58000 / MB/s`. אם מעל 8 שעות: לפצל `public` לזרמים מקבילים לפי `--table` ולמדוד שוב |
| גודל על הדיסק ב-xhostd מול `pg_total_relation_size` של `idx` ב-Neon | יחס ההידוק האמיתי (ולא הנחת TOAST) |
| `geom` שרד: `select count(*) from idx.<טבלה> where geom is not null` שווה ל-Neon | PostGIS בפועל |
| 10 שאילתות מייצגות של `/data` (מ-`docs/` דוגמאות הקונסולה) על `DATABASE_URL_READONLY`, חציון | מול אותן שאילתות ב-Neon. זה **המדד היחיד** לביצועי הקונסולה על xhostd |
| latency `db.xhostd.com`: 200 `select 1` מתוך `over` (גל 2) לעבר מסד `over-rehearsal` דרך הכתובת החיצונית | סף §3: חציון ≤ 5 ms מאפשר א׳-הפוכה. דורש **External database access** בקונסולה (המשתמש) |
| `verify_db_parity.py --schema idx` | IDENTICAL |

בסוף: `delete_app("over-rehearsal")`. אם המשתמש מעדיף לשמור אותה כאפליקציית-הטעינה של גל 7: להשאיר.

### קריטריון קבלה של גל 5
שש השורות בטבלה ממולאות ב-`progress.md`, ו-ETA כתוב ל-58 GB. הטופולוגיה (§3) **נקבעת כאן** ונרשמת.

---

## 10. גל 6 — מסד ה-app (1.5 GB): נוהל החלון בסיכון נמוך

זה הגל שמריץ לראשונה את כל הנוהל של גל 8, על מסד קטן.

1. `create_app(name="over-core", template="app")`: שרת-סרק (`http.server`) + העתקה חד-פעמית מגודרת על "היעד ריק", כמו ב-court/shomrim. `pg_trgm` על היעד קודם.
2. **המשתמש**: External database access ל-`over-core`; מנפיק טוקן סטטי `xh_...` בקונסולה ומוסר לסוכן לצורך `set_env XHOST_API_TOKEN` (secret) ב-`over`.
3. ב-`over`: `set_env XHOST_DB_NAME=over-core`, deploy, לוודא בלוג שג'וב החידוש הצליח פעם אחת ושה-callable מחזיר סיסמה.
4. **חלון** (מינימלי, דקות): `MAINTENANCE_MODE=true` ב-`over` + deploy → `pg_stat_activity` על Neon app DB מראה 0 כותבים → `pg_resync_data.py` (מסד קטן: על הכל) → `verify_db_parity.py` IDENTICAL → `set_env OVER_DATABASE_URL=<DSN חיצוני של over-core>` → deploy → `MAINTENANCE_MODE=false` → `alembic current` ב-`launch.sh` מדפיס את אותה רוויזיה.
5. **rollback** בתוך 24 שעות: להחזיר `OVER_DATABASE_URL` ל-Neon + deploy, ואז `pg_resync_data.py` הפוך (מ-xhostd ל-Neon) כדי לא לאבד כניסות/טוקנים שנוצרו בינתיים.

### קריטריון קבלה של גל 6
72 שעות: כניסות SSO עובדות, `api_users` מקבל טוקנים חדשים, `activity_log` נכתב, ג'וב החידוש רץ פעם אחת (לוג). `quiet-tooth` ב-Neon ללא חיבורים.

---

## 11. מסלול ג׳ (מקביל, לא גל) — להרדים את ה-computes של Neon

מבוצע אם גל 7 מתעכב מעל שבועיים אחרי גל 3, או אם גלים 5 ואילך נעצרו. הוא לבדו
מוריד את רוב חשבון Neon ($54.85 מתוך $56.08 הוא compute), בלי xhostd.

1. `drain_datastore_push_queue` ו-`drain_drive_export_queue` (30 שניות): להפוך ל-`LISTEN/NOTIFY` או לטריגר ב-enqueue, כך שלא יסקרו.
2. שאר הג'ובים ב-≤5 דקות (`resume_interrupted_appends`, `govmap_coverage_rollout`, `knesset_db_sync`, שלושת ה-`cleanup_*`): יישור פאזה לפרץ אחד כל 15 דקות, בשתי קבוצות (app DB / append DB).
3. מדידה: `active_time` של שני הפרויקטים ב-Neon API לפני ואחרי שבוע.

לא נוגעים ב-`max_instances=1`.

---

## 12. גל 7 — הפס הקר: 58 GB בזמן שהאתר חי

אפליקציית טעינה `over-loader` (או `over-rehearsal` שנשמרה). **היעד הוא `DATABASE_URL` של `over`**
(א׳-הפוכה), ולכן ההעתקה רצה מתוך `over` עצמה או מ-`over-loader` דרך `db.xhostd.com` של `over`.
עדיף מתוך `over`: `launch.sh` של `over` מקבל מצב `LOADER_MODE=true` שבו הוא מעלה רק את
`/healthz` ואת ההעתקה ברקע עם `/loader/status` (התקדמות פר-סכימה/פר-קבוצת טבלאות).
`over` ממשיך לשרת מ-Neon כל הזמן הזה, כי `OVER_APPEND_DATABASE_URL` עדיין מצביע ל-Neon.

סדר:
1. על היעד: `CREATE SCHEMA extensions; CREATE EXTENSION postgis SCHEMA extensions;` + `pg_trgm`, `pgcrypto`, `unaccent`. **לא** `postgres_fdw`.
2. `pg_dump --schema-only` של הכל (בלי `pilot_idx`/`idx_backup`), `psql` על היעד. `GRANT` לתפקיד readonly על כל הסכימות (הסקריפט מגל 1 פריט 10).
3. `pg_dump --data-only` לפי `classify.csv`: קודם **cold** (בזרמים מקבילים לפי קבוצות טבלאות של ~5 GB, N זרמים לפי ה-MB/s מגל 5), אחר כך **hot-derived** כולל `idx._sync_state`, ואחרון **hot-source** (הוא יסונכרן שוב בחלון, אז מספיק ההעתק הראשון).
4. אינדקסים: `pg_dump --section=post-data` בסוף (GiST על 318 שכבות, btree על הכל). `maintenance_work_mem` מוגדר ב-session אם מותר.
5. `verify_db_parity.py` פר-סכימה על **cold**: חייב IDENTICAL. על hot: `count(*)` בלבד (הוא יזוז ממילא).
6. `pg_database_size` על היעד. לרשום מול התקרה.

### קריטריון קבלה של גל 7
cold IDENTICAL בכל הסכימות; אינדקסים קיימים (`select count(*) from pg_indexes` שווה ל-Neon פחות המחיקות); הגודל מתחת לתקרה שאושרה בגל 0 עם שוליים של 12 חודשי גידול.

---

## 13. גל 8 — חלון ההחלפה של ה-append

הסדר. אין קיצור דרך.

1. `MAINTENANCE_MODE=true` ב-`over` + deploy. הצי מקבל 503 עם `Retry-After` (גל 1 פריט 11 מבטיח שהוא ממתין ולא מאבד).
2. `select count(*) from pg_stat_activity where datname=current_database() and state<>'idle' and pid<>pg_backend_pid()` על Neon append = 0. להמתין ל-`resume_interrupted_appends` שאינו באמצע COPY (`activity_log`).
3. `pg_resync_data.py --tables-from classify.csv --class hot-source`: truncate+COPY **רק** של hot-source. `FORMAT BINARY`, רשימת עמודות מפורשת (שני התיקונים מ-findings §ד כבר בכלי).
4. `verify_db_parity.py --class hot-source` → IDENTICAL. `verify_db_parity.py --counts-only` על הכל → זהה.
5. `set_env OVER_APPEND_DATABASE_URL=$DATABASE_URL` (המוזרק), `OVER_APPEND_READONLY_DATABASE_URL=$DATABASE_URL_READONLY`; deploy עם `MAINTENANCE_MODE=false`.
6. אימות: `/data` מריץ שאילתה על `idx` עם `ST_AsText`; `/knesset`; `POST /api/tables/sql` מחזיר תוצאה מ-`ocal`; `pg_stat_activity` על היעד מראה את תפקיד ה-readonly בחיבור של הקונסולה (הטריק מהזיכרון: קודם לפגוע ב-`/data`, אז לשאול).
7. הצי חוזר לבד. `activity_log` מראה poll ראשון שהסתיים על היעד.

**rollback:** עד 24 שעות: להחזיר את שני המשתנים ל-Neon + deploy, ואז דלתא הפוכה של hot-source
מ-xhostd ל-Neon. אחרי 24 שעות: אין rollback, יש תיקון קדימה.

### קריטריון קבלה של גל 8
72 שעות: `hidden-forest` ב-Neon ללא חיבורים; `index_mirror` השלים tick עם `postgis_rows>0`; `oom_kills=0`; זמן חציוני של 10 שאילתות הקונסולה (מגל 5) לא גרוע מ-Neon.

---

## 14. גל 9 — פירוק, בסדר הזה

1. 72 שעות יציבות אחרי גל 8.
2. **dump קר ל-R2**: `pg_dump -Fc` של שני מסדי Neon ישירות ל-`files.over.org.il`/`archive/neon-final-<תאריך>/` (egress חינמי מ-xhostd, R2 כבר קיים; לוודר שאין חיוב egress ב-Neon, §2 שאלה 7). לרשום `sha256`.
3. `export_data(scope="app")` ב-xhostd ל-`over` ו-`over-core` (takeout).
4. השהיית שירות Render `ckan-version-tracker` (**המשתמש**). שבוע.
5. מחיקת `quiet-tooth-91355739` ו-`hidden-forest-76634201` (**המשתמש מאשר**), ואז `masham` לפי v1 §10.
6. `delete_env NEON_SOURCE_DSN` ו-`NEON_*` בכל אפליקציית xhostd. סגירת חשבון Neon.
7. עדכון `README.md` בתיקייה הזו, `xhostd/05-render-exit-map.md`, והזיכרון `over-xhostd-migration-plan`.

---

## 15. מי עושה מה

**רק המשתמש:** לאשר את הפנייה ל-xhostd (§4ב) · להדליק External database access ל-`over-core` ולאפליקציית החזרה · להנפיק טוקן סטטי `xh_...` · לספק את ערכי הסודות מ-Render · להוסיף redirect URI ב-Google Cloud Console (זמני ואז קנוני) · לשנות DNS ב-Cloudflare · לאשר כל `DROP` בגל 4 · להשהות Render · לאשר מחיקת פרויקטי Neon.

**הסוכן:** הכל האחר: אפליקציית הבדיקה, מדידות Neon, הקוד ב-branch, `create_app`/`set_env`/`deploy`/לוגים, `db_classify.py`, החזרה הגנרלית, ההעתקות, `verify_db_parity`, הסנכרון בחלון, הניטור, `progress.md`.

## 16. איסורים

- לעולם לא `set_env DATABASE_URL` (reserved) ולא `--forwarded-allow-ips="*"`.
- לא להפנות שירות **ב-Render** ל-DSN של `db.xhostd.com` (הטוקן פג; Render לא מריץ את ג'וב החידוש).
- לא להריץ שני סקדיולרים על אותם מסדים יותר מדקות (גל 3 סעיף 3).
- לא `pg_resync_data.py` על כל 2,545 הטבלאות; רק לפי `classify.csv`.
- לא למחוק דבר ב-Neon או ב-xhostd בלי אישור פר-פריט.
- לא לקבוע ש-PostGIS "כנראה קיים": רק הלוג של גל 0.

## 17. ניהול ההתקדמות

`docs/xhostd-db-migration/progress.md`: שורה לכל גל עם תאריך, קריטריון הקבלה כפי שנמדד,
וקישור לקובץ הלוג/המדידה. סשן שמתחיל קורא קודם את `progress.md`, ואז את הגל הפתוח כאן.
המסמך הזה לא נערך במהלך הביצוע; סטיות נרשמות ב-`progress.md` עם הסיבה.

## 18. מה חוסך, ומתי

| אחרי גל | Render | Neon | הערה |
|---|---|---|---|
| 3 | 0 | ~$56 | **לא** חיסכון ב-Neon. מסלול ג׳ (§11) הוא היחיד שמוריד אותו בשלב הזה |
| 6 | 0 | ~$28 (`hidden-forest` בלבד) | `quiet-tooth` נסגר |
| 8 | 0 | ~$0 | הכל ב-xhostd (plan pro, כבר משולם) |
| 9 | 0 | 0 | החשבון נסגר |
