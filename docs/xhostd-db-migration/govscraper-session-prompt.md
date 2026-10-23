# פרומפט לסשן: העברת GOVSCRAPER מ-Render ל-xhostd

> להדביק כמו שהוא כהודעה הראשונה בסשן חדש, שתיקיית העבודה שלו היא `C:\Users\zomer\CLAUDE CODE\GOV scraper`.
> נכתב ב-11.9.2026 בסשן שמעביר את OVER. כל העובדות בסעיף 2 אומתו באותו יום מול Render, הקוד ומסד OVER.

---

## 1. המשימה וההיקף

אתה מעביר את **GOVSCRAPER** (ריפו `zomer-g/govil-scraper`, פרטי, בנתיב `C:\Users\zomer\CLAUDE CODE\GOV scraper`) מ-Render ל-xhostd. זה כולל את שירות הרשת, המסד שלו ב-Render והדיסק שלו.

**הסשן הזה עוסק רק ב-GOVSCRAPER.** סשן אחר מעביר במקביל את OVER (over.org.il, הריפו `ckan-version-tracker`). אל תיגע ב:
- הריפו של OVER, ובאפליקציה `over` ב-xhostd;
- השירות `ckan-version-tracker` ב-Render;
- פרויקטי Neon של OVER (`quiet-tooth-91355739`, `hidden-forest-76634201`);
- כל פרויקט אחר על xhostd או על Render.

היעד: GOVSCRAPER רץ על xhostd, מאומת, והמשאבים שלו ב-Render מושהים. המחיקה בסוף היא החלטה של המשתמש.

**לפני כל פעולה:** הפעל את ה-skill `xhostd-migrate` וקרא אותו. אחר כך קרא את `C:\Users\zomer\CLAUDE CODE\Ckan versions\docs\xhostd-db-migration\progress.md` כדי לדעת איפה נמצא המעבר של OVER (ראה סעיף 4).

---

## 2. המלאי (אומת 11.9.2026)

### ב-Render

| משאב | מזהה | פרטים |
|---|---|---|
| שירות רשת `govil-scraper` | `srv-d6pfv914tr6s73enb7o0` | Docker, plan starter (512 MB), `https://govil-scraper.onrender.com`, **בלי דומיין מותאם**, autodeploy מ-`master` |
| דיסק מתמיד | `dsk-d7qaaln7f7vs73cr4epg` | 5 GB ב-`/var/data/govil_scraper` (זה ה-`TEMP_DIR`) |
| Postgres `govil-scraper-db` | `dpg-d7qpkcmgvqtc73b114cg-a` | basic_256mb, **PG 18**, frankfurt, דיסק 5 GB |
| worker ברקע `over-worker` | `srv-d7gjfbm7r5hc738eaef0` | Docker, starter, **מושהה** (suspended). משתנים: `DISABLE_PLAYWRIGHT`, `OVER_API_KEY` |

**משתני `govil-scraper` (שמות בלבד):** ADMIN_EMAILS, DATABASE_URL, DISABLE_PLAYWRIGHT (`1` ב-Render), FLASK_SECRET_KEY, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, NADLAN_TRIGGER_KEY, OVER_API_KEY, WORKER_API_KEY.

### בקוד

- **הריצה:** `Dockerfile` מבוסס `python:3.12-slim`, עם תלויות Chromium ו-`playwright install chromium`. הפקודה: `gunicorn --bind 0.0.0.0:${PORT:-5000} --workers 2 --threads 4 --timeout 300 app:app`. הקובץ `app.py` הוא shim, והאפליקציה עצמה ב-`govscraper/web/app.py` (כ-1,400 שורות: jobs בזיכרון, SSE, ו-routes ב-`govscraper/web/routes/`).
- **תלות בדיסק, החסם העיקרי:**
  - `CollectionStore(TEMP_DIR)` הוא **SQLite** בקובץ `collections.db`, ומחזיק collections, קבצים, משימות וארכיונים מתוזמנים (`govscraper/storage/store.py`).
  - כל job כותב קבצים ל-`TEMP_DIR/<job_id>` (CSV, Excel, GeoJSON), והם מוגשים להורדה.
  - ב-xhostd מערכת הקבצים **נמחקת בכל deploy**.
- **Scheduler בתוך תהליך הרשת:** `_archive_scheduler_loop` הוא thread שנפתח **בכל תהליך gunicorn**, כלומר 2 כאלה. הוא בודק כל דקה ארכיונים מתוזמנים ומגן מפני כפילות עם `claim_archive_run`.
- **Postgres ב-Render (`pg_store.py`, psycopg 3):**
  - טבלאות: `nadlan_tasks`, `nadlan_settlement_tasks`, `nadlan_settlement_slices`, `nadlan_deals`, `sql_access`, `disabled_scrapers`, `taxes_nadlan_deals`, `taxes_nadlan_gushim`.
  - `sql_access` היא allowlist ברמת האפליקציה. **אין `CREATE ROLE` ואין `GRANT`**, כך שאין חסם של xhostd.
  - משתמשים בהן: `web/auth.py`, `web/routes/admin.py`, `compare.py`, `nadlan.py`, `analysis/deal_match.py`.
- **הדפדפן:** `DISABLE_PLAYWRIGHT=1` ב-Render, כלומר שירות הרשת לא משתמש בו בפועל. זה פותח אפשרות ל-image קטן בהרבה.
- **`NADLAN_TRIGGER_KEY` ו-`OVER_API_KEY`:** לא מופיעים ב-`govscraper/web`. **ברר מי משתמש בהם** (legacy, workers של נדל"ן, או קורא חיצוני).

### צי ה-workers של OVER, והקשר ל-OVER

- **איפה הם רצים:** אף worker לא רץ על Render. הצי רץ **על המחשבים של המשתמש** (`ThinkPad14`, `NucBox_G10`, `GZ-14`) דרך `run_over_worker.bat`, עם `.env` מקומי שמכיל `OVER_API_KEY`.
- **המצב כרגע:** ב-11.9 בבוקר, ה-worker האחרון (ThinkPad14, גרסה `eb82840`) נראה לפני 31 שעות, ו-104 משימות ב-7 הימים האחרונים. **ייתכן שכרגע אין worker פעיל.** זו תצפית למסור למשתמש, לא דבר לתקן.
- **הכתובת של OVER:** `govscraper/legacy/over_worker.py` מקודד `SERVER = "https://www.over.org.il"`.
- **עדכון עצמי:** ה-worker **מעדכן את עצמו** בין משימות. הוא עושה `git fetch`, fast-forward ל-upstream ו-`os.execv`. המשמעות: **push ל-`master` של GOVSCRAPER מגיע תוך דקות לכל הצי החי.** `WORKER_AUTO_UPDATE=0` מבטל את זה ב-worker מקומי.
- **השער ב-OVER:** ה-worker שולח `X-Worker-Upstream: current|behind|unknown`, ו-OVER דוחה רק `behind`. ל-OVER יש גם `worker_required_version` כעצירת חירום שמקבעת את הצי על commit ידוע.
- **OVER לא קורא ל-GOVSCRAPER בשום מקום.** התלות היא בכיוון אחד: ה-workers, והשירות, קוראים ל-OVER.
- **קוראים חיצוניים של `govil-scraper.onrender.com`:** חיפוש ב-11.9 בתוסף הכרום (`GOV scraper extension`), ב-OVER, ב-GOVMAP, ב-smart-dms וב-Z-G **לא מצא שום הפניה**. עדיין כדאי לבדוק bookmarks, משימות מתוזמנות ושירותים חיצוניים (למשל מי שולח `NADLAN_TRIGGER_KEY`), כי כל קורא כזה יישבר כשהכתובת תשתנה.

---

## 3. כללים שאין לעקוף

1. **שום מחיקה או פעולה בלתי הפיכה בלי אישור מפורש מהמשתמש, לכל פעולה בנפרד.** זה כולל מסדים, דיסקים, שירותים ב-Render, אפליקציות ב-xhostd ו-DROP. השהיה של שירות ב-Render היא שינוי בפרודקשן, ולכן גם עליה שואלים לפני.
2. **סודות:** ערכים לא מודפסים לשיחה, ללוגים או לקבצים.
   - מפתח ה-API של Render נמצא ב-`C:\Users\zomer\.claude\hooks\hook-render-deploy.js`. מחלצים אותו למשתנה shell ולא מדפיסים אותו.
   - העתקת סודות מ-Render ל-xhostd אפשרית בלי לחשוף ערכים. קוראים את `GET https://api.render.com/v1/services/{id}/env-vars`, וכותבים ל-`POST https://api.xhostd.com/apps/{app_id}/env` עם `{"key","value","kind":"secret"}`. ה-token מגיע מ-`get_credentials(scopes=["deploy:*","channel:*"], expires_in=3600)`. הפלט: שם, אורך וסטטוס בלבד. הכלי `set_env` שם את הערך בקריאה, ולכן אסור להשתמש בו לסודות.
   - **המשתמש אישר את השיטה הזו ל-OVER בלבד.** בסשן הזה צריך לבקש אישור מחדש.
3. **git:**
   - שינוי שנוגע ב-worker נעשה **על ענף**, ולא מתמזג ל-`master` עד שנבדק. push ל-`master` מגיע לצי החי, ראה סעיף 2.
   - commit עם pathspec, כי סשנים מקבילים עלולים לחלוק index.
   - סקריפטי shell עם LF ועם exec-bit: `git update-index --chmod=+x`.
4. **חסימות הרשאה:** אם מנגנון ההרשאות של הסשן חוסם פעולה, למשל `add_custom_domain` או שינוי בפרודקשן, **לא עוקפים.** עוצרים, מסבירים למשתמש מה ניסית ולמה, ונותנים לו להחליט.
5. **דיווח:** בעברית, ברור, בלי קו מפריד ארוך (—). במקומו כותבים פסיק ורווח.

---

## 4. תאימות למעבר של OVER, הסעיף החשוב

### החוזה שחייב להישאר

| נקודה | מה נדרש |
|---|---|
| **כתובת השרת** | ה-worker פונה ל-`https://www.over.org.il`. **לא לשנות** לכתובת xhostd. המעבר של OVER מעביר את אותו שם (Cloudflare → xhostd), כך שה-worker לא אמור להרגיש כלום. |
| **מפתח** | `OVER_API_KEY` ב-worker שווה ל-`WORKER_API_KEY` ב-OVER. הערך הזה כבר הועתק לאפליקציית OVER ב-xhostd. אם מחליפים אותו, מחליפים בשני הצדדים ובאותו חלון. |
| **מגבלת גוף** | OVER נשאר מאחורי Cloudflare, כך שמגבלת ~100 MB לבקשה ממשיכה לחול. העלאות R2 ישירות דרך `upload-r2/*` של OVER לא משתנות. |
| **השער** | `X-Worker-Upstream` נשאר. ענף מקומי שלא נדחף נחשב `current`. |

### שינוי חובה ב-worker: עמידות ל-503 (מצב תחזוקה של OVER)

- **מה OVER עושה בחלונות העתקת המסד** (גלים 6 ו-8 בתוכנית שלו): `MAINTENANCE_MODE=true` מחזיר **503 עם `Retry-After: 300` לכל `/api/worker/*`**, כולל לקיחת משימות, ולכל כתיבה אחרת תחת `/api/`. חלון כזה יכול להימשך דקות עד שעות.
- **מה ה-worker עושה היום** (נבדק 11.9):
  - לולאת ה-poll רושמת אזהרה וישנה, וזה בסדר.
  - `report_failure`: עד 3 ניסיונות עם 5 שניות ביניהם.
  - `upload_geojson` ו-`upload_csv`: עד 3 ניסיונות עם backoff קצר.
  - **`upload_zip` ו-push-version: בלי ניסיון חוזר בכלל.**
- **המשמעות:** חלון תחזוקה יאבד סריקות שהסתיימו.
- **הדרישה:**
  - retry עם backoff אקספוננציאלי שמכבד `Retry-After`, עד כ-6 שעות;
  - שמירת התוצאה בדיסק המקומי של ה-worker בין ניסיונות, כדי שתשרוד גם הפעלה מחדש;
  - אותו טיפול לכל קריאה שכותבת ל-OVER.
- **בדיקה:** מול שרת מדומה שמחזיר 503 עם `Retry-After`, ובנוסף מול OVER מקומי עם `MAINTENANCE_MODE=true`. הקוד של OVER תומך בזה מ-commit `8b5c77c`.
- **תזמון:** **חייב להיות ממוזג ופרוס לצי לפני גל 6 של OVER.** לפני המיזוג, קרא את `progress.md` של OVER וודא שאין חלון תחזוקה פתוח.

### לוח זמנים ותיאום

- **מצב OVER ב-11.9:** גל 2 חי. OVER רץ על xhostd בכתובת `over-zomerg.xhostd.app` מול Neon, והסקדיולר שם כבוי. `over.org.il` עדיין על Render.
- **גל 3 של OVER:** מעבר DNS, הכי מוקדם ב-**12.9.2026 בשעה 06:10 UTC**. **אל תמזג שינוי ב-worker ואל תחליף מפתחות** ביום של גל 3 או בזמן חלון תחזוקה.
- **תיאום:**
  - לפני כל מיזוג שנוגע בחוזה worker↔OVER, קרא את `progress.md` של OVER.
  - **אל תכתוב ל-repo של OVER.**
  - ניהול ההתקדמות שלך: `docs/xhostd-migration/progress.md` בריפו GOVSCRAPER.
  - אם מתגלה סתירה לחוזה, עצור ודווח למשתמש.
- **זיכרון ב-xhostd:** לכל החשבון יש 3 GiB משותפים, ו-OVER לבדו תופס ~1 GB ויותר. קרא `get_account_overview().plan_headroom` לפני deploy, ושקול `--workers 1` ב-gunicorn.

---

## 5. החלטות וחסמים לפני קוד (שלב 0)

1. **מלאי הדיסק:** מה באמת נמצא על 5 ה-GB. גודל `collections.db`, כמה תיקיות jobs, ומה מתוך זה חייב לשרוד. בלי shell ב-Render צריך endpoint של admin או את המשתמש (Render Shell).
2. **איפה יישב ה-store:** האפשרויות הן להעביר את `CollectionStore` מ-SQLite ל-Postgres (המסד המוזרק של xhostd), להשאיר SQLite ולגבות ל-Blob, או משהו שלישי. המלצה: Postgres, כי ה-scheduler כבר מגן מפני כפילות, וזה מבטל את התלות בדיסק.
3. **קבצי jobs:**
   - Blob המובנה של xhostd (`S3_ENDPOINT`/`S3_BUCKET` מוזרקים, 150 GB) או R2;
   - או להחליט שהם זמניים ומתחדשים.
   - ב-GOVSCRAPER **אין** היום משתני `S3_*`, כך שה-Blob המוזרק פנוי לשימוש, בניגוד ל-OVER.
4. **Playwright:** אם `DISABLE_PLAYWRIGHT=1` נשאר, אפשר לוותר על Chromium ב-image. אם לא, זה עולה בזיכרון.
5. **תבנית ב-xhostd:**
   - `docker`: ה-`Dockerfile` הקיים, ה-base `python:3.12-slim` הוא warm-base, צריך להחליף את `${PORT}` ב-`$XHOST_HTTP_PORT`, ומיגרציות רצות ב-CMD.
   - `app`: `install.sh` ו-`launch.sh`.
6. **מסד Render → המסד של ערוץ xhostd:**
   - PG 18 בשני הצדדים.
   - הדפוס המוכח: `pg_dump <render> | psql "$DATABASE_URL"` מתוך הקונטיינר, רק כשהיעד ריק, עם `postgresql-client-18` מ-PGDG.
   - **SSL:** המסד המוזרק של xhostd לא תומך בו. חיבור עם `sslmode=require` אליו ייכשל.
   - **כלים מוכחים לאימות ולסנכרון, להעתיק מ-OVER:** `C:\Users\zomer\CLAUDE CODE\Ckan versions\scripts\verify_db_parity.py` (checksum שלא תלוי בסדר) ו-`pg_resync_data.py` (binary COPY, רשימת עמודות מפורשת, FK retry).
   - **כתובת חיצונית ל-Render Postgres:** המשתמש צריך לספק אותה, או להדליק גישה חיצונית.
7. **`over-worker` המושהה ב-Render:** להשאיר כמו שהוא או למחוק בסוף. זו החלטה של המשתמש. הצי נשאר מקומי.
8. **מי משתמש ב-`NADLAN_TRIGGER_KEY` וב-`OVER_API_KEY`**, ומי קורא ל-`govil-scraper.onrender.com`.

---

## 6. גלים מוצעים

| גל | מה | קריטריון קבלה |
|---|---|---|
| **G0** | מלאי וסעיף 5, ודוח החלטות למשתמש | המשתמש אישר את ההחלטות |
| **G1** | קוד על ענף `xhostd-migration`, תואם-לאחור ל-Render: פורט, store, קבצים, קריאת DB, הפעלה. בדיקות. | `pytest` ירוק, Render ממשיך לעבוד מה-`master` |
| **W** (מקביל) | עמידות ה-worker ל-503, על ענף נפרד | עובר מול 503 מדומה ומול OVER מקומי במצב תחזוקה, וממוזג **לפני גל 6 של OVER** |
| **G2** | `create_app`, העתקת env באישור, push, deploy. **scheduler הארכיונים כבוי** כל עוד Render חי (מתג env), כדי שלא ירוצו שני schedulers. | deploy success, `/` מחזיר 200, לוגים נקיים |
| **G3** | העתקת מסד Render לערוץ xhostd ואימות parity. redirect URI ב-Google (המשתמש). בדיקה צד-לצד. | parity IDENTICAL, כניסה עובדת |
| **G4** | חלון מעבר: Render במצב קריאה בלבד או מושהה (באישור), סנכרון אחרון, הדלקת ה-scheduler ב-xhostd, עדכון קוראים חיצוניים לכתובת החדשה | 72 שעות יציבות, ארכיונים מתוזמנים רצו |
| **G5** | פירוק: dump קר של מסד Render לגיבוי, השהיה ואז מחיקה, **כל פריט באישור המשתמש** | Render של GOVSCRAPER על אפס |

---

## 7. מלכודות שכבר נלמדו (במעבר של OVER ובפרויקטים קודמים)

- `install.sh` רץ כ-root **בלי env**: אין מסד בזמן build, ולכן מיגרציות רצות ב-launch.
- `launch.sh` חייב `exec`, האזנה על `0.0.0.0:$XHOST_HTTP_PORT`, ו-`/` שמחזיר 2xx תוך 120 שניות.
- **משתנים שמורים שאי אפשר להגדיר:** `DATABASE_URL`, `DATABASE_URL_READONLY`, `DATABASE_HOST`, `DATABASE_PASSWORD`, `S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `PORT`, `XHOST_*`. אם צריך להפנות למסד אחר, משתמשים בשם אחר (למשל `GOVSCRAPER_DATABASE_URL`) וממפים אותו ב-launch.
- xhostd מזריק `postgres://`. psycopg 3 מקבל את זה, SQLAlchemy צריך dialect.
- **תבנית `app` ב-xhostd:** Python 3.13 ו-Node 22. **תבנית `docker`:** ה-base שבחרת.
- ב-xhostd, push ל-git **לא** מפעיל deploy. צריך לקרוא ל-`deploy` במפורש.
- **לוג build כושל ב-Render:** ה-hook מחזיר "(failed to fetch logs)". משתמשים ב-`GET https://api.render.com/v1/logs?ownerId=…&resource=<srv>&type=build&direction=backward` (את `ownerId` לוקחים מ-`GET /v1/services/<srv>`), וקוראים את ה-JSON כ-UTF-8.
- **שני תהליכים מול אותו מסד** (Render ו-xhostd בזמן מעבר): כל מה שכותב ברקע (ה-scheduler של הארכיונים) חייב מתג כיבוי.
- **migration שנועלת כמה טבלאות עמוסות** במסד חי צריכה טרנזקציה נפרדת לכל טבלה, עם `lock_timeout` וניסיונות חוזרים. אחרת היא נתקעת ב-deadlock מול האתר.

---

## 8. תוצרים

- `docs/xhostd-migration/progress.md` בריפו GOVSCRAPER: שורה לכל גל, עם תאריך, מה נמדד והחלטות המשתמש.
- בסיום כל גל: דיווח קצר למשתמש בעברית, מה נעשה, מה נבדק ומה נשאר, וכל פעולה שממתינה לאישור שלו מסומנת במפורש.
