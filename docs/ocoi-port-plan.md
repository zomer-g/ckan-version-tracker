# תוכנית הטמעת "ניגוד עניינים לעם" (OCOI) בתוך "גרסאות לעם" (OVER)

> נכתב 2026-08-13, על בסיס סקר קוד מלא של `github.com/zomer-g/ocoi` @ master
> והניסיון המלא מהעברת "יומן לעם" (Ocal) שהושלמה היום.
> **כלל מחייב (כמו ב-Ocal): כל שלב = commit נפרד ב-GitHub.**

---

## 0. תקציר מנהלים — במה זה שונה מיומן לעם

ההעברה הזאת **קלה יותר** מ-Ocal בציר אחד קריטי, וקשה יותר בשניים.

| ציר | Ocal (הושלם) | OCOI (התוכנית הזאת) |
|---|---|---|
| Backend | Node/Express/Knex → **כתיבה מחדש מלאה** | **Python 3.12 + FastAPI + SQLAlchemy 2 async — אותו stack בדיוק כמו OVER** ✅ |
| Frontend | React | Next.js 15 App Router → פורט ל-Vite/React Router של OVER |
| היקף נתונים | 493k אירועים | **קטן**: 2,971 מסמכים / 2,312 אנשים / 8,939 חברות / 1,463 עמותות (+721k שורות registry) |
| Migrations | 34 קבצי Knex | **אין בכלל** — `create_all()` + סקריפט ALTER ידני בכל boot |
| MCP | נבנה מאפס | **קיים כבר** (OAuth 2.1+PKCE, 10 כלים) |
| Runtime | Node על Render | **Docker** על Render (בגלל poppler+tesseract) ❗ |
| חידושים | — | **Stripe metered billing**, תוסף כרום, גרף Cytoscape, PDF blobs ב-BYTEA |

**המסקנה המרכזית:** רוב שכבת ה-API/DB עוברת כמעט as-is (אותו stack). שני הקשיים האמיתיים הם
(1) ה-Docker/OCR, ו-(2) פורט הפרונט מ-Next.js.

### הכרעות המשתמש (13.8.2026)
1. **צינור העיבוד → צי ה-worker הביתי.** לא Render.
2. **Stripe → לוותר על החיוב, לשמר את הנתונים** כארכיון קפוא. MCP הופך לחינמי invite-only דרך `api_users` של OVER.
3. **היקף → הכל**: אדמין מלא, גרף, API, עמודי פרויקט, MCP, **וגם תשאול הנתונים כטבלאות ב-`/data`**.

---

## 1. הממצא שמשנה את התוכנית: יש *שני* צינורות עיבוד

זו הנקודה הכי חשובה בכל המסמך. ב-repo יש **שתי מימושים מקבילים** של אותו צינור:

| | חבילות ה-CLI (`ocoi-converter`, `ocoi-extractor`) | מה שבאמת רץ בפרודקשן (`ocoi-api/services/*`) |
|---|---|---|
| PDF | marker-pdf + Surya OCR | `pdftotext`/`tesseract` דרך subprocess (~5MB/~30MB RSS) |
| NER | DictaBERT (torch, ~1.5GB RAM) | **אין** — DeepSeek LLM בלבד |
| נפרס? | **לא** — ה-Dockerfile מוציא אותן במפורש | כן |

כלומר **torch/DictaBERT/marker הם כלי פיתוח בלבד ולא נפרסים היום**. אין צורך להעביר אותם.

החסם האמיתי ל-OVER צר ומדויק הרבה יותר: **ארבעה בינאריים מערכתיים** —
`pdftotext`, `pdfinfo`, `pdftoppm` (poppler-utils) ו-`tesseract` + מודל עברית `heb.traineddata` —
ש-OCOI מתקין ב-`apt-get` בתוך Docker, ו-OVER (Python רגיל על Render) **לא יכול להתקין**.
בלעדיהם `convert_pdf` מחזיר `None`, כל מסמך נתקע ב-`conversion_status='no_text'`, והחילוץ לא מייצר כלום.

### וכבר קיים פתרון מובנה בדיוק לזה
OCOI מגיע עם `POST /api/v1/push/documents` (מאובטח ב-`X-Push-Key`), שנבנה כדי שמכונה **מחוץ לשרת**
תעשה את ה-OCR/חילוץ ותשלח **רק את התוצאות** כ-JSON. `tools/local_app.py` (903 שורות) הוא בדיוק זה —
צינור מלא שרץ על מחשב מקומי עם PyMuPDF + Tesseract ב-200dpi בלי מגבלת 4 עמודים.

**זה זהה מבנית ל-`import_diary_bytes` שבניתי היום ל-Ocal.** כלומר הבחירה שלך ב-worker נוחתת על
חוזה קיים ונתמך, לא על משהו שאמציא.

---

## 2. ארכיטקטורה — היכן יושב כל דבר

### 2.1 הנתונים: schema `ocoi` ב-APPEND DB (כמו Ocal)
20 טבלאות מתחלקות לשלוש קבוצות, ורק הראשונה עוברת ל-DB החי:

**(א) 13 טבלאות DATA → append DB, schema `ocoi`** (כדי ש-`/data` יוכל לעשות JOIN):
`sources`, `documents`, `persons`, `companies`, `associations`, `domains`,
`entity_relationships`, `registry_records`, `extraction_runs`, `suggestions`,
`entity_match_proposals`, `ignored_resources`, `site_content`

**(ב) 6 טבלאות AUTH/BILLING → לא עוברות ל-DB החי**, נשמרות כ-dump קפוא:
`users`, `oauth_clients`, `oauth_authorization_codes`, `oauth_refresh_tokens`,
`billing_accounts`, `usage_events`
→ בדיוק עקרון ה-EXCLUSION שהוכח ב-Ocal: הקונסולה הציבורית לעולם לא רואה טוקנים.
→ `usage_events` נשמר בשלמותו (כולל שורות `stripe_pushed_at IS NULL` — התור שטרם חויב).

**(ג) 1 bookkeeping:** `registry_sync_status` → עובר (לא רגיש).

**מיפוי המשתמשים** (במקום להעביר את `users`):
- `role='admin'` / `'content_manager'` → משתמשי האדמין של OVER + מודל ההרשאות שלו
- `role='mcp_user'` → `api_users` של OVER (allow-list ל-MCP)
- `documents.verified_by` ו-`entity_match_proposals.reviewed_by` מצביעים ל-`users.id` שלא יעבור →
  **להמיר ל-`verified_by_email` TEXT בזמן ההגירה** כדי שה-UI ימשיך להציג "אומת ע"י X" בלי לייבא טבלת auth.

### 2.2 ה-PDF: מ-BYTEA ל-R2 (שיפור, לא רק פורט)
`documents.pdf_content` הוא **BYTEA בתוך Postgres** והוא עיקר נפח ה-DB. OCOI נלחם בזה כל הזמן:
יש שומר `DB_STORAGE_LIMIT_MB=4500`, ה-endpoint `backfill-pdf` **מושבת** עם הערה שהבלובים
"פרצו את מגבלת 1GB של Render", ונתיב ה-CKAN שומר metadata בלבד בכוונה.

ל-OVER כבר יש **Cloudflare R2** (`app/services/storage_client.py`). לכן:
- להעביר את הבלובים ל-R2, ולהשאיר ב-DB רק מפתח אובייקט
- זה **הכרחי** כאן ולא רק נחמד: ה-append DB משותף לקונסולת ה-SQL הציבורית — אסור שיישבו בו 2GB של PDF-ים
- מבטל את כל לחץ האחסון שהפרויקט נאבק בו

### 2.3 צינור העיבוד: על צי ה-worker
```
GOVSCRAPER worker (IP ביתי, RAM אמיתי, PyMuPDF+Tesseract)
   │  1. import   — CKAN odata.org.il  ← עוקף את חסימת Cloudflare
   │  2. convert  — PDF→Markdown + OCR עברית
   │  3. extract  — DeepSeek → ישויות + קשרים
   ↓  4. push
OVER  POST /api/worker/ocoi-push   (מאחורי WORKER_API_KEY)
   → כותב ל-schema ocoi, מעלה את ה-PDF ל-R2
```
**חשוב:** ל-OCOI **אין שום תזמון היום** — אין cron, אין APScheduler, הכל מופעל ידנית מהאדמין.
ב-OVER נוסיף job מתוזמן (כמו `ocal_import`), כך שהצינור ירוץ גם כשהמחשב שלך כבוי.

---

## 3. השלבים

### Phase 0 — הגירת נתונים + תשתית גישה
1. **לצלם את הסכימה החיה לפני הכל.** `models.py` **אינו** מקור אמת: כל ה-ALTER-ים ב-`engine.py`
   עטופים ב-try/except שבולע שגיאות, ולכן ייתכן שהפרודקשן שונה. להריץ
   `information_schema.columns` + `pg_indexes` + `table_constraints` ולעשות diff מול `create_all()` על DB ריק.
2. `pg_dump` → schema `ocoi` ב-append DB. **סדר טעינה טופולוגי** (אין מעגלי FK — DAG נקי):
   `sources, registry_records, site_content, ignored_resources, registry_sync_status, persons, domains`
   → `companies, associations` → `documents` → `entity_relationships, extraction_runs, suggestions`
   → `entity_match_proposals`
3. להוציא את 6 טבלאות ה-auth (`--exclude-table`), לשמור dump נפרד כארכיון.
4. `documents.pdf_content` → R2; להוסיף עמודת מפתח אובייקט; לאפס את הבלוב.
5. תפקיד `ocoi_app` + `ALTER ROLE ocoi_app SET search_path` (הלקח מ-Ocal: default של role שורד
   `RESET ALL` של ה-pooler; `SET` פר-חיבור לא).
5b. **אינדקס trigram — התברר כלא נחוץ. נמדד אחרי ההגירה:**
   `/registry/lookup?q=בע` לקח **39 שניות** מול Render, ו-**900 מילישניות** מול Neon —
   אותה שאילתה בדיוק, בלי אינדקס חדש. כלומר הזמן היה של ה-free tier והרשת, לא של חוסר אינדקס.
   הספירה החסומה ב-API נשארת (הגנה מפני צמיחה), האינדקס יורד לרשימת "נחמד שיהיה".
   ⚠ אם בכל זאת יוסיפו אותו: `pg_trgm` **מותקן ב-schema `ocal`** (לא ב-`extensions`, שם יושב
   רק PostGIS), ול-`ocoi_app` אין USAGE על `ocal` — מכוון. הפתרון הנכון הוא
   `ALTER EXTENSION pg_trgm SET SCHEMA extensions` (נבדק שבטוח: אין הפניה מפורשת ל-`ocal.similarity`
   בקוד, ו-6 האינדקסים הקיימים של ocal נקשרים ב-OID ולכן לא נשברים).
6. `over_readonly` ← `GRANT USAGE, SELECT ON SCHEMA ocoi` בלבד.
7. `app/services/ocoi_db.py` — pool asyncpg עצל (`statement_cache_size=0` ל-pooler של Neon).
8. **סריקת יתומים אחרי הטעינה** — `entity_relationships` הוא polymorphic **בלי FK**, וידוע שיש
   יתומים בפרודקשן (יש endpoint `/audit/orphans-and-garbage` בדיוק בשביל זה).

> ### ✅ Phase 0 בוצע (14.8.2026) — מה קרה בפועל
> **DB:** schema `ocoi` ב-append DB (Neon `over-datastore-archive`), בבעלות תפקיד חדש `ocoi_app`
> (`search_path = ocoi, extensions, public` כברירת מחדל של ה-role). `over_readonly` קיבל
> USAGE+SELECT על `ocoi` בלבד → הנתונים ניתנים לתשאול ב-`/data`, ו-6 טבלאות ה-auth/billing
> **לא הועברו כלל**. כל 14 הטבלאות הועתקו עם התאמת ספירה מלאה (`ALL TABLES MATCH`),
> כולל 797,900 שורות registry.
> **PDF:** 854 קבצים (417MB) → R2 `over-files` תחת `ocoi/documents/<id>.<ext>`; 0 כשלונות.
> 2,117 המסמכים הנותרים מעולם לא אחסנו bytes (metadata בלבד — כך ב-OCOI במקור).
> `documents.pdf_content` הושמט מהיעד, `pdf_r2_key` נוסף במקומו.
> **מיפוי משתמשים:** `verified_by`/`reviewed_by` הומרו למייל (`*_email`) בזמן ההעתקה.
>
> **שלוש הפתעות:**
> 1. `pg_dump` שבור במכונה (DLL חסר) → המיגרציה נכתבה ב-asyncpg. יצא לטובה — אפשר לשנות את
>    `documents` תוך כדי מעבר.
> 2. העתקה מלאה של טבלת ה-798k **נתקעה** ב-127MB (Render free tier ניתק את הזרם, asyncpg נתלה
>    עד ה-timeout) → הוחלף ב-**keyset pagination** במקטעים של 20k, ניתן להמשך. לא OFFSET —
>    הוא סורק מחדש את הקידומת בכל מקטע והופך העתקה לינארית לריבועית.
> 3. `is_configured()` של `storage_client` דורש גם `S3_PUBLIC_BASE_URL`, אחרת גם נתיב **הקריאה**
>    מסרב. הערך: `https://pub-63c02556dabd4956af9500eb8fe7198c.r2.dev`.

### Phase 1 — API ציבורי → `app/api/ocoi.py`
~25 endpoints ציבוריים, בקבוצות:
- **חיפוש**: `/search`, `/search/suggest`
- **ישויות**: `/persons|companies|associations|domains` (+`/{id}`, +`/{id}/documents`),
  `/entities/top-connected`, `/entities/ministries`, `/lookup`, `/registry/lookup`
- **גרף**: `/graph/neighbors/{id}`, `/graph/path`, `/graph/showcase`, `/graph/subgraph`
- **מסמכים**: `/documents`, `/{id}`, `/{id}/markdown`, `/{id}/entities`, `/{id}/graph`, `/{id}/pdf`
- **חיצוני**: `/external/by-company|by-person|by-ministry|stats`
- **תוכן והצעות**: `/site/content/{key}`, `POST /suggestions` (כתיבה אנונימית!)

לשמר: מעטפת `{status, data, meta}`; פינוי ישויות `hidden` **וכל קשת שנוגעת בהן**;
כותרת RFC-5987 `filename*=UTF-8''…` (שמות קבצים בעברית).

**חובה להוסיף (חסר ב-OCOI):**
- `@limiter.limit(...)` על כל endpoint ציבורי — חוק OVER הוא דקורטורים מפורשים
- `User-Agent` על כל fetch יוצא (ב-OCOI אין בכלל — יוצא כ-`python-httpx`)
- **לא** לפורט את `/api/db-health` — הוא מחזיר 3000 תווים של traceback ב-500

### Phase 2 — פרונט ציבורי: `OcoiPage.tsx`
מחליף את ה-placeholder הקיים (`ProjectImportPage project="ocoi"` ב-`App.tsx:147`).
הסלוט **כבר בנוי**: Navbar, Footer ומפתחות i18n he/en כבר מצביעים ל-`/projects/ocoi`.

לשוניות (בתבנית `OcalPage`/`KnessetDbPage`, מצב ב-`?tab=`): **חיפוש / גרף / מסמכים / ישויות**
- ~2,000 שורות ב-7 עמודים ציבוריים
- **להוסיף ל-OVER**: `cytoscape` + `cytoscape-fcose` (אינם קיימים היום)
- המרה: Next.js App Router → React Router; Tailwind → inline styles של OVER; `output: export` → SPA

### Phase 3 — MCP ייעודי `/ocoi/mcp`
בתבנית `/cbs/mcp`, `/knesset/mcp`, `/ocal/mcp`: `app/mcp/ocoi_server.py` + `ocoi_routes.py`.
10 כלים: `search, entity_get, graph_neighbors, graph_path, document_get, document_entities,
top_connected, by_ministry, registry_lookup, stats`.

**לשימוש חוזר ב-OAuth המשותף של OVER** — הוכח ב-Ocal ש**אין מה לשנות ב-Google Console**:
כל שרתי ה-MCP חולקים authorization server אחד ואת אותו callback.
- להוריד את שכבת Stripe/quota; לשמור רישום שימוש דרך מנגנון ה-usage הקיים של OVER
- לשמר את `data_attribution` + `ocoi_url` + `sources[]` בכל תשובה (כללי הציטוט של OCOI)
- ⚠ `top_connected` מזריק `entity_type` ל-SQL ב-f-string (מאומת מול whitelist, אבל לפורט בזהירות)

**מלכודות mounting שחייבות לעבור אחד לאחד** (מתועדות בקוד):
1. lifespan של תת-אפליקציה לא נקרא ע"י Starlette → להריץ startup/shutdown מה-lifespan של האב,
   אחרת כל בקשה נופלת ב-`RuntimeError: Task group is not initialized`
2. נרמול נתיב `/mcp` → `/mcp/` ב-ASGI scope (בלי redirect — POST לא עוקב)
3. metadata חייב לשבת ב-`/.well-known/oauth-authorization-server/mcp` — Claude.ai מאמת בדיוק

### Phase 4 — צינור העיבוד על ה-worker  ⭐ הנתיב הקריטי
**צד OVER:**
- `POST /api/worker/ocoi-push` — פורט של `push.py`, מאחורי `_verify_worker_key` (עקביות עם נתיב ocal)
- `GET /api/worker/ocoi-candidates` — מסמכים חדשים ב-CKAN שטרם יובאו (עם throttle כמו ב-ocal)
- job ב-APScheduler + מצב התקדמות **ב-DB ולא ב-dict** (ראה מלכודת §4.6)

**צד GOVSCRAPER** (`ocoi_pipeline.py`, בתבנית `ocal_diary_fetch.py`):
- להביא מועמדים → להוריד PDF-ים מ-odata (IP ביתי!) → convert+OCR → extract (DeepSeek) → push
- הבסיס קיים: `tools/local_app.py` + `tools/local_processor/` עושים בדיוק את זה כבר
- ⚠ להפעיל מ-`worker_supervisor.py main()` — **העדכון-העצמי לא מריץ מחדש את הסופרווייזר**
  (הלקח הכי יקר מהיום)

### Phase 5 — פורט האדמין (בגלים, כמו Ocal)
~80 endpoints, 15 עמודים, ~6,700 שורות. `app/api/ocoi_admin.py` + `OcoiAdminPanel.tsx`.
- **גל 1** — מסמכים: רשימה/פירוט/העלאה/מחיקה/אימות (`PATCH /verify` מדורג לכל הקשרים)/reconvert/reextract
- **גל 2** — ישויות וקשרים: CRUD מלא ל-4 סוגי ישויות, `?keep_alias`, מיזוג (כולל cross-type),
  קשרים + מחיקה מרובה + `replace-entity`
- **גל 3** — התאמות ורישום: סריקת כפילויות, אשכולות ומיזוג, סנכרון 5 מאגרי data.gov.il, התאמה
- **גל 4** — ייבוא/חילוץ/הצעות/תוכן/משתמשים: סטטוסים, prompt לעריכה, תור הצעות הציבור, RBAC
- **מיפוי הרשאות**: 8 המפתחות של OCOI (`manage_entities`, `manage_documents`, …) על מודל האדמין של OVER
- ⚠ **לא לפורט**: `/admin/settings` ו-`/admin/db-storage` — מופיעים במפת ההרשאות אבל **אין להם handler**
- ⚠ `extraction_prompt.json` יושב על דיסק אפמרי (מתאפס בכל deploy) → להעביר לשורת DB
  (יש ל-OVER בדיוק את התבנית: `page_content`)

### Phase 6 — Cutover וכיבוי
1. להריץ במקביל כמה ימים ולוודא זהות נתונים (בדיוק כמו ב-Ocal)
2. DNS `ocoi.org.il` → `over.org.il/projects/ocoi`
3. לגבות את 6 טבלאות ה-auth/billing לפני מחיקה (כמו `mk_expenses`)
4. לכבות ב-Render: שירות `ocoi` + מסד `ocoi-db`
5. תוסף הכרום: לעדכן `host_permissions` ל-over.org.il ולפרסם גרסה חדשה בחנות

---

## 4. מלכודות — כל אחת נמצאה בקוד

| # | מלכודת | למה זה מסוכן |
|---|---|---|
| 4.1 | **`_run_dedup_and_indexes` מוחק שורות** | בריצה ראשונה מול DB שאין בו `ix_documents_file_url_uniq` הוא מריץ DELETE-ים הרסניים על documents/extraction_runs/entity_relationships. **אסור שירוץ מול ה-DB המהוגר.** |
| 4.2 | **חותמות זמן מעורבות** | 3 עמודות `timestamptz`, כל השאר `timestamp` נאיבי שמכיל שעון **Asia/Jerusalem** ולא UTC. הנחה ש"נאיבי=UTC" תזיז הכל ב-2–3 שעות. |
| 4.3 | **הסכימה החיה ≠ `models.py`** | כל ALTER עטוף ב-try/except שבולע. לצלם `information_schema` לפני. |
| 4.4 | **קשתות בלי FK** | `entity_relationships` polymorphic; יתומים קיימים בפרודקשן ולא ייתפסו ב-restore. |
| 4.5 | **UUID כ-CHAR(36)** | לא `uuid` נייטיב. JOIN-ים הם השוואות מחרוזת. המרה = מיגרציית טיפוסים. |
| 4.6 | **הנחת תהליך יחיד** | כל ההתקדמות ב-dict-ים ברמת מודול (`_import_state`…). עם worker חיצוני זה נשבר — להעביר ל-DB. |
| 4.7 | **חסימת Cloudflare** | odata.org.il חוסם IP של Render (הקיר שהפיל את יומן לעם לשבועות). כל הורדה חייבת לעבור ב-worker. |
| 4.8 | **catch-all של SPA** | OCOI רושם `GET /{path:path}` — יתנגש בניתוב של OVER. לא לפורט; OVER כבר מגיש SPA. |
| 4.9 | **אין rate limiting בכלל** | כולל `POST /suggestions` (כתיבה אנונימית) ו-DCR אנונימי. חובה להוסיף. |
| 4.10 | **`push_api_key` לא ב-render.yaml** | ה-endpoint כנראה מחזיר 503 היום. לא להסתמך עליו כמו שהוא. |
| 4.11 | **אין `state` ב-OAuth של Google** | אין הגנת CSRF בזרימת הכניסה. ממילא מוחלף באימות של OVER. |
| 4.12 | **`_get_static_dir` / דיסק** | `pdf_dir`, `markdown_dir`, `extraction_prompt.json` — הכל על דיסק אפמרי. |

---

## 5. הערכת מאמץ

| שלב | היקף | הערכה |
|---|---|---|
| 0 — הגירת DB + R2 | 20 טבלאות, קורפוס קטן | קצר — הנתונים קטנים |
| 1 — API ציבורי | ~25 endpoints, אותו stack | בינוני |
| 2 — פרונט ציבורי | 7 עמודים, ~2,000 שורות + Cytoscape | בינוני-כבד (פורט מ-Next.js) |
| 3 — MCP | 10 כלים, תבנית קיימת | קצר |
| 4 — worker pipeline | OVER + GOVSCRAPER | בינוני-כבד ⭐ קריטי |
| 5 — אדמין | ~80 endpoints, 15 עמודים, ~6,700 שורות | **הכי כבד** (4 גלים) |
| 6 — cutover | תשתית + תוסף | קצר |

הנתיב הקריטי לכיבוי השרת הוא **4+5**, בדיוק כמו ב-Ocal.

---

## 6. מה נדרש ממך לפני התחלה
1. **`OCOI_DATABASE_URL`** של הפרודקשן (Render PG `ocoi-db`) — בלעדיו אין הגירה
2. **`DEEPSEEK_API_KEY`** — לחילוץ הישויות (יישב על מכונת ה-worker, לא על Render)
3. אישור שאפשר להריץ במקביל לפני הכיבוי

## 7. שיפורים אופציונליים (לא חוסמים)
- **חיפוש**: היום זה סריקת `LIKE '%…%'` בלי FTS. אפשר `pg_trgm`.
  ⚠ הלקח מ-Ocal: `to_tsvector('hebrew', …)` החזיר ריק — לא לסמוך על FTS עברי בלי מדידה.
- לנקות תלויות מתות: `pdfplumber` ו-`pymupdf` מוצהרות ב-`ocoi-api` ו**לא מיובאות בשום מקום**.
