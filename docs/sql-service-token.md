# SQL service token — גישת M2M לשאילתות OVER תחת SSO

> מסמך עיצוב. מטרה: כשה-SQL console/API של OVER (over.org.il/data ומקבילותיו) יעברו מאחורי SSO, לאפשר לגישה תוכניתית מהימנה (הסוכן של Claude, ה-gateway ל"חיפוש רוחבי", וכל שירות אחר) להמשיך לשאול — דרך **טוקן שירות scoped לקריאה-בלבד**, במקום SSO אינטראקטיבי.
>
> נכתב בהתאמה למנגנון הקיים ב-`app/mcp/auth.py` (ה-service-token path של `MCP_SERVICE_TOKEN`) ולהערת האבטחה שם שכבר מזהה את הצורך ב-scoping.

---

## 0. הרקע והבעיה

- היום ה-SQL MCP (`app/mcp/sql_routes.py`) כבר עובר את `authenticate()`: או OAuth per-user (allow-list `api_users`) או `MCP_SERVICE_TOKEN`.
- ה-`MCP_SERVICE_TOKEN` הקיים הוא **unscoped** — טוקן יחיד שמאמת כ-principal `service` לכל משאבי ה-MCP (main /mcp, CBS, Knesset, Ocal, וה-SQL). ההערה ב-`auth.py:99-110` מסמנת זאת כ"accepted risk" וממליצה על scoping אמיתי (service JWT עם `aud`/`scope`, או טוקן-per-resource).
- כשמוסיפים SSO לשאילתות, שימוש ב-`MCP_SERVICE_TOKEN` הקיים לגישת ה-SQL שלי = **over-privileged**: דליפה נותנת גישה לכל ה-MCP. לכן צריך **אישור scoped ל-SQL קריאה-בלבד**.

**עיקרון מנחה:** least privilege. הטוקן שאני מחזיק יאפשר בדיוק את מה שאני עושה היום — SELECT קריאה-בלבד מעל הטבלאות השאילתיות — ולא יותר.

---

## חלק 1 — מה OVER צריך לממש (Server-side spec)

### 1.1 המסלול המומלץ עכשiv: `SQL_SERVICE_TOKEN` ייעודי (bare secret, scoped-by-route)

שינוי מינימלי, מיושר לדפוס הקיים:

1. **סוד חדש** `SQL_SERVICE_TOKEN` (env/secrets manager, לא ב-repo). ייצור: `python -c "import secrets; print(secrets.token_urlsafe(48))"`. **נפרד** מ-`MCP_SERVICE_TOKEN` — כדי שדליפה של אחד לא תפגע בשני.

2. **principal חדש** `_sql_service_user()` ב-`app/mcp/auth.py`, במקביל ל-`_service_user()`:
   ```
   tier="sql-service", email="sql-service@over.org.il", name="SQL read-only service"
   ```

3. **בדיקת הטוקן רק במסלול ה-SQL.** שתי אפשרויות מימוש:
   - **א. פרמטר ל-`authenticate()`**: `authenticate(request, db, ..., allow_sql_service=False)`. רק `sql_routes.py` קורא עם `allow_sql_service=True`. בתוך `authenticate`, לפני בדיקת ה-JWT: אם `allow_sql_service` ו-`SQL_SERVICE_TOKEN` מוגדר ו-`hmac.compare_digest(token, SQL_SERVICE_TOKEN)` → החזר `_sql_service_user()`. שאר המשאבים (routes.py/cbs/knesset/ocal) **לא** מעבירים את הדגל → לא מקבלים את הטוקן הזה.
   - **ב. בדיקה ב-`sql_routes.py` עצמו** לפני הקריאה ל-`authenticate()`. פחות מרוכז, אבל מבודד לגמרי את ה-SQL.

   מומלץ (א) — שומר את כל לוגיקת האימות במקום אחד, timing-safe, עם scoping מפורש.

4. **הקשחה (רובה כבר קיימת ב-SQL console — לוודא שחלה גם על ה-principal הזה):**
   - **SELECT / WITH בלבד**, משפט יחיד, ללא `;` — כפי שכבר נאכף.
   - **read-only transaction** + `statement_timeout` + `sql_max_rows`/export caps.
   - `search_path` מוגבל לסכימות השאילתיות (public, knesset, idx, odata, ocal, extensions) — ללא `app`/סכימות ניהול.
   - **אין** DDL/DML/admin/write, אין גישה לטבלאות משתמשים/סודות.
   - ה-`sql-service` principal לא עובר `api_users` ולא tier-gating מעבר לקריאה.

5. **Audit + rate limit:**
   - לוג לכל בקשת `sql-service`: principal, timestamp, ה-SQL עצמו (או hash), IP/מקור — **לעולם לא הטוקן**.
   - rate-limit על ה-principal `sql-service` (למשל N שאילתות/דקה) כדי לתחום דליפה.

6. **SECURITY invariants (כמו ב-`MCP_SERVICE_TOKEN`):**
   - לעולם לא ללוגר את הטוקן / את header ה-Authorization.
   - secrets manager בלבד, לא ב-repo.
   - **rotation**: לשנות את `SQL_SERVICE_TOKEN` ב-OVER **וגם** אצל כל הלקוחות (הקונקטור שלי, ה-gateway) יחד. לתעד ב-docs.
   - להוסיף ל-docs (לצד `mcp-service-token.md`) ערך `sql-service-token.md`.

### 1.2 המסלול ה"נכון" לעתיד: service JWT חתום עם scope

כשיהיה זמן, לשדרג מ-bare secret ל-**JWT חתום** ש-OVER מנפיק ללקוח:
- claims: `sub="sql-service"`, `aud="sql"`, `scope="sql:read"`, `exp` קצר (למשל 24h), `jti`.
- `authenticate()` מאמת חתימה + `aud`/`scope` per-resource → scoping אמיתי נושא-claims, לא "רק ה-route בודק".
- יתרונות: תפוגה (rotation דרך expiry), revocation (denylist על `jti` או סבב מפתח), scoping מפורש. חיסרון: יותर עבודה + הלקוח צריך לרענן טוקן.
- זו ההמלצה של ההערה ב-`auth.py:107-109`. מסלול 1.1 הוא הצעד הפרגמטי; 1.2 הוא ה-hardening.

### 1.3 מה OVER מוסר לי
- ערך `SQL_SERVICE_TOKEN` (הסוד עצמו) — דרך ערוץ מאובטח, לא ב-issue/צ'אט פתוח.
- כתובת ה-endpoint של ה-SQL MCP שאני מתחבר אליו (למשל `https://www.over.org.il/sql/mcp/`) והפורמט (Bearer).
- רשימת הסכימות/הטבלאות שה-principal מורשה עליהן (אם מוגבל תת-קבוצה).

---

## חלק 2 — התוכנית שלי (Client-side)

### 2.1 איך אני ממשיך למשוך אחרי ה-SSO
- **הגישה שלי היא דרך קונקטור ה-MCP של OVER** (השרת שחושף את ה-SQL). כשה-endpoint ידרוש אימות, אני מגדיר בקונקטור את `SQL_SERVICE_TOKEN` כ-**Bearer credential**. מאותו רגע כל קריאת `run_sql`/`query_dataset_rows` נושאת `Authorization: Bearer <token>` → `authenticate()` של OVER מזהה את מסלול ה-`sql-service` → גישה, בלי SSO אינטראקטיבי.
- אם אי-פעם אגש ל-HTTP API של OVER ישירות (WebFetch/curl) במקום דרך ה-MCP — אזריק את ה-header `Authorization: Bearer <token>` מתוך משתנה סביבה/סוד, **לעולם לא במחרוזת ה-URL/query**.

### 2.2 טיפול בטוקן (הכללים שלי)
- **המשתמש/OVER מייצר את הטוקן.** אני לא מייצר סוד ואני לא מחזיק אותו מעבר לתצורת הקונקטור.
- לעולם לא בקוד, לא ב-repo, לא בלוגים, לא ב-URL, לא בהודעות. אם אצטרך להזין אותו — דרך תצורת קונקטור/סוד, לא inline בטקסט.
- אשתמש בו **אך ורק** לקריאת SQL/datasets מ-OVER — לא לשום דבר אחר.
- **rotation:** כשה-OVER יסובב את `SQL_SERVICE_TOKEN`, המשתמש יעדכן את הקונקטור שלי בערך החדש (הישן יפסיק לעבוד — וזו התנהגות רצויה).
- **fallback:** אם מסלול הטוקן לא זמין (פג/שגוי) — אני **עוצר ומדווח למשתמש**, לא מנסה לעקוף בדרך אחרת.

### 2.3 מה אני צריך מהמשתמש כדי להפעיל
1. הערך של `SQL_SERVICE_TOKEN` (אחרי ש-OVER יממש את חלק 1) — להזנה בקונקטור.
2. כתובת ה-SQL MCP endpoint + אישור שהפורמט Bearer.
3. (אופציונלי) רשימת הסכימות המורשות, אם מוגבל.

---

## סיכום החלטות
- **מנגנון:** טוקן שירות **ייעודי ל-SQL** (`SQL_SERVICE_TOKEN`), נפרד מ-`MCP_SERVICE_TOKEN`, scoped לקריאה-בלבד — least privilege.
- **צד OVER:** להוסיף את הסוד + principal `sql-service` + בדיקה במסלול ה-SQL בלבד + audit/rate-limit; לשמר את מסלול ה-SSO להאנשים. שדרוג עתידי: service JWT עם `aud`/`scope`.
- **צד שלי:** להגדיר את הטוקן כ-Bearer בקונקטור ה-SQL MCP; שימוש בלעדי לקריאה; טיפול-סוד קפדני; fallback לדיווח.
- **למה לא להשתמש ב-`MCP_SERVICE_TOKEN` הקיים:** הוא unscoped (מפתח-על לכל ה-MCP); דליפה שלו הרסנית. טוקן SQL ייעודי מגביל את הנזק לקריאה-בלבד.
