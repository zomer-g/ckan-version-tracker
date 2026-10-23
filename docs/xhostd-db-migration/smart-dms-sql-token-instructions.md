# הנחיות ל-SMART DMS: טוקן שירות ל-SQL של OVER

> להדביק בסשן "Tag-It to XhostD" (או כל סשן שעובד על smart-dms), אם ההודעה הישירה לא הגיעה.
> נכתב ב-11.9.2026 בסשן `ckan-versions-0f`.

**בקצרה:** OVER עומד לסגור לגישה אנונימית את נקודת ה-SQL של המאגרים הבודדים. ייבוא `over_sql_rows` של SMART DMS צריך להתחיל לשלוח טוקן שירות **לפני** הסגירה.

## למה

- TAG-IT/SMART DMS מייבא את החלטות הממשלה דרך `POST https://www.over.org.il/api/append/{dataset_id}/sql`, בלי שום הזדהות. הקריאה נמצאת במקור `over_sql_rows`, בפונקציה `_import_over_sql_rows` בקובץ `app/import_orchestrator.py`.
- OVER יגדיר בקרוב `APPEND_SQL_REQUIRE_AUTH=true`. מאותו רגע קריאה אנונימית תקבל 401, והייבוא ייעצר.
- OVER כבר מקבל את הטוקן: commit `4f7d4b6`, חי ב-Render וב-xhostd. זה אומת בקריאה שהחזירה 200, ובלוג הביקורת של OVER הופיע `caller=service:sql-service`.
- **שום דבר עוד לא נסגר.** OVER יסגור רק אחרי אישור שלכם.

## מה לשנות ב-SMART DMS

1. **לשלוח header.** בקריאה של `over_sql_rows`, ובכל קריאה אחרת ל-`/api/append/*/sql` או ל-`*/datastore_search_sql`, לשלוח:
   ```
   Authorization: Bearer <הערך של משתנה הסביבה OVER_SQL_SERVICE_TOKEN>
   ```
   - הטוקן נשלח רק ב-header. אסור לשים אותו ב-URL או ב-query string.
   - אסור לכתוב ללוג את ה-header או את הערך.
   - אם המשתנה ריק, להמשיך לשלוח בלי הזדהות כמו היום. כך סדר הפריסה לא משנה.
   - על 401, להיכשל בהודעה ברורה שמזכירה את `OVER_SQL_SERVICE_TOKEN`.
2. **להגדיר את הסוד.** להוסיף את `OVER_SQL_SERVICE_TOKEN` כסוד לכל פריסה של SMART DMS שמריצה ייבוא: שירות Render `smart-dms` (`srv-d6iqbdggjchc73dhqjeg`), או האפליקציה `smart-dms` ב-xhostd, זו שחיה.

## מאיפה הערך

אל תבקשו מאף אחד להדביק אותו.

1. **המקור:** שירות Render של OVER (`srv-d7caof0sfn5c73cc9t70`), משתנה `SQL_SERVICE_TOKEN`.
2. **קריאה:** `GET /v1/services/srv-d7caof0sfn5c73cc9t70/env-vars` ב-API של Render. המפתח נמצא ב-`C:\Users\zomer\.claude\hooks\hook-render-deploy.js`. לחלץ אותו למשתנה, ולא להדפיס.
3. **כתיבה ישירה ליעד, כסוד:**
   - **Render:** `PUT /v1/services/<id>/env-vars/OVER_SQL_SERVICE_TOKEN`.
   - **xhostd:** `POST https://api.xhostd.com/apps/<app_id>/env` עם `{"key","value","kind":"secret"}`, בעזרת `get_credentials(scopes=["deploy:*","channel:*"])`.
4. **אימות:** להדפיס רק את האורך ואת `sha256[:8]`. הערך חייב להיות **`d07a7324`**.
5. **אישור:** לבקש מהמשתמש אישור להעתקת הסוד בסשן שלכם. אישור שניתן ב-OVER לא עובר לסשן אחר.

## מה לא לעשות

- לא להשתמש ב-`MCP_SERVICE_TOKEN`. הוא פותח את כל משאבי ה-MCP של OVER. `SQL_SERVICE_TOKEN` פותח רק את שתי נקודות הקריאה האלה, והשאילתות רצות גם דרכו בתפקיד הקריאה בלבד של OVER.
- לא לשנות את כתובת OVER. `https://www.over.org.il` נשארת גם אחרי המעבר ל-xhostd.

## אימות ודיווח

1. לפרוס מחדש.
2. להריץ ייבוא אחד של `over_sql_rows`, או בקשה אחת, ולוודא שחזר 200.
3. לשלוח הודעה לסשן `ckan-versions-0f` שהייבוא עבר עם הטוקן, כולל השעה (UTC).

OVER יבדוק בלוג שלו שמופיע `caller=service:sql-service` באותה שעה, ורק אז יסגור את הגישה האנונימית.
