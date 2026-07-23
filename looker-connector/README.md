# OVER — Looker Studio Community Connector

מחבר את Looker Studio ל-SQL הציבורי של גרסאות לעם דרך ה-API המוגן במפתח
(`/api/connector`, ראו `app/api/connector.py`). המשתמשים לא רואים שום credentials —
המפתח יושב ב-Script Properties של פרויקט ה-Apps Script ורץ בצד של גוגל.

```
src/
  appsscript.json   מניפסט (dataStudio block, scope יחיד: external_request)
  api.gs            שכבת HTTP: apiSql / fetchTables + מיפוי שגיאות
  connector.gs      getConfig / getSchema / getData + מיפוי טיפוסים
```

## התקנה חד-פעמית (clasp)

1. `npm i -g @google/clasp`
2. `clasp login` — OAuth בדפדפן, בחשבון גוגל שיהיה הבעלים של ה-connector.
3. להפעיל את Apps Script API: https://script.google.com/home/usersettings
4. מתוך התיקייה הזו:
   ```
   clasp create --type standalone --title "OVER Looker Connector" --rootDir src
   clasp push -f
   ```
   (`.clasp.json` שנוצר מכיל רק scriptId — בטוח לקומיט.)
5. `clasp open` → Project Settings → Script Properties → להוסיף
   `OVER_CONNECTOR_KEY` = הערך של `CONNECTOR_API_KEY` מ-Render.

## צד השרת

- לייצר מפתח: `python -c "import secrets; print(secrets.token_urlsafe(32))"`
- לקבוע `CONNECTOR_API_KEY` בדשבורד של Render (מוצהר ב-render.yaml, `sync: false`).
- מפתח ריק ⇒ ה-API עונה 503 וה-connector מנוטרל.

## פריסה

- **לולאת פיתוח:** בעורך → Deploy → Test deployments → להעתיק את ה-Head
  deployment ID. קישור בדיקה:
  `https://lookerstudio.google.com/datasources/create?connectorId=<HEAD_ID>`
  (Head עוקב אחרי כל `clasp push` בלי פריסה מחדש.)
- **שחרור:** לפני שיתוף — לשנות `isAdminUser` ל-`false` ב-connector.gs. ואז
  Deploy → New deployment → להעתיק את ה-deployment ID המגורסן ולהדביק אותו
  בקבוע `LOOKER_CONNECTOR_ID` ב-`frontend/src/pages/ApiPage.tsx`.

## רוטציית מפתח

שני מקומות, באותו סדר: (1) ערך חדש ל-`CONNECTOR_API_KEY` ב-Render (deploy),
(2) אותו ערך ל-`OVER_CONNECTOR_KEY` ב-Script Properties. חלון קצר של 401 בין
השניים מקובל — Looker Studio מנסה שוב ברענון הבא.

## מגבלות מובנות

- 10,000 שורות ברירת מחדל, עד 50,000 (`app/api/connector.py`), timeout ‏30 שניות.
- SELECT/WITH בלבד — נאכף בשרת (`validate_readonly_sql`) על ה-role הקריאה-בלבד.
- דלי תקציב משותף לכל תעבורת ה-connector: ‏10GB/יום (env `CONNECTOR_DAILY_BYTE_BUDGET`).
- Apps Script: עד ‏50MB לתשובת fetch ו-6 דקות להרצה — טבלאות רחבות מאוד עם
  50k שורות עלולות להתקרב לזה; מקטינים row limit או מסכמים ב-SQL.

## בדיקת עשן מתוך העורך

```js
function smoke() { Logger.log(apiSql('SELECT 1 AS x', 1)); }
```
זו הבדיקה האמיתית של Cloudflare מול IP של גוגל (UrlFetchApp יוצא מהתשתית של
גוגל, לא מהמחשב שלך). אם חוזר HTML/403 — להוסיף ב-Cloudflare כלל WAF skip
ל-`/api/connector/*`, מותנה בנוכחות ה-header ‏`X-Connector-Key` (לא בערכו).
