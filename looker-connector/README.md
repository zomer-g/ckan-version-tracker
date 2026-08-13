# OVER — Looker Studio Community Connector

מחבר את Looker Studio ל-SQL הציבורי של גרסאות לעם דרך `/api/connector`
(ראו `app/api/connector.py`).

**אין סוד בצד הלקוח — בכוונה.** הפצה-בקישור מחייבת לשתף את פרויקט ה-Apps
Script כ-"Anyone with the link: Viewer", וצופים רואים גם את ה-Script
Properties — כלומר שום ערך בפרויקט אינו סוד. לכן הסקריפט לא שולח שום מפתח;
השרת מזהה תעבורת connector לפי טווחי ה-IP הרשמיים של גוגל (`goog.json`,
נטען ב-`app/services/google_ips.py` ומתרענן יומית), שמהם Apps Script יוצא
תמיד. ‏`CONNECTOR_API_KEY` קיים רק ב-Render env: מתג הפעלה (ריק ⇒ 503)
ו-override לבדיקות curl — לעולם לא בצד של גוגל.

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
5. שיתוף: כפתור השיתוף בעורך → General access → **Anyone with the link:
   Viewer** (בלעדיו משתמשים אחרים נופלים לרשימת המחברים הכללית במקום
   למסך ההגדרה). אין להוסיף שום Script Property — ראו למעלה.

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

מקום אחד בלבד: `CONNECTOR_API_KEY` ב-Render (ואז deploy). המפתח משמש רק
לבדיקות curl ולמתג הפעלה — המחבר עצמו לא תלוי בערכו, אז רוטציה לא משביתה
אף דשבורד.

## מגבלות מובנות

- 10,000 שורות ברירת מחדל, עד 50,000 (`app/api/connector.py`), timeout ‏30 שניות.
- SELECT/WITH בלבד — נאכף בשרת (`validate_readonly_sql`) על ה-role הקריאה-בלבד.
- דלי תקציב משותף לכל תעבורת ה-connector: ‏10GB/יום (env `CONNECTOR_DAILY_BYTE_BUDGET`).
- Apps Script: עד ‏50MB לתשובת fetch ו-6 דקות להרצה — טבלאות רחבות מאוד עם
  50k שורות עלולות להתקרב לזה; מקטינים row limit או מסכמים ב-SQL.

## בדיקת עשן מתוך העורך

`api.gs` כולל פונקציית `smoke()` — לבחור אותה בתפריט הפונקציות ולהריץ.
‏HTTP 200 = המסלול המלא תקין (כולל סיווג ה-IP של גוגל בשרת); ‏401 = השרת לא
סיווג את הבקשה כתעבורת גוגל (לבדוק את `google_ips.py` והלוגים); ‏HTML/403 =
‏Cloudflare חוסם — להוסיף כלל WAF skip ל-`/api/connector/*`.
