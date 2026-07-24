# תוכנית: פרסום המחבר בגלריה הרשמית של Looker Studio

מצב נוכחי (24.7.2026): המחבר **חי ופועל** כ-by-link — v1 מפורסם בעמוד ה-API של
over.org.il, עובד מקצה לקצה, אבל מציג למשתמשים מסך "Google hasn't verified this
app". פרסום בגלריה הרשמית (Partner Connector) יעלים את המסך הזה, יוסיף את המחבר
לחיפוש הפנימי של Looker Studio, ויקנה אמינות.

המקור הרשמי לדרישות: https://developers.google.com/looker-studio/connector/pscc-requirements

## עיקרון מנחה — קוד פתוח ואבטחה

כל הקוד ממילא פתוח: הריפו zomer-g/ckan-version-tracker ציבורי, וה-connector כולו
יושב ב-`looker-connector/`. אין דרישת open-source מצד גוגל, אבל אצלנו זה נתון.
לכן כל תוספת קוד במסגרת התוכנית נכנסת לאותו ריפו, בכפוף לכללים:

- **שום סוד בקוד.** המפתח המשותף חי אך ורק ב-Render env (`CONNECTOR_API_KEY`)
  וב-Script Properties (`OVER_CONNECTOR_KEY`). נכון להיום נבדק — אין ערכים
  שמורים בריפו. כל שינוי עתידי שומר על זה.
- `.clasp.json` (scriptId בלבד) בטוח לפרסום — scriptId אינו סוד; הגישה לפרויקט
  נשלטת בחשבון הגוגל. לעולם לא לקמט `~/.clasprc.json` (טוקני OAuth של clasp —
  יושב בתיקיית הבית, לא בריפו).
- החשיפה היחידה שהמפתח מגן עליה היא דלי-תקציב נפרד; גם דליפה שלו לא מקנה
  יותר ממה שיש לציבור ב-/data (קריאה בלבד, אותם caps). ובכל זאת — רוטציה לפי
  ה-runbook ב-README אם יש חשד.

## שלב א' — עמודים ציבוריים באתר (תנאי סף למניפסט)

גוגל דורשת ארבעה URL-ים מאוחסנים אצלנו (לא Google Sites, לא mailto):

1. **privacyPolicyUrl** — עמוד מדיניות פרטיות. צריך לכסות: מה המחבר אוסף (כלום —
   אין אחסון נתוני משתמש; השאילתות רצות נגד נתונים ציבוריים), עוגיות/זיהוי (אין),
   ומדיניות האתר הכללית. מוצע: `/privacy` (עמוד SPA חדש או תוכן דרך page_content).
2. **termsOfServiceUrl** — תנאי שימוש. מוצע: `/terms`.
3. **addOnUrl** — עמוד ייעודי למחבר: מה הוא עושה, הוראות שימוש, קישורי פרטיות
   ותנאים. הכרטיס ב-`/api#looker` קרוב, אבל הדרישה היא עמוד ייעודי — מוצע עמוד
   `/looker` שמטמיע את מדריך ה-HTML שהוכן (ראו מטלה נפרדת) + הקישורים.
4. **supportUrl** — עמוד תמיכה מאוחסן (לא כתובת מייל ערומה). מוצע: סעיף תמיכה
   בתוך `/looker` עם טופס/הפניה, או עמוד `/support` קצר.

כל הארבעה = קוד frontend פתוח בריפו, ללא סודות.

## שלב ב' — עדכוני מניפסט (`src/appsscript.json`)

- להוסיף: `shortDescription` (בלי URL-ים), `authType: ["NONE"]`, `feeType: ["FREE"]`,
  `privacyPolicyUrl`, `termsOfServiceUrl`, `sources` (ראו שלב ג').
- לעדכן: `addOnUrl` → `/looker`, `supportUrl` → עמוד התמיכה.
- לוודא: `logoUrl` סטטי ≥48×48 בשליטתנו (יש — `connector-logo.png` 128×128),
  `urlFetchWhitelist` מכסה את כל היעדים (יש — over.org.il בלבד), `oauthScopes`
  מפורש (יש — `script.external_request` בלבד).
- ניסוח: בלי המילה "connector" בשם, בלי אימוג'ים וקיצורים, בלי שגיאות כתיב —
  בשם ובתיאורים.

## שלב ג' — רישום מקור הנתונים (PR ציבורי)

גוגל מחזיקה רישום רשמי של מקורות: https://github.com/googledatastudio/ds-data-registry
- להגיש PR שמוסיף ארגון ("OVER — גרסאות לעם", over.org.il) ומקור נתונים.
- ה-`sources` במניפסט חייב להצביע על ערך שקיים שם — אחרת הביקורת נכשלת אוטומטית.
- זהו קוד/קובץ ציבורי בריפו של גוגל — אין רגישויות.

## שלב ד' — אימות OAuth (החלק הארוך)

חובה לכל מחבר, גם עם `AuthType.NONE`. תהליך נפרד מול צוות אחר בגוגל:

1. להעביר את פרויקט ה-Apps Script מ-GCP Default ל-**GCP Standard Project**
   (Project Settings → Change project) — תנאי לניהול מסך ההסכמה.
2. להגדיר **OAuth consent screen** בפרויקט: User type External, שם המותג,
   לוגו, דומיין over.org.il, קישורי פרטיות/תנאים, וה-scope
   `script.external_request`.
3. **אימות דומיין** over.org.il ב-Google Search Console לאותו חשבון (אם טרם).
4. להגיש **Brand/OAuth verification** ולהמתין (ימים עד שבועות; ייתכנו שאלות
   הבהרה במייל).
5. בסיום — בדיקה מחשבון גוגל חדש שלא מופיע יותר מסך "Unverified app".

הערה: כל זה קונפיגורציה בקונסולות של גוגל — אין קוד חדש ואין סודות.

## שלב ה' — הכנת פרויקט ה-Apps Script לביקורת

- לשתף גישת **צפייה** לפרויקט עם `data-studio-contrib-qa@googlegroups.com`
  ו-`data-studio-contrib@google.com`.
- ליצור deployment בשם **Production** עם גרסת הקוד המיועדת לפרסום (בנוסף ל-v1).
- להדליק Project Settings → **Show "appsscript.json" manifest file in editor**.
- לוודא הודעות שגיאה ברורות בכל כשל (יש — `newUserError` בעברית עם detail מהשרת;
  שווה מעבר ניסוח לפני הגשה) ושאין מצב "beta" — המחבר חייב להיות שלם ותפקודי.

## שלב ו' — הגשה וביקורת

- למלא את טופס ההגשה הרשמי שבסוף עמוד הדרישות.
- מחזור ביקורת: תיקונים לפי הערות → הגשה חוזרת. לתעד כל הערה ותיקון בריפו.

## אחרי הפרסום — התחייבויות שוטפות

- תמיכה פעילה: מענה לפניות דרך supportUrl, תיקון תקלות.
- תחזוקת ה-deployment: פרסום גרסאות = עדכון ה-Production deployment (הגלריה
  מפנה אליו) — כבר לא צריך לעדכן ID באתר בכל שחרור.
- לשקול אז: CacheService בסקריפט (TTL 1–6 שעות) אם עומס ה-Neon יעלה עם החשיפה.

## סדר וצפי

| # | משימה | תלות | הערכת זמן עבודה |
|---|---|---|---|
| 1 | עמודי privacy/terms/looker/support באתר | — | חצי יום |
| 2 | עדכון מניפסט + ניסוחים | 1 | שעה |
| 3 | PR ל-ds-data-registry | — | שעה + המתנה למיזוג |
| 4 | GCP standard + consent screen + אימות דומיין | — | חצי יום |
| 5 | הגשת OAuth verification | 1,4 | המתנה: ימים–שבועות |
| 6 | שיתוף גישה + Production deployment | 2 | חצי שעה |
| 7 | טופס הגשה + מחזור ביקורת | 2,3,5,6 | המתנה + תיקונים |

צוואר הבקבוק הוא שלב 5 (אימות OAuth) — כדאי להתחיל אותו מוקדם, במקביל לשאר.
