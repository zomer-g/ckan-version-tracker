# קבצי staging ל-PR מול ds-data-registry

שני הקבצים כאן הם התוכן שיוגש כ-PR לרישום המקורות הרשמי של גוגל:
https://github.com/googledatastudio/ds-data-registry

- `organizations/over.json` → יועתק ל-`organizations/over.json` בריפו של גוגל
- `sources/over.json` → יועתק ל-`sources/over.json` בריפו של גוגל

לפני ההגשה:
1. להחליף את `CATEGORY_ID_TBD` בקטגוריה קיימת מתוך `categories.json` שבריפו של
   גוגל (מועמדות סבירות: קטגוריית ממשל/ציבור — לבדוק את ה-id המדויק שם).
2. `npm install && npm run prettier` בתוך ה-fork, כנדרש בהנחיות שלהם.
3. ה-`id` ("OVER") חייב להתאים ל-`sources` שבמניפסט `src/appsscript.json` —
   אם גוגל יבקשו id אחר במהלך הביקורת, לעדכן את שניהם יחד.
