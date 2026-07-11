# מדריך תשאול SQL — מסד הנתונים של הכנסת

**גרסאות לעם · over.org.il/knesset** · עודכן: יולי 2026

מדריך זה מבוסס על המסמך הרשמי של הכנסת — *"שירות ODATA לחשיפת מידע פרלמנטרי"* — ומותאם לממשק התשאול של **גרסאות לעם**: מראָה מלאה של כל טבלאות המידע הפרלמנטרי, מסונכרנת אל מסד PostgreSQL וזמינה לתשאול **SQL חופשי (קריאה בלבד)**, ללא צורך בהיכרות עם תחביר OData.

- ממשק התשאול: [over.org.il/knesset](https://over.org.il/knesset)
- הפיד המקורי של הכנסת: <https://knesset.gov.il/OdataV4/ParliamentInfo>
- תיעוד הכנסת: [main.knesset.gov.il — מאגרי מידע](https://main.knesset.gov.il/activity/info/pages/databases.aspx)

---

## 1. מה יש כאן

כל **48 טבלאות** שירות ה-ODATA של הכנסת (כ-**3,354,302** שורות), מאז הכנסת הראשונה ועד היום:

| תחום | מה כלול |
|---|---|
| הצעות חוק | כל הצעות החוק והחוקים שנחקקו, יוזמים, מיזוגים/פיצולים, מסמכי נוסח |
| חוקי מדינת ישראל | חוקי האב, שרים ממונים, סיווג נושאי, תיקוני חקיקה |
| חקיקת משנה | תקנות וצווים, הרגולטור, החוק המסמיך |
| ועדות | כל הוועדות, הישיבות, הנושאים שנדונו, פרוטוקולים ושידורים |
| מליאה | ישיבות, נושאים, הצבעות — כולל **איך הצביע כל ח"כ בכל הצבעה** |
| חברי הכנסת | אנשים, תפקידים לאורך זמן, סיעות |
| שאילתות והצעות לסדר | כולל קישורים למסמכים |
| שדלנים | שדלנים רשומים ולקוחותיהם |

הנתונים מסונכרנים מהפיד הרשמי: טעינה מלאה, ולאחריה רענון כל ~12 שעות לפי `lastupdateddate`. שינויים במקור מתעדכנים; **מחיקות במקור אינן מזוהות**. לכל שורה נוספת עמודת `_synced_at` — מועד הסנכרון שלה.

---

## 2. שלושה דברים שחשוב לדעת לפני שמתחילים

**א. שמות באותיות קטנות.** שמות הטבלאות והעמודות מהמדריך הרשמי הופכים לאותיות קטנות: `KNS_Bill` ← `kns_bill`, ‏`KnessetNum` ← `knessetnum`. אפשר לכתוב בכל צורת אותיות — PostgreSQL מנרמל אוטומטית.

**ב. עמודה בשם שמור.** בטבלת `kns_status` (ובדומות לה) יש עמודה בשם `desc` — מילה שמורה ב-SQL. עוטפים בגרשיים כפולים: `s."desc"`.

**ג. טיפוסים אמיתיים.** בניגוד לתשאול טקסטואלי, כאן לעמודות יש טיפוסים אמיתיים — מספרים (`integer`/`bigint`), בוליאנים (`boolean`) ותאריכים (`timestamptz`). לכן השוואות תאריכים, חישובים ו-JOIN עובדים ישירות:

```sql
SELECT count(*) FROM kns_plenumvoteresult WHERE votedate > '2025-01-01'
```

---

## 3. מ-OData ל-SQL — טבלת המרה

מי שמכיר את המדריך הרשמי של הכנסת — כך מתרגמים:

| OData (המדריך הרשמי) | SQL (כאן) |
|---|---|
| `$filter=Id eq 2216582` | `WHERE id = 2216582` |
| `eq / ne / gt / lt / ge / le` | `= / <> / > / < / >= / <=` |
| `$filter=contains(Name,'חינוך')` | `WHERE name ILIKE '%חינוך%'` |
| `$orderby=Number desc` | `ORDER BY number DESC` |
| `$top=10` | `LIMIT 10` |
| `$count=true` | `SELECT count(*)` |
| `$expand=KNS_DocumentAgenda` | `JOIN kns_documentagenda ON …` |
| `and / or / not` | `AND / OR / NOT` |

> הערה: הטבלאות `KNS_Law` ו-`KNS_DocumentLaw` קיימות רק בפיד הישן (ODATA-v2) ואינן כלולות — בהתאם לפיד v4 הרשמי.

---

## 4. דוגמאות

הדוגמאות שלהלן כוללות את כל הדוגמאות מהמדריך הרשמי, מתורגמות ל-SQL, ולצדן שאילתות מתקדמות שמנצלות את יכולת ה-JOIN. את כולן אפשר להדביק ישירות בקונסולה ב-[/knesset](https://over.org.il/knesset).

### 4.1 פרטי הצעת חוק לפי מזהה

מהמדריך הרשמי: `KNS_Bill?$filter=ID eq 2216582`

```sql
SELECT * FROM kns_bill WHERE id = 2216582
```

### 4.2 הצעת החוק עם המספר הגבוה ביותר בכנסת 25

מהמדריך הרשמי: מיון לפי `Number` והחזרת תוצאה ראשונה

```sql
SELECT id, number, name
FROM kns_bill
WHERE knessetnum = 25
ORDER BY number DESC
LIMIT 1
```

### 4.3 כמה הצעות חוק בכל כנסת

```sql
SELECT knessetnum, count(*) AS bills
FROM kns_bill
GROUP BY knessetnum
ORDER BY knessetnum DESC
```

### 4.4 הצעות חוק ששמן מכיל 'חינוך' + הסטטוס שלהן

שימו לב לגרשיים סביב `"desc"`

```sql
SELECT b.knessetnum, b.name, s."desc" AS status
FROM kns_bill b
JOIN kns_status s ON s.id = b.statusid
WHERE b.name ILIKE '%חינוך%'
ORDER BY b.id DESC
LIMIT 50
```

### 4.5 מי יזם הצעת חוק מסוימת

‏`$expand` של המדריך הופך כאן ל-JOIN פשוט

```sql
SELECT p.firstname, p.lastname, i.isinitiator
FROM kns_billinitiator i
JOIN kns_person p ON p.id = i.personid
WHERE i.billid = 2216582
ORDER BY i.ordinal
```

### 4.6 ח"כים לפי שם משפחה

מהמדריך הרשמי: `KNS_Person?$filter=contains(LastName,'אדלשטיין')`

```sql
SELECT id, firstname, lastname, genderdesc, email
FROM kns_person
WHERE lastname ILIKE '%אדלשטיין%'
```

### 4.7 פירוט הצבעה במליאה — איך הצביע כל ח"כ

מהמדריך הרשמי: `KNS_PlenumVoteResult?$filter=VoteID eq 42594`

```sql
SELECT firstname, lastname, resultdesc
FROM kns_plenumvoteresult
WHERE voteid = 42594
ORDER BY lastname
```

### 4.8 סיכום תוצאות של הצבעה אחת

```sql
SELECT resultdesc, count(*) AS votes
FROM kns_plenumvoteresult
WHERE voteid = 42594
GROUP BY resultdesc
ORDER BY votes DESC
```

### 4.9 מי הצביע הכי הרבה מאז 2025

```sql
SELECT firstname, lastname, count(*) AS votes
FROM kns_plenumvoteresult
WHERE votedate > '2025-01-01'
GROUP BY firstname, lastname
ORDER BY votes DESC
LIMIT 30
```

### 4.10 גלגולי ועדה לאורך הכנסות

מהמדריך הרשמי: `KNS_Committee?$filter=CategoryID eq 28&$orderby=KnessetNum`

```sql
SELECT id, knessetnum, name
FROM kns_committee
WHERE categoryid = 28
ORDER BY knessetnum
```

### 4.11 ישיבות ועדה + הפרוטוקולים שלהן

מסמכי ישיבות ועדה יושבים ב-kns_documentcommitteesession; `grouptypeid = 23` = פרוטוקול

```sql
SELECT cs.id, cs.startdate, d.filepath
FROM kns_committeesession cs
JOIN kns_documentcommitteesession d
  ON d.committeesessionid = cs.id AND d.grouptypeid = 23
WHERE cs.committeeid = 4202
ORDER BY cs.startdate DESC
LIMIT 50
```

### 4.12 שאילתות לפי משרד ממשלתי

מהמדריך הרשמי: `KNS_GovMinistry?$filter=contains(Name,'משפטים')` — כאן ישר עם JOIN

```sql
SELECT q.id, q.name, q.typedesc, m.name AS ministry, q.submitdate
FROM kns_query q
JOIN kns_govministry m ON m.id = q.govministryid
WHERE m.name ILIKE '%משפטים%'
ORDER BY q.id DESC
LIMIT 50
```

### 4.13 הצעה לסדר + המסמכים שלה

מהמדריך הרשמי: `KNS_Agenda?$filter=Id eq 2202059&$expand=KNS_DocumentAgenda`

```sql
SELECT a.id, a.name, d.filepath, d.applicationdesc
FROM kns_agenda a
LEFT JOIN kns_documentagenda d ON d.agendaid = a.id
WHERE a.id = 2202059
```

### 4.14 תפקידי ח"כ לאורך הקריירה

```sql
SELECT pp.knessetnum, po.description AS position, f.name AS faction,
       pp.startdate, pp.finishdate
FROM kns_persontoposition pp
JOIN kns_position po ON po.id = pp.positionid
LEFT JOIN kns_faction f ON f.id = pp.factionid
WHERE pp.personid = 90
ORDER BY pp.startdate
```

### 4.15 חקיקת משנה + החוק המסמיך

```sql
SELECT sl.id, sl.name, il.name AS authorizing_law
FROM kns_secondarylaw sl
JOIN kns_seclawauthorizinglaw al ON al.secondarylawid = sl.id
JOIN kns_israellaw il ON il.id = al.authorizinglawid
ORDER BY sl.id DESC
LIMIT 30
```

### 4.16 שדלנים ולקוחותיהם

```sql
SELECT l.fullname, l.corporationname, c.name AS client
FROM v_lobbyists l
JOIN v_lobbyistsclients c ON c.lobbyistid = l.id
ORDER BY l.fullname
LIMIT 100
```

### 4.17 אילו שדלנים מייצגים הכי הרבה לקוחות

```sql
SELECT l.fullname, l.corporationname, count(*) AS clients
FROM v_lobbyists l
JOIN v_lobbyistsclients c ON c.lobbyistid = l.id
GROUP BY l.id, l.fullname, l.corporationname
ORDER BY clients DESC
LIMIT 20
```

### 4.18 תאריכי הכנסות — מתי כיהנה כל כנסת

```sql
SELECT knessetnum, min(plenumstart) AS from_date, max(plenumfinish) AS to_date
FROM kns_knessetdates
GROUP BY knessetnum
ORDER BY knessetnum
```

### 4.19 מה עודכן במקור בשבוע האחרון

עמודת `lastupdateddate` קיימת כמעט בכל הטבלאות

```sql
SELECT id, name, lastupdateddate
FROM kns_bill
WHERE lastupdateddate > now() - interval '7 days'
ORDER BY lastupdateddate DESC
LIMIT 100
```

---

## 5. גישה תכנותית (API)

אותו מנוע זמין גם כ-REST — נוח לסקריפטים, מחברות Jupyter וכלי BI:

```bash
# שאילתה (JSON, עד 1,000 שורות)
curl -X POST https://over.org.il/api/knesset-db/sql \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT knessetnum, count(*) FROM kns_bill GROUP BY knessetnum"}'

# ייצוא CSV מלא (עד 200,000 שורות, בזרימה)
curl -G https://over.org.il/api/knesset-db/export.csv \
  --data-urlencode "sql=SELECT * FROM kns_faction WHERE knessetnum = 25" -o factions.csv

# רשימת הטבלאות + סכימות + מצב סנכרון
curl https://over.org.il/api/knesset-db/tables
```

**מגבלות:** ‏SELECT יחיד בלבד (קריאה בלבד, ללא `;`), ‏timeout ‏20 שניות, עד 1,000 שורות בתצוגה / 200,000 בייצוא, ומכסת תעבורה יומית לכל כתובת IP. לצרכים חורגים — צרו קשר דרך עמוד "אודות".

**MCP (לסוכני AI):** שרת MCP ייעודי לפרוטוקולי ועדות זמין ב-`https://www.over.org.il/knesset/mcp` — כלי חיפוש ועדות / ישיבות / פרוטוקולים (מטא-דאטה וקישורים בלבד, ללא תוכן המסמכים) + ‏`run_sql` חופשי מעל כל הסכימה. גישה בהזמנה בלבד — OAuth עם חשבון Google מורשה, כמו יתר שרתי ה-MCP של גרסאות לעם.

---

## 6. קטלוג הטבלאות המלא

לכל טבלה: השם בממשק ה-SQL, השם המקורי בפיד, מספר השורות, ותיאור העמודות — **התיאורים בעברית לקוחים מהמדריך הרשמי של הכנסת**.

## הצעות חוק

### `kns_bill`

*KNS_Bill* · 60,240 שורות  
הצעות חוק — כל הצעות החוק שטופלו בכנסת מאז הכנסת הראשונה ועד היום, וכל החוקים שנחקקו מכל התקופות ובכל סוגי ההליכים.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `knessetnum` | integer | מספר הכנסת |
| `name` | text | שם הצעת החוק |
| `typeid` | integer |  |
| `typedesc` | text |  |
| `subtypeid` | integer | קוד סוג הצעת החוק לכל הצעה יוצג סוג הצעת החוק ותיאור הסוג |
| `subtypedesc` | text | תיאור סוג הצעת החוק (פרטית, ממשלתית, וועדה) |
| `privatenumber` | integer | מספר ה-פ' של הצעת החוק |
| `committeeid` | integer | קוד הוועדה המטפלת בהצעת החוק כל עוד לא נקבעה ועדה מטפלת העמודה תהיה ריקה |
| `statusid` | integer | קוד סטטוס |
| `number` | integer | מספר ה-כ' או ה-מ' של הצעת החוק ההצעה ממשלתית תקבל מספר מ' - מספר החוברת בסדרת הפרסום של הצעות חוק הממשלה הצעה פרטית או של הצעה של ועדה תקבל מספר כ' - מספר החוברת בסדרת הפרסום של הצעות חוק הכנסת העמודה תהיה ריקה טרם פרסום נוסח הצעת החוק לקריאה הראשונה |
| `postponementreasonid` | integer | קוד סיבת העצירה (אם ההצעה נעצרה) |
| `postponementreasondesc` | text | תיאור סיבת העצירה (אם ההצעה נעצרה) |
| `publicationdate` | timestamptz | תאריך פרסום בספר החוקים תאריך זה מעודכן רק לאחר הפרסום בספר החוקים ורק בשלב של יצירת הקשר בין הצעת החוק שהתקבלה בקריאה שלישית לבין חוק מדינת ישראל שאותו היא מתקנת או יוצרת. לכן העמודה עשויה להיות ריקה כל עוד המידע לא התעדכן. |
| `publicationseriesid` | integer | קוד סדרת הפרסום של החוק |
| `publicationseriesdesc` | text | תיאור סדרת הפרסום של החוק (ספר החוקים, דיני מדינת ישראל, עיתון רשמי מועצת המדינה הזמנית, עיתון רשמי מנדטורי, חוקי ארץ ישראל) כך אפשר להבחין בין חוקים שמקורם בהצעות חוק שהתקיים בהן הליך חקיקה בכנסת לבין אלה שנחקקו בתקופה אחרת או בהליך אחר. |
| `publicationseriesfirstcallid` | integer | קוד סדרת הפרסום של הצעת החוק לקריאה הראשונה – המידע יוצג רק לגבי הצעות חוק שפורסמו לקראת קריאה ראשונה |
| `publicationseriesfirstcalldesc` | text |  |
| `magazinenumber` | text | מספר חוברת בספר החוקים – ראה הערה לעיל לגבי PublicationDate |
| `pagenumber` | text | מספר עמוד בספר החוקים – ראה הערה לעיל לגבי PublicationDate |
| `iscontinuationbill` | boolean | האם הוחל על הצעת החוק דין רציפות |
| `summarylaw` | text | תקציר החוק.  יופיע תקציר רק לגבי הצעות חוק שהתקבלו בקריאה שלישית. העמודה תהיה ריקה בהצעות חוק שהתקבלו בקריאה שלישית לפני הכנסת ה-17 שכן רק מאז תקופה זו החלו להכין אותם, או אם התקציר טרם הוכן. |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_billhistoryinitiator`

*KNS_BillHistoryInitiator* · 10,449 שורות  
חברי כנסת ששמם הוסר מהצעת חוק, כולל סיבת ההסרה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `billid` | integer | קוד הצעת החוק |
| `personid` | integer | קוד חבר הכנסת |
| `isinitiator` | boolean | האם חבר הכנסת הוא ברשימת היוזמים (1=יוזם, 0=מצטרף) |
| `startdate` | timestamptz | התאריך בו צורף חבר הכנסת לרשימת המציעים |
| `enddate` | timestamptz | התאריך בו הוסר חבר הכנסת מרשימת המציעים |
| `reasonid` | integer | קוד סיבת ההסרה |
| `reasondesc` | text | תיאור סיבת ההסרה |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_billinitiator`

*KNS_BillInitiator* · 169,884 שורות  
יוזמי הצעות החוק — חברי הכנסת שהגישו את ההצעה, בחלוקה ליוזמים ולמצטרפים.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `billid` | integer | קוד הצעת החוק |
| `personid` | integer | קוד חבר הכנסת |
| `isinitiator` | boolean | האם חבר הכנסת הוא ברשימת היוזמים (1=יוזם, 0=מצטרף) |
| `ordinal` | integer | מקומו של חבר הכנסת ברשימת המציעים של הצעת החוק  (מציעים = יוזמים + מצטרפים) |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_billname`

*KNS_BillName* · 28,035 שורות  
שמות קודמים של הצעות חוק — שם ההצעה עשוי להשתנות בין הקריאות; כאן מרוכזים כל השמות שקדמו לשם הנוכחי המוצג ב-KNS_Bill.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `billid` | integer | קוד הצעת החוק |
| `name` | text | שם הצעת החוק |
| `namehistorytypeid` | integer | קוד סוג שינוי שם הצעת החוק, כלומר האם השינוי נעשה לקראת הקריאה הראשונה או לקראת הקריאה השנייה (שם לקריאה הראשונה, שם לקריאה השנייה וכד') |
| `namehistorytypedesc` | text | תיאור סוג שם הצעת החוק (שם לקריאה הראשונה, שם לקריאה השנייה וכד') |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_billsplit`

*KNS_BillSplit* · 885 שורות  
פיצולי הצעות חוק — הצעות שפוצלו מהצעת חוק אחרת.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `mainbillid` | integer | קוד הצעת החוק ממנה בוצע הפיצול |
| `splitbillid` | integer | קוד הצעת החוק שנוצרה כתוצאה מהפיצול בשלב בו הפיצול עדיין לא אושר, עדיין אין מספר והוא יופיע רק לאחר אישור הפיצול של ההצעה |
| `name` | text | שם הצעת החוק שנוצרה כתוצאה מהפיצול כל עוד לא אושרה בקשת הוועדה לפיצול יוצג רק השם המוצע על ידי הוועדה וטרם יוצג קוד הצעת החוק החדשה. לאחר אישור הפיצול, השם העדכני יופיע בטבלת KNS_Bill |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_billunion`

*KNS_BillUnion* · 1,625 שורות  
מיזוגי הצעות חוק — הצעות שמוזגו עם הצעות חוק אחרות.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `mainbillid` | integer | קוד הצעת החוק המובילה במיזוג |
| `unionbillid` | integer | קוד הצעת החוק הממוזגת |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_documentbill`

*KNS_DocumentBill* · 111,600 שורות  
מסמכי הצעות החוק — נוסח ההצעה בקריאות השונות ולאחר קבלת החוק (קישור לקובץ).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `billid` | integer | קוד הצעת החוק |
| `grouptypeid` | smallint | קוד סוג המסמך |
| `grouptypedesc` | text | תיאור סוג המסמך  (לדוגמה: נוסח לקריאה הראשונה, חוק - נוסח לא רשמי, חוק - פרסום ברשומות) שם המסמך מעיד על תוכנו. למשל המסמך חוק - פרסום ברשומות ישויך לפריט הצעת חוק שהתקבלה בקריאה שלישית רק לאחר הפרסום ברשומות ורק לאחר שנוצר הקשר בין הצעת החוק לחוק מדינת ישראל אותו היא מתקנת. |
| `applicationid` | smallint | קוד פורמט המסמך |
| `applicationdesc` | text | תיאור פורמט המסמך (Word, PDF, TIFF) |
| `filepath` | text | הנתיב אל המסמך |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

## חוקי מדינת ישראל

### `kns_documentisraellaw`

*KNS_DocumentIsraelLaw* · 0 שורות  
מסמכים המקושרים לחוקי מדינת ישראל (נוסח מלא ועוד).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מספר השורה בטבלה זו |
| `israellawid` | integer | קוד החוק |
| `grouptypeid` | smallint |  |
| `grouptypedesc` | text |  |
| `applicationid` | smallint |  |
| `applicationdesc` | text |  |
| `filepath` | text |  |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_israellaw`

*KNS_IsraelLaw* · 2,015 שורות  
חוקי מדינת ישראל (חוקי אב) — מתוך מאגר החקיקה הלאומי.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `knessetnum` | integer | מספר הכנסת |
| `name` | text | שם החוק |
| `isbasiclaw` | boolean | האם זהו חוק יסוד? |
| `isfavoritelaw` | boolean | האם זהו חוק מפתח? רשימת חוקים שמערכת המאגר סברה שהם מהווים מעין ארגז כלים למשפטן ולכן מוצגים במרוכז - למשל חוק הפרשנות, חוק העונשין וכדומה |
| `publicationdate` | timestamptz | תאריך הפרסום לראשונה של חוק מדינת ישראל כלומר הפעם הראשונה שבה פורסם החוק שיצר אותו בעיתון רשמי או ברשומות לפי העניין |
| `latestpublicationdate` | timestamptz | תאריך הפרסום של התיקון האחרון שתיקן את חוק מדינת ישראל |
| `isbudgetlaw` | boolean | האם זהו חוק תקציב? |
| `lawvalidityid` | integer | קוד תוקף |
| `lawvaliditydesc` | text | תיאור תוקף (תקף, בטל, פקע, נושן)  תקף - החוקים התקפים שלא נקבעה מגבלה בהוראות החוק באשר לתוקפם.  חוקים בטלים - חוקים שבוטלו על-ידי חוק אחר.  חוקים שפקעו - חוקים שנקבעה בהם מגבלת זמן, למשל חוקי הוראת שעה.  חוקים שנושנו - חוקים שמערכת מאגר החקיקה סברה שמילאו את תפקידם ואינם פעילים עוד למשל חוק להתפזרות הכנסת ה-18 או חוק להתפזרות הכנסת ה-19. |
| `validitystartdate` | timestamptz | תאריך תחילת תוקף |
| `validitystartdatenotes` | text | הערה לתחילת התוקף |
| `validityfinishdate` | timestamptz | תאריך פקיעה |
| `validityfinishdatenotes` | text | הערות לתאריך הפקיעה |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_israellawbinding`

*KNS_IsraelLawBinding* · 374 שורות  
חוקי מדינת ישראל שהחליפו זה את זה — חוק שהחליף חוק קודם.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `israellawid` | integer | קוד חוק האב |
| `israellawreplacedid` | integer | קוד חוק האב המוחלף |
| `lawid` | integer | קוד חוק הבן שגרם להחלפה |
| `lawtypeid` | integer | קוד סוג חוק הבן שגרם להחלפה |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_israellawclassificiation`

*KNS_IsraelLawClassificiation* · 2,905 שורות  
שיוך חוקי מדינת ישראל לנושאים המשפטיים המוסדרים בהם (סיווג מאגר החקיקה הלאומי).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `israellawid` | integer | קוד חוק האב |
| `classificiationid` | integer | קוד הנושא |
| `classificiationdesc` | text | תיאור הנושא (בחירות, ביטחון, חינוך וכד') |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_israellawlawcorrections`

*KNS_IsraelLawLawCorrections* · 706 שורות  
קשר בין חוק מדינת ישראל לבין תיקוני החקיקה שבוצעו בו.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה רשומה |
| `lawcorrectionid` | integer | מזהה התיקון (מפתח אל KNS_LawCorrections) |
| `israellawid` | integer | מזהה חוק המדינה |
| `lastupdatedby` | integer |  |
| `lastupdateddate` | timestamptz | תאריך עדכון אחרון |

### `kns_israellawministry`

*KNS_IsraelLawMinistry* · 1,726 שורות  
השר הממונה על כל אחד מחוקי מדינת ישראל (ייתכן יותר משר אחד לחוק).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `israellawid` | integer | קוד חוק האב |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |
| `ministrycategoryid` | integer |  |
| `ministrycategorydesc` | text |  |

### `kns_israellawname`

*KNS_IsraelLawName* · 2,171 שורות  
שמות קודמים של חוקי מדינת ישראל ששמם שונה מאז שנחקקו.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `israellawid` | integer | קוד חוק האב |
| `lawid` | integer | קוד החוק שיצר את השינוי בשם |
| `lawtypeid` | integer | קוד סוג החוק שיצר את השינוי בשם |
| `name` | text | שם חוק האב שנוצר מהחוק |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_lawbinding`

*KNS_LawBinding* · 15,202 שורות  
קשר בין החוקים כפי שנחקקו (הצעות חוק שהתקבלו בקריאה שלישית ועוד) לבין חוקי מדינת ישראל שאליהם הם שייכים.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `lawid` | integer | קוד החוק |
| `israellawid` | integer | קוד חוק האב |
| `parentlawid` | integer | קוד חוק האב, במקרה שהאב הוא בעצמו בן (במילים אחרות, קישור של נכד לאבא שלו) |
| `lawtypeid` | integer | קוד סוג החוק |
| `lawparenttypeid` | integer | קוד סוג חוק האב, במקרה שהאב הוא בעצמו בן (במילים אחרות, קישור של נכד לאבא שלו) |
| `bindingtype` | integer | קוד סוג קשר |
| `bindingtypedesc` | text | תיאור סוג הקשר |
| `pagenumber` | text | מספר עמוד |
| `amendmenttype` | integer | קוד סוג תיקון |
| `amendmenttypedesc` | text | תיאור סוג תיקון |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |
| `istemplegislation` | boolean |  |
| `issecondaryamendment` | boolean |  |
| `correctionnumber` | integer |  |
| `paragraphnumber` | text |  |

### `kns_lawcorrections`

*KNS_LawCorrections* · 610 שורות  
תיקוני חקיקה — פרטי התיקונים לחוקי מדינת ישראל.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה התיקון |
| `billid` | integer |  |
| `correctiontypeid` | integer |  |
| `correctiontypedesc` | text |  |
| `isknessetinvolvement` | boolean |  |
| `committeeid` | integer |  |
| `correctionstatusid` | integer |  |
| `correctionstatusdesc` | text |  |
| `votedate` | timestamptz |  |
| `publicationdate` | timestamptz |  |
| `publicationseriesid` | integer |  |
| `publicationseriesdesc` | text |  |
| `magazinenumber` | text |  |
| `pagenumber` | text |  |
| `commencementdate` | timestamptz |  |
| `lastupdatedby` | integer |  |
| `lastupdateddate` | timestamptz | תאריך עדכון אחרון |
| `createddate` | timestamptz |  |

## חקיקת משנה

### `kns_documentsecondarylaw`

*KNS_DocumentSecondaryLaw* · 7,432 שורות  
מסמכים המקושרים לפריטי חקיקת משנה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה שורה |
| `secondarylawid` | integer | מזהה פריט חקיקת משנה |
| `grouptypeid` | smallint | קוד סוג מסמך |
| `grouptypedesc` | text | תיאור סוג מסמך (לדוגמה: חקיקת משנה - פניית הגורם המוסמך) |
| `applicationid` | smallint | קוד סוג הקובץ |
| `applicationdesc` | text | תיאור סוג הקובץ (למשל: PDF) |
| `filepath` | text | נתיב מלא אל הקובץ באתר הכנסת |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_seclawauthorizinglaw`

*KNS_SecLawAuthorizingLaw* · 68,217 שורות  
החוק המסמיך שמכוחו הותקן כל פריט חקיקת משנה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה שורה |
| `authorizinglawid` | integer | מזהה חוק מסמיך |
| `secondarylawid` | integer | מזהה פריט חקיקת משנה |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_seclawregulator`

*KNS_SecLawRegulator* · 5,139 שורות  
הרגולטור (הגורם המתקין) של כל פריט חקיקת משנה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה שורה |
| `secondarylawid` | integer | מזהה פריט חקיקת משנה |
| `regulatortypeid` | integer | קוד סוג הרגולטור |
| `regulatortypedesc` | text | תיאור סוג הרגולטור (לדוגמה: משרדים) |
| `regulatorid` | integer | קוד הרגולטור |
| `regulatordesc` | text | שם הרגולטור (לדוגמה: משרד האוצר) |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_sectosecbinding`

*KNS_SecToSecBinding* · 23,763 שורות  
פריטי חקיקת משנה שתוקנו על-ידי פריטי חקיקת משנה אחרים.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה שורה |
| `secchildid` | integer | מזהה פריט חקיקת משנה בן |
| `secchildtypeid` | integer | סוג פריט חקיקת משנה בן |
| `secparentid` | integer | מזהה פריט חקיקת משנה אב |
| `secparenttypeid` | integer | סוג פריט חקיקת משנה בן |
| `secmainid` | integer | מזהה פריט חקיקת משנה ראשי |
| `secmaintypeid` | integer | סוג פריט חקיקת משנה בן |
| `bindingtypeid` | integer | קוד סוג קשר |
| `bindingtypedesc` | text | תיאור סוג קשר (לדוגמה: מתקן) |
| `istemplegislation` | boolean | האם התיקון זמני |
| `issecondaryamendment` | boolean |  |
| `correctionnumber` | integer | מספר תיקון |
| `amendmenttypeid` | integer | קוד סוג התיקון |
| `amendmenttypedesc` | text | תיאור סוג התיקון (לדוגמה: ישיר) |
| `paragraphnumber` | text | מספר עמוד |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_secondarylaw`

*KNS_SecondaryLaw* · 60,221 שורות  
חקיקת משנה — תקנות, צווים ושאר פריטי חקיקת משנה שהובאו לכנסת.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `knessetnum` | integer | כנסת |
| `name` | text | שם |
| `completioncauseid` | integer | קוד סיבת השלמת טיפול |
| `completioncausedesc` | text | תיאור סיבת השלמת טיפול |
| `postponementreasonid` | integer | קוד סיבת דחייה |
| `postponementreasondesc` | text | תיאור סיבת דחייה |
| `knessetinvolvementid` | integer | קוד מעורבות כנסת: |
| `knessetinvolvementdesc` | text | תיאור מעורבות כנסת: |
| `committeeid` | integer | קוד וועדה |
| `publicationseriesid` | integer | קוד סדרת פרסום |
| `publicationseriesdesc` | text | תיאור סדרת פרסום |
| `magazinenumber` | text | מספר חוברת: |
| `pagenumber` | text | מספר עמוד: |
| `publicationdate` | timestamptz | תאריך פרסום ברשומות |
| `majorauthorizinglawid` | integer |  |
| `committeereceiveddate` | timestamptz | תאריך קבלה בוועדה |
| `committeeapprovaldate` | timestamptz | תאריך אישור בוועדה |
| `approvaldatewithoutdiscussion` | timestamptz | אריך אישור בהעדר דיון |
| `isammendinglaworiginal` | boolean |  |
| `classificationid` | integer | קוד סווג משנה (תקנות, צו, כללים, החלטה, הודעה, אכרזה, תקנון, הוראות, תיקון טעות, אחר) |
| `classificationdesc` | text | תיאור סווג משנה |
| `isemergency` | boolean | שעת חירום |
| `secretaryreceiveddate` | timestamptz | תאריך קבלה במזכירות |
| `plenumapprovaldate` | timestamptz | תאריך אישור במליאה |
| `typeid` | integer | קוד סוג |
| `typedesc` | text | חקיקת משנה, דיווח על פי חוק, פעולה אחרת על פי חוק |
| `statusid` | integer | קוד סטטוס |
| `statusname` | text | תיאור סטטוס |
| `iscurrent` | boolean |  |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

## ועדות הכנסת

### `kns_broadcastcommittesession`

*KNS_BroadcastCommitteSession* · 109,200 שורות  
שידורי ישיבות ועדה — קישורי צפייה בשידורי הוועדות.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מספר השורה בטבלה זו |
| `broadcastid` | integer | קוד השידור |
| `broadcasturl` | text | קישור לשידור |

### `kns_cmtsessionitem`

*KNS_CmtSessionItem* · 80,535 שורות  
הפריטים (נושאים, הצעות חוק, שאילתות) שנדונו בכל ישיבת ועדה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `itemid` | integer | קוד הפריט ששובץ לישיבת הוועדה |
| `committeesessionid` | integer | קוד ישיבת הוועדה |
| `ordinal` | integer | מספר סידורי של הפריט בישיבה |
| `statusid` | integer | קוד סטטוס של הפריט בזמן הישיבה |
| `name` | text | שם הפריט בישיבה |
| `itemtypeid` | integer | קוד סוג הפריט |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_cmtsitecode`

*KNS_CmtSiteCode* · 720 שורות  
טבלת עזר — מיפוי קודי הוועדות לעמודיהן באתר הכנסת.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `knsid` | integer | קוד הוועדה במערכת סנהדרין (המשתקף בטבלאות אלו) |
| `siteid` | integer | קוד הוועדה בבסיס הנתונים של אתר הכנסת  (משמש את חלק מדפי האתר) |

### `kns_committee`

*KNS_Committee* · 2,901 שורות  
ועדות הכנסת — פרטי כל הוועדות מאז הכנסת הראשונה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `name` | text | שם הוועדה |
| `categoryid` | smallint | קוד הקטגוריה של הוועדה |
| `categorydesc` | text | תיאור הקטגוריה של הוועדה  בכל כנסת, כל הוועדות מוקמות מחדש. השדה קטגוריה כולל את רשימת הקטגוריות הנושאיות שאליהן משויכות הוועדות. למשל הקטגוריה של ועדת הפנים והגנת הסביבה היא "פנים" וכך היה גם כאשר שם הוועדה היה ועדת הפנים ואיכות הסביבה. גם ועדות המשנה של כל ועדה משויכות לקטגוריה שלה. |
| `knessetnum` | integer | מספר הכנסת |
| `committeetypeid` | integer | קוד סוג הוועדה |
| `committeetypedesc` | text | תיאור סוג הוועדה (קבועה, מיוחדת, משנה, משותפת, הכנסת) |
| `email` | text | כתובת הדוא"ל של הוועדה |
| `startdate` | timestamptz | תאריך התחלה |
| `finishdate` | timestamptz | תאריך סיום |
| `additionaltypeid` | integer | קוד סוג משנה של הוועדה |
| `additionaltypedesc` | text | תיאור סוג משנה של הוועדה (קבועה, מיוחדת, חקירה) |
| `parentcommitteeid` | integer | קוד ועדת האם (רלוונטי רק לוועדת משנה) |
| `committeeparentname` | text | תיאור ועדת האם |
| `iscurrent` | boolean | האם הוועדה פעילה? זו עמודה מחושבת הנגזרת מתאריך הסיום של הוועדה. אם אין תאריך סיום, IsCurrent=1. עדיף שלא להסתמך על העמודה הזו, אלא לסנן לפי מספר הכנסת. למשל KnessetNum=25. |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_committeesession`

*KNS_CommitteeSession* · 109,188 שורות  
ישיבות ועדות הכנסת — מועד, מיקום ונושא כל ישיבה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `number` | integer | מספר הישיבה |
| `knessetnum` | integer | מספר הכנסת |
| `typeid` | integer | קוד סוג הישיבה |
| `typedesc` | text | תיאור סוג הישיבה (רגילה, סיור) |
| `committeeid` | integer | קוד הוועדה |
| `statusid` | integer |  |
| `statusdesc` | text |  |
| `location` | text | מיקום הישיבה |
| `sessionurl` | text | קישור לישיבה באתר הכנסת בגרסת ODATA-v4 יש להתעלם מהשדה הזה, ובמקומו לבדוק את טבלת KNS_BroadcastCommitteSession |
| `broadcasturl` | text | קישור לשידור הישיבה באתר הכנסת |
| `startdate` | timestamptz | תאריך התחלה |
| `finishdate` | timestamptz | תאריך סיום |
| `note` | text | הערה |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_documentcommitteesession`

*KNS_DocumentCommitteeSession* · 200,719 שורות  
מסמכי ישיבות ועדה — פרוטוקולים, תמלילים והקלטות (קישור לקובץ).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `committeesessionid` | integer | קוד ישיבת הוועדה |
| `grouptypeid` | smallint | קוד סוג המסמך |
| `grouptypedesc` | text | תיאור סוג המסמך |
| `documentname` | text |  |
| `applicationid` | smallint | קוד פורמט המסמך |
| `applicationdesc` | text | תיאור פורמט המסמך (Word, PDF, TIFF) |
| `filepath` | text | הנתיב אל המסמך |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_jointcommittee`

*KNS_JointCommittee* · 1,176 שורות  
ועדות משותפות — הרכב הוועדות המשותפות והוועדות המרכיבות אותן.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `committeeid` | integer | קוד הוועדה המשותפת |
| `participantcommitteeid` | integer | קוד הוועדה המשתתפת |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

## מליאת הכנסת

### `kns_documentplenumsession`

*KNS_DocumentPlenumSession* · 21,913 שורות  
מסמכי ישיבות המליאה — סדר יום ופרוטוקולים (קישור לקובץ).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `plenumsessionid` | integer | קוד ישיבת המליאה |
| `grouptypeid` | smallint | קוד סוג המסמך |
| `grouptypedesc` | text | תיאור סוג המסמך |
| `applicationid` | smallint | קוד פורמט המסמך |
| `applicationdesc` | text | תיאור פורמט המסמך (Word, PDF, TIFF) |
| `filepath` | text | הנתיב אל המסמך |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_plenumsession`

*KNS_PlenumSession* · 8,792 שורות  
ישיבות מליאת הכנסת — מועד וכותרת כל ישיבה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `number` | integer | מספר ישיבת המליאה |
| `knessetnum` | integer | מספר הכנסת |
| `name` | text | שם הישיבה |
| `startdate` | timestamptz | תאריך התחלה |
| `finishdate` | timestamptz | תאריך סיום |
| `isspecialmeeting` | boolean | האם הישיבה הוגדרה כישיבה מיוחדת (למשל ישיבת זיכרון) |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_plenumvote`

*KNS_PlenumVote* · 35,767 שורות  
הצבעות שנערכו במליאת הכנסת — נושא ההצבעה, מועדה ותוצאתה המסכמת.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `votedatetime` | timestamptz | תאריך ההצבעה |
| `sessionid` | integer | מזהה הישיבה |
| `itemid` | integer | מזהה הפריט שעליו הצביעו |
| `ordinal` | integer | מספר סידורי של הצבעה זו בתוך ישיבת המליאה |
| `votemethodid` | integer | קוד סוג ההצבעה |
| `votemethoddesc` | text | תיאור סוג ההצבעה (אלקטרונית / ידנית /שמית) |
| `votestatuscode` | integer | קוד סטטוס ההצבעה |
| `votestatusdesc` | text | תיאור סטטוס ההצבעה (מפורסם / ) |
| `votetitle` | text | כותרת ההצבעה |
| `votesubject` | text | כותרת משנה של ההצבעה |
| `isnoconfidenceingov` | boolean | האם זו הצבעת אי אמון בממשלה? |
| `lastupdateddate` | timestamptz | מועד העדכון האחרון של הרשומה במקור |
| `foroptionid` | integer | קוד האפשרות "בעד" |
| `foroptiondesc` | text | תיאור האפשרות "בעד" |
| `againstoptionid` | integer | קוד אפשרות "נגד" |
| `againstoptiondesc` | text | תיאור אפשרות "נגד" |

### `kns_plenumvoteresult`

*KNS_PlenumVoteResult* · 1,917,454 שורות  
תוצאות הצבעה פרטניות — כיצד הצביע כל חבר כנסת בכל הצבעה במליאה (הטבלה הגדולה בשירות, כ-2 מיליון שורות).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מספר השורה בטבלה |
| `mkid` | integer | מזהה חבר הכנסת |
| `voteid` | integer | מזהה הצבעה |
| `votedate` | timestamptz | תאריך ההצבעה |
| `resultcode` | integer | קוד תוצאת ההצבעה |
| `resultdesc` | text | תיאור תוצאת ההצבעה |
| `lastupdateddate` | timestamptz | מועד העדכון האחרון של הרשומה במקור |
| `lastname` | text |  |
| `firstname` | text |  |
| `sessionid` | integer |  |
| `itemid` | integer |  |

### `kns_plmsessionitem`

*KNS_PlmSessionItem* · 168,787 שורות  
הפריטים שנדונו בכל ישיבת מליאה (הצעות חוק, הצעות לסדר ועוד).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `itemid` | integer | קוד הפריט ששובץ לישיבת המליאה |
| `plenumsessionid` | integer | קוד ישיבת המליאה |
| `itemtypeid` | integer | קוד סוג הפריט (ראו בטבלת סוגי פריטים) |
| `itemtypedesc` | text | תיאור סוג הפריט |
| `ordinal` | bigint | מספר סידורי של הפריט בישיבה |
| `name` | text | שם הפריט בישיבה |
| `statusid` | integer | קוד סטטוס (ראו בטבלת הסטטוסים) |
| `isdiscussion` | integer | האם זהו דיון המשך בפריט זה? |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

## חברי הכנסת

### `kns_faction`

*KNS_Faction* · 545 שורות  
סיעות הכנסת — כל הסיעות מאז הכנסת הראשונה, כולל תקופת פעילותן.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `name` | text | שם הסיעה |
| `knessetnum` | integer | מספר הכנסת |
| `startdate` | timestamptz | תאריך התחלה |
| `finishdate` | timestamptz | תאריך סיום |
| `iscurrent` | boolean | האם הסיעה פעילה? |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_mksitecode`

*KNS_MkSiteCode* · 1,112 שורות  
טבלת עזר — מיפוי קודי חברי הכנסת לעמודיהם באתר הכנסת.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `knsid` | integer | קוד הח"כ בבסיס הנתונים של סנהדרין (משמש את הטבלאות המתוארות במסמך זה) |
| `siteid` | integer | קוד הח"כ בטבלאות אתר הכנסת הישן (משמש לזיהוי הח"כ בחלק מדפי האתר) |

### `kns_person`

*KNS_Person* · 1,185 שורות  
אנשים — חברי הכנסת ובעלי תפקידים בכנסת, מכל התקופות.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `lastname` | text | שם משפחה |
| `firstname` | text | שם פרטי |
| `genderid` | integer | קוד מגדר |
| `genderdesc` | text | תיאור מגדר |
| `email` | text | כתובת דוא"ל |
| `iscurrent` | boolean | האם מכהן כעת? |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_persontoposition`

*KNS_PersonToPosition* · 23,627 שורות  
שיוך אנשים לתפקידים לאורך זמן — חבר כנסת, שר, יו"ר ועדה וכד', כולל תקופת הכהונה, הכנסת והסיעה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `personid` | integer | קוד זיהוי לאדם |
| `positionid` | integer | קוד התפקיד |
| `knessetnum` | integer | מספר הכנסת |
| `startdate` | timestamptz | תאריך התחלה |
| `finishdate` | timestamptz | תאריך סיום |
| `govministryid` | integer | קוד המשרד הממשלתי |
| `govministryname` | text | שם המשרד הממשלתי |
| `dutydesc` | text | תיאור התפקיד (השר לענייני מודיעין, השר לשירותי דת) |
| `factionid` | integer | קוד הסיעה |
| `factionname` | text | שם הסיעה |
| `governmentnum` | integer | מספר הממשלה |
| `committeeid` | integer | קוד הוועדה |
| `committeename` | text | שם הוועדה |
| `iscurrent` | boolean | האם פעיל? |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_position`

*KNS_Position* · 29 שורות  
טבלת עזר — סוגי התפקידים האפשריים.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `description` | text | תיאור התפקיד (חבר כנסת, יו"ר ועדה, שר ועוד) |
| `genderid` | integer | קוד מגדר |
| `genderdesc` | text | תיאור מגדר |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

## שאילתות

### `kns_documentquerie`

*KNS_DocumentQuerie* · —  
מסמכים המקושרים לשאילתות — נוסח השאילתה והתשובה (קישור לקובץ).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `queryid` | integer | קוד השאילתה |
| `grouptypeid` | smallint | קוד סוג המסמך |
| `grouptypedesc` | text | תיאור סוג המסמך |
| `applicationid` | smallint | קוד פורמט המסמך |
| `applicationdesc` | text | תיאור פורמט המסמך (Word, PDF, TIFF) |
| `filepath` | text | הנתיב אל המסמך |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_query`

*KNS_Query* · 42,827 שורות  
שאילתות — פניות רשמיות של חברי כנסת לשרי הממשלה, כולל סטטוס ומועד המענה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `number` | integer | מספר השאילתה |
| `knessetnum` | integer | מספר הכנסת |
| `name` | text | שם השאילתה |
| `typeid` | integer | קוד סוג השאילתה |
| `typedesc` | text | תיאור סוג השאילתה (רגילה, דחופה) |
| `statusid` | integer | קוד סטטוס |
| `personid` | integer | קוד חבר הכנסת מגיש השאילתה |
| `govministryid` | integer | קוד המשרד הממשלתי שאליו הופנתה השאילתה |
| `submitdate` | timestamptz | תאריך הגשת השאילתה |
| `replyministerdate` | timestamptz | תאריך תשובה בפועל |
| `replydateplanned` | timestamptz | תאריך היעד למתן תשובה |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

## הצעות לסדר היום

### `kns_agenda`

*KNS_Agenda* · 22,072 שורות  
הצעות לסדר היום — כל ההצעות לסדר שהוגשו לכנסת.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `number` | integer | מספר ההצעה |
| `classificationid` | integer | קוד סוג ההצעה |
| `classificationdesc` | text | תיאור סוג ההצעה (רגילה, דחופה, דיון מהיר, תקופת פגרה) |
| `leadingagendaid` | integer | קוד הלס"י כוללת (הלס"י כוללת היא הצעה לסדר היום הכוללת בתוכה מספר הצעות לסדר היום שהועברו לטיפול ביחד) |
| `knessetnum` | integer | מספר הכנסת |
| `name` | text | שם ההצעה |
| `subtypeid` | integer | קוד סוג משנה |
| `subtypedesc` | text | תיאור סוג משנה (כוללת, עצמאית) |
| `statusid` | integer | קוד סטטוס |
| `initiatorpersonid` | integer | קוד חבר הכנסת מגיש ההצעה |
| `govrecommendationid` | integer | קוד עמדת הממשלה |
| `govrecommendationdesc` | text | תיאור עמדת הממשלה |
| `presidentdecisiondate` | timestamptz | תאריך החלטת נשיאות הכנסת |
| `postopenmentreasonid` | integer | קוד סיבת הדחייה |
| `postopenmentreasondesc` | text | תיאור סיבת הדחייה |
| `committeeid` | integer | קוד הוועדה המטפלת |
| `recommendcommitteeid` | integer | קוד הוועדה המוצעת לטיפול |
| `ministerpersonid` | integer | קוד חבר הממשלה המשיב |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_documentagenda`

*KNS_DocumentAgenda* · 27,541 שורות  
מסמכים המקושרים להצעות לסדר היום (קישור לקובץ).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `agendaid` | integer | קוד ההצעה לסדר-היום |
| `grouptypeid` | smallint | קוד סוג המסמך |
| `grouptypedesc` | text | תיאור סוג המסמך |
| `applicationid` | smallint | קוד פורמט המסמך |
| `applicationdesc` | text | תיאור פורמט המסמך (Word, PDF, TIFF) |
| `filepath` | text | הנתיב אל המסמך |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

## שדלנים

### `v_lobbyists`

*V_Lobbyists* · 827 שורות  
שדלנים (לוביסטים) בעלי היתר פעילות בכנסת — פרטי השדלן, התאגיד ומסגרת הפעילות.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה השדלן |
| `identitynumber` | text | מספר זהות |
| `fullname` | text | שם השדלן |
| `permittypevalue` | text | סוג ההיתר (קבוע/זמני) |
| `key` | integer | מפתח רשומה |
| `corporationname` | text | שם תאגיד השדלנות |
| `isindependent` | boolean | האם שדלן עצמאי |
| `corpnumber` | text | מספר התאגיד |
| `practiceframework` | text | מסגרת הפעילות |
| `ismemberinfaction` | text | חברות בסיעה |
| `memberinfaction` | boolean | פירוט חברות בסיעה |

### `v_lobbyistsclients`

*V_LobbyistsClients* · 3,010 שורות  
לקוחות השדלנים — מי מיוצג על-ידי כל שדלן.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה רשומה |
| `lobbyistid` | integer | מזהה השדלן (מפתח אל V_Lobbyists) |
| `clientid` | integer | מזהה הלקוח |
| `name` | text | שם הלקוח |
| `clientsnames` | text | שמות הלקוחות המיוצגים |

## טבלאות עזר

### `kns_govministry`

*KNS_GovMinistry* · 922 שורות  
משרדי הממשלה — טבלת עזר לשיוך שאילתות, חוקים וחקיקת משנה.

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `name` | text | שם המשרד הממשלתי |
| `isactive` | boolean | האם המשרד פעיל? |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |
| `categoryid` | integer | קוד קטגוריה |
| `categoryname` | text | שם הקטגוריה (למשל: "משפטים" עבור משרד המשפטים לדורותיו) |
| `govid` | integer | מספר הממשלה |

### `kns_itemtype`

*KNS_ItemType* · 8 שורות  
טבלת עזר — סוגי הפריטים בשירות (הצעת חוק, שאילתה, הצעה לסדר וכו').

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `"desc"` | text | תיאור סוג הפריט (למשל: שאילתה, הצ"ח, הלס"י, ישיבת מליאה, ישיבת ועדה, פעולה ע"פ חוק, חוק בן חיצוני, חוק אב) |
| `tablename` | text | שם הטבלה הרלוונטית: KNS_Query, KNS_Bill, KNS_Agenda וכד' |

### `kns_knessetdates`

*KNS_KnessetDates* · 165 שורות  
מועדי הכנסות — תאריכי הכהונה של כל כנסת ומושביה (כינוסים ופגרות).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `knessetnum` | integer | מספר הכנסת |
| `name` | text | שם הכנסת |
| `assembly` | integer | מספר המושב |
| `plenum` | integer | מספר הכנס (קיץ, חורף) |
| `plenumstart` | timestamptz | תאריך תחילה |
| `plenumfinish` | timestamptz | תאריך סיום |
| `iscurrent` | boolean | האם זו הכנסת הנוכחית? |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

### `kns_status`

*KNS_Status* · 81 שורות  
טבלת עזר — הסטטוסים האפשריים של כל סוגי הפריטים (למשל שלבי הליך החקיקה).

| עמודה | טיפוס | תיאור |
|---|---|---|
| `id` | integer | מזהה הרשומה (מפתח ראשי) |
| `"desc"` | text | תיאור הסטטוס (למשל: אושרה בוועדה לקריאה ראשונה, התקבלה בקריאה שלישית ועוד) |
| `typeid` | integer | קוד סוג הפריט הרלוונטי |
| `typedesc` | text | שם סוג הפריט הרלוונטי |
| `ordertransition` | integer | סדר המעבר בין הסטטוסים. תיאור ראשוני בלבד.  מחזור חיי הפריט מאפשר לפעמים מעבר בין סטטוסים שונים) |
| `isactive` | boolean | האם הסטטוס נמצא עדיין בשימוש קיימים סטטוסים שהשימוש בהם הופסק, אבל קיימים עדיין פריטים ישנים בסטטוס זה |
| `lastupdateddate` | timestamptz | תאריך ושעת עדכון אחרון |

---

## 7. מקורות וקרדיטים

- המידע: שירות ה-ODATA הרשמי של הכנסת (רישיון שימוש חופשי במידע פרלמנטרי). שאלות על המידע עצמו: <dataknesset@knesset.gov.il>
- תיאורי הטבלאות והעמודות: המסמך הרשמי *"שירות ODATA לחשיפת מידע פרלמנטרי"*, בהתאמה לשמות ה-SQL.
- הממשק: **גרסאות לעם** — [over.org.il](https://over.org.il). המראה מתרעננת כל ~12 שעות; מועד הסנכרון האחרון מוצג בעמוד [/knesset](https://over.org.il/knesset).

