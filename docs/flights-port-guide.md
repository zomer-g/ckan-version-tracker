# מאגר הטיסות — מדריך הקמת פרויקט מקביל

איך לקחת את איסוף הטיסות של גרסאות לעם ולהקים ממנו מערכת עצמאית.
המדריך המפורט על המימוש הקיים ב־OVER נמצא ב־[flights-source.md](flights-source.md);
כאן רק מה שצריך כדי להרים את זה מחדש.

---

## הדבר היחיד שחייבים להבין לפני הקוד

**הורדת הקובץ מ־data.gov.il חסומה.** הכתובת
`e.data.gov.il/…/download/datafile.csv` עושה 302 ל־Google-OAuth ומחזירה HTML,
לא CSV. לכן כל ניסיון לבנות את זה כ"הורד קובץ ופרסר" ייכשל, וכל האיסוף חייב
לעבור דרך ה־API של ה־datastore:

```
GET https://data.gov.il/api/3/action/datastore_search
    ?resource_id=e83f763b-b7d7-479e-b172-ae981ddc6de5&limit=10000&offset=0
```

ה־endpoint הזה פתוח, לא דורש מפתח, ומחזיר JSON. זה כל המקור.

**והדבר השני:** המקור הוא לוח טיסות חי, לא ארכיון. כל טיסה מופיעה, משנה מצב
לאורך חייה, ואז נעלמת. ההיסטוריה לא קיימת בשום מקום אצל רשות שדות התעופה —
היא נוצרת רק על ידי מי שדוגם ושומר.

---

## הדרך המהירה: קובץ אחד, מוכן להרצה

[`scripts/standalone_flights_collector.py`](../scripts/standalone_flights_collector.py) —
כל הפייפליין ב־250 שורות, בלי שום תלות ב־OVER. אין tracked_datasets, אין
VersionIndex, אין תוכניות אחסון, אין ממשק ניהול. טבלה אחת ולולאה אחת.

```bash
pip install httpx asyncpg
export DATABASE_URL="postgresql://user:pass@host/db"
python scripts/standalone_flights_collector.py          # מעבר אחד
python scripts/standalone_flights_collector.py --loop   # כל 15 דקות, לנצח
```

זה מה שהוא עושה בכל מעבר:

1. קורא את כל הלוח דרך `datastore_search` בדפים.
2. מחשב SHA-256 על כל שורה שלמה.
3. `INSERT … ON CONFLICT (row_hash) DO NOTHING` ל־Postgres.

התוצאה: כל **מצב שונה** של כל טיסה, כל אחד עם `first_seen` — הרגע שבו נראה
לראשונה. מצב שכבר נשמר נדחה בשקט על ידי ה־unique index, ולכן קריאה חוזרת של
לוח שלא השתנה היא no-op זול.

אומת מול המקור החי (2026-08-11): 17 עמודות, 3,231 שורות בלוח, דף אחד למעבר.

---

## חמשת העקרונות שהופכים את זה לעובד

אלה ההחלטות שנלמדו בדם. מי שמעתיק את הקוד ומשנה אותן — ישבור אותו.

### 1. הזהות היא hash של השורה השלמה, לא מפתח

אין במקור עמודה מזהה יחידה. הזהות האמיתית של טיסה היא
`CHOPER|CHFLTN|CHSTOL|CHAORD`, אבל dedup לפי הצירוף הזה ישמור רק את המצב
הראשון של כל טיסה ויזרוק את כל העדכונים. לכן: hash על כל 17 הערכים.

```python
canonical = json.dumps({c: ("" if row.get(c) is None else str(row.get(c)))
                        for c in cols}, sort_keys=True, ensure_ascii=False)
hashlib.sha256(canonical.encode("utf-8")).hexdigest()
```

`sort_keys` כדי שסדר המפתחות ב־JSON שהשרת החזיר לא ישנה את ה־hash.
`None → ""` כדי ש־null ומחרוזת ריקה לא ייחשבו שני מצבים שונים.

### 2. חייבים להסיר את `_id`

CKAN מוסיף לכל שורה מונה סינתטי. הוא משתנה כשהטבלה נבנית מחדש במקור — ואם הוא
נכנס ל־hash, **כל השורות ייראו חדשות** בבת אחת והארכיון יתפוצץ.

### 3. ה־dedup הוא UNIQUE index, לא רשימה בזיכרון

```sql
CREATE UNIQUE INDEX ... ON flights (row_hash);
INSERT ... ON CONFLICT ("row_hash") DO NOTHING
```

זה נראה כמו פרט מימוש, וזה לא. הגרסה הראשונה של OVER סחבה קבוצת
"שורות שכבר ראינו" ב־JSONB וכתבה אותה מחדש בכל דגימה — ב־96 דגימות ביום זה
write amplification ריבועי. המעבר להשארת ה־dedup למסד ביטל את הבעיה מהשורש,
וגם ייתר את כל מנגנון החלון (`seen_window_versions`) שנבנה כדי לעקוף אותה.

### 4. `first_seen` חייב להיות `DEFAULT now()` בעמודה, לא ערך מחושב

```sql
"first_seen" timestamptz NOT NULL DEFAULT now()
```

ובתוך ה־INSERT הערך נכתב כ־`now()` inline. כך רק שורות שבאמת נכנסו מקבלות
חותמת, ושורה שכבר קיימת שומרת את ה־`first_seen` המקורי שלה. זו בדיוק הסמנטיקה
של "מתי ראינו את זה לראשונה" — ואי אפשר לקבל אותה אם מחשבים את החותמת בקוד.

### 5. dedup גם בתוך אותו INSERT

Postgres נופל אם statement בודד נושא פעמיים את אותו conflict key, ולוח טיסות
בהחלט יכול להחזיר שורה כפולה. `build_insert` מסנן לפני שהוא בונה את ה־VALUES.

---

## אם רוצים להרים את זה בשפה אחרת

הפייפליין כולו הוא ארבעה שלבים. אין בו שום דבר ספציפי ל־Python:

| שלב | מה עושים |
|---|---|
| 1 | `GET datastore_search` עם `limit`/`offset` עד שדף חוזר ריק |
| 2 | להסיר `_id`, לחשב SHA-256 על JSON קנוני של שאר הערכים |
| 3 | `INSERT … ON CONFLICT DO NOTHING` בקבוצות |
| 4 | לחזור על הכל כל 15 דקות |

**סכימת הטבלה** (כל העמודות `text` — עמידות ארכיונית על פני טיפוסים; מקור
שיתחיל להחזיר `"N/A"` בעמודה מספרית לא ישבור את האיסוף):

```sql
CREATE TABLE flights (
  "CHOPER" text, "CHFLTN" text, "CHOPERD" text, "CHSTOL" text, "CHPTOL" text,
  "CHAORD" text, "CHLOC1" text, "CHLOC1D" text, "CHLOC1TH" text, "CHLOC1T" text,
  "CHLOC1CH" text, "CHLOCCT" text, "CHTERM" text, "CHCINT" text, "CHCKZN" text,
  "CHRMINE" text, "CHRMINH" text,
  "first_seen" timestamptz NOT NULL DEFAULT now(),
  "row_hash" text
);
CREATE UNIQUE INDEX flights_row_hash_uq ON flights ("row_hash");
```

המרכאות הכפולות חובה — שמות העמודות באותיות גדולות, ובלעדיהן Postgres יהפוך
אותן לאותיות קטנות.

---

## פרמטרים ומה הם שווים

| פרמטר | ערך | למה |
|---|---|---|
| `PAGE_SIZE` | 10,000 | הדף מוחזק בזיכרון. הלוח כולו ~3,200 שורות ⇒ דף אחד למעבר |
| `POLL_INTERVAL` | 900 שנ' | תואם לקצב הרענון של המקור. תדיר יותר = בזבוז; נדיר יותר = מצבים אבודים |
| `FLUSH_EVERY` | 5,000 | חוסם את שיא הזיכרון. לא רלוונטי בגודל הנוכחי, קריטי אם מרחיבים למקור גדול |
| `MAX_PARAMS` | 30,000 | תקרת ה־bind params של Postgres היא 32,767 |
| `PAGE_MAX_RETRIES` | 6 | backoff `min(30, 2**n)`. 4xx לא מנוסה מחדש — הוא לא יסתדר מעצמו |

`min_size=0` ב־connection pool: אם מריצים על Postgres serverless (Neon), זה
מאפשר ל־compute לרדת לאפס בין המעברים — שזה רוב היממה בקצב של 15 דקות.

---

## מה מ־OVER **לא** צריך להעתיק

הקוד ב־OVER מסובך יותר כי הוא משרת ~250 מאגרים בעשרות תצורות. לפרויקט
שעוסק בטיסות בלבד, כל אלה מיותרים:

* **`conditional_archiver`** — הארכיון המותנה. הוא ממילא יוצא מיד ב־FALLBACK
  עבור מאגרים מסוג זה.
* **`seen_window_versions` וכל מסלול ה־seen-set** ב־`archive_via_datastore_streaming` —
  fallback לעולם בלי Postgres ייעודי. בפרודקשן הענף הזה לא רץ, והוא מטעה
  את מי שקורא את הקוד.
* **`seed_offset` / checkpoint** — נועד לסריקות של מיליוני שורות שנהרגות
  באמצע (מרשם הרכב, 4.1M). לוח של 3,200 שורות נקרא בבקשה אחת.
* **`append_key`, `capture_changes`, `append_diff`** — מסלולי dedup למקורות
  אחרים. לטיסות נכון keyless בלבד.
* **`VersionIndex`, `TrackedDataset`, תוכניות אחסון, ממשק ניהול** — התשתית
  הרב־מאגרית של OVER.
* **snapshot ל־R2** — OVER שומר בנוסף קובץ CSV מלא לכל גרסה. זה שימושי אם
  רוצים "הלוח כפי שנראה ברגע X" כקובץ להורדה, אבל הוא כפילות מלאה של מה
  שכבר יושב במסד. אפשר לוותר.

**מה כן להעתיק כלשונו:** חמשת העקרונות למעלה. הם לא סגנון — הם התנאים שבלעדיהם
הארכיון מזייף.

---

## אם רוצים בכל זאת ללכת דרך OVER

הליבה נמצאת בשני קבצים בלבד:

| קובץ | מה לקחת |
|---|---|
| `app/services/delta_archiver.py` | `_stream_datastore_pages`, `_fetch_datastore_page` — הקריאה מהמקור |
| `app/services/append_store.py` | `row_hash`, `build_insert`, `ensure_table`, `append_rows`, `chunk_size_for` |

השאר (`poll_job.py`, `scheduler.py`, `storage_client.py`, `append_registrar.py`)
הוא תזמור וניתוב של OVER, ואפשר להחליף אותו בלולאת `sleep` פשוטה.

---

## איך לוודא שזה עובד

```sql
-- קצב צבירה יומי
SELECT date_trunc('day', first_seen) d, count(*)
FROM flights GROUP BY 1 ORDER BY 1 DESC LIMIT 7;

-- טיסה אחת לאורך חייה — כל המצבים שנתפסו, לפי הסדר
SELECT first_seen, "CHRMINE", "CHPTOL", "CHTERM"
FROM flights
WHERE "CHOPER" = 'LY' AND "CHFLTN" = '001'
ORDER BY first_seen;
```

השאילתה השנייה היא המבחן האמיתי: אם היא מחזירה **שורה אחת** לכל טיסה, ה־dedup
תופס לפי מפתח במקום לפי hash של השורה השלמה, וההיסטוריה נזרקת.
