# אינדקס האתר — `over_datasets` / `over_dataset_files`

שתי טבלאות שגרסאות לעם מייצרת על עצמה, ניתנות לשאילתה מ-[/data](https://www.over.org.il/data)
ככל טבלה אחרת. שורה אחת לכל מאגר: מה יש בו, כמה הוא גדול, באילו פורמטים,
ומה אפשר לעשות איתו כאן.

נבנות מחדש כל שעה (ובכל קריאה ל-`POST /api/admin/site-index/refresh`). הן
**נגזרות במלואן** — שום דבר בהן אינו מקור אמת, ולכן בנייה מחדש היא תמיד בטוחה.

## מה הטבלה הזאת לא אומרת

**איך המידע נאסף.** אין בה סוג מקור, מנוע, וורקר, תצורת איסוף או פרטי אחסון.
כל עמודה בה היא תכונה של **המידע** או של **מה שהאתר יודע לעשות איתו** — דבר
שכל קורא של האתר יכול לראות בעצמו, ושנשאר נכון גם כשדרך האיסוף משתנה.

היוצא מן הכלל היחיד הוא `sql_schema`, שנחוץ כדי לכתוב שאילתה — הוא מיקום, לא שיטה.

## `over_datasets`

### זיהוי וקישורים
| עמודה | מה זה |
|---|---|
| `dataset_id` | **המזהה למשיכה ב-API** — הוא שהולך ב-`/api/v1/datasets/<id>` |
| `title`, `publisher`, `subject_tags` | שם, גוף מפרסם, תגיות נושא |
| `source_url` | עמוד המקור אצל המפרסם |
| `page_url` | עמוד הגרסאות באתר |
| `api_url` | כתובת ה-API המלאה, מוכנה להעתקה |

### מה המאגר מכיל
| עמודה | ערכים |
|---|---|
| `content_type` | `mapping_layer` \| `data_table` \| `document_collection` \| `catalog_only` |
| `size_class` | `small` (<10k) \| `medium` (<100k) \| `large` (<1M) \| `very_large` — ‏`NULL` כשמספר השורות לא ידוע, וזה בכוונה: לא מנחשים |
| `rows_latest` | שורות בגרסה האחרונה |
| `files_latest` | מספר הקבצים בגרסה האחרונה |
| `file_formats` | רשימה מופרדת בפסיקים: `CSV, GeoJSON` |

### מה אפשר לעשות איתו כאן
| עמודה | ערכים |
|---|---|
| `sql_status` | `queryable` (יש טבלה ב-SQL) \| `download_only` (רק קבצים) \| `none` |
| `sql_schema`, `sql_table` | הטבלה שאפשר לשאול עליה, כשיש |
| `sql_rows` | הערכת שורות בטבלה הזו |
| `geometry_status` | `spatial_queries` (יש `geom` — ‏ST_Intersects וחברים) \| `text_only` (רק `geometry_wkt`) \| `download_only` \| `not_applicable` |
| `map_preview` | האם לגרסה יש GeoJSON להצגה במפה |

### היסטוריה ומצב
`versions`, `first_version_at`, `last_version_at`, `last_checked_at`,
`check_interval_hours`, `status`, `source_available` (‏`false` = המקור הוסר
אצל המפרסם), `quality_note` (סיבה לחשוד בתוכן הגרסה האחרונה, בעברית).

## `over_dataset_files`

שורה לכל קובץ בגרסה האחרונה: `dataset_id`, `title`, `version_number`,
`file_name`, `file_format`.

## שאילתות שימושיות

```sql
-- שכבות מיפוי שעדיין אי אפשר לשאול עליהן
SELECT title, size_class, file_formats, page_url
  FROM over_datasets
 WHERE content_type = 'mapping_layer' AND sql_status = 'download_only'
 ORDER BY rows_latest DESC NULLS LAST;
```

```sql
-- שכבות שהגיאומטריה שלהן טקסט בלבד (אין שאילתות מרחביות)
SELECT title, sql_schema || '.' || sql_table AS tbl, sql_rows
  FROM over_datasets
 WHERE geometry_status = 'text_only';
```

```sql
-- אילו פורמטים נפוצים באתר
SELECT file_format, count(*) AS files, count(DISTINCT dataset_id) AS datasets
  FROM over_dataset_files
 GROUP BY 1 ORDER BY files DESC;
```

```sql
-- מאגרים שהמקור שלהם הוסר — הארכיון הוא כל מה שנשאר
SELECT title, publisher, last_version_at, page_url
  FROM over_datasets
 WHERE NOT source_available
 ORDER BY last_version_at DESC;
```
