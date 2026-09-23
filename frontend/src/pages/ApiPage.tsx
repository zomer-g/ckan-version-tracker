import { useState } from "react";
import { useTranslation } from "react-i18next";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import { PlainSummary } from "../components/a11y";
import Abbr from "../components/a11y/Abbr";
/**
 * /api page — public API documentation + MCP cards.
 *
 * Covers every source OVER serves:
 *   • OVER    — tracked government datasets  (/api/v1, /api/append)
 *   • CBS     — הלמ״ס content index          (/api/cbs)
 *   • Knesset — committee protocols + ODATA  (/api/knesset-db, /api/knesset-protocols)
 *   • Ocal    — public-figure diaries        (/api/ocal)
 *   • שאלות לעם — cross-source deep search   (/api/deep-search)
 *   • נדל"ן לעם — one property, every identity (/api/nadlan)
 *   • עסקאות נדל"ן — the מיסוי מקרקעין register (/api/deals)
 *   • SQL       — every queryable table + GeoJSON (/api/tables)
 * Each source also has a dedicated MCP server (see MCP_SERVERS / McpCard) —
 * including two with no public REST surface of their own (SQL, מידע לעם).
 *
 * Counts in the prose are DERIVED from the arrays below, never written out:
 * the lead said "ארבעה שרתי MCP" while the list already held six.
 *
 * Visual contract: matches the "API ציבורי" pages in Ocoi and Ocal so the
 * לעם sites read as siblings (amber MCP card, primary base-URL card, GET
 * endpoint cards).
 */

// The "Production" deployment of the Looker Studio community connector —
// the same deployment the official-gallery review points at, so there is
// exactly one public deployment to maintain (looker-connector/README.md).
// When empty, the card shows a "coming soon" note instead of the link.
const LOOKER_CONNECTOR_ID =
  "AKfycbz8XAR4jVtxxIweaF2lIlDbNZWI9EjY9lUly4dirJzf78wYXFSpqxHCd-xu2yZ4seWF4Q";

interface ApiParam {
  name: string;
  desc: string;
}

interface ApiEndpoint {
  path: string;
  method?: "GET" | "POST";
  description: string;
  params?: ApiParam[];
  example: string;
}

interface ApiGroup {
  id: string;
  title: string;
  note?: string;
  endpoints: ApiEndpoint[];
}

const ENDPOINT_GROUPS: ApiGroup[] = [
  {
    id: "over",
    title: "OVER — מאגרי מידע ממשלתיים",
    note: "מעקב גרסאות אחרי מאגרי data.gov.il ומקורות ממשלתיים נוספים. כתובת בסיס: /api/v1 (מטא-דאטה) ו-/api/append (תוכן השורות). · מאגרי דגימות: בחלק מהמאגרים כל ישות (תוכנית בנייה, תיק, רשומה) נדגמת שוב ושוב, ולכל דגימה שורה משלה עם חותמת זמן — כך שהטבלה היא ההיסטוריה של כל ישות ולא תמונת מצב. שתי השאלות שאפשר לשאול בה: \"מה המצב העדכני של כל הישויות\" (rows?latest=true) ו-\"מה ההיסטוריה של ישות אחת\" (item?value=…). מאגר כזה מזוהה לפי supports_latest ב-/schema.",
    endpoints: [
      {
        path: "/api/v1/datasets",
        description:
          "רשימת כל המאגרים שבמעקב — כל פריט כולל id (ה-UUID של OVER לשאר הקריאות), וגם ckan_id + ckan_name (מזהה ה-dataset וה-slug ב-data.gov.il). סינון לפי ארגון, תגית, סטטוס או מקור, עם עימוד.",
        params: [
          { name: "organization_id", desc: "UUID של ארגון" },
          { name: "tag / tag_id", desc: "שם תגית או UUID — ניתן לחזור (AND)" },
          { name: "status", desc: "active | pending | all (ברירת מחדל: active)" },
          { name: "ckan_id", desc: "גישור מ-data.gov.il: מחזיר את מאגר ה-OVER שעוקב אחרי dataset נתון. מתאים ל-ckan_id (ה-UUID שב-data.gov.il/dataset/<id>) או ל-ckan_name (slug)." },
          { name: "limit / offset", desc: "עימוד (limit 1-500, ברירת מחדל 100)" },
        ],
        example: "/api/v1/datasets?status=active&limit=10",
      },
      {
        path: "/api/v1/datasets/{id}",
        description: "פרטי מאגר בודד לפי UUID — כולל מקור, תגיות, וקישור ל-ODATA.",
        example: "/api/v1/datasets/00000000-0000-0000-0000-000000000000",
      },
      {
        path: "/api/v1/datasets/{id}/versions",
        description:
          "היסטוריית הגרסאות של מאגר (מהחדשה לישנה) — כל גרסה עם מספרה, תאריך זיהוי, סיכום שינויים ורשימת קבצים, כשלכל קובץ יש download_url ישיר.",
        example: "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions",
      },
      {
        path: "/api/v1/datasets/{id}/versions/latest",
        description:
          "הגרסה העדכנית ביותר של מאגר (מספר הגרסה הגבוה ביותר) — אותו מבנה כמו גרסה ברשימה, כולל download_url לכל קובץ. מחזיר 404 אם אין עדיין גרסאות.",
        example: "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions/latest",
      },
      {
        path: "/api/v1/datasets/{id}/versions/{number}",
        description:
          "גרסה ספציפית לפי מספרה (1-based, כפי שמופיע ברשימת הגרסאות). מחזיר 404 אם הגרסה לא קיימת.",
        example: "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions/1",
      },
      {
        path: "/api/v1/tags",
        description: "כל התגיות במערכת כולל מספר המאגרים תחת כל תגית.",
        example: "/api/v1/tags",
      },
      {
        path: "/api/v1/tags/{id}",
        description: "פרטי תגית בודדת + רשימת כל המאגרים תחתיה.",
        example: "/api/v1/tags/00000000-0000-0000-0000-000000000000",
      },
      {
        path: "/api/v1/organizations",
        description: "כל הארגונים הציבוריים כולל מספר המאגרים שמתחזק כל ארגון.",
        example: "/api/v1/organizations",
      },
      {
        path: "/api/v1/organizations/{id}",
        description: "פרטי ארגון בודד לפי UUID.",
        example: "/api/v1/organizations/00000000-0000-0000-0000-000000000000",
      },
      {
        path: "/api/append/{id}/datastore_search",
        description:
          "תשאול תוכן מאגר (השורות עצמן, לא רק הקבצים) — בהשראת datastore_search של CKAN. זמין למאגרים שתוכנם נשמר ב-NEON (append). מחזיר עטיפת CKAN: {success, result:{fields:[{id,type}], records, total, _links}}.",
        params: [
          { name: "filters", desc: "אובייקט JSON של עמודה→ערך (התאמה מדויקת; ערך יכול להיות רשימה ל-IN)" },
          { name: "q", desc: "חיפוש מחרוזת בכל העמודות" },
          { name: "fields", desc: "רשימת עמודות מופרדת בפסיקים (projection)" },
          { name: "sort", desc: '"עמודה" או "עמודה desc, עמודה2 asc"' },
          { name: "limit / offset", desc: "עימוד (limit עד 500)" },
          { name: "distinct / include_total", desc: "boolean" },
          { name: "latest", desc: "במאגר דגימות: שורה אחת לכל ישות — הדגימה האחרונה שלה (ראו למטה)" },
        ],
        example:
          '/api/append/e437ab0b-c247-4d35-b2c4-79c2d19dbabd/datastore_search?limit=5&filters={"tozeret_nm":"קיה קוריאה"}',
      },
      {
        path: "/api/append/{id}/rows",
        description:
          "עמוד שורות מהארכיון, עם סינון חופשי (q) וסינון לפי עמודה (כל פרמטר ששמו עמודה אמיתית). במאגרי דגימות — מאגרים שבהם כל ישות נדגמת שוב ושוב ולכל דגימה שורה משלה, למשל תיקי הרישוי של עיריית ירושלים — ברירת המחדל היא כל ההיסטוריה; latest=true מצמצם לשורה אחת לכל ישות, הדגימה האחרונה שלה, והסינון חל על המצב העדכני.",
        params: [
          { name: "latest", desc: "true = הדגימה האחרונה בלבד לכל ישות (דורש item_key — ראו /schema)" },
          { name: "q / <עמודה>", desc: "חיפוש חופשי / סינון לפי עמודה" },
          { name: "limit / offset / sort / order", desc: "עימוד ומיון" },
          { name: "table", desc: "במאגר מרובה טבלאות: שם הטבלה או שם המשאב" },
        ],
        example: "/api/append/76f0e069-f269-46fc-bcc8-f704ebbf17ff/rows?latest=true&limit=20",
      },
      {
        path: "/api/append/{id}/item",
        description:
          "כל הדגימות של ישות אחת — ההיסטוריה המלאה של תיק/תוכנית/רשומה בודדת, מהחדשה לישנה. ההתאמה ל-value היא מדויקת (לא הכלה), כי מזהה הוא מזהה. התשובה מציינת את עמודת המזהה, את עמודת מועד הדגימה ואת מספר הדגימות.",
        params: [
          { name: "value", desc: "ערך המזהה, למשל מספר תיק (חובה)" },
          { name: "order", desc: "desc (ברירת מחדל) או asc" },
          { name: "limit / offset / table", desc: "עימוד ובחירת טבלה" },
        ],
        example:
          "/api/append/76f0e069-f269-46fc-bcc8-f704ebbf17ff/item?value=2026/0100.00",
      },
      {
        path: "/api/append/{id}/download.csv",
        description:
          "כל הארכיון (או התוצאה המסוננת) כ-CSV בזרימה. latest=true מוריד שורה אחת לכל ישות במקום את היסטוריית הדגימות.",
        params: [{ name: "latest / q / <עמודה> / table", desc: "כמו ב-/rows" }],
        example: "/api/append/76f0e069-f269-46fc-bcc8-f704ebbf17ff/download.csv?latest=true",
      },
      {
        path: "/api/append/{id}/datastore_search_sql",
        description:
          "שאילתת SQL גולמית (SELECT/WITH בלבד) על תוכן המאגר — בהשראת datastore_search_sql של CKAN. רצה בטרנזקציית READ ONLY עם הגבלת זמן ושורות. שם הטבלה זמין ב-/schema.",
        params: [{ name: "sql", desc: "משפט SELECT/WITH יחיד" }],
        example:
          "/api/append/e437ab0b-c247-4d35-b2c4-79c2d19dbabd/datastore_search_sql?sql=SELECT tozeret_nm, count(*) FROM append_private_and_commercial_vehicles_e437ab0b GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
      },
      {
        path: "/api/append/{id}/schema",
        description:
          "סכמת תוכן המאגר ב-NEON: שם הטבלה, מספר השורות, רשימת העמודות, ועמודת first_seen (זמן הוספת כל שורה). במאגר דגימות מוחזרים גם item_key (העמודה שמזהה ישות), sample_column (מתי השורה נדגמה) ו-supports_latest — כך אפשר לגלות שיש כמה שורות לאותה ישות במקום להניח ששורה = ישות.",
        example: "/api/append/e437ab0b-c247-4d35-b2c4-79c2d19dbabd/schema",
      },
    ],
  },
  {
    id: "sql",
    title: "SQL מרכזי + שכבות מפה — /api/tables",
    note:
      "אותו מנוע שמאחורי הקונסולה ב-/data, פתוח כ-API: קטלוג של כל הטבלאות השאילתיות באתר — מאגרי data.gov.il, מראה ה-ODATA של הכנסת, אינדקסי האוספים (סכימת idx), מידע לעם ויומן לעם — ו-SQL חופשי לקריאה בלבד מעליהן. · שאילתות מרחביות: 815 משכבות ממ״ג מוחזקות עם עמודת geom מסוג PostGIS ב-EPSG:4326, כלומר WGS84 במעלות lon/lat. חיתוך לפי מלבן, מרחק ושטח רצים בצד השרת, ואין שום צורך בהמרת קואורדינטות בצד הלקוח — גאומטריה שפורסמה במקור ב-ITM (EPSG:6991) כבר הומרה בעת המראה. שכבה עם גאומטריה מסומנת ב-field_flags.has_geometry בקטלוג.",
    endpoints: [
      {
        path: "/api/tables",
        description:
          "קטלוג כל הטבלאות: שם הטבלה, הסכימה, כותרת, ארגון, תגיות, רשימת העמודות, הערכת מספר שורות ו-field_flags (has_geometry, has_date, has_parcel, has_locality). זו נקודת ההתחלה — שם הטבלה מכאן הוא הקלט לכל שאר הקריאות בקבוצה.",
        example: "/api/tables",
      },
      {
        path: "/api/tables/{table}/detail",
        description:
          "קוביית פירוט לטבלה אחת: ספירת שורות מדויקת, 20 שורות דוגמה, קישור למקור ולקבצי הגרסה, ופרופיל העמודות אם הורץ. עמודות גאומטריה כבדות מדווחות ב-omitted_columns ולא נשלפות בדוגמה (הן TOASTed — שליפתן הופכת סריקה של שנייה ל-46 שניות).",
        example: "/api/tables/govmap_200541_b19acb42_c2bd90e1/detail",
      },
      {
        path: "/api/tables/{table}/features",
        description:
          "שכבת מפה כ-GeoJSON FeatureCollection, עם סינון אופציונלי לפי מלבן — התשובה הישירה ל״תן לי רק את מה שנמצא בחלון התצוגה״. הקואורדינטות ב-WGS84 (lon,lat) ונכנסות כמו שהן ל-Leaflet או ל-MapLibre. numberReturned ו-exceededTransferLimit אומרים אם הדף נחתך ויש להמשיך ב-offset.",
        params: [
          { name: "bbox", desc: "min_lon,min_lat,max_lon,max_lat במעלות WGS84 — בדיוק הסדר של getBounds().toBBoxString()" },
          { name: "columns", desc: "רשימת עמודות לתכונות (properties), מופרדת בפסיקים. ברירת מחדל: כל העמודות למעט הגאומטריה" },
          { name: "limit / offset", desc: "עימוד (limit 1-5000, ברירת מחדל 500)" },
        ],
        example:
          "/api/tables/govmap_200541_b19acb42_c2bd90e1/features?bbox=35.0,31.8,35.3,32.0&limit=50",
      },
      {
        path: "/api/tables/sql",
        method: "POST",
        description:
          "משפט SELECT/WITH יחיד מעל כל הסכימות יחד (search_path = public, knesset, idx, odata, ocal, extensions), כך שאפשר להצליב בין מקורות בשאילתה אחת. רץ בטרנזקציית READ ONLY תחת תפקיד DB עם הרשאת SELECT בלבד, 10 שניות, עד 1,000 שורות. גוף הבקשה: {sql} או {sql_b64} — הגרסה ה-base64 קיימת כדי שחוקי WAF שחוסמים מילות SQL בכתובת לא יפילו שאילתה תקינה. לסינון מרחבי: geom OPERATOR(extensions.&&) extensions.ST_MakeEnvelope(min_lon, min_lat, max_lon, max_lat, 4326), ולפלט גאומטרי extensions.ST_AsGeoJSON(geom).",
        params: [
          { name: "sql", desc: "משפט SELECT/WITH יחיד, ללא ; בסוף" },
          { name: "sql_b64", desc: "אותו משפט מקודד base64 (חלופה ל-sql)" },
        ],
        example: "/api/tables/sql",
      },
      {
        path: "/api/tables/export.csv",
        description:
          "אותה שאילתה בדיוק, בזרימה כ-CSV מלא — עד 200,000 שורות ו-60 שניות, במקום 1,000 ו-10 שניות של /sql. זו הדרך למשוך שכבה שלמה או תוצאת הצלבה גדולה בקריאה אחת.",
        params: [{ name: "sql / sql_b64", desc: "כמו ב-POST /api/tables/sql" }],
        example:
          "/api/tables/export.csv?sql=SELECT name_name FROM idx.govmap_200541_b19acb42_c2bd90e1 LIMIT 20",
      },
      {
        path: "/api/tables/schema.txt",
        description:
          "ה-DDL של הקטלוג כטקסט רגיל: ?table= לטבלה אחת, ?schema=public|knesset|idx לסכימה שלמה, ובלי פרמטרים — שורת CREATE TABLE אחת לכל טבלה באתר. נועד להדבקה למודל שפה שיכתוב את השאילתה במקומכם.",
        params: [
          { name: "table", desc: "טבלה אחת, ב-DDL מלא" },
          { name: "schema", desc: "public | knesset | idx" },
        ],
        example: "/api/tables/schema.txt?table=govmap_200541_b19acb42_c2bd90e1",
      },
    ],
  },
  {
    id: "cbs",
    title: 'למ״ס (CBS) — אינדקס פרסומי הלשכה המרכזית לסטטיסטיקה',
    note: "אינדקס תוכן (HEAD-only) של פרסומי cbs.gov.il: כותרות, נושאים, סוגי קבצים ושנות נתונים — לא בתי הקבצים עצמם. כתובת בסיס: /api/cbs.",
    endpoints: [
      {
        path: "/api/cbs/search",
        description: "חיפוש טקסט חופשי + פאסטים על אינדקס הלמ״ס.",
        params: [
          { name: "q", desc: "טקסט חופשי (עברית/אנגלית)" },
          { name: "subject / geo / file_type / section / item_type / lang", desc: "סינון פר-פאסט" },
          { name: "year_from / year_to", desc: "טווח שנת נתונים" },
          { name: "sort", desc: "relevance (ברירת מחדל) | chrono" },
          { name: "limit / offset", desc: "עימוד (limit 1-100, ברירת מחדל 30)" },
        ],
        example: "/api/cbs/search?q=אוכלוסייה&sort=chrono&limit=5",
      },
      {
        path: "/api/cbs/facets",
        description: "ספירת הפאסטים הזמינים (נושאים, אזורים גאוגרפיים, סוגי קבצים, מדורים ועוד) לבניית מסנני חיפוש.",
        example: "/api/cbs/facets",
      },
      {
        path: "/api/cbs/stats",
        description: "היקף האינדקס וטריות: מספר הפריטים ומתי סונכרן לאחרונה.",
        example: "/api/cbs/stats",
      },
      {
        path: "/api/cbs/featured",
        description: "פריטים נבחרים (מובלטים ידנית) מתוך האינדקס.",
        example: "/api/cbs/featured",
      },
    ],
  },
  {
    id: "knesset",
    title: "כנסת — מראה ה-ODATA + פרוטוקולי ועדות",
    note: "מראה מלא של 48 טבלאות ה-ODATA של הכנסת (קונסולת SQL), קטלוג מסמכי מרכז המחקר והמידע (ממ״מ), וחיפוש/הורדה בכמות של פרוטוקולי ועדות. כתובות בסיס: /api/knesset-db ו-/api/knesset-protocols.",
    endpoints: [
      {
        path: "/api/knesset-db/tables",
        description: "קטלוג טבלאות מראה הכנסת: שם SQL, תיאור בעברית, מספר שורות ורשימת עמודות — הבסיס לכתיבת שאילתות.",
        example: "/api/knesset-db/tables",
      },
      {
        path: "/api/knesset-db/sql",
        method: "POST",
        description: "שאילתת SQL גולמית (SELECT/WITH בלבד) על מראה הכנסת, READ ONLY עם הגבלת זמן ושורות. גוף הבקשה: JSON עם {\"sql\": \"...\"}.",
        params: [{ name: "sql (body)", desc: "משפט SELECT/WITH יחיד" }],
        example: "/api/knesset-db/tables",
      },
      {
        path: "/api/knesset-db/export.csv",
        description: "הרצת שאילתת SQL והזרמת התוצאה כקובץ CSV להורדה (UTF-8 עם BOM).",
        params: [{ name: "sql", desc: "משפט SELECT/WITH יחיד" }],
        example: "/api/knesset-db/export.csv?sql=SELECT * FROM knesset_committee LIMIT 100",
      },
      {
        path: "/api/knesset-db/mmm/search",
        description: "חיפוש בקטלוג מסמכי מרכז המחקר והמידע (ממ״מ) — מטא-דאטה של מחקרי הכנסת.",
        params: [
          { name: "q", desc: "טקסט חופשי בכותרת/תקציר" },
          { name: "author / doc_type", desc: "סינון לפי מחבר או סוג מסמך" },
          { name: "year_from / year_to", desc: "טווח שנים" },
          { name: "limit / offset", desc: "עימוד (ברירת מחדל 20)" },
        ],
        example: "/api/knesset-db/mmm/search?q=דיור&limit=5",
      },
      {
        path: "/api/knesset-protocols/search",
        description: "חיפוש פרוטוקולי ועדות (מסמך אחד לכל פרוטוקול) לפי טקסט חופשי, מספר כנסת ושם ועדה — כל תוצאה מקשרת לקובץ ב-fs.knesset.gov.il.",
        params: [
          { name: "q", desc: "טקסט חופשי במסמך/ועדה/ישיבה" },
          { name: "knesset", desc: "מספר כנסת (למשל 25)" },
          { name: "committee", desc: "שם ועדה (ILIKE)" },
          { name: "limit / offset", desc: "עימוד (limit 1-200, ברירת מחדל 50)" },
        ],
        example: "/api/knesset-protocols/search?q=תקציב&knesset=25&limit=5",
      },
      {
        path: "/api/knesset-db/protocols/batch.zip",
        description: "הורדה בכמות: אורז את כל קובצי הפרוטוקולים התואמים לפילטר ל-ZIP יחיד (עד תקרת קבצים; ראו /protocols/count).",
        params: [
          { name: "knesset_num / committee_id / q", desc: "אותם מסננים כמו החיפוש" },
        ],
        example: "/api/knesset-db/protocols/count?knesset_num=25",
      },
    ],
  },
  {
    id: "ocal",
    title: "יומן לעם — יומני נבחרי ובכירי ציבור",
    note: "יומני פגישות של נבחרי ובכירי ציבור בישראל, מאוחדים ומועשרים (ישויות, הצלבות בין יומנים, קיבוץ אירועים חופפים). כתובת בסיס: /api/ocal.",
    endpoints: [
      {
        path: "/api/ocal/events",
        description: "חיפוש אירועי יומן: טקסט חופשי, טווח תאריכים, מקורות, מיקום, משתתפים וישויות.",
        params: [
          { name: "q", desc: "טקסט חופשי בכותרת/מיקום/משתתפים" },
          { name: "from_date / to_date", desc: "טווח תאריכים (YYYY-MM-DD)" },
          { name: "source_ids", desc: "סינון לפי יומני מקור (מזהים, מופרדים בפסיק)" },
          { name: "location / participants / entity_names", desc: "סינון לפי מיקום / משתתפים / ישויות" },
          { name: "page / per_page", desc: "עימוד (per_page עד 100)" },
          { name: "sort", desc: "date_desc (ברירת מחדל) | date_asc" },
        ],
        example: "/api/ocal/events?q=פגישה&from_date=2024-01-01&per_page=5",
      },
      {
        path: "/api/ocal/events/{id}",
        description: "אירוע בודד על כל פרטיו, כולל ישויות מקושרות, הצלבות בין יומנים ואירועים חופפים.",
        example: "/api/ocal/events/{event_id}",
      },
      {
        path: "/api/ocal/calendar",
        description: "תצוגת לוח-שנה של אירועים לפי חודש/שבוע/יום, עם ספירות וצביעה לפי מקור.",
        params: [
          { name: "date", desc: "תאריך עוגן (YYYY-MM-DD)" },
          { name: "view", desc: "month (ברירת מחדל) | week | 4day | day" },
          { name: "source_ids / entity_names", desc: "סינון לפי מקורות / ישויות" },
        ],
        example: "/api/ocal/calendar?date=2024-03-01&view=month",
      },
      {
        path: "/api/ocal/sources",
        description: "קטלוג יומני המקור: שם, בעלים, צבע, טווח תאריכים ומספר אירועים.",
        example: "/api/ocal/sources",
      },
      {
        path: "/api/ocal/stats",
        description: "היקף הקורפוס: מספר האירועים, היומנים והארגונים.",
        example: "/api/ocal/stats",
      },
      {
        path: "/api/ocal/download/source/{id}",
        description: "הורדת כל אירועי יומן מקור כ-CSV או JSON (UTF-8 עם BOM, כותרות עבריות).",
        params: [{ name: "format", desc: "csv (ברירת מחדל) | json" }],
        example: "/api/ocal/download/source/{source_id}?format=csv",
      },
    ],
  },
  {
    id: "nadlan",
    title: 'נדל"ן לעם — נכס אחד, כל צורות הזיהוי',
    note: "הצלבה ברמת הנכס בין שכבת החלקות, גזטיר הנכסים, קובץ המיקוד ורשימת הכתובות. כתובת בסיס: /api/nadlan. · העיקרון: נכנסים עם צורת הזיהוי שיש לכם ויוצאים עם כל השאר, באותה עטיפה בדיוק — לא משנה באיזו נקודת כניסה השתמשתם. מעבר לזהות, כל תשובה נושאת גם את שתי השכבות שמתארות את הנכס: האזור הסטטיסטי (א\"ס) של הנקודה, שנקבע לפי מרכז החלקה בתוך שכבת הלמ\"ס, וסיכום עסקאות מיסוי מקרקעין שדווחו על אותם גוש וחלקה. · הנתונים מעובדים (processed=true) וכל תשובה נושאת את מגבלות הכיסוי שלה בשדה caveats — אלה מדידות, לא הסתייגות כללית.",
    endpoints: [
      {
        path: "/api/nadlan/lookup",
        description:
          "נקודת הכניסה האחת: מזינים כל אחד מהמזהים — נקודה (lat+lon), כתובת (city+street+number), מיקוד (zip) או גוש־חלקה (gush+helka) — ובוחרים ב-fields אילו חלקים מהתשובה מעניינים אתכם. מזהה מפורש גובר על q: מי שנקב בגוש ובחלקה אמר מה כוונתו. שם שדה שאינו חלק מהעטיפה מוחזר כשגיאה ולא מתעלמים ממנו בשקט.",
        params: [
          { name: "lat / lon / radius_m", desc: "נקודה על המפה; radius_m=0 מחזיר את החלקה שמתחת לנקודה, וערך גדול יותר את החלקות שמרכזן במרחק הזה (עד 2,000 מ׳)" },
          { name: "gush / helka / suffix", desc: "גוש, חלקה ותת-גוש" },
          { name: "zip", desc: "מיקוד, 5 או 7 ספרות" },
          { name: "city / street / number", desc: "כתובת" },
          { name: "q", desc: "טקסט חופשי — נקודה, מיקוד או \"גוש 6319 חלקה 225\". נופל אחורה לכתובת מפורשת" },
          { name: "fields", desc: 'אילו חלקים להחזיר, מופרדים בפסיק, או all: identity, point, zip, streets, addresses, stat_area, deals, geometry, sources, match. ברירת המחדל היא הכול למעט geometry — הוא הקריאה היחידה שנוגעת בטבלת החלקות (4.58 GB)' },
          { name: "limit", desc: "מספר חלקות בתשובה (1-200, ברירת מחדל 50)" },
        ],
        example: "/api/nadlan/lookup?lat=32.0682&lon=34.7847&fields=identity,zip,stat_area,deals",
      },
      {
        path: "/api/nadlan/parcel/{gush}/{helka}",
        description:
          "אותה תשובה, לפי גוש וחלקה. suffix מצמצם לתת-גוש; geometry=true מצרף את פוליגון החלקה; stat_area=false / deals=false מכבים את שתי שכבות התיאור כשלא צריך אותן.",
        example: "/api/nadlan/parcel/6319/225?geometry=true",
      },
      {
        path: "/api/nadlan/parcel/{gush}/{helka}/deals",
        description:
          "כל עסקאות מיסוי מקרקעין שדווחו על החלקה, מהחדשה לישנה, עם עימוד. בעטיפה עצמה יש רק סיכום, כי חלקה של מגדל מגורים מחזיקה מאות עסקאות (1,850 בשיא שנמדד). sub_parcel מצמצם לתת-חלקה אחת — במגדל זו הדירה הבודדת, והרמה היחידה שבה סדרת מחירים אומרת משהו.",
        params: [
          { name: "sub_parcel", desc: "תת-חלקה (שלוש ספרות; גם 7 יתקבל)" },
          { name: "limit / offset", desc: "עימוד (limit 1-200)" },
        ],
        example: "/api/nadlan/parcel/7104/289/deals?sub_parcel=118",
      },
      {
        path: "/api/nadlan/point / /zip/{zip} / /address",
        description:
          "שלוש נקודות הכניסה האחרות, כל אחת עם אותה עטיפה: לפי נקודה ורדיוס, לפי מיקוד (5 או 7 ספרות), ולפי יישוב+רחוב+מספר.",
        example: "/api/nadlan/address?city=פתח תקווה&street=אבימלך&number=8",
      },
      {
        path: "/api/nadlan/streets",
        description: "השלמה אוטומטית לשמות רחובות, עם אפשרות לצמצם ליישוב אחד.",
        example: "/api/nadlan/streets?q=אבימ&settlement=7900",
      },
      {
        path: "/api/nadlan/stats",
        description: "מוני העל של הפרויקט ואחוזי הכיסוי בפועל — כמה כתובות עם נקודה, כמה משויכות לחלקה, כמה עם מיקוד ברמת הכתובת.",
        example: "/api/nadlan/stats",
      },
    ],
  },
  {
    id: "deals",
    title: 'עסקאות נדל"ן — מאגר מיסוי מקרקעין',
    note: "3.84 מיליון עסקאות מקרקעין מדווחות מ-1998 ואילך, שהמפרסם מאפשר לקרוא גוש אחד בכל פעם. כתובת בסיס: /api/deals. · שלוש נקודות הקצה /search, /series ו-/breakdown מקבלות את אותם מסננים בדיוק, כדי שסיכום לא יוכל לתאר אוכלוסייה אחרת מזו שבטבלה שלידו. · בניגוד לשאר פרויקטי \"לעם\", כאן processed=false: אלה שורות המקור כפי שפורסמו, בלי חישוב ובלי תיקון. · total נספר עד 10,000 ואז מדווח total_capped=true — ספירה מדויקת של היסטוריית עיר שלמה היא עלות אמיתית עבור מספר שאיש לא קורא.",
    endpoints: [
      {
        path: "/api/deals/search",
        description:
          "המאגר, מסונן וממוין. הסינון לפי יישוב הוא לפי השם כפי שפורסם (ל-17.5% מהשורות אין קוד יישוב במקור), והשמות המדויקים מגיעים מ-/settlements. כל תשובה נושאת גם console_sql ו-row_url — אותה שאילתה עצמה, להרצה ישירה בקונסולת /data.",
        params: [
          { name: "settlement", desc: "שם היישוב כפי שמופיע ב-/api/deals/settlements" },
          { name: "gush / helka / sub_parcel", desc: "גוש, חלקה ותת-חלקה" },
          { name: "nature", desc: "מהות העסקה, מתוך /api/deals/natures" },
          { name: "date_from / date_to", desc: "טווח תאריכים בפורמט YYYY-MM-DD" },
          { name: "min_amount / max_amount", desc: "טווח שווי מדווח בשקלים" },
          { name: "min_rooms / max_rooms", desc: "טווח חדרים" },
          { name: "sort", desc: "date_desc (ברירת מחדל) | date_asc | amount_desc | amount_asc | area_desc" },
          { name: "limit / offset", desc: "עימוד (limit 1-200)" },
        ],
        example: "/api/deals/search?settlement=חיפה&nature=דירה&date_from=2024-01-01&sort=amount_desc",
      },
      {
        path: "/api/deals/series",
        description:
          "מספר העסקאות וחציון השווי המדווח לכל שנה, תחת אותם מסננים בדיוק כמו /search. חציון ולא ממוצע: המאגר מערבב דירה אחת עם מכירת בניין שלם, ושורה אחת כזו מזיזה ממוצע במיליונים.",
        example: "/api/deals/series?settlement=חיפה&nature=דירה",
      },
      {
        path: "/api/deals/breakdown",
        description: "פילוח לפי מהות העסקה תחת אותם מסננים — כמה עסקאות וחציון שווי לכל סוג.",
        example: "/api/deals/breakdown?settlement=חיפה",
      },
      {
        path: "/api/deals/compare",
        description:
          "שתי שנים, כל היישובים, זה לצד זה: מספר העסקאות וחציון המחיר בכל אחת, והשינוי באחוזים. min_deals חל על שתי השנים בנפרד, כדי שיישוב עם קומץ מכירות לא יראה קפיצה שהיא עסקה חריגה אחת. בלי nature התשובה מחזירה warning: תמהיל שהשתנה בין השנים נראה כמו שינוי מחיר.",
        params: [
          { name: "year_from / year_to", desc: "שתי שנים להשוואה" },
          { name: "nature", desc: "מהות העסקה — מומלץ מאוד" },
          { name: "min_deals", desc: "מינימום עסקאות בכל אחת מהשנים (ברירת מחדל 30)" },
          { name: "order", desc: "change_desc (ברירת מחדל) | change_asc | median_desc | deals_desc" },
          { name: "limit", desc: "מספר יישובים (1-200)" },
        ],
        example: "/api/deals/compare?year_from=2019&year_to=2025&nature=דירה בבית קומות",
      },
      {
        path: "/api/deals/settlements",
        description:
          "כל היישובים שבמאגר, עם מספר העסקאות ותאריך העסקה האחרונה בכל אחד. השמות מוחזרים כלשונם במקור — הם המחרוזות המדויקות ש-/search מסנן לפיהן.",
        example: "/api/deals/settlements",
      },
      {
        path: "/api/deals/natures",
        description: "סוגי העסקאות (47 במאגר), עם מספר העסקאות וחציון השווי בכל סוג.",
        example: "/api/deals/natures",
      },
      {
        path: "/api/deals/parcel/{gush}/{helka}",
        description: "כל העסקאות בחלקה אחת, מהחדשה לישנה — אותה רשימה שמופיעה בעמוד הנכס בנדל\"ן לעם.",
        example: "/api/deals/parcel/7104/289",
      },
      {
        path: "/api/deals/stats",
        description: "מוני העל: כמה עסקאות, מאיזה תאריך עד איזה, כמה יישובים וכמה חלקות — ומזהה המאגר במעקב, כדי שכל מספר יהיה קליק אחד ממקורו.",
        example: "/api/deals/stats",
      },
    ],
  },
  {
    id: "deep-search",
    title: "שאלות לעם — חיפוש רוחבי בכל המקורות",
    note: "שאילתה אחת שנשלחת במקביל לכל הקורפוסים שגרסאות לעם מגיעה אליהם — מאגרים במעקב, טבלאות SQL, הלמ״ס, פרוטוקולי ועדות, ממ״מ, דוחות מבקר המדינה, החלטות ממשלה, יומני נבחרי ציבור, תאגידים, מפתח התקציב ומידע לעם. כתובת בסיס: /api/deep-search. · שני דברים שכדאי לדעת לפני שמשתמשים: (1) בקשה אחת לכל מקור — הדף שולח את המקורות בנפרד כדי שכל עמודה תיצבע ברגע שהיא חוזרת, וזו גם הסיבה שהמסננים שטוחים (f_<id>) ולא ממוענים לפי מקור. (2) total יכול לחזור null, וזה לעולם לא אומר אפס — הוא אומר שהספירה לא בוצעה (בקורפוסי טקסט מלא מוותרים עליה כי היא מכפילה את זמן התשובה). המספר האמיתי של התוצאות שהוחזרו הוא אורך results.",
    endpoints: [
      {
        path: "/api/deep-search/sources",
        description:
          "קטלוג המקורות שאפשר לחפש בהם: מזהה, שם, צבע, ייחוס, האם המקור חיצוני, אילו מסננים הוא מציע, והאם הוא מוגדר בשרת. קורפוסי הטקסט המלא מדווחים כאן גם את הכיסוי שלהם בזמן אמת (טווח שנים ומספר מסמכים) — הוא נקרא מהשירות ולא כתוב בקוד, ומסנן תאריכים נמשך אוטומטית ממקור שלא יכול לכבד אותו. אף ערך סוד אינו נחשף — רק configured.",
        example: "/api/deep-search/sources",
      },
      {
        path: "/api/deep-search/search",
        description:
          "הרצת שאילתה על מקור אחד או על תת-קבוצה. כל עמודה מוחזרת עם results, total, ו-error משלה — כשל במקור אחד לעולם לא מפיל את השאר, ומקור שלא סיים לחפש מדווח שגיאה ולא רשימה ריקה. תומך באופרטורים: \"ציטוט מדויק\", מינוס להחרגה, ו-OR. מקור שיודע לפרסר אותם בעצמו מקבל את השאילתה כלשונה; מקור שלא — נשאל שאלה רחבה יותר והתוצאות מסוננות כאן.",
        params: [
          { name: "q", desc: "טקסט חופשי (עד 200 תווים), כולל אופרטורים" },
          { name: "sources", desc: "רשימת מזהי מקורות מופרדת בפסיקים; ריק ⇒ כל המקורות הפעילים" },
          { name: "limit", desc: "מספר תוצאות לכל מקור (1-50, ברירת מחדל 15)" },
          { name: "f_<id>", desc: "מסנן של אותו מקור, לפי המזהה שמופיע ב-/sources — למשל f_organization=jerusalem_muni או f_date_from=2020-01-01" },
        ],
        example: "/api/deep-search/search?q=%22תקציב%20הביטחון%22&sources=gov_decisions,mmm_text&limit=5",
      },
    ],
  },
];

const MCP_SERVERS: {
  key: string;
  label: string;
  path: string;
  purpose: string;
  tools: string[];
}[] = [
  {
    key: "over",
    label: "OVER — מאגרי מידע ממשלתיים",
    path: "/mcp",
    purpose: "חיפוש מאגרים שבמעקב, שליפת גרסאות וקבצים, תגיות וארגונים, ותשאול תוכן השורות (NEON).",
    tools: ["search_datasets", "get_dataset", "query_dataset_rows", "list_tags", "list_organizations", "get_stats"],
  },
  {
    key: "sql",
    label: "SQL — כל מסד הנתונים של האתר",
    path: "/data/mcp",
    purpose:
      "קטלוג כל הטבלאות השאילתיות באתר, מבנה הטבלאות (DDL) ושאילתות SQL חופשיות (קריאה בלבד) מעל כל הסכימות — מאגרים, כנסת, אינדקסי אוספים, מידע לעם ויומן לעם. אותו מנוע שמאחורי קונסולת /data.",
    tools: ["list_schemas", "list_tables", "describe_schema", "run_sql", "get_table", "get_table_profile"],
  },
  {
    key: "cbs",
    label: "למ״ס (CBS)",
    path: "/cbs/mcp",
    purpose: "חיפוש באינדקס פרסומי הלמ״ס, שליפת עמוד, פאסטים ופריטים נבחרים.",
    tools: ["search", "get_page", "facets", "list_featured", "get_stats"],
  },
  {
    key: "knesset",
    label: "כנסת — פרוטוקולי ועדות + ODATA",
    path: "/knesset/mcp",
    purpose: "חיפוש ועדות/ישיבות/פרוטוקולים, מסמכי ממ״מ, שליפת ישיבה, ו-SQL חופשי על מראה ה-ODATA של הכנסת.",
    tools: ["search_committees", "search_sessions", "search_protocols", "get_session", "search_mmm", "run_sql", "list_tables", "get_stats"],
  },
  {
    key: "ocal",
    label: "יומן לעם — יומני נבחרי ציבור",
    path: "/ocal/mcp",
    purpose: "חיפוש אירועי יומן, שליפת אירוע, רשימת ישויות ומקורות, איתור פגישות משותפות בין שני אישים, וסטטיסטיקות.",
    tools: ["search_events", "get_event", "list_entities", "list_sources", "find_meetings_between", "get_stats"],
  },
  {
    key: "ocoi",
    label: "ניגוד עניינים לעם — הסדרי ניגוד עניינים",
    path: "/ocoi/mcp",
    purpose:
      "חיפוש אנשים, חברות, עמותות ותחומים; רשת הקשרים של ישות ומסלול בין שתי ישויות; מסמכי ההצהרות והקשרים שחולצו מהם; דירוג המקושרים ביותר, פילוח לפי משרד, ומראת רשם החברות.",
    tools: ["search", "entity_get", "graph_neighbors", "graph_path", "document_get", "document_entities", "top_connected", "by_ministry", "registry_lookup", "stats"],
  },
  {
    key: "elections",
    label: "מימון בחירות לעם — תרומות ומימון מערכות בחירות",
    path: "/elections/mcp",
    purpose:
      "מרשם מימון הבחירות של מבקר המדינה: כל תרומה, ערבות והלוואה שדווחו בבחירות לרשויות המקומיות, למועצות אזוריות, למפלגות, בבחירות מקדימות ובבחירות מיוחדות. תשאול לפי שם אדם — מה נתן תורם מסוים ולמי, ומי מימן מועמד או מפלגה — עם סינון לפי סוג הפרסום (תרומה/ערבות/הלוואה), סוג הבחירות, יישוב, טווח תאריכים וטווח סכומים. שם אינו מזהה ייחודי, ולכן כל תשובה מפרטת אילו כתיבים נכללו בסכום.",
    tools: ["search_donations", "donor_profile", "recipient_profile", "top_donors", "stats", "list_election_types"],
  },
  {
    key: "nadlan",
    label: 'נדל"ן לעם — נכס אחד, כל צורות הזיהוי',
    path: "/nadlan/mcp",
    purpose:
      "שואלים בשפה חופשית על נכס — \"מה יש בגוש 6319 חלקה 225\", \"מה הכתובת הזו\" — ומקבלים את כל צורות הזיהוי האחרות: גוש, חלקה ותת-גוש, יישוב ומחוז, כתובות ומיקודים, הנקודה על המפה, האזור הסטטיסטי (א\"ס) עם אוכלוסייה ומדד חברתי-כלכלי, וסיכום עסקאות מיסוי מקרקעין. השרת יודע את המלכודות שמפילות תשובה שנראית נכונה: צמד גוש-חלקה עמום, שיוך כתובת שהוא ספציאלי ולא השוואת מחרוזות, ומדד חברתי-כלכלי שמתפרסם על חלוקת אזורים אחרת.",
    tools: ["lookup_property", "parcel_deals", "suggest_streets", "parcel_geometry", "coverage_stats"],
  },
  {
    key: "deals",
    label: 'עסקאות נדל"ן — מאגר מיסוי מקרקעין',
    path: "/deals/mcp",
    purpose:
      "שאלות בשפה חופשית על שוק הנדל\"ן מעל 3.84 מיליון עסקאות מדווחות מ-1998: כמה נמכר, באיזה מחיר, איך זה השתנה לאורך השנים, ואיפה עלה או ירד יותר מכל. כל חישוב הוא חציון ולא ממוצע (שורה אחת יכולה להיות דירה או בניין שלם), ההשוואה בין יישובים דורשת מינימום עסקאות בשתי השנים, והסינון לפי יישוב הוא לפי השם — ל-17.5% מהשורות אין קוד יישוב במקור.",
    tools: ["search_deals", "price_series", "compare_settlements", "list_settlements", "list_deal_types", "parcel_deals", "register_stats"],
  },
  {
    key: "prices",
    label: "שקיפות מחירים — מחירי המזון בכל הרשתות",
    path: "/prices/mcp",
    purpose:
      "המחירים, הסניפים והמבצעים שכל רשת מזון גדולה מחויבת לפרסם לפי חוק קידום התחרות — כ-30 רשתות, כל אחת נאספת כמאגר נפרד פעם ביום, ומתושאלות כאן יחד: איפה מוצר זול יותר, כמה עולה סל קניות בכל סניף בעיר, אילו מבצעים חלים עליו ואיך המחיר השתנה לאורך זמן. הזיהוי האמין בין רשתות הוא הברקוד, לכל מחיר מצורף תאריך הקובץ שממנו נלקח, ומבצעים מוצגים בנפרד ואינם מקוזזים מהמחיר.",
    tools: ["search_products", "compare_prices", "compare_basket", "item_promotions", "price_history", "find_stores", "store_prices", "list_chains"],
  },
  {
    key: "odata",
    label: "מידע לעם — בקשות חופש מידע",
    path: "/odata/mcp",
    purpose:
      "חיפוש בקטלוג מידע לעם (odata.org.il): מאגרים שהתקבלו בעקבות בקשות חופש מידע, סינון לפי הגוף המפרסם, ושליפת מאגר בודד עם כל קבציו. מעבר ישיר לאתר המקור — הקבצים אינם מאוחסנים בגרסאות לעם, והגוף המפרסם הוא מי שביקש את המידע ולא בהכרח מי שהפיק אותו.",
    tools: ["search_datasets", "get_dataset", "list_organizations"],
  },
];

// Hebrew number words for the lead sentence. Derived from MCP_SERVERS.length
// rather than written out, because the previous hardcoded "ארבעה" was still
// there when the list had already grown to six — the same way every hardcoded
// coverage number on this site has gone stale.
const HE_COUNT: Record<number, string> = {
  1: "שרת MCP אחד",
  2: "שני שרתי MCP",
  3: "שלושה שרתי MCP",
  4: "ארבעה שרתי MCP",
  5: "חמישה שרתי MCP",
  6: "שישה שרתי MCP",
  7: "שבעה שרתי MCP",
  8: "שמונה שרתי MCP",
  9: "תשעה שרתי MCP",
  10: "עשרה שרתי MCP",
  11: "אחד-עשר שרתי MCP",
  12: "שנים-עשר שרתי MCP",
};

export function mcpServerCountLabel(n: number): string {
  return HE_COUNT[n] ?? `${n} שרתי MCP`;
}

function CopyUrlButton({ value }: { value: string }) {
  const [copied, setCopied] = useState(false);

  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      /* ignore — older browsers without clipboard API */
    }
  };

  return (
    <button type="button" onClick={onCopy} className="api-copy-btn" aria-label="העתק כתובת">
      {copied ? "✓ הועתק" : "העתק"}
    </button>
  );
}

function McpCard() {
  const origin =
    typeof window !== "undefined" ? window.location.origin : "https://www.over.org.il";

  return (
    <section className="api-mcp-card" aria-labelledby="api-mcp-title">
      <div className="api-mcp-header">
        <h2 id="api-mcp-title" className="api-mcp-title">
          MCP — חיבור ישיר ל-Claude / ChatGPT / Cursor / סוכני AI
        </h2>
        <span className="api-mcp-badge">ביתא</span>
      </div>

      <p className="api-mcp-lead">
        גישה מובנית לדאטה דרך Model Context Protocol — ה-LLM מחפש ומושך נתונים
        מתוך השיחה, בלי לעבור דרך ה-API הציבורי. {mcpServerCountLabel(MCP_SERVERS.length)}{" "}
        <strong>חיים</strong>, אחד לכל מקור. הגישה פתוחה לכל מי שמתחבר עם חשבון Google, בלי
        הרשמה מראש ובלי לחכות לאישור: ההתחברות הראשונה פותחת לכם גישה (ביתא) לכל השרתים.
      </p>

      <div className="api-mcp-grid">
        {MCP_SERVERS.map((s) => {
          const url = `${origin}${s.path}`;
          return (
            <div className="api-mcp-subcard" key={s.key}>
              <h3 className="api-mcp-subtitle">{s.label}</h3>
              <p>{s.purpose}</p>
              <div className="api-mcp-url-row" dir="ltr">
                <code className="api-mcp-url">{url}</code>
                <CopyUrlButton value={url} />
              </div>
              <p style={{ marginTop: "0.5rem", fontSize: "0.8rem", opacity: 0.85 }} dir="ltr">
                {s.tools.join(" · ")}
              </p>
            </div>
          );
        })}
      </div>

      <div className="api-mcp-subcard" style={{ marginTop: "1rem" }}>
        <h3 className="api-mcp-subtitle">איך מתחברים מ-Claude</h3>
        <ol className="api-mcp-steps">
          <li>
            ב-Claude (דסקטופ או claude.ai) פתחו <strong>Settings → Connectors</strong> ולחצו{" "}
            <strong>Add custom connector</strong>.
          </li>
          <li>
            ב-<em>Name</em> כתבו שם שתזהו (למשל "כנסת — גרסאות לעם"), וב-<em>Server URL</em>{" "}
            הדביקו את כתובת השרת הרצוי מלמעלה (למשל <code dir="ltr">{origin}/knesset/mcp</code>).
          </li>
          <li>
            לחצו <strong>Connect</strong>. ייפתח חלון Google, ובו בוחרים את החשבון שאיתו
            מתחברים. בהתחברות הראשונה החשבון נרשם אוטומטית.
          </li>
          <li>
            ה-connector יסומן Connected, ובסרגל הכלים של השיחה יופיעו הפעולות של
            אותו שרת. אפשר לחבר את כל השרתים במקביל, וכולם עובדים עם אותה התחברות.
          </li>
        </ol>
      </div>
    </section>
  );
}

function LookerCard() {
  const connectorUrl = LOOKER_CONNECTOR_ID
    ? `https://lookerstudio.google.com/datasources/create?connectorId=${LOOKER_CONNECTOR_ID}`
    : null;

  return (
    <section id="looker" className="api-mcp-card" aria-labelledby="api-looker-title">
      <div className="api-mcp-header">
        <h2 id="api-looker-title" className="api-mcp-title">
          Looker Studio — דשבורדים ישירות על ה-SQL
        </h2>
        <span className="api-mcp-badge">ביתא</span>
      </div>

      <p className="api-mcp-lead">
        מחבר (Community Connector) רשמי של גרסאות לעם ל-Looker Studio של גוגל:
        בוחרים טבלה מהקטלוג או כותבים שאילתת SQL חופשית — כולל JOIN בין מאגרי
        data.gov.il לטבלאות הכנסת — ובונים עליה דשבורד ציבורי, בלי שום פרטי
        התחברות למסד הנתונים. שמות עמודות בעברית מוצגים כמו שהם.
      </p>

      <div className="api-mcp-subcard">
        <h3 className="api-mcp-subtitle">איך מתחילים</h3>
        {connectorUrl ? (
          <ol className="api-mcp-steps">
            <li>
              פתחו את{" "}
              <a href={connectorUrl} target="_blank" rel="noopener noreferrer">
                קישור ההוספה של המחבר
              <span className="sr-only"> (נפתח בחלון חדש)</span></a>{" "}
              והתחברו עם חשבון Google.
            </li>
            <li>
              במסך ההרשאות יופיע "Google hasn't verified this app" — זה צפוי
              במחברים קהילתיים: לחצו <strong>Advanced → Go to OVER</strong>. ההרשאה
              היחידה שהמחבר מבקש היא פנייה לכתובת חיצונית (over.org.il) — הוא לא
              ניגש לקבצים או למייל שלכם.
            </li>
            <li>בחרו טבלה מהרשימה או הדביקו SQL חופשי, ולחצו Connect.</li>
            <li>Create Report — ומכאן זה Looker Studio רגיל: גרפים, פילטרים ושיתוף.</li>
          </ol>
        ) : (
          <p style={{ margin: 0 }}>קישור ההוספה יתפרסם כאן בקרוב, עם שחרור המחבר.</p>
        )}
      </div>

      <div className="api-mcp-subcard" style={{ marginTop: "1rem" }}>
        <h3 className="api-mcp-subtitle">מגבלות</h3>
        <p style={{ margin: 0 }}>
          עד 10,000 שורות לשאילתה כברירת מחדל (ניתן להגדיל עד 50,000 בהגדרות
          המחבר), 30 שניות לשאילתה, קריאה בלבד. מומלץ להשאיר את רענון הנתונים
          (Data freshness) על ברירת המחדל של 12 שעות. נקודות הקצה{" "}
          <code dir="ltr">/api/connector/*</code> הן תשתית המחבר ומוגנות במפתח —
          לשימוש ישיר ב-SQL יש את <a href="/data">/data</a> ואת ה-API הפתוח שמתועד כאן.
        </p>
      </div>
    </section>
  );
}

function EndpointCard({ ep }: { ep: ApiEndpoint }) {
  const baseUrl = typeof window !== "undefined" ? window.location.origin : "";
  const method = ep.method ?? "GET";

  return (
    <div className="api-endpoint">
      <div className="api-endpoint-head">
        <span className="api-method-badge">{method}</span>
        <code className="api-endpoint-path" dir="ltr">
          {ep.path}
        </code>
      </div>
      <p className="api-endpoint-desc">{ep.description}</p>

      {ep.params && ep.params.length > 0 && (
        <ul className="api-param-list">
          {ep.params.map((p) => (
            <li key={p.name}>
              <code className="api-param-name">{p.name}</code>
              <span> — {p.desc}</span>
            </li>
          ))}
        </ul>
      )}

      <div className="api-endpoint-example">
        <span className="api-endpoint-example-label">
          {method === "POST" ? "נתיב (POST — עם גוף JSON):" : "דוגמה:"}
        </span>
        <a
          href={`${baseUrl}${ep.example}`}
          target="_blank"
          rel="noopener noreferrer"
          className="api-endpoint-example-link"
          dir="ltr"
        >
          {ep.example}
        <span className="sr-only"> (נפתח בחלון חדש)</span></a>
      </div>
    </div>
  );
}

export default function ApiPage() {
  useDocumentTitle("API ציבורי");
  const { t } = useTranslation();

  return (
    <>
      <section className="hero" style={{ textAlign: "center" }}>
        <div className="container">
          <h1 style={{ fontSize: "2rem", fontWeight: 700, marginBottom: "0.4rem" }}>
            {t("api.title", "API ציבורי")}
          </h1>
          <p style={{ color: "#E4F1F5", fontSize: "0.95rem" }}>
            {t(
              "api.subtitle",
              "ממשק פתוח לארבעה מקורות — מאגרי ממשלה (OVER), אינדקס הלמ״ס, פרוטוקולי הכנסת, ויומני נבחרי ציבור (יומן לעם) — ב-REST וב-MCP",
            )}
          </p>
        </div>
      </section>

      <div className="container" style={{ paddingTop: "1.5rem", paddingBottom: "3rem" }}>
      <PlainSummary>
        העמוד הזה מסביר איך לקבל את הנתונים של האתר ישירות לתוכנה שלכם (<Abbr>API</Abbr>), בלי לעבור דרך הדפדפן. אין צורך בהרשמה או במפתח. כל כתובת בעמוד מחזירה נתונים בפורמט שתוכנות קוראות; אפשר להעתיק אותה ולהדביק בדפדפן כדי לראות מה מגיע.
      </PlainSummary>

        <div className="api-base-card">
          <p>
            {t(
              "api.intro",
              "כל ה-endpoints הם ציבוריים (רובם GET) — אין צורך באימות, אין מפתח API, ו-CORS פתוח כך שאפשר לקרוא להם ישירות מדפדפן. לכל מקור קידומת כתובת משלו: OVER תחת /api/v1 (מטא-דאטה) ו-/api/append (שורות), הלמ״ס תחת /api/cbs, הכנסת תחת /api/knesset-db ו-/api/knesset-protocols, ויומן לעם תחת /api/ocal. מעליהם כולם יושב /api/tables — SQL חופשי לקריאה בלבד על כל הטבלאות באתר, כולל שכבות ממ״ג עם PostGIS ופלט GeoJSON לפי מלבן.",
            )}
          </p>
          <p>
            {t(
              "api.intro_2",
              "השימוש פתוח לחוקרים, עיתונאים, פעילי שקיפות ופרויקטים אזרחיים. אם אתם בונים אינטגרציה רחבה — נא להימנע ממיליוני קריאות מקבילות ולשמור מטמון מקומי.",
            )}
          </p>
          <p>
            {t(
              "api.intro_versions",
              "תיעוד אינטראקטיבי מלא (OpenAPI) לכל נקודות הקצה של כל המקורות זמין ב-",
            )}
            <a href="/docs" target="_blank" rel="noopener noreferrer" dir="ltr">/docs<span className="sr-only"> (נפתח בחלון חדש)</span></a>
            {" · "}
            <a href="/redoc" target="_blank" rel="noopener noreferrer" dir="ltr">/redoc<span className="sr-only"> (נפתח בחלון חדש)</span></a>
            .
          </p>
        </div>

        <McpCard />
        <LookerCard />

        <h2 className="api-endpoints-heading">{t("api.endpoints", "נקודות קצה")}</h2>
        {ENDPOINT_GROUPS.map((group) => (
          <section key={group.id} className="api-endpoint-group" aria-label={group.title}>
            <h3 className="api-endpoint-group-title">{group.title}</h3>
            {group.note && <p className="api-endpoint-group-note">{group.note}</p>}
            <div className="api-endpoint-list">
              {group.endpoints.map((ep) => (
                <EndpointCard key={`${ep.method ?? "GET"} ${ep.path}`} ep={ep} />
              ))}
            </div>
          </section>
        ))}
      </div>
    </>
  );
}
