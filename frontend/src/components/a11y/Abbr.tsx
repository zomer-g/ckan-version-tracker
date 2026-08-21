/**
 * Abbreviations and jargon, expanded on first use (WCAG 3.1.4 Abbreviations,
 * 3.1.3 Unusual Words).
 *
 * The interface is dense with terms that cannot be worked out from context —
 * CKAN, ODATA, MCP, WFS, ITM, גוש/חלקה, מבא"ת, ממ"מ. There were zero <abbr>
 * or <dfn> elements in the codebase. `<Abbr>CKAN</Abbr>` looks the term up
 * here and renders a real <abbr title>, so the expansion is available to a
 * screen reader and on hover, and the glossary page below stays in step
 * because both read the same table.
 */

export const GLOSSARY: Record<string, string> = {
  CKAN: "Comprehensive Knowledge Archive Network — תוכנת הקטלוג שעליה בנוי data.gov.il ורוב פורטלי המידע הפתוח בעולם",
  ODATA: "Open Data Protocol — תקן לחשיפת נתונים טבלאיים דרך שירות אינטרנט",
  MCP: "Model Context Protocol — תקן שמאפשר למודלי שפה לקרוא נתונים ממקור חיצוני",
  API: "Application Programming Interface — ממשק לקריאת נתונים בתוכנה, במקום דרך דפדפן",
  SQL: "Structured Query Language — שפת השאילתות שבה שואלים שאלות ממסד נתונים",
  WFS: "Web Feature Service — תקן להורדת שכבות מידע גיאוגרפיות מלאות",
  WMS: "Web Map Service — תקן להצגת מפות כתמונה",
  ITM: "Israel Transverse Mercator — רשת הקואורדינטות הרשמית של ישראל",
  EPSG: "מספר מזהה בינלאומי של מערכת קואורדינטות",
  CSV: "Comma-Separated Values — קובץ טבלה פשוט שכל תוכנת גיליון פותחת",
  JSON: "פורמט טקסט מובנה שתוכנות קוראות בקלות",
  R2: "שירות אחסון הקבצים של Cloudflare, שבו נשמרים העותקים של הקבצים",
  NEON: "שירות מסד הנתונים שבו נשמרים הנתונים הטבלאיים לשאילתות",
  'מבא"ת': "מרשם בקשות ואישורי תכנון — מאגר התוכניות של מינהל התכנון",
  'ממ"מ': "מרכז המחקר והמידע של הכנסת",
  'למ"ס': "הלשכה המרכזית לסטטיסטיקה",
  גוש: "יחידת רישום קרקע — מספר הגוש והחלקה יחד מזהים נכס בטאבו",
  חלקה: "יחידת רישום קרקע בתוך גוש",
  גזטיר: "מילון שמות מקומות רשמי, שממפה שם יישוב או רחוב לקוד קבוע",
  סקרייפר: "תוכנה שאוספת נתונים מאתר אינטרנט באופן אוטומטי",
};

export default function Abbr(props: { children: string; title?: string }) {
  const term = props.children;
  const title = props.title || GLOSSARY[term];
  if (!title) return <>{term}</>;
  return <abbr title={title}>{term}</abbr>;
}
