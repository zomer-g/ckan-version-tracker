// Human (Hebrew) labels for the coarse CBS site sections the crawler stores in
// `section` (the first path segment after the language). Shown in the section
// filter dropdown and on each result card. Falls back to the raw value for any
// section not listed here.
export const SECTION_LABELS: Record<string, string> = {
  publications: "פרסומים",
  mediarelease: "הודעות לתקשורת",
  subjects: "נושאים",
  databank: "בנק נתונים",
  Surveys: "סקרים",
  tools: "כלים",
  Pages: "עמודי תוכן",
  About: "אודות",
  cbsNewBrand: "עמודי מותג",
  intent: "דפי נחיתה",
};

export const sectionLabel = (s: string | null | undefined): string =>
  (s && (SECTION_LABELS[s] || s)) || "";

// Geographic-granularity labels. The crawler now emits Hebrew level names
// directly (e.g. "ארצי", "יישוב"), so this mostly passes through; the mapping
// still covers the older English codes for safety.
export const GEO_LABELS: Record<string, string> = {
  national: "ארצי",
  district: "מחוז",
  subdistrict: "נפה",
  municipality: "רשות מקומית",
  locality: "יישוב",
};

export const geoLabel = (g: string): string => GEO_LABELS[g] || g;

// User-facing product taxonomy (the enrichment layer's product_form column):
// "what do I get" — replaces site-speak section/item_type in the primary UI.
export const PRODUCT_FORM_LABELS: Record<string, string> = {
  data_file: "קובץ נתונים",
  gis_layer: "שכבה גאוגרפית",
  puf: "קובץ פרט (PUF)",
  generator: "מחולל",
  dashboard: "דשבורד",
  api: "API",
  database: "בנק נתונים",
  publication: "פרסום",
  methodology: "מתודולוגיה והגדרות",
};

export const productFormLabel = (p: string): string => PRODUCT_FORM_LABELS[p] || p;

export const PRODUCT_FORM_ICONS: Record<string, string> = {
  data_file: "📊",
  gis_layer: "🗺️",
  puf: "🔐",
  generator: "⚙️",
  dashboard: "📈",
  api: "🔌",
  database: "🗄️",
  publication: "📄",
  methodology: "📖",
};

// Measure types (enrichment `metrics`) and population cuts (`cuts`) are stored
// as English codes; the UI is Hebrew.
export const METRIC_LABELS: Record<string, string> = {
  count: "ספירה",
  pct: "אחוז/שיעור",
  avg: "ממוצע",
  median: "חציון",
  distribution: "התפלגות",
  index: "מדד",
};

export const metricLabel = (m: string): string => METRIC_LABELS[m] || m;

export const CUT_LABELS: Record<string, string> = {
  age: "גיל",
  gender: "מגדר",
  sector_religion: "מגזר/דתיות",
  immigration: "עולים/הגירה",
  education: "השכלה",
  industry: "ענף כלכלי",
  ses: "סוציו-אקונומי",
};

export const cutLabel = (c: string): string => CUT_LABELS[c] || c;
