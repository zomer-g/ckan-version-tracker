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
