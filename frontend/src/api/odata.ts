/**
 * Client for מידע לעם (odata.org.il) — an EXTERNAL CKAN whose action API is
 * CORS-open, so everything here runs straight from the browser.
 *
 * odata data is PROCESSED, not an original public source. In the SQL console
 * we surface only its DATA STRUCTURE (the list of items + their files); the
 * items are NOT queryable. Importing specific files into SQL is a later,
 * separate step.
 */
export const ODATA_BASE = "https://www.odata.org.il";

export type OdataResource = {
  id?: string;
  name?: string;
  format?: string;
  url?: string;
  datastore_active?: boolean;
};
export type OdataDataset = {
  name: string;
  title?: string;
  notes?: string;
  num_resources?: number;
  organization?: { title?: string; name?: string } | null;
  resources?: OdataResource[];
};

export function odataDatasetUrl(name: string, lang: string) {
  const prefix = lang === "en" ? "/en" : "";
  return `${ODATA_BASE}${prefix}/dataset/${encodeURIComponent(name)}`;
}

// Formats the SQL importer can turn into a table (mirrors odata_import.py).
const IMPORTABLE_FORMATS = new Set(["CSV", "XLS", "XLSX", "ICS", "ICAL", "ICA"]);
// File extension → format, for resources odata publishes with a BLANK format.
const EXT_FORMATS: Record<string, string> = {
  csv: "CSV", xlsx: "XLSX", xlsm: "XLSX", xls: "XLS",
  ics: "ICS", ical: "ICAL", ica: "ICA",
};

function extFormat(candidate?: string | null): string {
  const s = (candidate || "").trim();
  if (!s) return "";
  // An extension lives in the path — never in a URL's query string.
  const path = s.includes("://") ? s.split(/[?#]/)[0] : s;
  const ext = decodeURIComponent(path).split(".").pop()?.toLowerCase() || "";
  return EXT_FORMATS[ext] || "";
}

/** A resource's real format. Some odata resources declare an EMPTY `format`
 *  (e.g. "רשימת כתובות בישראל עם קואורדינטות", published as `כתובות.xlsx`), so
 *  fall back to the extension of the file name / download URL. Mirrors
 *  `infer_format` in odata_import.py. */
export function resourceFormat(r: OdataResource): string {
  const declared = (r.format || "").trim().toUpperCase();
  if (IMPORTABLE_FORMATS.has(declared)) return declared;
  return extFormat(r.name) || extFormat(r.url) || declared;
}

/** Can this resource be imported into a SQL table? Supported file format, or a
 *  datastore-active resource (whose dump we can always pull as CSV). */
export function isImportableResource(r: OdataResource): boolean {
  if (!r.id) return false;
  if (IMPORTABLE_FORMATS.has(resourceFormat(r))) return true;
  return !!r.datastore_active;
}

export async function odataPackageSearch(
  q: string,
  rows = 20,
  signal?: AbortSignal,
): Promise<{ count: number; results: OdataDataset[] }> {
  const url =
    `${ODATA_BASE}/api/3/action/package_search` +
    `?q=${encodeURIComponent(q)}&rows=${rows}`;
  const res = await fetch(url, { signal });
  const data = await res.json();
  if (!data?.success) throw new Error("odata search failed");
  return { count: data.result?.count || 0, results: data.result?.results || [] };
}
