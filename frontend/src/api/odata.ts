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
