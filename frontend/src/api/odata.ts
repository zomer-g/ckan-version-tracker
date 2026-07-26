import { KnessetDbSqlResult } from "./client";

/**
 * Client for מידע לעם (odata.org.il) — an EXTERNAL CKAN whose action API is
 * CORS-open, so everything here runs straight from the browser.
 *
 * odata data is PROCESSED, not an original public source. In the SQL console
 * its resources are queryable via a passthrough: a query marks an odata table
 * with the `odata.` schema (odata."<resource_id>"); we detect that, strip the
 * marker, and run it against odata's own datastore_search_sql engine — never
 * against our Neon DB. Callers use isOdataSql() to route and to raise the
 * "processed data" banner.
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

// The marker that flags a SQL statement as an odata passthrough. A resource id
// is a UUID (has hyphens), so it must be quoted — hence the trailing `"`.
const ODATA_MARKER = /\bodata\s*\.\s*"/i;

export function isOdataSql(sql: string): boolean {
  return ODATA_MARKER.test(sql);
}

// A ready "query this resource" statement for the editor.
export function odataResourceQuery(resourceId: string): string {
  return `SELECT *\nFROM odata."${resourceId}"\nLIMIT 100`;
}

// Run a passthrough query against odata's datastore, normalized to the same
// shape the Neon console uses so the results table renders unchanged.
export async function odataSql(sql: string): Promise<KnessetDbSqlResult> {
  // Strip the marker schema so odata's engine sees a bare "resource_id".
  const clean = sql.replace(/\bodata\s*\.\s*(")/gi, "$1");
  const url =
    `${ODATA_BASE}/api/3/action/datastore_search_sql` +
    `?sql=${encodeURIComponent(clean)}`;
  const res = await fetch(url);
  const data = await res.json();
  if (!data?.success) {
    const err = data?.error || {};
    const msg =
      err?.info?.orig ||
      (Array.isArray(err?.query) ? err.query.join(" ") : err?.message) ||
      "שאילתת מידע לעם נכשלה";
    throw new Error(String(msg));
  }
  const fields = (data.result?.fields || []).filter(
    (f: { id?: string }) => f.id !== "_full_text",
  );
  const columns = fields.map((f: { id: string }) => f.id);
  let rows = (data.result?.records || []) as Array<
    Record<string, string | number | boolean | null>
  >;
  const truncated = rows.length > 1000;
  if (truncated) rows = rows.slice(0, 1000);
  return { columns, rows, row_count: rows.length, truncated };
}
