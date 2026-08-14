import type { RegistrySourceView } from "../utils/sourceBadge";

const BASE = "/api";

function getToken(): string | null {
  return localStorage.getItem("token");
}

export function setToken(token: string) {
  localStorage.setItem("token", token);
}

export function clearToken() {
  localStorage.removeItem("token");
}

async function request<T>(
  path: string,
  options: RequestInit = {}
): Promise<T> {
  const token = getToken();
  const headers: Record<string, string> = {
    ...(options.headers as Record<string, string>),
  };
  if (token) {
    headers["Authorization"] = `Bearer ${token}`;
  }
  if (options.body && typeof options.body === "string") {
    headers["Content-Type"] = "application/json";
  }

  const resp = await fetch(`${BASE}${path}`, { ...options, headers });

  if (!resp.ok) {
    // Read as text first so a NON-JSON error (e.g. a Cloudflare WAF block page,
    // which is HTML) still yields an informative message instead of a blank one.
    const raw = await resp.text().catch(() => "");
    let detail = "";
    try {
      const j = JSON.parse(raw);
      detail = j?.detail || j?.message || "";
    } catch {
      /* not JSON — fall through to the heuristics below */
    }
    if (!detail) {
      const blocked = resp.status === 403 && /cloudflare|blocked|attention required|<html/i.test(raw);
      detail = blocked
        ? 'הבקשה נחסמה (403) על-ידי Cloudflare — ברוב המקרים זו הגבלת-קצב (המתינו כמה שניות ונסו שוב). אם זה חוזר גם אחרי המתנה, ייתכן שחוקת אבטחה חסמה את השאילתה — נסו לנסח אותה מחדש.'
        : `שגיאת שרת (${resp.status}${resp.statusText ? " " + resp.statusText : ""})`;
    }
    throw new Error(detail);
  }

  if (resp.status === 204) return undefined as T;
  return resp.json();
}

// UTF-8-safe base64 (btoa alone mangles Hebrew). Wraps console SQL so a WAF
// doesn't pattern-match the query as an injection attempt.
export function utf8ToBase64(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = "";
  for (let i = 0; i < bytes.length; i++) bin += String.fromCharCode(bytes[i]);
  return btoa(bin);
}

export function base64ToUtf8(s: string): string {
  const bin = atob(s);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return new TextDecoder().decode(bytes);
}

// Auth
export const auth = {
  me: () => request<{ id: string; email: string; display_name: string; is_admin: boolean }>("/auth/me"),
  ssoProviders: () =>
    request<{ google: boolean }>("/auth/sso/providers"),
  // Swap the one-time login code (delivered by the SSO callback as ?code=) for
  // a JWT. The token comes back in the POST body — never in a URL.
  exchange: (code: string) =>
    request<{ token: string }>("/auth/sso/exchange", {
      method: "POST",
      body: JSON.stringify({ code }),
    }),
  // Slide the (short-lived) session forward. Called on load + on a timer.
  refresh: () => request<{ token: string }>("/auth/refresh", { method: "POST" }),
};

// CKAN Proxy
export const ckan = {
  search: (q: string, rows = 20, start = 0) =>
    request<{ count: number; results: any[] }>(
      `/ckan/search?q=${encodeURIComponent(q)}&rows=${rows}&start=${start}`
    ),
  dataset: (id: string) => request<any>(`/ckan/dataset/${id}`),
  organizations: () => request<any[]>("/ckan/organizations"),
};

// Tags
export interface Tag {
  id: string;
  name: string;
}

export interface TagWithCount extends Tag {
  description: string | null;
  dataset_count: number;
}

export interface TagDataset {
  id: string;
  title: string;
  ckan_name: string;
  // Needed for the source chip — sourceBadgeFor keys on the ckan_id prefix,
  // which is the only signal that separates the scraper sources from each
  // other (organization drifts, and several share one ministry).
  ckan_id: string | null;
  organization: string | null;
  organization_id: string | null;
  organization_title: string | null;
  source_type: string;
  version_count: number;
  last_polled_at: string | null;
  // The source's own timestamp on the newest version we hold — when the DATA
  // last changed, as opposed to `last_polled_at`, which is when we last looked.
  last_modified: string | null;
  tags: Tag[];
}

export interface TagDetail extends TagWithCount {
  datasets: TagDataset[];
}

// Tracked Datasets
// Unified per-dataset storage plan. Folds the file destination (local / r2 /
// odata) and the NEON tabular-rows archive into ONE selector. NEON options are
// only valid for CKAN tabular sources (see `neon_eligible`).
export type StorageTarget =
  | "local"
  | "odata"
  | "r2"
  | "neon"
  | "r2+neon"
  | "odata+neon";

/**
 * What a dataset's archive ACTUALLY holds — derived server-side from the LATEST
 * version's `resource_mappings`, NOT from the configured `storage_target`.
 *
 * The two disagree regularly: a CKAN dataset above the 50k-row threshold that
 * wasn't opted into a NEON archive keeps a plan of "r2" while archiving only a
 * metadata stub (counts + column names + 200 sample rows). See
 * app/services/archive_state.py.
 *
 * `fidelity: null` = this endpoint didn't compute it. The unpaginated public
 * catalog (`GET /api/datasets`) deliberately skips it — loading 1,090 versions'
 * mappings is the payload that OOM-killed the 512MB dyno. The dataset detail
 * endpoint and the paginated admin list both populate it.
 */
export interface ArchiveState {
  // full = files + queryable rows · rows = NEON only · files = objects only
  // sample = metadata stub, no data · none = nothing archived
  fidelity: "full" | "rows" | "files" | "sample" | "none" | null;
  // Where the files actually are, read off the mapping values.
  file_store: "r2" | "odata" | "mixed" | "none";
  row_store: "neon" | "none";
  // Source row count behind a metadata stub (what ISN'T archived).
  sample_of: number | null;
  // Plan-vs-reality flags: no_version | sample_only | file_store. Admin surface.
  mismatch: string[];
}

export interface TrackedDataset {
  id: string;
  ckan_id: string;
  ckan_name: string;
  title: string;
  organization: string | null;
  organization_id: string | null;
  organization_title: string | null;
  odata_dataset_id: string | null;
  poll_interval: number;
  is_active: boolean;
  status: string;
  last_polled_at: string | null;
  last_modified: string | null;
  version_count: number;
  requester_name: string | null;
  requester_email: string | null;
  resource_id: string | null;
  resource_name: string | null;
  source_type: string;
  source_url: string | null;
  storage_mode: "full_snapshot" | "append_only";
  append_key: string | null;
  upload_mode: "full" | "local_only";
  storage_target: StorageTarget;
  // Whether NEON (tabular-rows) plans are offered for this source (CKAN only).
  neon_eligible: boolean;
  // DIFF mode (append_only only): capture changes to existing rows via a
  // COPY-staged content diff. Heavy — reserved for rare/extreme cases.
  capture_changes: boolean;
  // What the archive actually holds. Populated by the dataset-detail and admin
  // list endpoints; `fidelity` is null elsewhere.
  archive?: ArchiveState;
  last_error: string | null;
  // ISO timestamp of when the PUBLISHER was confirmed to have removed the
  // source this dataset tracks; null while it is present. The archive stays
  // fully readable — this only means no further versions are coming.
  source_gone_at?: string | null;
  /** Reader-facing reason to distrust the LATEST version's contents; null when
   *  none. A complete row count does not prove a faithful import. */
  import_warning?: string | null;
  import_warning_at?: string | null;
  resource_ids: string[] | null;
  new_resources_at_source: Array<{ id: string; name?: string | null; format?: string | null }> | null;
  tags?: Tag[];
}

export const datasets = {
  list: () => request<TrackedDataset[]>("/datasets"),
  // Public, lightweight count of pending tracking requests — powers the
  // subtle "requests waiting" dot in the navbar (visible to everyone).
  pendingCount: () => request<{ count: number }>("/datasets/pending-count"),
  track: (ckan_id: string, poll_interval = 3600, resource_id?: string) =>
    request<TrackedDataset>("/datasets", {
      method: "POST",
      body: JSON.stringify({ ckan_id, poll_interval, resource_id }),
    }),
  trackScraper: (source_url: string, title: string, poll_interval = 604800) =>
    request<TrackedDataset>("/datasets", {
      method: "POST",
      body: JSON.stringify({ source_type: "scraper", source_url, title, poll_interval }),
    }),
  trackGovmap: (source_url: string, title?: string, poll_interval = 604800) =>
    request<TrackedDataset>("/datasets", {
      method: "POST",
      body: JSON.stringify({ source_type: "govmap", source_url, title, poll_interval }),
    }),
  update: (id: string, data: { poll_interval?: number; is_active?: boolean; title?: string; organization_id?: string | null; storage_mode?: "full_snapshot" | "append_only"; append_key?: string | null; upload_mode?: "full" | "local_only"; storage_target?: StorageTarget; capture_changes?: boolean; resource_ids?: string[]; dismiss_new_resources?: boolean }) =>
    request<TrackedDataset>(`/datasets/${id}`, {
      method: "PATCH",
      body: JSON.stringify(data),
    }),
  untrack: (id: string) =>
    request<void>(`/datasets/${id}`, { method: "DELETE" }),
  poll: (id: string) =>
    request<{ message: string }>(`/datasets/${id}/poll`, { method: "POST" }),
  // Live progress of a running collection — PUBLIC (/api/v1), no credentials.
  // A collection that runs for days publishes nothing until it finishes, so
  // without this the only evidence it is alive is an admin token.
  scrapeStatus: (id: string) => request<ScrapeStatus>(`/v1/datasets/${id}/status`),
  // Targeted re-sampling (admin). `sampling` describes what this dataset can be
  // asked for — the modes its source declares, the statuses its items are
  // currently at, how far each key series has got; `sample` queues one run.
  sampling: (id: string) =>
    request<SamplingOptions>(`/datasets/${id}/sampling`),
  sample: (
    id: string,
    body: {
      mode: string;
      status?: string;
      item?: string;
      targets_from_dataset?: string;
      /** For mode="group": which named group the source declares. */
      group?: string;
    },
  ) =>
    request<{ message: string; task_id: string; mode: string; summary: string }>(
      `/datasets/${id}/sample`,
      { method: "POST", body: JSON.stringify(body) },
    ),
};

export interface ScrapeStatus {
  dataset_id: string;
  running: boolean;
  phase?: string | null;
  message?: string | null;
  percentage?: number | null;
  started_at?: string | null;
  updated_at?: string | null;
  elapsed_seconds?: number | null;
  /** The worker heartbeats every 30s; a climbing value means the run is in trouble. */
  seconds_since_heartbeat?: number | null;
  last_outcome?: string | null;
  last_finished_at?: string | null;
}

export interface SamplingOptions {
  enabled: boolean;
  modes: string[];
  mode_labels?: Record<string, string>;
  item_key?: string | null;
  status_column?: string | null;
  sample_column?: string | null;
  table?: string | null;
  // One entry per status the register's items currently sit at, commonest
  // first — `items` is how many files a "by status" run would re-sample.
  statuses?: { value: string; items: number }[];
  frontier?: Record<string, string>;
  max_targets?: number;
  // The runs OVER already performs by itself, per the source's manifest, keyed
  // by "new" / "group:<name>". Absent means every run is a click — which is a
  // different dataset to be looking at, so the panel says which it is.
  schedule?: Record<
    string,
    {
      mode: string;
      group: string | null;
      label: string;
      interval_seconds: number;
      last_run_at: string | null;
    }
  >;
  // Named target lists the source declares — "the publication clocks",
  // "everything that moved in the past year". `items` is how many the group
  // holds right now, which is what makes running one a considered choice.
  groups?: { name: string; label: string; items: number | null; error?: string }[];
  error?: string;
}

// Versions
export interface Version {
  id: string;
  version_number: number;
  metadata_modified: string;
  detected_at: string;
  odata_metadata_resource_id: string | null;
  change_summary: {
    type?: string;
    resources_added?: string[];
    resources_removed?: string[];
    resources_modified?: { resource_id: string; name: string; format: string }[];
    total_resources?: number;
    record_count?: number;
    previous_count?: number;
    delta?: number;
    fields?: string[];
    sample_rows?: number;
    // Total feature/row count of a scraper/govmap version. Used to suppress
    // the in-browser map for heavy GovMap layers whose GeoJSON is too large
    // to load in a browser tab without crashing it.
    total_rows?: number;
    // This version read a SUBSET of the corpus on purpose — one item, the items
    // at one status, only what is new, only what is still open (see
    // app/services/sampling_runs.py). Set by push-version, and absent rather
    // than false on a full pass.
    //
    // Load-bearing for a reader, not only for the shrink guard: a partial
    // version's row count is a count of what it READ, and next to a full pass's
    // it reads as a collapse. The register of מבא"ת publishes 11,047 rows after
    // 36,784 every week and is perfectly healthy.
    partial_run?: boolean;
    run_mode?: string;
  } | null;
  resource_mappings: Record<string, any> | null;
  dataset_title?: string | null;
  dataset_source_type?: string | null;
}

export const versions = {
  list: (datasetId: string) => request<Version[]>(`/datasets/${datasetId}/versions`),
  get: (versionId: string) => request<Version>(`/versions/${versionId}`),
  delete: (versionId: string) =>
    request<void>(`/versions/${versionId}`, { method: "DELETE" }),
  diff: (fromId: string, toId: string) =>
    request<{
      from_version: string;
      to_version: string;
      from_number: number;
      to_number: number;
      diff: Array<{
        type: string;
        field: string;
        old_value: any;
        new_value: any;
      }>;
    }>(`/diff?from=${fromId}&to=${toId}`),
};

// Append archive (per-dataset Postgres tables — the row-level APPEND store).
// Public read: browse, filter, and download the accumulated rows of a
// data.gov.il datastore dataset that OVER archives append-only.
// One physical NEON table of a dataset. A CKAN dataset archived as
// append_db_multi has one per datastore resource; a scraper dataset that
// publishes several tabular resources has one per resource (resource_id is
// null there — a scraper resource is identified by its name).
export interface AppendTableRef {
  table: string;
  resource_id: string | null;
  resource_name: string | null;
}

export interface AppendSchema {
  dataset_id: string;
  dataset_title: string;
  table: string;
  resource_id?: string | null;
  resource_name?: string | null;
  // Every table of the dataset, first-registered first. Length > 1 only for
  // multi-resource datasets; `multi_table` is the server's own verdict.
  tables?: AppendTableRef[];
  multi_table?: boolean;
  total: number;
  columns: string[];
  key: string | null;
  capture_changes?: boolean;
  first_seen_column: string;
  // Set when the archive holds a SAMPLING HISTORY — several rows per real-world
  // item, one per time it was sampled. `item_key` identifies the item,
  // `sample_column` says when a row was taken, and `supports_latest` means
  // ?latest=true collapses the table to the newest sample of each item.
  item_key?: string | null;
  sample_column?: string | null;
  supports_latest?: boolean;
}

export interface AppendRows {
  columns: string[];
  rows: Array<Record<string, string | null>>;
  total: number;
  limit: number;
  offset: number;
  sort: string;
  order: string;
}

// Build the query string from paging + sort + free-text q + per-column filters.
function appendQuery(opts: {
  limit?: number;
  offset?: number;
  sort?: string;
  order?: string;
  q?: string;
  // Which table of a multi-resource dataset. Must be sent BEFORE the per-column
  // filters below and is a reserved name server-side, so it can never be
  // mistaken for a column filter.
  table?: string;
  // One row per item (its newest sample) instead of the full sampling history.
  // Reserved server-side, like `table`.
  latest?: boolean;
  filters?: Record<string, string>;
}): string {
  const p = new URLSearchParams();
  if (opts.limit != null) p.set("limit", String(opts.limit));
  if (opts.offset != null) p.set("offset", String(opts.offset));
  if (opts.sort) p.set("sort", opts.sort);
  if (opts.order) p.set("order", opts.order);
  if (opts.q) p.set("q", opts.q);
  if (opts.table) p.set("table", opts.table);
  if (opts.latest) p.set("latest", "true");
  for (const [k, v] of Object.entries(opts.filters || {})) {
    if (v) p.set(k, v);
  }
  const s = p.toString();
  return s ? `?${s}` : "";
}

export interface AppendItemHistory {
  dataset_id: string;
  dataset_title: string;
  table: string;
  item_key: string;
  item: string;
  sample_column: string;
  samples: number;
  rows: Array<Record<string, string | number | boolean | null>>;
  limit: number;
  offset: number;
}

export interface AppendSqlResult {
  columns: string[];
  rows: Array<Record<string, string | number | boolean | null>>;
  truncated: boolean;
  row_count: number;
}

export const appendArchive = {
  schema: (datasetId: string, table?: string) =>
    request<AppendSchema>(
      `/append/${datasetId}/schema${table ? `?table=${encodeURIComponent(table)}` : ""}`,
    ),
  rows: (datasetId: string, opts: Parameters<typeof appendQuery>[0] = {}) =>
    request<AppendRows>(`/append/${datasetId}/rows${appendQuery(opts)}`),
  // Every sample of ONE item, newest first — the history of a single building
  // file / plan / record. Exact match on the dataset's item_key.
  item: (datasetId: string, value: string, table?: string) =>
    request<AppendItemHistory>(
      `/append/${datasetId}/item?value=${encodeURIComponent(value)}` +
        (table ? `&table=${encodeURIComponent(table)}` : ""),
    ),
  // Direct browser download (streams server-side); not a fetch.
  downloadUrl: (datasetId: string, opts: Parameters<typeof appendQuery>[0] = {}) =>
    `/api/append/${datasetId}/download.csv${appendQuery({ ...opts, limit: undefined, offset: undefined })}`,
  // Read-only SQL (single SELECT/WITH); server runs it in a READ ONLY tx.
  sql: (datasetId: string, sql: string) =>
    request<AppendSqlResult>(`/append/${datasetId}/sql`, {
      method: "POST",
      body: JSON.stringify({ sql }),
    }),
};

// Google Drive export (admin)
export interface DriveExportJob {
  id: string;
  status: "pending" | "running" | "success" | "failed";
  // SOURCE files (ZIP parts + CSV) — the coarse progress bar.
  total_files: number;
  completed_files: number;
  // Individual documents extracted from the ZIPs and uploaded — the headline.
  documents_uploaded: number;
  current_file: string | null;
  error: string | null;
}

export const drive = {
  status: () => request<{ connected: boolean }>("/drive/status"),
  // Begin the Drive-consent flow. Authenticated POST (JWT in the header, not
  // the URL); the server mints a one-time code, puts only that opaque code in
  // Google's `state`, and returns the authorize URL to navigate to. No token
  // ever rides in a query string.
  connect: (next: string) =>
    request<{ authorize_url: string }>("/auth/sso/google/drive/connect", {
      method: "POST",
      body: JSON.stringify({ next }),
    }),
  disconnect: () =>
    request<{ connected: boolean; was_connected: boolean }>("/drive/disconnect", {
      method: "POST",
    }),
  exportVersion: (versionId: string, folderUrl: string) =>
    request<DriveExportJob>(`/versions/${versionId}/export-to-drive`, {
      method: "POST",
      body: JSON.stringify({ folder_url: folderUrl }),
    }),
  exportStatus: (jobId: string) =>
    request<DriveExportJob>(`/drive/exports/${jobId}`),
};

// Gov.il Validation
export interface GovIlValidation {
  valid: boolean;
  page_type?: string;
  collector_name?: string;
  title?: string;
  url?: string;
  error?: string;
}

export const govil = {
  validate: (url: string) =>
    request<GovIlValidation>("/govil/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// GovMap Validation
export interface GovMapValidation {
  valid: boolean;
  layer_id?: string;
  center_itm?: { x: number; y: number } | null;
  url?: string;
  title?: string;
  error?: string;
}

export const govmap = {
  validate: (url: string) =>
    request<GovMapValidation>("/govmap/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// Source-URL resolver — "does OVER already track what this URL points at?"
// Backs the /lookup deep link and the home search box, so a pasted link that
// is already tracked opens its versions page instead of a request form.
export interface ResolveMatch {
  id: string;
  title: string;
  ckan_id?: string | null;
  organization?: string | null;
  source_type?: string | null;
  source_url?: string | null;
  status?: string | null;
  is_active: boolean;
  version_count: number;
  /** "identity" = same thing; "path" = same page, may be more than one. */
  match: "identity" | "path" | string;
}

export interface ResolveResponse {
  url: string;
  found: boolean;
  matches: ResolveMatch[];
}

export const resolve = {
  lookup: (url: string) =>
    request<ResolveResponse>(`/resolve?url=${encodeURIComponent(url)}`),
};

/**
 * The shareable deep link for a source URL: it opens the dataset's versions
 * page if OVER tracks it, and the collection request form if it doesn't.
 * One link, correct before and after the source is onboarded.
 *
 * The shape is a fixed prefix plus the source URL verbatim —
 * `over.org.il/direct/<source url>` — so any tool (the browser extension,
 * a script, a spreadsheet formula) can build it by concatenation, with no
 * encoding step and nothing to look up. Only the two characters that would
 * break reassembly are escaped: "#" (everything after it never reaches the
 * server) and spaces.
 */
export function lookupLinkFor(sourceUrl: string, origin?: string): string {
  const base = origin ?? (typeof window !== "undefined" ? window.location.origin : "");
  return `${base}/direct/${encodeURI(sourceUrl.trim()).replace(/#/g, "%23")}`;
}

/**
 * Twin of ``rebuild_source_url`` in app/api/resolve.py — reassembles the
 * source URL from a /direct/<url> route. The path part and the query string
 * arrive separately; a percent-encoded URL arrives whole, with no query.
 * In production the server redirects /direct/* before the SPA loads, so this
 * runs only in dev and as a safety net.
 */
export function sourceUrlFromDirectPath(splat: string, search: string): string {
  let url = (splat || "").replace(/^\/+/, "");
  const query = (search || "").replace(/^\?/, "");
  if (query) url += (url.includes("?") ? "&" : "?") + query;
  return url.replace(/^(https?):\/+/i, "$1://");
}

// IDF Validation — shares the same response shape as gov.il
// (page_type, collector_name, title, url, error). The server side lives
// at app/api/idf.py; only Military-Prosecution unit pages are accepted
// in v1.
export const idf = {
  validate: (url: string) =>
    request<GovIlValidation>("/idf/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// practitioners.health.gov.il validation — shares the same response
// shape as gov.il / idf (page_type, collector_name, title, url, error).
// Server side at app/api/health.py; only per-registry
// /Practitioners/{id} URLs accepted.
export const health = {
  validate: (url: string) =>
    request<GovIlValidation>("/health/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// registries.health.gov.il validation — same response shape. Server
// side at app/api/registries.py; only per-registry /<RegistryPath> URLs
// (Ambulances, FoodImporters, MedicalDevices, ...) are accepted.
export const registries = {
  validate: (url: string) =>
    request<GovIlValidation>("/registries/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// avodata.labor.gov.il validation — same response shape. Server side
// at app/api/avodata.py; only /search?scope=<known-slug> URLs accepted
// (22 scopes; the backend enforces the allowlist).
export const avodata = {
  validate: (url: string) =>
    request<GovIlValidation>("/avodata/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// municipal-data.org validation — same response shape. Server side at
// app/api/munidata.py; only per-metric URLs (/<slug>?metric=<id>) accepted,
// one dataset per metric ("מצב השלטון המקומי", Ministry of Interior).
export const munidata = {
  validate: (url: string) =>
    request<GovIlValidation>("/munidata/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// govextra.gov.il/pmo/emun validation — same response shape. Server side at
// app/api/emun.py; any /pmo/emun path is accepted and collapses to the single
// dashboard dataset ("מערכת אמו״ן", Prime Minister's Office).
export const emun = {
  validate: (url: string) =>
    request<GovIlValidation>("/emun/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// gov.il/apps/servicescompass validation — same response shape. Server side
// at app/api/servicescompass.py; only the single app page is accepted
// ("מצפן השירותים הממשלתיים", National Digital Agency), one dataset.
export const servicescompass = {
  validate: (url: string) =>
    request<GovIlValidation>("/servicescompass/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// mevaker.gov.il validation — same response shape. Server side at
// app/api/mevaker.py; only the /subjects reports index is accepted (the
// whole State Comptroller corpus, tracked as one dataset).
export const mevaker = {
  validate: (url: string) =>
    request<GovIlValidation>("/mevaker/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// geo.mot.gov.il (חצב) validation — same response shape. Server side at
// app/api/hatzav.py; only the portal root is accepted (the whole layer
// catalog is tracked as one dataset).
export const hatzav = {
  validate: (url: string) =>
    request<GovIlValidation>("/hatzav/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// apps.education.gov.il/Mankal validation — same response shape. Server
// side at app/api/mankal.py; only the portal index is accepted (the whole
// חוזרי מנכ"ל corpus is tracked as one dataset).
export const mankal = {
  validate: (url: string) =>
    request<GovIlValidation>("/mankal/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// jda.gov.il (הרשות לפיתוח ירושלים) validation — same response shape. Server
// side at app/api/jda.py; only the three tenders-domain index pages are
// accepted (מכרזים / הודעות לפי תקנות חובת המכרזים / החלטות ועדת המכרזים),
// each its own dataset.
export const jda = {
  validate: (url: string) =>
    request<GovIlValidation>("/jda/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// jeden.co.il (חברת עדן / Eden Company) validation — same response shape.
// Server side at app/api/eden.py; two corpora (eden_tenders / eden_decisions)
// share ONE page, so the corpus is chosen by a ?category=tenders /
// ?category=decisions query param. A bare jeden.co.il URL (no category)
// validates as INVALID and the endpoint explains which param to add.
export const eden = {
  validate: (url: string) =>
    request<GovIlValidation>("/eden/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// knesset.gov.il committee protocols — same response shape. Server side at
// app/api/knesset.py. Each committee is its own dataset, tracked from the open
// ODATA-v4 feed via a KNS_Committee query with a committee scope
// (?$filter=CategoryID eq N, or ?$filter=Id eq N). A KNS_Committee query with
// no committee scope validates as INVALID and the endpoint explains what to add.
export const knesset = {
  validate: (url: string) =>
    request<GovIlValidation>("/knesset/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
};

// Sources declared by the scraper worker rather than hardcoded here. One
// generic validate replaces the per-source endpoints above for every source
// added from now on: the manifest's URL patterns live on the server (they're
// Python regexes — `(?P<name>…)` is a syntax error in a JS RegExp), so the
// browser asks the server to classify a pasted URL instead of matching it
// locally. See app/api/sources.py.
export interface RegistrySourceValidation extends GovIlValidation {
  source_id?: string;
  label_he?: string;
  label_en?: string;
  badge?: { bg: string; fg: string; accent: string; label: string };
  source_link_he?: string;
  source_link_en?: string;
  default_poll_interval?: number;
  // The pasted page publishes several files and this build knows how to list
  // them, so the tracking form should offer them to pick from.
  file_picker?: boolean;
}

// One file on a previewed page. `path` is the server-relative path and is what
// travels back as `selected_files`; `on_page` is false for a file that shares
// the page's folder without being part of the page's own table.
export interface SourceFile {
  path: string;
  name: string;
  title: string;
  chapter: string;
  subject: string;
  ext: string;
  size: number;
  modified: string;
  url: string;
  on_page: boolean;
  tabular: boolean;
  // OVER already has a dataset for this file — active or awaiting approval.
  tracked?: boolean;
  tracked_dataset?: { dataset_id: string; status: string };
}

export const sources = {
  validate: (url: string) =>
    request<RegistrySourceValidation>("/sources/validate", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
  // Live and read-only: asks the source site what the pasted page publishes.
  // No dataset exists yet and no file is downloaded.
  preview: (url: string) =>
    request<{ title: string; url: string; source_id: string; files: SourceFile[] }>(
      "/sources/preview",
      { method: "POST", body: JSON.stringify({ url }) },
    ),
  registry: () =>
    request<{ sources: RegistrySourceView[] }>("/sources/registry"),
};

// CBS (cbs.gov.il) content index — a searchable catalog of the Central Bureau
// of Statistics site (one row per crawled page). Server side at app/api/cbs.py;
// the table is populated by the govil-scraper `cbs` engine (Playwright crawl,
// HEAD-only file sizing — bytes are never mirrored). Read-only + public.
export interface CbsFileLink {
  label: string | null;
  href: string;
  ext: string | null;
  size: number | null;
  last_modified: string | null;
}

export interface CbsResult {
  url: string;
  lang: string | null;
  section: string | null;
  series: string | null;
  item_type: string | null;
  title: string | null;
  title_en: string | null;
  summary: string | null;
  subject_tags: string[] | null;
  year_start: number | null;
  year_end: number | null;
  geo_levels: string[] | null;
  file_links: CbsFileLink[] | null;
  file_types: string[] | null;
  last_crawled: string | null;
  // Enrichment layer (server-derived, migration 038) — null before backfill.
  product_form: string | null;
  freq: string | null;
  source_op: string | null;
  data_vintage: number | null;
  geo_vintage: string | null;
  geo_coverage: string | null;
  series_key: string | null;
  edition_year: number | null;
  is_latest_edition: boolean | null;
  metrics: string[] | null;
  cuts: string[] | null;
}

export interface CbsSearchResponse {
  total: number;
  results: CbsResult[];
}

export interface CbsFacets {
  subjects: string[];
  geo_levels: string[];
  file_types: string[];
  sections: string[];
  item_types: string[];
  year_min: number | null;
  year_max: number | null;
  // Enrichment facets — empty until the server-side enrich backfill has run.
  product_forms: string[];
  freqs: string[];
  source_ops: string[];
}

export interface CbsStats {
  total: number;
  crawled: number;
  pending: number;
  errored: number;
  by_section: Record<string, number>;
}

export interface CbsSearchParams {
  q?: string;
  subject?: string;
  geo?: string;
  file_type?: string;
  section?: string;
  item_type?: string;
  lang?: string;
  year_from?: number;
  year_to?: number;
  product_form?: string;
  freq?: string;
  source_op?: string;
  latest_only?: boolean;
  sort?: "relevance" | "chrono";
  limit?: number;
  offset?: number;
}

export interface CbsFeaturedResponse {
  results: CbsResult[];
}

// Natural-language resolution (POST /api/cbs/resolve): an LLM parses the free
// text into filters, runs the shared intent-aware search, and classifies the top
// hit so the UI can render an actionable card instead of a raw list. Measured on
// the WhatsApp benchmark, this NL path finds the right page far more often than
// keyword /search (which barely bridges Hebrew surface-form gaps). answer_type:
//   guidance           — a curated intent points straight at the source
//   generator          — the source is a מחולל/dashboard to run
//   data_file          — a direct xlsx/csv download exists on the page
//   publication        — a relevant publication/page
//   special_processing — CBS HAS it, but only via a request / the research room
//   not_available      — CBS does not hold this (community-confirmed); see `answer`
//   no_results         — nothing matched
// The last two are opposite answers and must stay visually distinct: one is a
// dead end, the other is "you can get this — here's who to ask".
export type CbsAnswerType =
  | "guidance"
  | "generator"
  | "data_file"
  | "publication"
  | "special_processing"
  | "not_available"
  | "no_results";

export interface CbsResolvePrimary {
  title: string | null;
  url: string | null;
  link: string; // clean navigational target (intents keep it here, not in url)
  item_type: string | null;
  section: string | null;
  product_form: string | null;
  data_vintage: number | null;
  series_key: string | null;
}

// The deterministic parse of the question — what the "הבנתי:" chips render.
// geo_entity comes from the locality gazetteer; the rest is regex over the
// question itself (no LLM). See app/api/cbs_parse.py.
export interface CbsUnderstood {
  geo_level: string | null;
  years: number[];
  latest: boolean;
  series: boolean;
  product_form: string | null;
  metrics: string[];
  cuts: string[];
  source_op: string | null;
  geo_entity: {
    code: number;
    name: string;
    district: string | null;
    subdistrict: string | null;
    population: number | null;
  } | null;
}

export interface CbsEdition {
  title: string | null;
  url: string;
  edition_year: number | null;
  is_latest_edition: boolean | null;
}

export interface CbsResolveResponse {
  answer: string;
  answer_type: CbsAnswerType;
  provider: string;
  primary: CbsResolvePrimary | null;
  understood: CbsUnderstood;
  // Availability by resolution over the found sources (ladder-ordered server
  // side): level → available? Includes the requested level even when false.
  geo_matrix: Record<string, boolean>;
  editions: CbsEdition[];
  geo_available: string | null;
  caveats: string[];
  filters: Record<string, string | number | null>;
  total: number;
  results: CbsResult[];
  source: string;
}

// Like/dislike feedback on a search (POST /api/cbs/feedback).
export interface CbsFeedbackBody {
  query: string;
  vote: 1 | -1;
  mode: "ask" | "advanced";
  answer_type?: string | null;
  top_url?: string | null;
  source?: string; // "web" | "extension"
}

export type CbsFeedbackOrder = "dislikes" | "likes" | "total" | "recent";

export interface CbsFeedbackQueryRow {
  query: string;
  likes: number;
  dislikes: number;
  total: number;
  score: number;
  last_at: string | null;
}

export interface CbsFeedbackReport {
  total_votes: number;
  likes: number;
  dislikes: number;
  queries: CbsFeedbackQueryRow[];
}

export interface CbsGazetteerEntry {
  code: number;
  name: string;
  name_en: string | null;
  district: string | null;
  subdistrict: string | null;
  municipal_status: string | null;
  regional_council: string | null;
  population: number | null;
}

export const cbs = {
  search: (params: CbsSearchParams = {}) => {
    const p = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== null && v !== "") p.set(k, String(v));
    }
    const qs = p.toString();
    return request<CbsSearchResponse>(`/cbs/search${qs ? `?${qs}` : ""}`);
  },
  // Natural-language search — the recommended path for human-worded questions.
  resolve: (q: string, limit = 10) =>
    request<CbsResolveResponse>("/cbs/resolve", {
      method: "POST",
      body: JSON.stringify({ q, limit }),
    }),
  facets: () => request<CbsFacets>("/cbs/facets"),
  stats: () => request<CbsStats>("/cbs/stats"),
  // Locality autocomplete (name/alias/English), biggest-first within prefix
  // matches. Backed by the CBS bycode gazetteer.
  gazetteer: (q: string, limit = 8) =>
    request<{ results: CbsGazetteerEntry[] }>(
      `/cbs/gazetteer?q=${encodeURIComponent(q)}&limit=${limit}`,
    ),
  // Edition history of one series ("מהדורות קודמות").
  series: (key: string) =>
    request<{ results: CbsResult[] }>(`/cbs/series?key=${encodeURIComponent(key)}`),
  // Record one like (+1) / dislike (-1) on a search. Public, fire-and-forget.
  feedback: (body: CbsFeedbackBody) =>
    request<{ ok: boolean }>("/cbs/feedback", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  // Admin-only aggregated report, grouped by query (default: most-disliked first).
  feedbackReport: (order: CbsFeedbackOrder = "dislikes", limit = 200) =>
    request<CbsFeedbackReport>(`/cbs/feedback/report?order=${order}&limit=${limit}`),
  // Admin-pinned quick-access pages (public read; pin/unpin are admin-only and
  // return the updated list). See app/api/cbs.py.
  featured: () => request<CbsFeaturedResponse>("/cbs/featured"),
  pin: (url: string) =>
    request<CbsFeaturedResponse>("/cbs/featured", {
      method: "POST",
      body: JSON.stringify({ url }),
    }),
  unpin: (url: string) =>
    request<CbsFeaturedResponse>(`/cbs/featured?url=${encodeURIComponent(url)}`, {
      method: "DELETE",
    }),
};

// ── Knesset ODATA mirror (מסד הנתונים של הכנסת) ──────────────────────────────
export interface KnessetDbColumn {
  name: string;
  type: string;
}

export interface KnessetDbTable {
  table: string;
  entity_set: string;
  group: string;
  description: string;
  columns: KnessetDbColumn[];
  total_rows: number;
  source_count: number | null;
  full_loaded: boolean;
  status: string;
  error: string | null;
  last_synced_at: string | null;
}

export interface KnessetDbStatus {
  enabled: boolean;
  tables?: number;
  loaded?: number;
  rows?: number;
  last_sync?: string | null;
  last_activity?: string | null;
}

export interface KnessetDbSqlResult {
  columns: string[];
  rows: Array<Record<string, string | number | boolean | null>>;
  truncated: boolean;
  row_count: number;
}

// Protocol batches (the "אצוות" tab): filter values + count + download URLs.
export interface KnessetProtocolFacets {
  knessets: Array<{ knesset_num: number; protocols: number }>;
  committees: Array<{ id: number; name: string; knesset_num: number | null; protocols: number }>;
}

export interface KnessetBatchFilter {
  knesset_num?: number;
  committee_id?: number;
  q?: string;
}

function knessetBatchQs(f: KnessetBatchFilter): string {
  const p = new URLSearchParams();
  if (f.knesset_num !== undefined) p.set("knesset_num", String(f.knesset_num));
  if (f.committee_id !== undefined) p.set("committee_id", String(f.committee_id));
  if (f.q) p.set("q", f.q);
  return p.toString();
}

// MMM (מרכז המחקר והמידע) document catalog — knesset.mmm_documents.
export interface MmmDocument {
  rid: number;
  title: string | null;
  doc_type: string | null;
  date: string | null;
  date_text: string | null;
  author: string | null;
  approver: string | null;
  requested_by: string | null;
  keywords: string | null;
  abstract: string | null;
  incident_url: string | null;
  pdf_url: string | null;
}

export interface MmmSearchResult {
  total: number;
  items: MmmDocument[];
  limit: number;
  offset: number;
}

export interface MmmFacets {
  doc_types: Array<{ doc_type: string; count: number }>;
  year_min: number | null;
  year_max: number | null;
  total: number;
}

// Deep/slow search — full-text INSIDE document bodies via TAG-IT (scope 14).
// Fields are best-effort (TAG-IT's schema is opaque); `fields` carries the raw
// hit. See app/services/tagit_mcp.py.
export interface MmmDeepHit {
  doc_id: number | string | null;
  title: string | null;
  date: string | null;
  abstract: string | null;
  doc_type: string | null;
  link: string | null;
  snippet: string | null;
  rank: number | null;
  fields: Record<string, unknown>;
}

export interface MmmDeepResult {
  items: MmmDeepHit[];
  total: number;
  total_exact: boolean;
  page: number;
  size: number;
}

export const knessetDb = {
  status: () => request<KnessetDbStatus>("/knesset-db/status"),
  tables: () => request<{ tables: KnessetDbTable[] }>("/knesset-db/tables"),
  mmmSearch: (p: { q?: string; author?: string; doc_type?: string; year_from?: number; year_to?: number; limit?: number; offset?: number } = {}) => {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(p)) {
      if (v !== undefined && v !== null && v !== "") qs.set(k, String(v));
    }
    return request<MmmSearchResult>(`/knesset-db/mmm/search?${qs.toString()}`);
  },
  mmmFacets: () => request<MmmFacets>("/knesset-db/mmm/facets"),
  mmmDeepSearch: (p: { q: string; page?: number; size?: number }) => {
    const qs = new URLSearchParams({ q: p.q });
    if (p.page) qs.set("page", String(p.page));
    if (p.size) qs.set("size", String(p.size));
    return request<MmmDeepResult>(`/knesset-db/mmm/deep-search?${qs.toString()}`);
  },
  protocolFacets: () => request<KnessetProtocolFacets>("/knesset-db/protocols/facets"),
  protocolCount: (f: KnessetBatchFilter) =>
    request<{ files: number; zip_max_files: number }>(
      `/knesset-db/protocols/count?${knessetBatchQs(f)}`),
  // Direct browser downloads (server streams); not fetches.
  batchZipUrl: (f: KnessetBatchFilter) => `/api/knesset-db/protocols/batch.zip?${knessetBatchQs(f)}`,
  batchLinksUrl: (f: KnessetBatchFilter) => `/api/knesset-db/protocols/links.csv?${knessetBatchQs(f)}`,
  sql: (sql: string) =>
    request<KnessetDbSqlResult>("/knesset-db/sql", {
      method: "POST",
      body: JSON.stringify({ sql }),
    }),
  // Direct browser download (streams server-side); not a fetch.
  exportUrl: (sql: string) =>
    `/api/knesset-db/export.csv?sql=${encodeURIComponent(sql)}`,
  // Admin: kick a sync pass now (optionally one table, optionally re-walk it).
  sync: (opts: { table?: string; reset?: boolean } = {}) =>
    request<{ started: boolean }>("/knesset-db/sync", {
      method: "POST",
      body: JSON.stringify(opts),
    }),
};

// ---- Central data catalog + SQL console (/data page) ----
// One entry per queryable table on the site: every NEON dataset table (public
// schema) plus the 48 Knesset schema tables. `columns` powers autocomplete +
// the schema reference; `est_rows` is a planner estimate (exact count lives in
// the detail cube).
export interface CatalogColumn {
  name: string;
  type: string;
  // The Hebrew caption of a machine-named field (GovMap layers publish
  // `shem_yishuv`, not "שם יישוב"). Present only where the source documents
  // one; see app/services/column_aliases.py. The NAME is what SQL answers to —
  // the alias is for finding it.
  alias?: string;
}
export interface CatalogTable {
  table: string;
  // Kept in step with data_catalog: `idx` (the mirrored index CSVs, kind
  // "index") joined public and knesset and this union was never widened, so
  // TypeScript has been quietly wrong about every GovMap layer since.
  schema: "public" | "knesset" | "idx" | "odata" | "ocal";
  kind: "dataset" | "knesset" | "index" | "odata" | "ocal" | "over";
  title: string;
  description?: string;
  group?: string | null;
  // dataset-only linkage + source signals (drive the source badge)
  dataset_id?: string;
  version_id?: string | null;
  organization?: string | null;
  ckan_id?: string | null;
  source_type: string;
  source_url: string;
  archive_url?: string;
  versions_url?: string;
  page_url?: string;
  tags: string[];
  // Content field-flags metadata (e.g. { has_locality: true }); see
  // app/services/field_flags.py. Absent/empty until the recompute job has run.
  field_flags?: Record<string, boolean>;
  columns: CatalogColumn[];
  est_rows: number | null;
}
export interface CatalogFile {
  name: string;
  url: string;
}
export interface TableProfileColumn {
  detected_kind?: string;
  non_null?: number;
  fill_rate?: number;
  distinct_est?: number;
  distinct_ratio?: number;
  min?: number | string | null;
  max?: number | string | null;
  avg?: number | null;
  native?: boolean;
  numeric_rate?: number;
  date_format?: { python?: string; postgres?: string; match_rate?: number; ambiguous?: boolean } | null;
  entity_guess?: { guess?: string; confidence?: number; evidence?: string[] };
  top_values?: Array<{ value: string; count: number }>;
}

// The auto-computed profile / metadata for one table (see table_profiler).
export interface TableProfile {
  schema_name: string;
  table_name: string;
  row_count: number | null;
  column_count: number | null;
  status?: string;
  summary_he?: string | null;
  profiled_at?: string | null;
  enriched_at?: string | null;
  sql_profile?: {
    candidate_key?: string | null;
    keywords?: Array<{ token: string; count: number }>;
    columns?: Record<string, TableProfileColumn>;
    geometry_columns?: string[];
  };
  llm_enrichment?: {
    summary_he?: string;
    tags?: string[];
    keywords?: string[];
    columns?: Record<string, { description_he?: string; semantic_type?: string; date_format_ok?: boolean }>;
  };
  date_parse_specs?: Record<string, unknown>;
}

export interface CatalogTableDetail extends CatalogTable {
  row_count: number | null;
  profile?: TableProfile | null;
  files: CatalogFile[];
  // `omitted_columns` are real columns deliberately NOT fetched for the preview
  // (geometry/WKT — see append_store._BULK_COLS): they live in TOAST and pulling
  // them turned a sub-second sample into a 46-second one. They must still be
  // SHOWN, or a layer's spatial column looks like it does not exist at all.
  sample: {
    columns: string[];
    rows: Array<Record<string, unknown>>;
    omitted_columns?: string[];
  };
  csv_url?: string;
  csv_export?: boolean;
}

export const dataCatalog = {
  tables: () => request<{ tables: CatalogTable[] }>("/tables"),
  tableDetail: (table: string) =>
    request<CatalogTableDetail>(`/tables/${encodeURIComponent(table)}/detail`),
  tableProfile: (table: string) =>
    request<TableProfile>(`/tables/${encodeURIComponent(table)}/profile`),
  // Read-only SQL over public + knesset (single SELECT/WITH, READ ONLY tx).
  // Sent base64-encoded so a Cloudflare/WAF managed rule doesn't false-positive
  // the SQL keywords in the body as an injection attack (→ 403 block page).
  sql: (sql: string) =>
    request<KnessetDbSqlResult>("/tables/sql", {
      method: "POST",
      body: JSON.stringify({ sql_b64: utf8ToBase64(sql) }),
    }),
  // Direct browser download (server streams); not a fetch. base64 for the same
  // WAF reason — the query would otherwise sit in the URL as plain SQL.
  exportUrl: (sql: string) =>
    `/api/tables/export.csv?sql_b64=${encodeURIComponent(utf8ToBase64(sql))}`,
  schemaTxtUrl: (table?: string) =>
    `/api/tables/schema.txt${table ? `?table=${encodeURIComponent(table)}` : ""}`,
};

// ---- Free-text query over the semantic model (/api/nl) ----
// A Hebrew question is compiled SERVER-SIDE into SQL: a language model may pick
// the dataset and fields, but only out of a declared model, and its output is
// validated before any SQL exists. So the SQL below was written by the server,
// not by a model — which is why it is always shown to the user.
export interface NlExample {
  question: string;
  table: string;
}
export interface NlQueryResponse {
  answered: boolean;
  // present when answered
  sql?: string;
  query?: Record<string, unknown>;
  entity?: string;
  explanation?: string;
  // "template" (deterministic, free) | "deepseek" | "anthropic"
  source?: string;
  // The exact model id that answered, when a model did.
  model?: string;
  // True when the cheap tier was tried first and could not answer.
  escalated?: boolean;
  cached?: boolean;
  result?: KnessetDbSqlResult;
  error?: string;
  // present when not answered — this is the semantic layer's designed failure
  // mode, not an exception, so it arrives with HTTP 200.
  reason?: string;
  candidates?: Array<{ table: string; title: string; rows: number | null; page_url: string }>;
}
// ── Guided explorer (no model, no cost) ──
// Same retrieval as the retired autopilot, in the role it measures well at:
// top-1 94%, top-5 100% on the gold set. The person picks, so "include the
// right one" replaces "be right".
export interface NlSuggestion {
  table: string;
  schema: string;
  title: string;
  summary: string;
  rows: number | null;
  score: number;
  matched: { title: string[]; summary: string[]; columns: string[]; values: string[] };
  why: string;
  can_join: boolean;
  // Official publication (or a scrape of one) vs contributed / OVER-processed
  // data such as מידע לעם. Official sources rank ahead of processed ones and
  // are badged, because for the same question they are not interchangeable.
  official: boolean;
  source_type: string;
  organization: string;
  page_url: string;
  source_url: string;
  // Matched only by a shared Hebrew word-prefix (שמאויות ~ שמאות). A guess —
  // labelled as one, because a guess shown as a match is how the previous
  // version went wrong.
  approximate?: boolean;
}
export interface NlJoinable {
  table: string;
  schema: string;
  title: string;
  rows: number | null;
  via: string;
  official: boolean;
}
export const nlExplore = {
  // Two dataset keys -> the fan-trap-safe cross SQL (each side pre-aggregated
  // to the canonical settlement code, FULL OUTER join). ok:false + reason when
  // the pair has no join key.
  cross: (left: string, right: string) =>
    request<{ ok: boolean; sql?: string; explanation?: string; reason?: string }>(
      "/nl/cross", { method: "POST", body: JSON.stringify({ left, right }) }),
  suggest: (q: string, limit = 8) =>
    request<{ query: string; suggest_id: number | null; total_entities: number;
              suggestions: NlSuggestion[] }>(
      "/nl/suggest", { method: "POST", body: JSON.stringify({ q, limit }) }),
  // Fire-and-forget: which suggestion was chosen, at what rank. Every pick is a
  // labelled example — the ground truth the hand-written benchmark cannot be.
  picked: (suggest_id: number, table: string, rank: number, approximate = false) =>
    request<{ ok: boolean }>("/nl/picked", {
      method: "POST",
      body: JSON.stringify({ suggest_id, table, rank, approximate }),
    }).catch(() => undefined),
  joinable: (table: string, q = "") =>
    request<{ table: string; joinable: NlJoinable[]; reason?: string }>(
      `/nl/joinable/${encodeURIComponent(table)}${q ? `?q=${encodeURIComponent(q)}` : ""}`),
};

export const nlQuery = {
  // `run: false` compiles without executing — the console then runs the SQL
  // through its normal path, so the result table, charts and CSV export all
  // work unchanged and the query is not executed twice.
  query: (q: string, run = true) =>
    request<NlQueryResponse>("/nl/query", {
      method: "POST",
      body: JSON.stringify({ q, run }),
    }),
  examples: () =>
    request<{
      examples: NlExample[];
      model_size: number;
      enabled: boolean;
      llm: boolean;
      // The escalation ladder as configured on this deployment, cheapest first.
      tiers: Array<{ provider: string; model: string }>;
    }>("/nl/examples"),
};

// ── Free-text query admin (requires an OVER admin JWT) — /api/admin/nl ──
// The log holds raw user questions, so every endpoint here is admin-only and
// the data lives in the app DB, never the publicly-queryable append DB.
export interface NlAdminLogRow {
  id: number;
  created_at: string;
  question: string;
  answered: boolean;
  // cache | template | deepseek | anthropic | refused | invalid | error
  stage: string;
  attempts: string | null;
  model: string | null;
  escalated: boolean;
  entity: string | null;
  sql: string | null;
  reason: string | null;
  input_tokens: number;
  output_tokens: number;
  duration_ms: number | null;
}
export interface NlAdminStats {
  days: number;
  total: number;
  free_share: number | null;
  answered_share: number | null;
  // Calls that consumed tokens — includes refusals and unusable output, which
  // the per-stage buckets alone would not count as paid.
  paid: number;
  // Of those, the ones that produced nothing usable. This is the number that
  // says whether the cheap model is good enough.
  wasted: number;
  wasted_share: number | null;
  by_stage: Array<{
    stage: string; n: number; answered: number; escalated: number;
    input_tokens: number; output_tokens: number; median_ms: number | null;
  }>;
  budget_today: {
    calls: number; input_tokens: number; output_tokens: number;
    call_budget: number; output_token_budget: number; enabled: boolean;
  };
}
export interface NlAdminConfig {
  config: {
    enabled: boolean;
    allow_deepseek: boolean;
    allow_anthropic: boolean;
    escalate_on_unanswerable: boolean;
    daily_call_budget: number | null;
    daily_output_token_budget: number | null;
  };
  active_tiers: Array<{ provider: string; model: string }>;
  keys: { deepseek: boolean; anthropic: boolean };
  defaults: { daily_call_budget: number; daily_output_token_budget: number };
}
export interface NlSuggestLogRow {
  id: number;
  created_at: string;
  query: string;
  suggestions_count: number;
  approximate_count: number;
  top_table: string | null;
  picked_table: string | null;
  picked_rank: number | null;
  picked_approximate: boolean | null;
  picked_at: string | null;
}
export const adminNlQuery = {
  // The explorer's click-through log. `totals` gives recall-in-the-wild;
  // `synonym_candidates` are approximate suggestions users then picked — the
  // scorer admitting it guessed and the user confirming the guess.
  suggestLog: (limit = 100) =>
    request<{
      rows: NlSuggestLogRow[];
      totals: { searches: number; picked: number; picked_at_1: number; empty: number };
      synonym_candidates: Array<{ query: string; picked_table: string; n: number }>;
    }>(`/admin/nl/suggest-log?limit=${limit}`),
  adoptSynonym: (word: string, table: string) =>
    request<{ synonyms: Array<{ word: string; table_key: string; created_at: string }> }>(
      "/admin/nl/synonyms", { method: "POST", body: JSON.stringify({ word, table }) }),
  log: (p: { limit?: number; offset?: number; stage?: string; answered?: boolean } = {}) => {
    const qs = new URLSearchParams();
    if (p.limit) qs.set("limit", String(p.limit));
    if (p.offset) qs.set("offset", String(p.offset));
    if (p.stage) qs.set("stage", p.stage);
    if (p.answered !== undefined) qs.set("answered", String(p.answered));
    return request<{ total: number; rows: NlAdminLogRow[] }>(
      `/admin/nl/log${qs.toString() ? `?${qs}` : ""}`);
  },
  stats: (days = 7) => request<NlAdminStats>(`/admin/nl/stats?days=${days}`),
  config: () => request<NlAdminConfig>("/admin/nl/config"),
  setConfig: (patch: Record<string, unknown>) =>
    request<NlAdminConfig>("/admin/nl/config", {
      method: "POST", body: JSON.stringify(patch),
    }),
  prune: (days = 90) =>
    request<{ deleted: number; older_than_days: number }>(
      `/admin/nl/prune?days=${days}`, { method: "POST" }),
};

// ---- Settlement / authority normalizer ----
export interface ResolvedName {
  input: string;
  official: string | null;
  code: number | null;
  entity: "settlement" | "authority" | null;
  matched: boolean;
}
export const settlements = {
  // Resolve a pasted list of locality names to their official names. Stateless
  // and read-only on the server (not stored); safe, parameterized lookup.
  resolveBatch: (names: string[]) =>
    request<{ total: number; matched: number; results: ResolvedName[] }>(
      "/settlements/resolve-batch",
      { method: "POST", body: JSON.stringify({ names }) },
    ),
};

// ---- Knesset committee-protocol search (over the Neon `knesset` schema) ----
export interface ProtocolRow {
  document_id: number;
  document_name?: string | null;
  application?: string | null;
  file_url?: string | null;
  last_updated?: string | null;
  session_id?: number | null;
  session_number?: number | null;
  session_date?: string | null;
  session_location?: string | null;
  session_note?: string | null;
  knesset_num?: number | null;
  committee_id?: number | null;
  committee_name?: string | null;
  committee_type?: string | null;
}
export interface ProtocolSearchResult {
  total: number;
  limit: number;
  offset: number;
  rows: ProtocolRow[];
}
export interface ProtocolCommittee {
  name: string;
  committee_type?: string | null;
  doc_count: number;
}
function _qs(params: Record<string, unknown>): string {
  const q = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== "") q.set(k, String(v));
  }
  const s = q.toString();
  return s ? `?${s}` : "";
}
export const knessetProtocols = {
  knessets: () =>
    request<{ knessets: { knesset: number; doc_count: number }[] }>(
      "/knesset-protocols/knessets",
    ),
  committees: (params: { knesset?: number; q?: string; limit?: number } = {}) =>
    request<{ committees: ProtocolCommittee[] }>(
      `/knesset-protocols/committees${_qs(params)}`,
    ),
  search: (params: {
    q?: string;
    knesset?: number;
    committee_id?: number;
    committee?: string;
    limit?: number;
    offset?: number;
  }) => request<ProtocolSearchResult>(`/knesset-protocols/search${_qs(params)}`),
  // Deep/slow full-text search inside protocol bodies via TAG-IT (scope 15).
  // Reuses the shared MmmDeep* shape. Lives under /knesset-db (the TAG-IT path).
  deepSearch: (p: { q: string; page?: number; size?: number }) => {
    const qs = new URLSearchParams({ q: p.q });
    if (p.page) qs.set("page", String(p.page));
    if (p.size) qs.set("size", String(p.size));
    return request<MmmDeepResult>(`/knesset-db/protocols/deep-search?${qs.toString()}`);
  },
};

// Site-wide totals for the home hero. Any field is null when that total could
// not be computed (see app/services/site_stats.py — each half fails soft).
export interface SiteStats {
  tables: number | null;
  rows: number | null;
  files: number | null;
}

// One file of a CKAN collection, and whether OVER already holds it.
//   collected — an active dataset archives it; not selectable, link instead
//   pending   — a request for it is in the approval queue
//   free      — nobody holds it; this is what "can be added" means
export interface CkanCoverageResource {
  id: string;
  name: string;
  format: string | null;
  last_modified: string | null;
  size: number | null;
  datastore_active: boolean;
  state: "collected" | "pending" | "free";
  selectable: boolean;
  dataset_id?: string;
  dataset_title?: string;
}

export interface CkanCoverage {
  ckan_id: string;
  ckan_name: string;
  title: string;
  total: number;
  collected: number;
  pending: number;
  free: number;
  resources: CkanCoverageResource[];
}

// Public API (no auth required)
export const publicApi = {
  datasets: () => request<TrackedDataset[]>("/datasets"),
  siteStats: () => request<SiteStats>("/stats"),
  dataset: (id: string) => request<TrackedDataset>(`/datasets/public/${id}`),
  // What of a data.gov.il collection this site already archives, per file.
  // Asked BEFORE the picker is shown, so nobody ticks a file the submit would
  // then refuse as a duplicate.
  ckanCoverage: (ckanId: string) =>
    request<CkanCoverage>(`/datasets/ckan-coverage/${encodeURIComponent(ckanId)}`),
  request: (data: {
    ckan_id: string;
    resource_id?: string;
    resource_ids?: string[];
    // One independent dataset per picked file (own cadence + own SQL table)
    // instead of a single dataset mirroring them together.
    split_resources?: boolean;
    preferred_interval?: number;
    requester_name?: string;
    requester_notes?: string;
    requester_contact?: string;
  }) =>
    request<{
      message: string;
      // Only split_resources requests carry a per-file breakdown.
      status?: string;
      created?: number;
      // Pending combined requests this submit replaced with per-file ones.
      superseded?: number;
      results?: Array<{
        resource_id: string;
        name: string;
        status: "pending" | "duplicate";
        // On "duplicate": the dataset already holding this file.
        dataset_id?: string;
        dataset_title?: string;
        dataset_status?: string;
      }>;
    }>("/datasets/requests", {
      method: "POST",
      body: JSON.stringify(data),
    }),
  requestScraper: (data: {
    source_url: string;
    title: string;
    preferred_interval?: number;
    requester_name?: string;
    requester_notes?: string;
    requester_contact?: string;
    // Registered sources with a file picker: what was ticked. Server-relative
    // paths normally; absolute file URLs when split_files is set, since each
    // then has to be classified by the registry on its own. Omitted means
    // "whatever the page itself lists".
    selected_files?: string[];
    // One dataset per ticked file instead of one dataset holding them all.
    split_files?: boolean;
    // url → the label the picker showed. The server can only title a file from
    // its URL, and a filename is not what the site calls the table.
    file_titles?: Record<string, string>;
  }) =>
    request<{
      message: string;
      // Present only for split_files: one entry per picked file.
      created?: number;
      results?: Array<{
        url: string;
        status: "pending" | "duplicate" | "invalid";
        name?: string;
        error?: string;
        dataset_id?: string;
        dataset_title?: string;
      }>;
    }>("/datasets/requests", {
      method: "POST",
      body: JSON.stringify({ source_type: "scraper", ...data }),
    }),
  requestGovmap: (data: {
    source_urls: string[];
    title?: string;
    preferred_interval?: number;
    requester_name?: string;
    requester_notes?: string;
    requester_contact?: string;
  }) =>
    request<{
      message: string;
      status: string;
      results: Array<{
        url: string;
        status: "pending" | "duplicate" | "invalid";
        layer_id?: string;
        error?: string;
        // Present on "duplicate": the dataset that already tracks this layer,
        // so the form can link straight to it.
        dataset_id?: string;
        dataset_title?: string;
      }>;
    }>("/datasets/requests", {
      method: "POST",
      body: JSON.stringify({ source_type: "govmap", ...data }),
    }),
};

// Admin
export interface PendingRequest {
  id: string;
  ckan_id: string;
  ckan_name: string;
  title: string;
  organization: string | null;
  organization_id: string | null;
  organization_title: string | null;
  poll_interval: number;
  status: string;
  created_at: string;
  requester_email: string;
  requester_name: string;
  source_type: string;
  source_url: string | null;
  // Always sent (GET /pending sets it on every row): the plan approve will
  // apply if the admin does not override it. Required, so the approval select
  // cannot fall back to a guess of its own.
  storage_target: StorageTarget;
  neon_eligible?: boolean;
  resource_ids?: string[] | null;
  resource_id?: string | null;
}

export interface ScrapeQueueRunning {
  task_id: string;
  dataset_id: string;
  dataset_title: string;
  /** Claim band — see PRIORITY_* in app/models/scrape_task.py. */
  priority: number;
  phase: string | null;
  progress: number;
  message: string | null;
  worker_ip: string | null;
  worker_id: string | null;
  created_at: string | null;
  /** Last heartbeat. The liveness signal — task age is not one. */
  updated_at: string | null;
}

export interface ScrapeQueuePending {
  task_id: string;
  dataset_id: string;
  dataset_title: string;
  /** Claim band. `pending` arrives already sorted by priority, then age. */
  priority: number;
  worker_ip: string | null;
  worker_id: string | null;
  created_at: string | null;
}

export interface ScrapeQueueFailed {
  task_id: string;
  dataset_id: string;
  dataset_title: string;
  phase: string | null;
  error: string | null;
  worker_ip: string | null;
  worker_id: string | null;
  completed_at: string | null;
}

/** What a worker is holding right now, if anything. */
export interface WorkerCurrentTask {
  task_id: string;
  dataset_id: string;
  dataset_title: string;
  phase: string | null;
  progress: number;
  /** Which process on the machine holds it (`<hostname>#<token>`). */
  worker_instance: string | null;
  started_at: string | null;
  last_report_at: string | null;
}

/** One machine in the scraping fleet, as it last reported itself. */
export interface WorkerNode {
  /**
   * The MACHINE — hostname, or "ip:<addr>" for workers too old to send an id.
   * Deliberately not the full `<hostname>#<token>` the worker sends: that
   * token is per-process, so keying on it made every restart a new row.
   */
  worker_key: string;
  /** Last instance seen, token included — a changed token means a restart. */
  worker_id: string | null;
  worker_ip: string | null;
  worker_version: string | null;
  /** The worker's own verdict on its code: "current" | "behind" | "unknown". */
  worker_upstream: string | null;
  last_seen_at: string | null;
  /** No poll for over 10 minutes — presumed down or mid-restart. */
  offline: boolean;
  /** Paused: handed no new tasks. Does NOT stop the task it already holds. */
  paused: boolean;
  paused_at: string | null;
  paused_by: string | null;
  /** Everything the machine holds — several processes can share one box. */
  current_tasks: WorkerCurrentTask[];
  /** Paused AND holding nothing — the signal that it is safe to restart. */
  drained: boolean;
}

/**
 * One upstream source, with its live load and its worker cap.
 * `source_key` is derived server-side from dataset columns (a scraper ckan_id
 * prefix like "munidata", or a source_type like "govmap") — see
 * app/services/source_load.py.
 */
export interface SourceLoadRow {
  source_key: string;
  datasets: number;
  active_datasets: number;
  /** Workers on this source right now. One worker runs one task. */
  running: number;
  pending: number;
  /** null = uncapped. 0 = hand this source no new work at all. */
  max_workers: number | null;
}

export interface PromoteTaskResult {
  status: "promoted" | "restored";
  priority: number;
  /** Pending tasks still claimed before this one — 0 when it is genuinely next. */
  ahead: number;
  /** Tasks occupying a worker right now. Promotion cannot preempt these. */
  running: number;
}

export interface ScrapeQueueResponse {
  running: ScrapeQueueRunning[];
  pending: ScrapeQueuePending[];
  /** Real scrape failures only. */
  failed: ScrapeQueueFailed[];
  /** Runs cut short — the worker stopped reporting (closed, machine slept).
   *  Same row shape as `failed`; kept apart so a closed laptop is never read
   *  as a defect in the scraper. Older builds omit it. */
  interrupted?: ScrapeQueueFailed[];
}

/**
 * GovMap coverage rollout, plus the engine-epoch backfill: the whole-catalog
 * re-scrape triggered when the scraper starts capturing materially more per
 * layer. `crossed` counts layers ATTEMPTED under the current engine, which is
 * why `failed` is reported separately rather than netted out.
 */
export interface GovmapCoverageStatus {
  total_layers: number;
  ever_triggered: number;
  not_yet_triggered: number;
  datasets_created: number;
  backfill:
    | { active: false }
    | {
        active: true;
        epoch: string;
        crossed: number;
        failed: number;
        remaining: number;
        in_flight: number;
      };
}

export interface ScheduledJobRow {
  dataset_id: string;
  title: string;
  source_type: string;
  poll_interval: number;
  last_polled_at: string | null;
  next_run_at: string | null;
  seconds_until_next_run: number | null;
  scheduled: boolean;
}

export interface ScheduledJobsResponse {
  scheduler_running: boolean;
  now: string;
  jobs: ScheduledJobRow[];
  orphan_jobs: { job_id: string; next_run_at: string }[];
}

export interface DatasetSizeVersion {
  version_id: string;
  version_number: number;
  total_bytes: number;
  type?: string | null;
}

export interface DatasetSizeRow {
  dataset_id: string;
  title: string;
  total_bytes: number;
  version_count: number;
  // Absent when fetched with `summary` (the admin datasets tab).
  versions?: DatasetSizeVersion[];
  latest_version_type?: string | null;
  suggest_delta_archive?: boolean;
}

export interface DatasetSizesResponse {
  datasets: DatasetSizeRow[];
}

// One row in the durable datastore-ingest queue (Render-recycle-safe
// replacement for FastAPI BackgroundTasks). See
// app/worker/datastore_push_runner.py.
export interface DatastorePushJob {
  id: string;
  tracked_dataset_id: string | null;
  tracked_dataset_title: string | null;
  resource_id: string;
  csv_path: string;
  csv_is_gzipped_in_source: boolean;
  status: "pending" | "running" | "success" | "failed";
  attempts: number;
  rows_pushed: number;
  total_rows: number | null;
  error: string | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  updated_at: string;
}

export function formatBytes(n: number | null | undefined): string {
  const v = Number(n) || 0;
  if (v <= 0) return "—";
  if (v < 1024) return `${v} B`;
  if (v < 1024 * 1024) return `${(v / 1024).toFixed(1)} KB`;
  if (v < 1024 * 1024 * 1024) return `${(v / 1024 / 1024).toFixed(1)} MB`;
  return `${(v / 1024 / 1024 / 1024).toFixed(2)} GB`;
}

export interface CoverageDataset {
  id: string;
  title: string;
  source_type: string;
  storage_target: StorageTarget;
  version_count: number;
  reason: string;
}

export interface CoverageReport {
  total_active: number;
  covered: number;
  missing: CoverageDataset[];
  local_only: CoverageDataset[];
}

// MCP closed-beta invited user (api_users row).
export interface McpUser {
  id: string;
  email: string;
  name: string | null;
  tier: string;
  is_active: boolean;
  monthly_quota: number | null;
  last_seen_at: string | null;
  created_at: string;
  calls_30d: number;
}

// Activity-log event types (mirror app/models/activity_log.py).
export type ActivityEvent =
  | "requested"
  | "approved"
  | "rejected"
  | "queued"
  | "started"
  | "completed"
  | "failed";

export interface ActivityLogEntry {
  id: string;
  tracked_dataset_id: string | null;
  dataset_title: string | null;
  source_type: string | null;
  event: ActivityEvent | string;
  status: "ok" | "error" | "info" | string;
  message: string | null;
  detail: string | null;
  actor: string | null;
  created_at: string;
}

export interface ActivityLogPage {
  entries: ActivityLogEntry[];
  total: number;
  limit: number;
  offset: number;
}

// One page of the admin active-datasets list (GET /admin/datasets).
export interface AdminDatasetsPage {
  items: TrackedDataset[];
  total: number;
  limit: number;
  offset: number;
}

/** The datasets tab's filter state — the same shape drives the list and the
 *  facet counts, so the two can never ask different questions. */
export interface AdminDatasetFilters {
  q?: string;
  /** Storage TARGET ("r2", "r2+neon", "local", …). */
  storage?: string;
  /** Storage MODE ("append_only" | "full_snapshot") — a separate axis from
   *  the target, so "append-only on NEON" is expressible. */
  mode?: string;
  /** Upstream site key ("govmap", "munidata", "mankal", … — source_key). */
  source?: string;
  /** "only" = just the datasets whose source the publisher removed; "exclude" = hide them. */
  source_gone?: string;
  /** "only" = just the datasets suspected of a faulty import; "exclude" = hide them. */
  import_warning?: string;
}

export interface AdminDatasetFacet {
  value: string;
  count: number;
}

/** Option lists for the datasets tab's filters, counted against the catalog.
 *  Each list is counted with its OWN filter lifted, so a count says what
 *  picking that option would actually yield. */
export interface AdminDatasetFacets {
  total: number;
  sources: AdminDatasetFacet[];
  storage_targets: AdminDatasetFacet[];
  storage_modes: AdminDatasetFacet[];
  source_gone: number;
  import_warning: number;
  /** {dimension: "ErrorType: message"} for any facet that could not be
   *  computed. The other dimensions still arrive — one dead facet is not a
   *  dead filter bar. */
  errors?: Record<string, string>;
}

function adminDatasetParams(opts: AdminDatasetFilters): URLSearchParams {
  const p = new URLSearchParams();
  if (opts.q) p.set("q", opts.q);
  if (opts.storage) p.set("storage", opts.storage);
  if (opts.mode) p.set("mode", opts.mode);
  if (opts.source) p.set("source", opts.source);
  if (opts.source_gone) p.set("source_gone", opts.source_gone);
  if (opts.import_warning) p.set("import_warning", opts.import_warning);
  return p;
}

export interface OdataImport {
  resource_id: string;
  table: string;
  dataset_name?: string | null;
  title?: string | null;
  organization?: string | null;
  format?: string | null;
  source_url?: string | null;
  source_file_url?: string | null;
  rows?: number | null;
  columns?: number | null;
  imported_at?: string | null;
}

/** A background odata import (parse + load runs for minutes on big files, so
 *  the upload returns this and the UI polls until it settles). */
export interface OdataImportJob {
  id: string;
  resource_id: string;
  title?: string | null;
  state: "running" | "done" | "error";
  rows?: number | null;
  columns?: number | null;
  table?: string | null;
  error?: string | null;
  elapsed?: number | null;
}

export const admin = {
  pending: () => request<PendingRequest[]>("/admin/pending"),
  // One page of active datasets for the admin "מאגרים פעילים" tab. The tab used
  // to pull the entire catalog (~1,100 rows / 1MB) from the public /datasets on
  // every admin page load; filtering + slicing now happen in SQL.
  datasetsPage: (opts: AdminDatasetFilters & { limit?: number; offset?: number } = {}) => {
    const p = adminDatasetParams(opts);
    if (opts.limit != null) p.set("limit", String(opts.limit));
    if (opts.offset != null) p.set("offset", String(opts.offset));
    const qs = p.toString();
    return request<AdminDatasetsPage>(`/admin/datasets${qs ? `?${qs}` : ""}`);
  },
  // The filter dropdowns' own contents: every source / storage plan actually
  // present in the catalog, counted. The tab used to hardcode four source
  // options for ~20 real upstream sites, so most of them were unfilterable.
  datasetFacets: (opts: AdminDatasetFilters = {}) => {
    const qs = adminDatasetParams(opts).toString();
    return request<AdminDatasetFacets>(`/admin/dataset-facets${qs ? `?${qs}` : ""}`);
  },
  // מידע לעם (odata) → queryable SQL tables (admin-curated import)
  odataImports: () =>
    request<{ imports: OdataImport[]; count: number }>("/admin/odata/imports"),
  odataImport: (resource_id: string) =>
    request<OdataImport>("/admin/odata/import", {
      method: "POST",
      body: JSON.stringify({ resource_id }),
    }),
  // Client-side path: the browser fetched the file (Cloudflare blocks our
  // datacenter IP) and uploads the bytes here. `fd` carries file + metadata.
  // Returns a JOB — the parse+load runs in the background, poll it below.
  odataImportFile: (fd: FormData) =>
    request<OdataImportJob>("/admin/odata/import-file", { method: "POST", body: fd }),
  // Big-file path. over.org.il is behind Cloudflare (~100MB body ceiling), so a
  // 413MB CSV cannot be POSTed in one piece. The browser slices it and sends the
  // slices HERE — same origin, so unlike a presigned PUT straight to R2 there is
  // no bucket CORS policy to depend on.
  odataUploadBegin: () =>
    request<{ upload_id: string; chunk_size: number }>(
      "/admin/odata/upload/begin", { method: "POST" }),
  odataUploadChunk: (fd: FormData) =>
    request<{ received: number; total: number }>(
      "/admin/odata/upload/chunk", { method: "POST", body: fd }),
  odataImportStaged: (fd: FormData) =>
    request<OdataImportJob>("/admin/odata/import-staged", { method: "POST", body: fd }),
  odataImportJob: (job_id: string) =>
    request<OdataImportJob>(
      `/admin/odata/import-jobs/${encodeURIComponent(job_id)}`,
    ),
  odataDeleteImport: (resource_id: string) =>
    request<{ deleted: boolean }>(
      `/admin/odata/imports/${encodeURIComponent(resource_id)}`,
      { method: "DELETE" },
    ),
  activityLog: (opts: { dataset_id?: string; event?: string; status?: string; q?: string; limit?: number; offset?: number } = {}) => {
    const p = new URLSearchParams();
    if (opts.dataset_id) p.set("dataset_id", opts.dataset_id);
    if (opts.event) p.set("event", opts.event);
    if (opts.status) p.set("status", opts.status);
    if (opts.q) p.set("q", opts.q);
    if (opts.limit != null) p.set("limit", String(opts.limit));
    if (opts.offset != null) p.set("offset", String(opts.offset));
    const qs = p.toString();
    return request<ActivityLogPage>(`/admin/activity-log${qs ? `?${qs}` : ""}`);
  },
  approve: (id: string, poll_interval?: number, title?: string, organization_id?: string, resource_ids?: string[], storage_target?: StorageTarget) =>
    request<void>(`/admin/approve/${id}`, {
      method: "POST",
      body: JSON.stringify({ poll_interval, title, organization_id, resource_ids, storage_target }),
    }),
  reject: (id: string) => request<void>(`/admin/reject/${id}`, { method: "POST" }),
  overCoverage: () => request<CoverageReport>("/admin/over-coverage"),
  overCoverageFix: () => request<CoverageReport>("/admin/over-coverage/fix", { method: "POST" }),
  mcpUsers: () => request<McpUser[]>("/admin/mcp-users"),
  mcpInvite: (email: string, name?: string, tier?: string) =>
    request<McpUser>("/admin/mcp-users", { method: "POST", body: JSON.stringify({ email, name, tier: tier || "beta" }) }),
  mcpUpdateUser: (id: string, data: { tier?: string; is_active?: boolean; monthly_quota?: number | null }) =>
    request<McpUser>(`/admin/mcp-users/${id}`, { method: "PATCH", body: JSON.stringify(data) }),
  mcpDisableUser: (id: string) => request<void>(`/admin/mcp-users/${id}`, { method: "DELETE" }),
  scrapeTasks: () => request<ScrapeQueueResponse>("/admin/scrape-tasks"),
  govmapCoverageStatus: () =>
    request<GovmapCoverageStatus>("/admin/govmap-coverage/status"),
  cancelScrapeTask: (taskId: string) =>
    request<{ status: string; was: string }>(`/admin/scrape-tasks/${taskId}`, {
      method: "DELETE",
    }),
  /**
   * Move one pending task to the head of the queue, or (promote=false) put it
   * back in the routine band. Only affects the CLAIM order — a task already
   * running keeps its worker, which is why the response reports `running`.
   */
  workers: () => request<{ workers: WorkerNode[] }>("/admin/workers"),
  /**
   * Drain one worker (or put it back). Takes effect on its NEXT poll — the task
   * it is running now finishes untouched. Watch `drained` for "safe to restart".
   */
  pauseWorker: (workerKey: string, paused: boolean) =>
    request<{ worker_key: string; paused: boolean }>(
      `/admin/workers/${encodeURIComponent(workerKey)}/pause`,
      { method: "PUT", body: JSON.stringify({ paused }) },
    ),
  forgetWorker: (workerKey: string) =>
    request<{ worker_key: string; status: string }>(
      `/admin/workers/${encodeURIComponent(workerKey)}`,
      { method: "DELETE" },
    ),
  sourceLimits: () => request<{ sources: SourceLoadRow[] }>("/admin/source-limits"),
  /**
   * Cap concurrent workers on one source. `null` removes the cap; 0 stops new
   * work for it. Applies to the next CLAIM — running scrapes are never aborted.
   */
  setSourceLimit: (sourceKey: string, maxWorkers: number | null) =>
    request<{ source_key: string; max_workers: number | null }>(
      `/admin/source-limits/${encodeURIComponent(sourceKey)}`,
      { method: "PUT", body: JSON.stringify({ max_workers: maxWorkers }) },
    ),
  promoteScrapeTask: (taskId: string, promote = true) =>
    request<PromoteTaskResult>(`/admin/scrape-tasks/${taskId}/promote`, {
      method: "POST",
      body: JSON.stringify({ promote }),
    }),
  scheduledJobs: () => request<ScheduledJobsResponse>("/admin/scheduled-jobs"),
  // `summary` drops the per-version breakdown (~10,800 rows catalog-wide) —
  // the admin datasets tab only shows the per-dataset total.
  datasetSizes: (summary = false) =>
    request<DatasetSizesResponse>(
      `/admin/dataset-sizes${summary ? "?summary=1" : ""}`,
    ),
  datastoreJobs: (status?: string) =>
    request<DatastorePushJob[]>(
      status
        ? `/admin/datastore-jobs?status=${encodeURIComponent(status)}`
        : "/admin/datastore-jobs",
    ),
  retryDatastoreJob: (id: string) =>
    request<{ status: string; id: string }>(
      `/admin/datastore-jobs/${id}/retry`,
      { method: "POST" },
    ),
  syncOrganizations: () =>
    request<{ created: number; updated: number; total: number; linked_datasets: number }>(
      "/admin/organizations/sync",
      { method: "POST" }
    ),
  syncOrganizationsGovIl: (offices: Array<{
    url_name: string;
    title: string;
    logo_url: string | null;
    external_website: string | null;
    org_type: number | null;
    offices: string[];
    units: Array<{ url_name: string; title: string }>;
  }>) =>
    request<{
      created: number;
      matched: number;
      total: number;
      children_created: number;
      children_matched: number;
    }>(
      "/admin/organizations/sync-gov-il",
      {
        method: "POST",
        body: JSON.stringify({ offices }),
      }
    ),
  linkScraperDatasetsToOrgs: () =>
    request<{
      linked_by_office_id: number;
      linked_by_path: number;
      unlinked: number;
      total_scraper_datasets: number;
    }>("/admin/organizations/link-scrapers", { method: "POST" }),
  updateOrgParent: (orgId: string, parentId: string | null) =>
    request<Organization>(`/admin/organizations/${orgId}`, {
      method: "PATCH",
      body: JSON.stringify({ parent_id: parentId ?? "" }),
    }),
  deleteTag: (tagId: string) =>
    request<void>(`/admin/tags/${tagId}`, { method: "DELETE" }),
  setDatasetTags: (datasetId: string, tagIds: string[]) =>
    request<TrackedDataset>(`/admin/datasets/${datasetId}/tags`, {
      method: "PUT",
      body: JSON.stringify({ tag_ids: tagIds }),
    }),
};

// Editable text overrides for the static About / Rationale pages.
// `get` is public (merged over the bundled i18n defaults at runtime);
// `save`/`revert` are admin-only.
export type PageContentOverrides = Record<string, Record<string, string>>;

export const pageContent = {
  get: (page: string) => request<PageContentOverrides>(`/page-content/${page}`),
  save: (page: string, lang: string, key: string, value: string) =>
    request<{ ok: boolean }>(`/admin/page-content`, {
      method: "PUT",
      body: JSON.stringify({ page, lang, key, value }),
    }),
  revert: (page: string, lang: string, key: string) =>
    request<{ ok: boolean }>(
      `/admin/page-content?page=${encodeURIComponent(page)}&lang=${encodeURIComponent(
        lang,
      )}&key=${encodeURIComponent(key)}`,
      { method: "DELETE" },
    ),
};

// Government-decision analysis (/rationale/1933) — the decision's full text,
// the operative tasks extracted from each clause, and the per-task analysis.
// `list` and `get` are public but return only PUBLISHED analyses; the admin
// endpoints serve and save the draft.
export type DecisionTaskStatus = "done" | "partial" | "not_done" | "unknown";

export interface DecisionTask {
  id: string;
  title: string;
  obligation: string;
  responsible: string;
  due: string;
  status: DecisionTaskStatus;
  potential: string;
  actual: string;
  damage: string;
}

export interface DecisionSection {
  id: string;
  part: string;
  label: string;
  heading: string;
  text: string;
  tasks: DecisionTask[];
}

export interface DecisionDoc {
  key: string;
  title: string;
  subtitle: string;
  intro: string;
  decision_number: string;
  decision_date: string;
  decision_url: string;
  labels: Record<string, string>;
  sections: DecisionSection[];
}

export interface DecisionAnalysisView {
  key: string;
  published: boolean;
  doc: DecisionDoc;
  // Admin payload only.
  is_customized?: boolean;
  updated_by?: string | null;
  updated_at?: string | null;
}

export interface DecisionAnalysisSummary {
  key: string;
  title: string;
  subtitle: string;
}

export const decisionAnalysis = {
  list: () => request<DecisionAnalysisSummary[]>("/decision-analysis"),
  get: (key: string) => request<DecisionAnalysisView>(`/decision-analysis/${key}`),
  getDraft: (key: string) =>
    request<DecisionAnalysisView>(`/admin/decision-analysis/${key}`),
  save: (key: string, body: { doc?: DecisionDoc; published?: boolean }) =>
    request<{ ok: boolean }>(`/admin/decision-analysis/${key}`, {
      method: "PUT",
      body: JSON.stringify(body),
    }),
  revert: (key: string) =>
    request<{ ok: boolean }>(`/admin/decision-analysis/${key}`, { method: "DELETE" }),
};

// Tags API
export const tagsApi = {
  list: () => request<TagWithCount[]>("/tags"),
  get: (id: string) => request<TagDetail>(`/tags/${id}`),
  // On 409 (already exists) the backend returns the existing tag in
  // detail.tag — we adopt it silently so the picker can use the same flow
  // for "create" and "reuse".
  create: async (name: string, description?: string): Promise<Tag> => {
    const token = getToken();
    const headers: Record<string, string> = { "Content-Type": "application/json" };
    if (token) headers["Authorization"] = `Bearer ${token}`;
    const resp = await fetch(`${BASE}/tags`, {
      method: "POST",
      headers,
      body: JSON.stringify({ name, description }),
    });
    if (resp.status === 409) {
      const body = await resp.json().catch(() => null);
      const existing = body?.detail?.tag;
      if (existing && existing.id && existing.name) {
        return existing as Tag;
      }
      throw new Error(body?.detail?.message || "Tag already exists");
    }
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ detail: resp.statusText }));
      throw new Error(err.detail || resp.statusText);
    }
    return resp.json();
  },
};

// Organizations
export interface Organization {
  id: string;
  name: string;
  title: string;
  description: string | null;
  image_url: string | null;
  data_gov_il_id: string | null;
  gov_il_url_name: string | null;
  gov_il_logo_url: string | null;
  external_website: string | null;
  parent_id: string | null;
  parent_title: string | null;
  children_count: number;
  dataset_count: number;
}

export interface OrganizationDetail extends Organization {
  data_gov_il_slug: string | null;
  parent: { id: string; name: string; title: string } | null;
  children: {
    id: string;
    name: string;
    title: string;
    gov_il_logo_url: string | null;
    dataset_count: number;
  }[];
  datasets: {
    id: string;
    title: string;
    ckan_name: string;
    // See TagDataset.ckan_id — the source chip's only reliable signal.
    ckan_id: string | null;
    source_type: string;
    version_count: number;
    last_polled_at: string | null;
    // See TagDataset.last_modified — the data's own timestamp, not ours.
    last_modified: string | null;
    tags?: Tag[];
  }[];
}

export const organizations = {
  list: () => request<Organization[]>("/organizations"),
  get: (id: string) => request<OrganizationDetail>(`/organizations/${id}`),
};

// ── יומן לעם (Ocal), migrated into OVER — /api/ocal ──
export interface OcalEvent {
  id: string;
  source_id: string;
  title: string;
  start_time: string | null;
  end_time: string | null;
  location: string | null;
  participants: string | null;
  dataset_name?: string;
  dataset_link: string | null;
  event_date: string;
  source_name: string;
  source_color: string;
  source_reviewed?: boolean;
  match_count?: number | null;
  top_entities?: { name: string; type: string }[] | null;
  cross_ref_summary?: { confirmed: number; unconfirmed: number; total: number } | null;
}
export interface OcalPagination {
  page: number;
  per_page: number;
  total: number;
  total_pages: number;
}
export interface OcalSearchResponse {
  data: OcalEvent[];
  pagination: OcalPagination;
}
export interface OcalSource {
  id: string;
  name: string;
  color: string;
  is_enabled: boolean;
  total_events: number;
  first_event_date: string | null;
  last_event_date: string | null;
  dataset_url?: string | null;
  resource_url?: string | null;
  person_name?: string | null;
  organization_name?: string | null;
}
export interface OcalStats {
  total_events: number;
  total_sources: number;
  total_organizations: number;
}
export interface OcalEntity {
  entity_name: string;
  entity_type: string;
  entity_id: string | null;
  event_count: number;
}
export interface OcalCalendarResponse {
  events: OcalEvent[];
  date_range: { from: string; to: string };
  event_counts: Record<string, number>;
}

export interface OcalSearchParams {
  q?: string;
  from_date?: string;
  to_date?: string;
  source_ids?: string[];
  location?: string;
  participants?: string;
  cross_ref_status?: "confirmed" | "unconfirmed";
  page?: number;
  per_page?: number;
  sort?: "date_asc" | "date_desc" | "relevance";
}

function ocalQS(params: Record<string, unknown>): string {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v == null || v === "") continue;
    if (Array.isArray(v)) {
      if (v.length) p.set(k, v.join(","));
    } else {
      p.set(k, String(v));
    }
  }
  const s = p.toString();
  return s ? `?${s}` : "";
}

export const ocal = {
  events: (params: OcalSearchParams = {}) =>
    request<OcalSearchResponse>(`/ocal/events${ocalQS(params as Record<string, unknown>)}`),
  event: (id: string) => request<OcalEvent>(`/ocal/events/${id}`),
  calendar: (params: {
    date: string;
    view?: string;
    source_ids?: string[];
    entity_names?: string[];
    max_date?: string;
  }) =>
    request<OcalCalendarResponse>(
      `/ocal/calendar${ocalQS(params as Record<string, unknown>)}`,
    ),
  sources: () => request<{ data: OcalSource[] }>("/ocal/sources"),
  stats: () => request<OcalStats>("/ocal/stats"),
  entities: (
    params: { source_ids?: string[]; type?: string; from_date?: string; to_date?: string } = {},
  ) => request<{ data: OcalEntity[] }>(`/ocal/entities${ocalQS(params as Record<string, unknown>)}`),
  // Public GET endpoints — safe as plain <a href> (no auth needed).
  downloadSourceUrl: (
    id: string,
    opts: { format?: "csv" | "json"; from_date?: string; to_date?: string } = {},
  ) => `${BASE}/ocal/download/source/${id}${ocalQS(opts as Record<string, unknown>)}`,
  downloadBulk: async (
    source_ids: string[],
    format: "csv" | "json" = "csv",
    range?: { from_date?: string; to_date?: string },
  ): Promise<Blob> => {
    const resp = await fetch(`${BASE}/ocal/download/bulk`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ source_ids, format, ...range }),
    });
    if (!resp.ok) {
      const e = await resp.json().catch(() => ({ detail: resp.statusText }));
      throw new Error(e.detail || resp.statusText);
    }
    return resp.blob();
  },
};

// ── Ocal admin (all require an OVER admin JWT) — /api/admin/ocal ──
export interface OcalAdminSource {
  id: string;
  name: string;
  color: string;
  is_enabled: boolean;
  total_events: number;
  first_event_date: string | null;
  last_event_date: string | null;
  resource_id: string | null;
  dataset_url: string | null;
  sync_status: string;
  last_sync_at: string | null;
  reviewed_at: string | null;
  review_notes: string | null;
  person_name: string | null;
  organization_name: string | null;
  person_id: string | null;
  organization_id: string | null;
}
export interface OcalAdminPerson {
  id: string;
  name: string;
  wikipedia_link: string | null;
  notes: string | null;
  organization_id: string | null;
  organization_name: string | null;
  source_count: number;
}
export interface OcalAdminOrg {
  id: string;
  name: string;
  website: string | null;
  description: string | null;
}
export interface OcalEntity {
  entity_name: string;
  entity_type: string;
  event_count: number;
  matched: boolean;
}
export interface OcalAutomationSettings {
  auto_scan_enabled: boolean;
  interval_hours: number;
  confidence: number;
  min_rows: number;
  updated_at?: string | null;
}
export interface OcalAutoImportLog {
  id?: string;
  started_at: string;
  finished_at: string | null;
  trigger: string;
  candidates: number;
  imported: number;
  skipped: number;
  errors: number;
}
export interface OcalCandidate {
  resource_id: string;
  resource_name: string | null;
  format: string | null;
  dataset_title: string | null;
  organization: string | null;
  last_modified: string | null;
}
export interface OcalException {
  resource_id: string;
  dataset_title: string | null;
  resource_format: string | null;
  resource_name: string | null;
  exception_reason: string;
  moved_at: string;
}

function aqs(params: Record<string, unknown>): string {
  const p = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v == null || v === "") continue;
    p.set(k, String(v));
  }
  const s = p.toString();
  return s ? `?${s}` : "";
}

export interface OcalDashboard {
  counts: { events: number; sources: number; enabled_sources: number; people: number; organizations: number; entities: number; rejected: number };
  recent_sources: Array<{ id: string; name: string; color: string | null; total_events: number; is_enabled: boolean; created_at: string; person_name: string | null }>;
}

export const ocalAdmin = {
  dashboard: () => request<OcalDashboard>(`/admin/ocal/dashboard`),
  // sources
  sources: (params: { q?: string; enabled?: boolean; reviewed?: boolean; limit?: number; offset?: number } = {}) =>
    request<{ sources: OcalAdminSource[]; total: number }>(`/admin/ocal/sources${aqs(params)}`),
  patchSource: (id: string, body: Partial<Pick<OcalAdminSource, "name" | "is_enabled" | "person_id" | "organization_id" | "review_notes" | "color">>) =>
    request<{ updated: boolean }>(`/admin/ocal/sources/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
  reviewSource: (id: string) => request(`/admin/ocal/sources/${id}/review`, { method: "POST" }),
  unreviewSource: (id: string) => request(`/admin/ocal/sources/${id}/unreview`, { method: "POST" }),
  deleteSource: (id: string) => request<void>(`/admin/ocal/sources/${id}`, { method: "DELETE" }),
  reimportSource: (id: string, clear = false) =>
    request<Record<string, unknown>>(`/admin/ocal/sources/${id}/reimport${aqs({ clear })}`, { method: "POST" }),
  enrichSource: (id: string, ai = false) => request<Record<string, unknown>>(`/admin/ocal/sources/${id}/enrich${aqs({ ai })}`, { method: "POST" }),
  aiNerStatus: () => request<{ enabled: boolean; available: boolean; provider: string | null; auto: boolean; batch: number }>(`/admin/ocal/ai-ner/status`),
  deduplicateSource: (id: string) => request<{ deleted: number }>(`/admin/ocal/sources/${id}/deduplicate`, { method: "POST" }),
  findMatchesSource: (id: string) => request<{ created?: number; joined?: number }>(`/admin/ocal/sources/${id}/find-matches`, { method: "POST" }),
  // candidates / scan / import (in admin.py)
  candidates: (limit = 50) => request<{ candidates: OcalCandidate[]; count: number }>(`/admin/ocal/candidates${aqs({ limit })}`),
  scan: (max_import = 5) =>
    request<{ started: boolean; max_import: number; message: string }>(`/admin/ocal/scan${aqs({ max_import })}`, { method: "POST" }),
  // automation settings / logs / status
  automationSettings: () => request<OcalAutomationSettings>(`/admin/ocal/automation/settings`),
  updateAutomationSettings: (body: Partial<Pick<OcalAutomationSettings, "auto_scan_enabled" | "interval_hours" | "confidence" | "min_rows">>) =>
    request<OcalAutomationSettings>(`/admin/ocal/automation/settings`, { method: "PUT", body: JSON.stringify(body) }),
  automationLogs: (limit = 50) => request<{ logs: OcalAutoImportLog[] }>(`/admin/ocal/automation/logs${aqs({ limit })}`),
  automationStatus: () => request<{ settings: OcalAutomationSettings; scheduler_interval_hours: number; per_tick: number; scan_running: boolean; last_run: OcalAutoImportLog | null }>(`/admin/ocal/automation/status`),
  importOne: (resource_id: string) =>
    request<{ events_upserted: number; source_id: string; rows_parsed: number }>(`/admin/ocal/import`, { method: "POST", body: JSON.stringify({ resource_id }) }),
  // exceptions
  exceptions: (limit = 200) => request<{ exceptions: OcalException[]; count: number }>(`/admin/ocal/exceptions${aqs({ limit })}`),
  clearException: (resource_id: string) => request<void>(`/admin/ocal/exceptions/${resource_id}`, { method: "DELETE" }),
  // people
  people: (q?: string) => request<{ people: OcalAdminPerson[]; count: number }>(`/admin/ocal/people${aqs({ q })}`),
  createPerson: (body: { name: string; organization_id?: string; wikipedia_link?: string; notes?: string }) =>
    request<{ id: string }>(`/admin/ocal/people`, { method: "POST", body: JSON.stringify(body) }),
  patchPerson: (id: string, body: { name: string; organization_id?: string; wikipedia_link?: string; notes?: string }) =>
    request(`/admin/ocal/people/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
  deletePerson: (id: string) => request<void>(`/admin/ocal/people/${id}`, { method: "DELETE" }),
  mergePeople: (source_ids: string[], target_id: string) =>
    request<{ merged: number; target_id: string }>(`/admin/ocal/people/merge`, { method: "POST", body: JSON.stringify({ source_ids, target_id }) }),
  bulkImportPeople: (rows: Array<{ name: string; organization_name?: string; wikipedia_link?: string; notes?: string }>) =>
    request<{ created: number; updated: number; skipped: number }>(`/admin/ocal/people/bulk-import`, { method: "POST", body: JSON.stringify({ rows }) }),
  // organizations
  organizations: (q?: string) => request<{ organizations: OcalAdminOrg[]; count: number }>(`/admin/ocal/organizations${aqs({ q })}`),
  createOrg: (body: { name: string; website?: string; description?: string }) =>
    request<{ id: string }>(`/admin/ocal/organizations`, { method: "POST", body: JSON.stringify(body) }),
  patchOrg: (id: string, body: { name: string; website?: string; description?: string }) =>
    request(`/admin/ocal/organizations/${id}`, { method: "PATCH", body: JSON.stringify(body) }),
  deleteOrg: (id: string) => request<void>(`/admin/ocal/organizations/${id}`, { method: "DELETE" }),
  // content
  content: () => request<{ content: { key: string; value: string; updated_at: string }[] }>(`/admin/ocal/content`),
  putContent: (key: string, value: string) =>
    request(`/admin/ocal/content/${key}`, { method: "PUT", body: JSON.stringify({ value }) }),
  // entities (extracted event_entities — global curation)
  entities: (params: { type?: string; q?: string; limit?: number; offset?: number } = {}) =>
    request<{ entities: OcalEntity[]; total: number; stats: { total_unique: number; person_count: number; org_count: number; place_count: number } }>(`/admin/ocal/entities${aqs(params)}`),
  deleteEntityByName: (entity_name: string, entity_type: string) =>
    request<{ deleted: number }>(`/admin/ocal/entities/delete-by-name`, { method: "POST", body: JSON.stringify({ entity_name, entity_type }) }),
  renameEntity: (old_name: string, new_name: string, entity_type?: string) =>
    request<{ renamed: boolean }>(`/admin/ocal/entities/rename`, { method: "POST", body: JSON.stringify({ old_name, new_name, entity_type }) }),
  mergeEntities: (names: string[], target_name: string, entity_type?: string) =>
    request<{ merged: number }>(`/admin/ocal/entities/merge`, { method: "POST", body: JSON.stringify({ names, target_name, entity_type }) }),
};

// ── נדל"ן לעם (nadlan) — the property-level spatial crosswalk ───────────────
// One envelope for all four entry modes (see app/api/nadlan.py): whichever
// spatial identity you start from, the answer carries the property's identity
// in every other codespace plus a per-source block linking to that source's
// untouched full row on /data.

export interface NadlanSourceBlock {
  table: string;
  fields: Record<string, unknown>;
  console_sql: string;
  row_url: string;
}

export interface NadlanAddress {
  street: string | null;
  house: number | null;
  suffix: string | null;
  entrance: string | null;
  zip7: string | null;
  neighbourhood: string | null;
  lat: number | null;
  lon: number | null;
  match: string | null;
}

export interface NadlanProperty {
  parcel_key: string;
  identity: {
    gush: number;
    gush_suffix: number;
    helka: number;
    gp_key: string;
    settlement: { code: number | null; name: string | null };
    region: { reg_mun: string | null; county: string | null; region: string | null };
    point: { lat: number; lon: number } | null;
    distance_m: number | null;
    zip7: string[];
    zip5: string[];
    streets: string[];
    addresses: NadlanAddress[];
  };
  sources: {
    parcels: NadlanSourceBlock;
    gazetteer: NadlanSourceBlock;
    postal: NadlanSourceBlock;
    address_list: NadlanSourceBlock;
  };
  match: { method: string | null; confidence: string; notes: string[] };
  // The parcel polygon as a GeoJSON *string*, present when the request asked
  // for it (`geometry=true`). Rides the same envelope as the identity so a
  // property found by zip is as locatable on the map as one found by clicking.
  geometry: string | null;
}

export interface NadlanEnvelope {
  query: Record<string, unknown>;
  data: NadlanProperty[];
  count: number;
  processed: boolean;
  caveats: string[];
  addresses?: Record<string, unknown>[];
  alternatives?: { mode: string; parsed: Record<string, unknown> }[];
  hint?: string;
}

export interface NadlanStats {
  parcels: number;
  parcels_ambiguous: number;
  parcels_with_settlement: number;
  parcels_with_gazetteer: number;
  addresses: number;
  addresses_with_point: number;
  addresses_with_zip: number;
  addresses_with_address_zip: number;
  addresses_with_locality_zip: number;
  addresses_linked_pip: number;
  streets: number;
  streets_in_gazetteer: number;
  zip5_codes: number;
  localities_with_addresses: number;
  coverage: Record<string, number>;
}

export interface NadlanStreet {
  street_key: string;
  name: string;
  settlement_code: number;
  settlement_name: string | null;
}

export const nadlan = {
  stats: () => request<NadlanStats>("/nadlan/stats"),
  parcel: (gush: number, helka: number, suffix?: number, geometry = false) =>
    request<NadlanEnvelope>(
      `/nadlan/parcel/${gush}/${helka}?geometry=${geometry}` +
      (suffix != null ? `&suffix=${suffix}` : "")),
  geometry: (gush: number, helka: number, suffix = 0) =>
    request<{ geojson: string; legal_area: string | null; status_text: string | null }>(
      `/nadlan/parcel/${gush}/${helka}/geometry?suffix=${suffix}`),
  point: (lat: number, lon: number, radius_m = 0, limit = 50, geometry = false) =>
    request<NadlanEnvelope>(
      `/nadlan/point?lat=${lat}&lon=${lon}&radius_m=${radius_m}&limit=${limit}` +
      `&geometry=${geometry}`),
  zip: (zip: string, geometry = false) =>
    request<NadlanEnvelope>(
      `/nadlan/zip/${encodeURIComponent(zip)}?geometry=${geometry}`),
  address: (city: string, street: string, number?: string, geometry = false) =>
    request<NadlanEnvelope>(
      `/nadlan/address?city=${encodeURIComponent(city)}&street=${encodeURIComponent(street)}` +
      `&geometry=${geometry}` + (number ? `&number=${encodeURIComponent(number)}` : "")),
  streets: (q: string, settlement?: number, limit = 20) =>
    request<{ data: NadlanStreet[] }>(
      `/nadlan/streets?q=${encodeURIComponent(q)}` +
      (settlement != null ? `&settlement=${settlement}` : "") + `&limit=${limit}`),
  resolve: (q: string, radius_m = 0) =>
    request<NadlanEnvelope>(
      `/nadlan/resolve?q=${encodeURIComponent(q)}&radius_m=${radius_m}`),
};

// ── שאלות לעם — חיפוש רוחבי (cross-source deep search) ──────────────────────
// The page issues ONE request per source so each column paints as soon as it
// lands; `sources` therefore normally carries a single id.

export interface DeepCard {
  title: string;
  snippet: string;
  url: string | null;
  date: string | null;
  badges: string[];
}
export interface DeepFilter {
  id: string;
  label: string;
  type: "text" | "date" | "number" | "select";
  options?: { value: string; label: string }[];
}
export interface DeepSource {
  id: string;
  name: string;
  color: string;
  attribution: { text: string; href: string };
  server: string;
  local: string | null;
  public: boolean;
  configured: boolean;
  /** Someone else's corpus (TAG-IT, מפתח התקציב) — surfaced as a marker. */
  external: boolean;
  hint: string;
  filters: DeepFilter[] | null;
}
export interface DeepColumn {
  id: string;
  name: string;
  color: string;
  attribution: { text: string; href: string };
  server: string;
  configured: boolean;
  error: string | null;
  total: number;
  results: DeepCard[];
}

export const deepSearch = {
  sources: () => request<{ sources: DeepSource[] }>("/deep-search/sources"),
  search: (p: {
    q: string;
    sources?: string;
    limit?: number;
    filters?: Record<string, string>;
  }) => {
    // Per-source filters ride as f_<filterId>=… — the backend only reads the
    // ids that source declared, so an unknown one is simply ignored.
    const params: Record<string, unknown> = {
      q: p.q,
      sources: p.sources,
      limit: p.limit,
    };
    for (const [k, v] of Object.entries(p.filters || {})) params[`f_${k}`] = v;
    return request<{ query: string; sources: DeepColumn[] }>(
      `/deep-search/search${_qs(params)}`,
    );
  },
};

// ── ניגוד עניינים לעם (OCOI) ────────────────────────────────────────────────
// Backed by app/api/ocoi.py over the migrated corpus (Neon schema `ocoi`).
// OCOI's own {status, data, meta} envelope is preserved end to end, so these
// types mirror the documented public API rather than re-shaping it.

export type OcoiEntityType = "person" | "company" | "association" | "domain";

export interface OcoiEnvelope<T> {
  status: string;
  data: T;
  meta?: OcoiMeta;
}
export interface OcoiMeta {
  total: number;
  page: number;
  limit: number;
  pages: number;
  /** Set when the count hit the server's cap — render as "10,000+", not exact. */
  total_capped?: boolean;
}
export interface OcoiEntity {
  id: string;
  entity_type: OcoiEntityType;
  name_hebrew?: string | null;
  name_english?: string | null;
  title?: string | null;
  position?: string | null;
  ministry?: string | null;
  registration_number?: string | null;
  company_type?: string | null;
  status?: string | null;
  description?: string | null;
  aliases?: string[];
}
export interface OcoiSearchHit {
  id: string;
  name: string;
  entity_type: OcoiEntityType;
}
export interface OcoiDocument {
  id: string;
  title: string;
  file_url: string;
  file_format: string;
  file_size?: number | null;
  conversion_status?: string;
  extraction_status?: string;
  verified?: boolean;
  verified_at?: string | null;
  created_at?: string | null;
  source_title?: string | null;
  source_type?: string | null;
  source_url?: string | null;
  relationships_count?: number;
}
export interface OcoiGraphNode {
  id: string;
  entity_type: OcoiEntityType;
  name: string;
  position?: string | null;
  ministry?: string | null;
  registration_number?: string | null;
}
export interface OcoiGraphEdge {
  source_id: string;
  source_type: OcoiEntityType;
  target_id: string;
  target_type: OcoiEntityType;
  relationship_type: string;
  details?: string | null;
  origin_kind?: string | null;
  verified?: boolean | null;
  document_id?: string | null;
  document_title?: string | null;
  document_url?: string | null;
}
export interface OcoiGraph {
  nodes: OcoiGraphNode[];
  edges: OcoiGraphEdge[];
  /** The server capped the walk — the view is partial, say so in the UI. */
  truncated?: boolean;
}
export interface OcoiStats {
  documents: number;
  persons: number;
  companies: number;
  associations: number;
  domains: number;
  relationships: number;
}
export interface OcoiMinistry {
  ministry: string;
  person_count: number;
  connection_count: number;
}
export interface OcoiTopConnected extends OcoiGraphNode {
  connections: number;
}

/** Plural path segment for an entity type — the API lists live under these. */
export const OCOI_PATHS: Record<OcoiEntityType, string> = {
  person: "persons",
  company: "companies",
  association: "associations",
  domain: "domains",
};

export const OCOI_TYPE_LABELS: Record<OcoiEntityType, string> = {
  person: "אדם",
  company: "חברה",
  association: "עמותה",
  domain: "תחום",
};

// The public site hides Knesset expense edges by default: they are an order of
// magnitude more numerous than the conflict-of-interest declarations and would
// otherwise dominate every graph. Same default the legacy site used.
export const OCOI_DEFAULT_EXCLUDE = "mk_expense";

export const ocoi = {
  stats: () => request<OcoiEnvelope<OcoiStats>>("/ocoi/stats"),
  search: (params: { q: string; type?: OcoiEntityType; page?: number; limit?: number }) =>
    request<OcoiEnvelope<OcoiSearchHit[]>>(`/ocoi/search${ocalQS(params as Record<string, unknown>)}`),
  suggest: (q: string) =>
    request<OcoiEnvelope<{ text: string; type: OcoiEntityType; id: string }[]>>(
      `/ocoi/search/suggest${ocalQS({ q })}`,
    ),
  list: (type: OcoiEntityType, params: { page?: number; limit?: number; q?: string } = {}) =>
    request<OcoiEnvelope<OcoiEntity[]>>(
      `/ocoi/${OCOI_PATHS[type]}${ocalQS(params as Record<string, unknown>)}`,
    ),
  entity: (type: OcoiEntityType, id: string) =>
    request<OcoiEnvelope<OcoiEntity>>(`/ocoi/${OCOI_PATHS[type]}/${id}`),
  entityDocuments: (type: OcoiEntityType, id: string) =>
    request<OcoiEnvelope<OcoiDocument[]>>(`/ocoi/${OCOI_PATHS[type]}/${id}/documents`),
  topConnected: (params: { type?: OcoiEntityType; limit?: number; exclude_origins?: string } = {}) =>
    request<OcoiEnvelope<OcoiTopConnected[]>>(
      `/ocoi/entities/top-connected${ocalQS(params as Record<string, unknown>)}`,
    ),
  ministries: (exclude_origins?: string) =>
    request<OcoiEnvelope<OcoiMinistry[]>>(`/ocoi/entities/ministries${ocalQS({ exclude_origins })}`),
  neighbors: (
    id: string,
    params: { type: OcoiEntityType; depth?: number; exclude_origins?: string },
  ) =>
    request<OcoiEnvelope<OcoiGraph>>(
      `/ocoi/graph/neighbors/${id}${ocalQS(params as Record<string, unknown>)}`,
    ),
  showcase: (exclude_origins?: string) =>
    request<OcoiEnvelope<OcoiGraph | null>>(`/ocoi/graph/showcase${ocalQS({ exclude_origins })}`),
  documents: (
    params: { page?: number; limit?: number; q?: string; status?: string; verified?: string } = {},
  ) => request<OcoiEnvelope<OcoiDocument[]>>(`/ocoi/documents${ocalQS(params as Record<string, unknown>)}`),
  document: (id: string) => request<OcoiEnvelope<OcoiDocument>>(`/ocoi/documents/${id}`),
  documentGraph: (id: string) => request<OcoiEnvelope<OcoiGraph>>(`/ocoi/documents/${id}/graph`),
  registryLookup: (params: { q?: string; registration_number?: string; page?: number; limit?: number }) =>
    request<OcoiEnvelope<Record<string, unknown>[]>>(
      `/ocoi/registry/lookup${ocalQS(params as Record<string, unknown>)}`,
    ),
  /** Public GET — safe as a plain href / iframe src (no auth needed). */
  fileUrl: (id: string) => `${BASE}/ocoi/documents/${id}/file`,
};
