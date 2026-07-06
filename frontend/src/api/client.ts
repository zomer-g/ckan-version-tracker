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
    const err = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(err.detail || resp.statusText);
  }

  if (resp.status === 204) return undefined as T;
  return resp.json();
}

// Auth
export const auth = {
  me: () => request<{ id: string; email: string; display_name: string; is_admin: boolean }>("/auth/me"),
  ssoProviders: () =>
    request<{ google: boolean }>("/auth/sso/providers"),
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
  organization: string | null;
  organization_id: string | null;
  organization_title: string | null;
  source_type: string;
  version_count: number;
  last_polled_at: string | null;
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
  last_error: string | null;
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
};

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
export interface AppendSchema {
  dataset_id: string;
  dataset_title: string;
  table: string;
  total: number;
  columns: string[];
  key: string | null;
  capture_changes?: boolean;
  first_seen_column: string;
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
  filters?: Record<string, string>;
}): string {
  const p = new URLSearchParams();
  if (opts.limit != null) p.set("limit", String(opts.limit));
  if (opts.offset != null) p.set("offset", String(opts.offset));
  if (opts.sort) p.set("sort", opts.sort);
  if (opts.order) p.set("order", opts.order);
  if (opts.q) p.set("q", opts.q);
  for (const [k, v] of Object.entries(opts.filters || {})) {
    if (v) p.set(k, v);
  }
  const s = p.toString();
  return s ? `?${s}` : "";
}

export interface AppendSqlResult {
  columns: string[];
  rows: Array<Record<string, string | number | boolean | null>>;
  truncated: boolean;
  row_count: number;
}

export const appendArchive = {
  schema: (datasetId: string) =>
    request<AppendSchema>(`/append/${datasetId}/schema`),
  rows: (datasetId: string, opts: Parameters<typeof appendQuery>[0] = {}) =>
    request<AppendRows>(`/append/${datasetId}/rows${appendQuery(opts)}`),
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
  // Top-level navigation (can't carry an auth header), so the JWT rides in
  // the query string — same pattern as the SSO callback's ?sso_token=.
  connectUrl: (next: string) => {
    const token = getToken() || "";
    return `/api/auth/sso/google/drive/connect?token=${encodeURIComponent(
      token
    )}&next=${encodeURIComponent(next)}`;
  },
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
  sort?: "relevance" | "chrono";
  limit?: number;
  offset?: number;
}

export interface CbsFeaturedResponse {
  results: CbsResult[];
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
  facets: () => request<CbsFacets>("/cbs/facets"),
  stats: () => request<CbsStats>("/cbs/stats"),
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

// Public API (no auth required)
export const publicApi = {
  datasets: () => request<TrackedDataset[]>("/datasets"),
  request: (data: {
    ckan_id: string;
    resource_id?: string;
    resource_ids?: string[];
    preferred_interval?: number;
    requester_name?: string;
    requester_notes?: string;
    requester_contact?: string;
  }) =>
    request<{ message: string }>("/datasets/requests", {
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
  }) =>
    request<{ message: string }>("/datasets/requests", {
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
  storage_target?: StorageTarget;
  neon_eligible?: boolean;
  resource_ids?: string[] | null;
  resource_id?: string | null;
}

export interface ScrapeQueueRunning {
  task_id: string;
  dataset_id: string;
  dataset_title: string;
  phase: string | null;
  progress: number;
  message: string | null;
  created_at: string | null;
}

export interface ScrapeQueuePending {
  task_id: string;
  dataset_id: string;
  dataset_title: string;
  created_at: string | null;
}

export interface ScrapeQueueFailed {
  task_id: string;
  dataset_id: string;
  dataset_title: string;
  phase: string | null;
  error: string | null;
  completed_at: string | null;
}

export interface ScrapeQueueResponse {
  running: ScrapeQueueRunning[];
  pending: ScrapeQueuePending[];
  failed: ScrapeQueueFailed[];
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
  versions: DatasetSizeVersion[];
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

export const admin = {
  pending: () => request<PendingRequest[]>("/admin/pending"),
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
  cancelScrapeTask: (taskId: string) =>
    request<{ status: string; was: string }>(`/admin/scrape-tasks/${taskId}`, {
      method: "DELETE",
    }),
  scheduledJobs: () => request<ScheduledJobsResponse>("/admin/scheduled-jobs"),
  datasetSizes: () => request<DatasetSizesResponse>("/admin/dataset-sizes"),
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
    source_type: string;
    version_count: number;
    last_polled_at: string | null;
    tags?: Tag[];
  }[];
}

export const organizations = {
  list: () => request<Organization[]>("/organizations"),
  get: (id: string) => request<OrganizationDetail>(`/organizations/${id}`),
};
