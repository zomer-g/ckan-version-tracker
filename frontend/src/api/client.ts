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

// Tracked Datasets
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
}

export const datasets = {
  list: () => request<TrackedDataset[]>("/datasets"),
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
  update: (id: string, data: { poll_interval?: number; is_active?: boolean; title?: string; organization_id?: string | null }) =>
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

// Public API (no auth required)
export const publicApi = {
  datasets: () => request<TrackedDataset[]>("/datasets"),
  request: (data: {
    ckan_id: string;
    resource_id?: string;
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

export const admin = {
  pending: () => request<PendingRequest[]>("/admin/pending"),
  approve: (id: string, poll_interval?: number, title?: string, organization_id?: string) =>
    request<void>(`/admin/approve/${id}`, {
      method: "POST",
      body: JSON.stringify({ poll_interval, title, organization_id }),
    }),
  reject: (id: string) => request<void>(`/admin/reject/${id}`, { method: "POST" }),
  scrapeTasks: () => request<ScrapeQueueResponse>("/admin/scrape-tasks"),
  cancelScrapeTask: (taskId: string) =>
    request<{ status: string; was: string }>(`/admin/scrape-tasks/${taskId}`, {
      method: "DELETE",
    }),
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
  }>) =>
    request<{ created: number; matched: number; total: number }>(
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
  dataset_count: number;
}

export interface OrganizationDetail extends Organization {
  data_gov_il_slug: string | null;
  datasets: {
    id: string;
    title: string;
    ckan_name: string;
    source_type: string;
    version_count: number;
    last_polled_at: string | null;
  }[];
}

export const organizations = {
  list: () => request<Organization[]>("/organizations"),
  get: (id: string) => request<OrganizationDetail>(`/organizations/${id}`),
};
