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
  last_error: string | null;
  resource_ids: string[] | null;
  new_resources_at_source: Array<{ id: string; name?: string | null; format?: string | null }> | null;
  tags?: Tag[];
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
  trackGovmap: (source_url: string, title?: string, poll_interval = 604800) =>
    request<TrackedDataset>("/datasets", {
      method: "POST",
      body: JSON.stringify({ source_type: "govmap", source_url, title, poll_interval }),
    }),
  update: (id: string, data: { poll_interval?: number; is_active?: boolean; title?: string; organization_id?: string | null; storage_mode?: "full_snapshot" | "append_only"; append_key?: string | null; resource_ids?: string[]; dismiss_new_resources?: boolean }) =>
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

export const admin = {
  pending: () => request<PendingRequest[]>("/admin/pending"),
  approve: (id: string, poll_interval?: number, title?: string, organization_id?: string, resource_ids?: string[]) =>
    request<void>(`/admin/approve/${id}`, {
      method: "POST",
      body: JSON.stringify({ poll_interval, title, organization_id, resource_ids }),
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
