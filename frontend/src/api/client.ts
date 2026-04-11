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
  odata_dataset_id: string | null;
  poll_interval: number;
  is_active: boolean;
  status: string;
  last_polled_at: string | null;
  last_modified: string | null;
  version_count: number;
  requester_name: string | null;
  requester_email: string | null;
}

export const datasets = {
  list: () => request<TrackedDataset[]>("/datasets"),
  track: (ckan_id: string, poll_interval = 3600) =>
    request<TrackedDataset>("/datasets", {
      method: "POST",
      body: JSON.stringify({ ckan_id, poll_interval }),
    }),
  update: (id: string, data: { poll_interval?: number; is_active?: boolean }) =>
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
    resources_added?: string[];
    resources_removed?: string[];
    resources_modified?: { resource_id: string; name: string; format: string }[];
    total_resources?: number;
  } | null;
  resource_mappings: Record<string, any> | null;
}

export const versions = {
  list: (datasetId: string) => request<Version[]>(`/datasets/${datasetId}/versions`),
  get: (versionId: string) => request<Version>(`/versions/${versionId}`),
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

// Admin
export interface PendingRequest {
  id: string;
  ckan_id: string;
  ckan_name: string;
  title: string;
  organization: string | null;
  poll_interval: number;
  status: string;
  created_at: string;
  requester_email: string;
  requester_name: string;
}

export const admin = {
  pending: () => request<PendingRequest[]>("/admin/pending"),
  approve: (id: string) => request<void>(`/admin/approve/${id}`, { method: "POST" }),
  reject: (id: string) => request<void>(`/admin/reject/${id}`, { method: "POST" }),
};
