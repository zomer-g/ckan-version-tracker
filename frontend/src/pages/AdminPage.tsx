import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import {
  admin as adminApi,
  datasets as datasetsApi,
  publicApi,
  organizations as orgsApi,
  tagsApi,
  PendingRequest,
  TrackedDataset,
  StorageTarget,
  CoverageReport,
  ScrapeQueueResponse,
  ScheduledJobsResponse,
  DatasetSizesResponse,
  formatBytes,
  Organization,
  Tag,
  TagWithCount,
} from "../api/client";
import TagPicker from "../components/TagPicker";
import ResourcePickerModal from "../components/ResourcePickerModal";
import ActivityLogPanel from "../components/ActivityLogPanel";
import CopyListButton from "../components/CopyListButton";
import McpUsersPanel from "../components/McpUsersPanel";
import PageContentPanel from "../components/PageContentPanel";
import { sourceBadgeFor as sourceBadgeForShared } from "../utils/sourceBadge";

// Unified storage-plan options for the admin selectors, source-aware:
//   • NEON (queryable tabular-rows DB) and "R2 + NEON" (a per-version CSV
//     snapshot on R2 AND the queryable NEON table, written in one pass) —
//     CKAN/data.gov.il tabular sources only.
//   • local (worker keeps files, nothing on OVER) — worker-driven sources only
//     (scraper/govmap); the CKAN inline poll has no worker machine, so it isn't
//     offered there. R2 / ODATA file snapshots are available to both.
// (odata+neon is accepted by the API but not offered — the dual-write file side
// is wired for R2 only.)
function storageTargetOptions(
  neonEligible: boolean,
): Array<{ value: StorageTarget; label: string; disabled: boolean }> {
  const opts: Array<{ value: StorageTarget; label: string; disabled: boolean }> = [];
  if (!neonEligible) opts.push({ value: "local", label: "מקומי (לא ב-OVER)", disabled: false });
  opts.push({ value: "r2", label: "R2 — סנפשוט מלא", disabled: false });
  opts.push({ value: "odata", label: "ODATA (legacy)", disabled: false });
  if (neonEligible) {
    opts.push({ value: "neon", label: "NEON — DB טבלאי", disabled: false });
    opts.push({ value: "r2+neon", label: "R2 + NEON — סנפשוט וגם DB", disabled: false });
  }
  return opts;
}

// Small caption above each control in the redesigned dataset cards.
const admFieldLabel = {
  fontSize: "0.68rem",
  color: "var(--text-muted)",
  marginBottom: "0.2rem",
  fontWeight: 600,
} as const;

function formatRelative(iso: string | null): string {
  if (!iso) return "";
  const ms = Date.now() - new Date(iso).getTime();
  const sec = Math.round(ms / 1000);
  if (sec < 60) return `לפני ${sec} שניות`;
  const min = Math.round(sec / 60);
  if (min < 60) return `לפני ${min} דקות`;
  const hr = Math.round(min / 60);
  if (hr < 24) return `לפני ${hr} שעות`;
  const days = Math.round(hr / 24);
  return `לפני ${days} ימים`;
}

// Human label for the machine running a scrape task. Prefers the explicit
// worker id (hostname#short — distinguishes workers behind a shared IP) and
// appends the IP when both are known; falls back to the IP for older workers
// that don't report an id. Empty string when neither is known.
function workerLabel(t: { worker_id?: string | null; worker_ip?: string | null }): string {
  const id = (t.worker_id || "").trim();
  const ip = (t.worker_ip || "").trim();
  if (id && ip && id !== ip) return `${id} (${ip})`;
  return id || ip || "";
}

const INTERVAL_OPTIONS = [
  { value: 900, label: "כל 15 דקות" },
  { value: 3600, label: "כל שעה" },
  { value: 43200, label: "כל 12 שעות" },
  { value: 86400, label: "כל יום" },
  { value: 604800, label: "כל שבוע" },
  { value: 2592000, label: "כל חודש" },
  { value: 7776000, label: "כל רבעון" },
];

function formatIntervalLabel(seconds: number): string {
  const match = INTERVAL_OPTIONS.find((o) => o.value === seconds);
  if (match) return match.label;
  if (seconds < 3600) return `כל ${Math.round(seconds / 60)} דקות`;
  if (seconds < 86400) return `כל ${Math.round(seconds / 3600)} שעות`;
  return `כל ${Math.round(seconds / 86400)} ימים`;
}

interface SourceBadge {
  bg: string;
  fg: string;
  label: string;
  accent: string;
}

function sourceBadge(
  source_type: string | null | undefined,
  organization: string | null | undefined = null,
  ckan_id: string | null | undefined = null,
): SourceBadge {
  // Delegate to the shared helper for the (source_type, organization,
  // ckan_id) → palette+label mapping. ckan_id is the most reliable
  // signal for IDF detection because the organization field drifts
  // when admins reassign datasets to real Organization entities.
  const p = sourceBadgeForShared(source_type, organization, ckan_id);
  return { bg: p.bg, fg: p.fg, label: p.label, accent: p.accent };
}

function isCkanLike(source_type: string | null | undefined): boolean {
  return source_type !== "scraper" && source_type !== "govmap";
}

type AdminTab = "queue" | "schedule" | "push_jobs" | "requests" | "datasets" | "log" | "mcp" | "orgs" | "tags" | "content";

const ADMIN_TABS: { id: AdminTab; label: string; emoji: string }[] = [
  { id: "queue",     label: "תור גירוד",        emoji: "⏳" },
  { id: "schedule",  label: "תזמון משימות",      emoji: "📅" },
  { id: "push_jobs", label: "תור Datastore",    emoji: "🛢" },
  { id: "requests",  label: "בקשות ממתינות",     emoji: "📥" },
  { id: "datasets",  label: "מאגרים פעילים",    emoji: "📂" },
  { id: "log",       label: "לוג משימות",        emoji: "📜" },
  { id: "mcp",       label: "גישת MCP",          emoji: "🔌" },
  { id: "orgs",      label: "ארגונים",           emoji: "🏛" },
  { id: "tags",      label: "תגיות",             emoji: "🏷" },
  { id: "content",   label: "טקסטים",            emoji: "📝" },
];

function readTabFromHash(): AdminTab {
  const h = (typeof window !== "undefined" ? window.location.hash : "").replace("#", "");
  return (ADMIN_TABS.find((t) => t.id === h)?.id) ?? "queue";
}

export default function AdminPage() {
  const { t } = useTranslation();
  const [requests, setRequests] = useState<PendingRequest[]>([]);
  const [allDatasets, setAllDatasets] = useState<TrackedDataset[]>([]);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState<Set<string>>(new Set());
  const [intervalOverrides, setIntervalOverrides] = useState<Record<string, number>>({});
  const [titleOverrides, setTitleOverrides] = useState<Record<string, string>>({});
  const [pollToast, setPollToast] = useState<{ id: string; ok: boolean; msg: string } | null>(null);
  // Active-dataset rename state
  const [editingTitleFor, setEditingTitleFor] = useState<string | null>(null);
  const [editingTitleValue, setEditingTitleValue] = useState("");
  const [savingTitle, setSavingTitle] = useState(false);
  // Resource picker modal for both pending requests and active datasets
  const [resourcePickerFor, setResourcePickerFor] = useState<
    | { kind: "active"; ds: TrackedDataset }
    | { kind: "pending"; req: PendingRequest }
    | null
  >(null);
  // Scrape queue state
  const [queue, setQueue] = useState<ScrapeQueueResponse | null>(null);
  // Scheduled jobs state (next-run preview)
  const [schedule, setSchedule] = useState<ScheduledJobsResponse | null>(null);
  // Dataset sizes (admin-only). Loaded once per panel open; cached on
  // server for 60s. Slow on cold-cache (one CKAN call per dataset).
  const [sizes, setSizes] = useState<DatasetSizesResponse | null>(null);
  const [sizesLoading, setSizesLoading] = useState(false);
  // Organizations
  const [orgs, setOrgs] = useState<Organization[]>([]);
  const [orgOverrides, setOrgOverrides] = useState<Record<string, string>>({});
  // Per-pending-request storage plan chosen at approval (the unified selector).
  const [storageOverrides, setStorageOverrides] = useState<Record<string, StorageTarget>>({});
  // OVER full-version coverage audit (#4): which active datasets lack a version.
  const [coverage, setCoverage] = useState<CoverageReport | null>(null);
  const [coverageBusy, setCoverageBusy] = useState(false);
  const [coverageMsg, setCoverageMsg] = useState<string | null>(null);
  const [syncingOrgs, setSyncingOrgs] = useState(false);
  const [syncToast, setSyncToast] = useState<string | null>(null);
  // Tags
  const [availableTags, setAvailableTags] = useState<TagWithCount[]>([]);
  // Sidebar nav tab — synced to URL hash so reload/share preserves state
  const [tab, setTabState] = useState<AdminTab>(readTabFromHash);
  const setTab = (next: AdminTab) => {
    setTabState(next);
    window.location.hash = next;
  };
  useEffect(() => {
    const onHashChange = () => setTabState(readTabFromHash());
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  useEffect(() => {
    loadAll();
    loadOrgs();
    loadTags();
    loadQueue();
    loadSchedule();
    loadSizes();
    const id = setInterval(() => {
      loadQueue();
      loadSchedule();
    }, 5000);
    return () => clearInterval(id);
  }, []);

  const loadTags = async () => {
    try {
      const list = await tagsApi.list();
      setAvailableTags(list);
    } catch (e) {
      console.error("Failed to load tags", e);
    }
  };

  const handleSetDatasetTags = async (datasetId: string, tags: Tag[]) => {
    try {
      const updated = await adminApi.setDatasetTags(
        datasetId,
        tags.map((t) => t.id)
      );
      setAllDatasets((prev) =>
        prev.map((d) => (d.id === datasetId ? updated : d))
      );
      // Re-pull tag counts so the picker stays accurate.
      loadTags();
    } catch (e: any) {
      alert(`שמירת תגיות נכשלה: ${e?.message || e}`);
    }
  };

  const handleCreateTag = async (name: string): Promise<Tag> => {
    const tag = await tagsApi.create(name);
    setAvailableTags((prev) => {
      if (prev.some((t) => t.id === tag.id)) return prev;
      return [...prev, { ...tag, description: null, dataset_count: 0 }];
    });
    return tag;
  };

  const handleDeleteTag = async (tag: TagWithCount) => {
    const ok = window.confirm(
      `למחוק את התגית "${tag.name}"? המאגרים יישארו, רק השיוך יוסר.`
    );
    if (!ok) return;
    try {
      await adminApi.deleteTag(tag.id);
      setAvailableTags((prev) => prev.filter((t) => t.id !== tag.id));
      // Also strip the tag from any cached dataset rows.
      setAllDatasets((prev) =>
        prev.map((d) =>
          d.tags
            ? { ...d, tags: d.tags.filter((t) => t.id !== tag.id) }
            : d
        )
      );
    } catch (e: any) {
      alert(`מחיקת התגית נכשלה: ${e?.message || e}`);
    }
  };

  const loadOrgs = async () => {
    try {
      const list = await orgsApi.list();
      setOrgs(list);
    } catch (e) {
      console.error("Failed to load organizations", e);
    }
  };

  const handleSyncOrgs = async () => {
    setSyncingOrgs(true);
    setSyncToast(null);
    try {
      const res = await adminApi.syncOrganizations();
      setSyncToast(`data.gov.il: נוספו ${res.created}, עודכנו ${res.updated}, שויכו ${res.linked_datasets} מאגרים`);
      await loadOrgs();
      await loadAll();
      setTimeout(() => setSyncToast(null), 6000);
    } catch (e: any) {
      setSyncToast(`שגיאה: ${e?.message || e}`);
      setTimeout(() => setSyncToast(null), 6000);
    }
    setSyncingOrgs(false);
  };

  const handleLinkScrapers = async () => {
    setSyncingOrgs(true);
    setSyncToast(null);
    try {
      const res = await adminApi.linkScraperDatasetsToOrgs();
      setSyncToast(
        `שויכו לפי officeId: ${res.linked_by_office_id}, לפי נתיב: ${res.linked_by_path}, ` +
        `ללא שיוך: ${res.unlinked} (מתוך ${res.total_scraper_datasets})`
      );
      await loadAll();
      setTimeout(() => setSyncToast(null), 8000);
    } catch (e: any) {
      setSyncToast(`שגיאה: ${e?.message || e}`);
      setTimeout(() => setSyncToast(null), 6000);
    }
    setSyncingOrgs(false);
  };

  const handleSyncOrgsGovIl = async () => {
    setSyncingOrgs(true);
    setSyncToast(null);
    try {
      // gov.il's Cloudflare blocks cloud IPs but allows residential browsers,
      // so we fetch the JSON here in the admin's browser and POST it up.
      const resp = await fetch("https://www.gov.il/govil-landing-page-api/he", {
        headers: { Accept: "application/json" },
      });
      if (!resp.ok) {
        throw new Error(`gov.il returned ${resp.status}`);
      }
      const data = await resp.json();
      const results: any[] = data?.results || [];
      const LOGO_BASE = "https://www.gov.il/BlobFolder/office";
      const offices = results
        .map((r) => {
          const urlName = (r.urlName || "").trim();
          const title = (r.title || "").trim();
          if (!urlName || !title) return null;
          const logoName = (r.logo?.name || "").trim();
          const logoUrl = logoName ? `${LOGO_BASE}/${urlName}/he/${logoName}` : null;
          // Flatten unitsList[].unitsSubList[] into a single array of sub-units
          const units: Array<{ url_name: string; title: string }> = [];
          const groups = Array.isArray(r.unitsList) ? r.unitsList : [];
          for (const g of groups) {
            const sub = Array.isArray(g?.unitsSubList) ? g.unitsSubList : [];
            for (const u of sub) {
              const ut = (u?.title || "").trim();
              const un = (u?.urlName || "").trim();
              if (ut && un) units.push({ title: ut, url_name: un });
            }
          }
          return {
            url_name: urlName,
            title,
            logo_url: logoUrl,
            external_website: r.externalWebsite || null,
            org_type: r.orgType ?? null,
            offices: Array.isArray(r.offices) ? r.offices.filter((x: any) => typeof x === "string") : [],
            units,
          };
        })
        .filter((o): o is NonNullable<typeof o> => o !== null);

      if (offices.length === 0) {
        throw new Error("gov.il החזיר רשימה ריקה");
      }
      const res = await adminApi.syncOrganizationsGovIl(offices);
      setSyncToast(
        `gov.il: משרדים — נוספו ${res.created}, חוברו ${res.matched}. ` +
        `תת-יחידות — נוספו ${res.children_created}, שויכו ${res.children_matched}.`
      );
      await loadOrgs();
      setTimeout(() => setSyncToast(null), 6000);
    } catch (e: any) {
      setSyncToast(`שגיאה: ${e?.message || e}`);
      setTimeout(() => setSyncToast(null), 6000);
    }
    setSyncingOrgs(false);
  };

  const handleChangeParentOrg = async (orgId: string, newParentId: string) => {
    try {
      const updated = await adminApi.updateOrgParent(orgId, newParentId || null);
      setOrgs((prev) => prev.map((o) => (o.id === orgId ? updated : o)));
    } catch (e: any) {
      alert(`שיוך הורה נכשל: ${e?.message || e}`);
    }
  };

  const handleChangeOrg = async (datasetId: string, newOrgId: string) => {
    try {
      await datasetsApi.update(datasetId, { organization_id: newOrgId || "" });
      const matched = orgs.find((o) => o.id === newOrgId);
      setAllDatasets((prev) =>
        prev.map((d) =>
          d.id === datasetId
            ? {
                ...d,
                organization_id: newOrgId || null,
                organization_title: matched?.title || null,
                organization: matched?.name || d.organization,
              }
            : d
        )
      );
    } catch (e: any) {
      alert(`שיוך ארגון נכשל: ${e?.message || e}`);
    }
  };

  const loadQueue = async () => {
    try {
      const q = await adminApi.scrapeTasks();
      setQueue(q);
    } catch (e) {
      console.error("Failed to load queue", e);
    }
  };

  const loadSchedule = async () => {
    try {
      const s = await adminApi.scheduledJobs();
      setSchedule(s);
    } catch (e) {
      console.error("Failed to load schedule", e);
    }
  };

  const loadSizes = async () => {
    setSizesLoading(true);
    try {
      const s = await adminApi.datasetSizes();
      setSizes(s);
    } catch (e) {
      console.error("Failed to load dataset sizes", e);
    } finally {
      setSizesLoading(false);
    }
  };

  const formatDuration = (totalSeconds: number): string => {
    const sign = totalSeconds < 0 ? "-" : "";
    const s = Math.abs(totalSeconds);
    if (s < 60) return `${sign}${s} שניות`;
    const m = Math.round(s / 60);
    if (m < 60) return `${sign}${m} דקות`;
    const h = Math.round(m / 60);
    if (h < 24) return `${sign}${h} שעות`;
    const d = Math.round(h / 24);
    if (d < 30) return `${sign}${d} ימים`;
    const months = Math.round(d / 30);
    return `${sign}${months} חודשים`;
  };

  const loadAll = async () => {
    setLoading(true);
    try {
      const [pending, all] = await Promise.all([
        adminApi.pending(),
        publicApi.datasets(),
      ]);
      setRequests(pending);
      setAllDatasets(all);
    } catch (e) {
      console.error(e);
    }
    setLoading(false);
  };

  const handleApprove = async (id: string) => {
    const reqRow = requests.find((r) => r.id === id);
    // For CKAN requests, force the admin to pick resources before approve
    // (the dataset row may have been created before this feature shipped).
    // Scraper and govmap have no resource concept — approve directly.
    if (
      reqRow &&
      isCkanLike(reqRow.source_type) &&
      !(reqRow.resource_ids && reqRow.resource_ids.length > 0) &&
      !reqRow.resource_id
    ) {
      setResourcePickerFor({ kind: "pending", req: reqRow });
      return;
    }
    setProcessing((prev) => new Set(prev).add(id));
    try {
      const intervalOverride = intervalOverrides[id];
      const titleOverride = titleOverrides[id]?.trim();
      // Only send title if it was actually edited (different from original)
      const req = requests.find((r) => r.id === id);
      const titleToSend = titleOverride && titleOverride !== req?.title ? titleOverride : undefined;
      const orgIdOverride = orgOverrides[id] || undefined;
      // Send the pre-picked resource_ids stored on the row by the
      // resource picker, if any.
      const pickedIds =
        reqRow?.resource_ids && reqRow.resource_ids.length > 0
          ? reqRow.resource_ids
          : undefined;
      // Storage destination chosen in the row (undefined → backend default,
      // which is R2 for scraper/govmap, ODATA for ckan).
      const storageTarget = storageOverrides[id];
      await adminApi.approve(id, intervalOverride, titleToSend, orgIdOverride, pickedIds, storageTarget);
      await loadAll();
    } catch (e) { console.error(e); }
    setProcessing((prev) => { const n = new Set(prev); n.delete(id); return n; });
  };

  const startEditTitle = (id: string, currentTitle: string) => {
    setEditingTitleFor(id);
    setEditingTitleValue(currentTitle);
  };

  const cancelEditTitle = () => {
    setEditingTitleFor(null);
    setEditingTitleValue("");
  };

  const saveEditTitle = async (id: string) => {
    const newTitle = editingTitleValue.trim();
    if (!newTitle) {
      cancelEditTitle();
      return;
    }
    setSavingTitle(true);
    try {
      await datasetsApi.update(id, { title: newTitle });
      setAllDatasets((prev) =>
        prev.map((d) => (d.id === id ? { ...d, title: newTitle } : d))
      );
      cancelEditTitle();
    } catch (e) {
      console.error(e);
      alert("שמירת השם נכשלה");
    }
    setSavingTitle(false);
  };

  const handleReject = async (id: string) => {
    setProcessing((prev) => new Set(prev).add(id));
    try {
      await adminApi.reject(id);
      await loadAll();
    } catch (e) { console.error(e); }
    setProcessing((prev) => { const n = new Set(prev); n.delete(id); return n; });
  };

  const handlePoll = async (id: string) => {
    setProcessing((prev) => new Set(prev).add(id));
    setPollToast(null);
    try {
      await datasetsApi.poll(id);
      setPollToast({ id, ok: true, msg: "נשלח לדגום ✓" });
      // Poll runs in the background. Refresh the list a few seconds later
      // so the row reflects last_polled_at / last_error / new version count
      // without the admin having to reload the page manually.
      setTimeout(() => { loadAll().catch(() => {}); }, 4000);
      setTimeout(() => setPollToast(null), 6000);
    } catch (e: any) {
      setPollToast({ id, ok: false, msg: e?.message || "שגיאה בדגום" });
      setTimeout(() => setPollToast(null), 4000);
    }
    setProcessing((prev) => { const n = new Set(prev); n.delete(id); return n; });
  };

  const handleDelete = async (id: string, title: string) => {
    if (!confirm(`למחוק את "${title}"?`)) return;
    setProcessing((prev) => new Set(prev).add(id));
    try {
      await datasetsApi.untrack(id);
      setAllDatasets((prev) => prev.filter((d) => d.id !== id));
    } catch (e) { console.error(e); }
    setProcessing((prev) => { const n = new Set(prev); n.delete(id); return n; });
  };

  const handleCancelTask = async (taskId: string, title: string, kind: "running" | "pending") => {
    const verb = kind === "running" ? "לאפס" : "להסיר מהתור";
    if (!confirm(`${verb} את המשימה של "${title}"?`)) return;
    try {
      await adminApi.cancelScrapeTask(taskId);
      await loadQueue();
    } catch (e: any) {
      alert(`שגיאה: ${e?.message || e}`);
    }
  };

  const handleDismissFailed = async (taskId: string, title: string) => {
    if (!confirm(`למחוק את שגיאת הגירוד של "${title}" מהיומן?`)) return;
    try {
      await adminApi.cancelScrapeTask(taskId);
      await loadQueue();
    } catch (e: any) {
      alert(`שגיאה: ${e?.message || e}`);
    }
  };

  const handleRetryFailed = async (taskId: string, datasetId: string, title: string) => {
    if (!confirm(`לנסות שוב לגרד את "${title}"?`)) return;
    try {
      // Drop the old failure entry first so the panel doesn't show stale red
      // alongside the new pending task. If the dismiss fails (e.g. another admin
      // already cleared it), continue anyway — the retry is what matters.
      try {
        await adminApi.cancelScrapeTask(taskId);
      } catch {
        // ignore — task may already be gone
      }
      await datasetsApi.poll(datasetId);
      await loadQueue();
    } catch (e: any) {
      alert(`שגיאה: ${e?.message || e}`);
    }
  };

  const handleUpdateInterval = async (id: string, interval: number) => {
    try {
      await datasetsApi.update(id, { poll_interval: interval });
      setAllDatasets((prev) =>
        prev.map((d) => (d.id === id ? { ...d, poll_interval: interval } : d))
      );
    } catch (e) { console.error(e); }
  };

  const handleUpdateStorageMode = async (
    id: string,
    storage_mode: "full_snapshot" | "append_only",
  ) => {
    try {
      const updated = await datasetsApi.update(id, { storage_mode });
      setAllDatasets((prev) =>
        prev.map((d) => (d.id === id ? { ...d, storage_mode: updated.storage_mode } : d))
      );
    } catch (e) { console.error(e); }
  };

  const handleUpdateStorageTarget = async (
    id: string,
    storage_target: StorageTarget,
  ) => {
    try {
      const updated = await datasetsApi.update(id, { storage_target });
      setAllDatasets((prev) =>
        prev.map((d) =>
          d.id === id
            ? { ...d, storage_target: updated.storage_target, upload_mode: updated.upload_mode }
            : d,
        )
      );
    } catch (e) {
      console.error(e);
      alert((e as Error)?.message || "שגיאה בעדכון יעד האחסון");
    }
  };

  const runCoverageScan = async () => {
    setCoverageBusy(true);
    setCoverageMsg(null);
    try {
      setCoverage(await adminApi.overCoverage());
    } catch (e) {
      setCoverageMsg((e as Error)?.message || "שגיאה בסריקה");
    } finally {
      setCoverageBusy(false);
    }
  };

  const runCoverageFix = async () => {
    if (!confirm("להפעיל סריקת snapshot מלאה לכל המאגרים שחסרה להם גרסה על OVER?")) return;
    setCoverageBusy(true);
    setCoverageMsg(null);
    try {
      const report = await adminApi.overCoverageFix();
      setCoverage(report);
      setCoverageMsg(`הופעלה סריקה מחדש ל-${report.missing.length} מאגרים. הגרסאות ייווצרו ברקע — רענן בעוד כמה דקות.`);
    } catch (e) {
      setCoverageMsg((e as Error)?.message || "שגיאה בהפעלת התיקון");
    } finally {
      setCoverageBusy(false);
    }
  };

  const handleSaveResourceIds = async (
    id: string,
    resource_ids: string[],
    kind: "active" | "pending",
  ) => {
    if (kind === "active") {
      const updated = await datasetsApi.update(id, { resource_ids });
      setAllDatasets((prev) =>
        prev.map((d) =>
          d.id === id
            ? {
                ...d,
                resource_ids: updated.resource_ids,
                new_resources_at_source: updated.new_resources_at_source,
              }
            : d
        )
      );
    } else {
      // Pending: store the chosen ids on the row so the next approve
      // call can ship them, then refresh from the server to confirm.
      setRequests((prev) =>
        prev.map((r) => (r.id === id ? { ...r, resource_ids } : r))
      );
    }
  };

  const handleDismissNewResources = async (id: string) => {
    try {
      const updated = await datasetsApi.update(id, { dismiss_new_resources: true });
      setAllDatasets((prev) =>
        prev.map((d) =>
          d.id === id
            ? { ...d, new_resources_at_source: updated.new_resources_at_source }
            : d
        )
      );
    } catch (e: any) {
      alert(`שגיאה: ${e?.message || e}`);
    }
  };

  const handleUpdateAppendKey = async (id: string, append_key: string) => {
    try {
      const updated = await datasetsApi.update(id, { append_key });
      setAllDatasets((prev) =>
        prev.map((d) => (d.id === id ? { ...d, append_key: updated.append_key } : d))
      );
    } catch (e) { console.error(e); }
  };

  // DIFF mode (capture_changes) — reserved for rare/extreme cases. Enabling it
  // triggers a one-time heavy table migration on the next poll, then per-poll
  // content diffs. Confirm before turning on.
  const handleUpdateCaptureChanges = async (id: string, capture_changes: boolean) => {
    if (capture_changes && !confirm(
      "מצב DIFF הוא כבד ושמור למקרים קיצוניים (כמו מאגר הרכב): בכל סריקה נלכדות גם שינויים בשורות קיימות, וההפעלה מריצה מיגרציה חד-פעמית על הטבלה. להפעיל?"
    )) return;
    try {
      const updated = await datasetsApi.update(id, { capture_changes });
      setAllDatasets((prev) =>
        prev.map((d) => (d.id === id ? { ...d, capture_changes: updated.capture_changes } : d))
      );
    } catch (e) { console.error(e); }
  };

  if (loading) return <div className="loading" role="status">{t("common.loading")}</div>;

  const activeDatasets = allDatasets.filter((d) => d.status === "active");

  // Build per-dataset status map for inline indicators
  const datasetStatusMap = new Map<string, { kind: "running" | "pending" | "failed"; tooltip?: string }>();
  if (queue) {
    queue.failed.forEach((t) =>
      datasetStatusMap.set(t.dataset_id, { kind: "failed", tooltip: t.error || undefined })
    );
    queue.pending.forEach((t) =>
      datasetStatusMap.set(t.dataset_id, { kind: "pending" })
    );
    // Running takes priority — set last so it overrides pending/failed for same dataset
    queue.running.forEach((t) =>
      datasetStatusMap.set(t.dataset_id, {
        kind: "running",
        tooltip: `${t.phase || ""} ${t.progress || 0}% — ${t.message || ""}`.trim(),
      })
    );
  }

  const activeTabLabel = ADMIN_TABS.find((tt) => tt.id === tab)?.label ?? "";
  const sizeByDsId = new Map<string, number>();
  const suggestDeltaByDsId = new Set<string>();
  if (sizes) {
    for (const r of sizes.datasets) {
      sizeByDsId.set(r.dataset_id, r.total_bytes);
      if (r.suggest_delta_archive) suggestDeltaByDsId.add(r.dataset_id);
    }
  }
  const orgSyncControls = (
    <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", flexWrap: "wrap" }}>
      <button className="btn-secondary" onClick={handleSyncOrgs} disabled={syncingOrgs} style={{ fontSize: "0.8rem", padding: "0.35rem 0.75rem" }}>
        {syncingOrgs ? "..." : t("organizations.admin_sync")}
      </button>
      <button className="btn-secondary" onClick={handleSyncOrgsGovIl} disabled={syncingOrgs} style={{ fontSize: "0.8rem", padding: "0.35rem 0.75rem" }}>
        {syncingOrgs ? "..." : t("organizations.admin_sync_gov_il")}
      </button>
      <button className="btn-secondary" onClick={handleLinkScrapers} disabled={syncingOrgs} style={{ fontSize: "0.8rem", padding: "0.35rem 0.75rem" }}>
        {syncingOrgs ? "..." : t("organizations.admin_link_scrapers", "שייך מאגרי gov.il")}
      </button>
      {syncToast && (
        <span style={{
          fontSize: "0.75rem",
          padding: "0.25rem 0.5rem",
          borderRadius: "4px",
          background: syncToast.startsWith("שגיאה") ? "#fee2e2" : "#dcfce7",
          color: syncToast.startsWith("שגיאה") ? "#991b1b" : "#166534",
        }}>{syncToast}</span>
      )}
    </div>
  );
  return (
    <div className="admin-layout" style={{
      display: "flex",
      gap: "1.25rem",
      alignItems: "flex-start",
      flexDirection: "row-reverse",
    }}>
      {/* Sidebar nav (right side under RTL) */}
      <aside style={{
        width: "210px",
        flexShrink: 0,
        position: "sticky",
        top: "1rem",
        background: "var(--surface)",
        borderRadius: "var(--radius)",
        border: "1px solid var(--border)",
        padding: "0.75rem",
        boxShadow: "var(--shadow-sm)",
      }}>
        <h1 style={{ margin: "0 0 0.75rem 0", fontSize: "1.2rem" }}>{t("admin.title")}</h1>
        <nav style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
          {ADMIN_TABS.map((tt) => {
            const isActive = tt.id === tab;
            return (
              <button
                key={tt.id}
                onClick={() => setTab(tt.id)}
                style={{
                  textAlign: "right",
                  fontSize: "0.9rem",
                  padding: "0.5rem 0.75rem",
                  borderRadius: "6px",
                  border: "1px solid",
                  borderColor: isActive ? "var(--primary)" : "transparent",
                  background: isActive ? "var(--primary-50, #e0e7ff)" : "transparent",
                  color: isActive ? "var(--primary)" : "var(--text)",
                  fontWeight: isActive ? 600 : 400,
                  cursor: "pointer",
                  display: "flex",
                  alignItems: "center",
                  gap: "0.5rem",
                }}
              >
                <span style={{ fontSize: "1rem" }}>{tt.emoji}</span>
                <span>{tt.label}</span>
              </button>
            );
          })}
        </nav>
      </aside>

      {/* Main content — only the selected tab renders */}
      <main style={{ flex: 1, minWidth: 0 }}>
        <div className="page-header flex-between" style={{
          borderBottom: "1px solid var(--border)",
          paddingBottom: "0.5rem",
          marginBottom: "1rem",
          flexWrap: "wrap",
          gap: "0.75rem",
        }}>
          <h2 style={{ margin: 0, fontSize: "1.4rem" }}>{activeTabLabel}</h2>
          {tab === "orgs" && orgSyncControls}
        </div>

      {tab === "queue" && (<>

      <section style={{
        marginBottom: "1.5rem",
        padding: "1rem 1.25rem",
        background: "var(--surface)",
        borderRadius: "var(--radius)",
        boxShadow: "var(--shadow-sm)",
        border: "1px solid var(--border)",
      }} aria-labelledby="queue-heading">
        <div className="flex-between" style={{ marginBottom: "0.75rem" }}>
          <h2 id="queue-heading" style={{ fontSize: "1.1rem", fontWeight: 700, margin: 0 }}>
            תור גירוד
          </h2>
          <button onClick={loadQueue} className="btn-secondary" style={{ fontSize: "0.75rem", padding: "0.25rem 0.6rem" }}>
            רענן ↻
          </button>
        </div>

        {!queue ? (
          <div className="text-sm text-muted">טוען...</div>
        ) : queue.running.length === 0 && queue.pending.length === 0 && queue.failed.length === 0 ? (
          <div className="text-sm text-muted">התור ריק — אין משימות גירוד פעילות</div>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
            {/* Running */}
            {queue.running.length > 0 && (
              <div>
                <div style={{ fontSize: "0.85rem", fontWeight: 600, marginBottom: "0.4rem", color: "#16a34a" }}>
                  🔄 בעבודה כרגע ({queue.running.length})
                </div>
                {queue.running.map((t) => {
                  const ageMs = t.created_at ? Date.now() - new Date(t.created_at).getTime() : 0;
                  const isStuck = ageMs > 30 * 60 * 1000;  // 30 minutes
                  return (
                    <div key={t.task_id} style={{
                      padding: "0.6rem 0.75rem",
                      marginBottom: "0.4rem",
                      background: isStuck ? "#fef2f2" : "#f0fdf4",
                      border: isStuck ? "1px solid #fca5a5" : "1px solid #bbf7d0",
                      borderRadius: "6px",
                    }}>
                      <div className="flex-between" style={{ gap: "0.5rem" }}>
                        <div style={{ fontWeight: 600, fontSize: "0.9rem", flex: 1 }}>
                          <Link to={`/versions/${t.dataset_id}`}>{t.dataset_title}</Link>
                          {isStuck && (
                            <span style={{
                              marginInlineStart: "0.5rem",
                              fontSize: "0.7rem",
                              padding: "0.1rem 0.4rem",
                              borderRadius: "9999px",
                              background: "#fee2e2",
                              color: "#991b1b",
                              fontWeight: 600,
                            }}>
                              ⚠ ייתכן שתקוע
                            </span>
                          )}
                        </div>
                        <button
                          onClick={() => handleCancelTask(t.task_id, t.dataset_title, "running")}
                          title="אפס משימה"
                          style={{
                            background: "none",
                            border: "1px solid #dc2626",
                            color: "#dc2626",
                            cursor: "pointer",
                            fontSize: "0.7rem",
                            padding: "0.2rem 0.5rem",
                            borderRadius: "4px",
                            whiteSpace: "nowrap",
                          }}
                        >
                          ✕ אפס
                        </button>
                      </div>
                      <div className="text-sm text-muted" style={{ marginTop: "0.2rem" }}>
                        שלב: <strong>{t.phase || "—"}</strong> · {t.progress}% · התחיל {formatRelative(t.created_at)}
                        {" · "}מכונה: <strong>{workerLabel(t) || "—"}</strong>
                      </div>
                      {t.message && (
                        <div className="text-sm" style={{ marginTop: "0.2rem", color: "#166534" }}>
                          {t.message}
                        </div>
                      )}
                      {/* Progress bar */}
                      <div style={{
                        marginTop: "0.4rem",
                        height: "6px",
                        background: isStuck ? "#fecaca" : "#dcfce7",
                        borderRadius: "3px",
                        overflow: "hidden",
                      }}>
                        <div style={{
                          width: `${Math.max(2, t.progress)}%`,
                          height: "100%",
                          background: isStuck ? "#dc2626" : "#16a34a",
                          transition: "width 0.5s",
                        }} />
                      </div>
                    </div>
                  );
                })}
              </div>
            )}

            {/* Pending */}
            {queue.pending.length > 0 && (
              <div>
                <div style={{ fontSize: "0.85rem", fontWeight: 600, marginBottom: "0.4rem", color: "#92400e" }}>
                  🕐 ממתין בתור ({queue.pending.length})
                  {queue.running.length === 0 && (
                    <span style={{
                      marginInlineStart: "0.5rem",
                      fontSize: "0.7rem",
                      padding: "0.1rem 0.4rem",
                      borderRadius: "9999px",
                      background: "#e0e7ff",
                      color: "#3730a3",
                      fontWeight: 500,
                    }}>
                      אין worker פעיל — המשימות יחכו עד שיעלה אחד
                    </span>
                  )}
                </div>
                <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
                  {queue.pending.map((t) => {
                    const ageMs = t.created_at ? Date.now() - new Date(t.created_at).getTime() : 0;
                    const isOld = ageMs > 60 * 60 * 1000;  // 1 hour
                    return (
                      <li key={t.task_id} style={{
                        padding: "0.4rem 0.6rem",
                        marginBottom: "0.2rem",
                        background: isOld ? "#fef3c7" : "#fffbeb",
                        border: isOld ? "1px solid #fbbf24" : "1px solid #fde68a",
                        borderRadius: "4px",
                        fontSize: "0.85rem",
                        display: "flex",
                        justifyContent: "space-between",
                        gap: "0.5rem",
                        alignItems: "center",
                      }}>
                        <Link to={`/versions/${t.dataset_id}`} style={{ flex: 1 }}>{t.dataset_title}</Link>
                        <span className="text-muted" style={{ fontSize: "0.8rem", whiteSpace: "nowrap" }}>
                          נוסף {formatRelative(t.created_at)}
                        </span>
                        <button
                          onClick={() => handleCancelTask(t.task_id, t.dataset_title, "pending")}
                          title="הסר מהתור"
                          style={{
                            background: "none",
                            border: "1px solid #92400e",
                            color: "#92400e",
                            cursor: "pointer",
                            fontSize: "0.7rem",
                            padding: "0.15rem 0.4rem",
                            borderRadius: "4px",
                            whiteSpace: "nowrap",
                          }}
                        >
                          ✕ הסר
                        </button>
                      </li>
                    );
                  })}
                </ul>
              </div>
            )}

            {/* Failed */}
            {queue.failed.length > 0 && (
              <div>
                <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: "0.4rem" }}>
                  <span style={{ fontSize: "0.85rem", fontWeight: 600, color: "#991b1b", flex: 1 }}>
                    ⚠ כשלים אחרונים — 24 שעות ({queue.failed.length})
                  </span>
                  {/* Plain-text digest of every failure (title | phase | time +
                      the FULL error) — built for pasting into a debugging chat. */}
                  <CopyListButton
                    label="העתק שגיאות"
                    getText={() =>
                      [
                        `כשלים אחרונים — תור גירוד (${queue.failed.length}), הועתק ${new Date().toLocaleString("he-IL")}`,
                        ...queue.failed.map((f, i) =>
                          `${i + 1}. ${f.dataset_title}` +
                          (f.phase ? ` | phase=${f.phase}` : "") +
                          (workerLabel(f) ? ` | worker=${workerLabel(f)}` : "") +
                          (f.completed_at ? ` | ${f.completed_at}` : "") +
                          "\n   " + (f.error || "(אין הודעת שגיאה)").replace(/\n/g, "\n   ")
                        ),
                      ].join("\n\n")
                    }
                  />
                </div>
                <ul style={{ listStyle: "none", padding: 0, margin: 0 }}>
                  {queue.failed.map((t) => (
                    <li key={t.task_id} style={{
                      padding: "0.4rem 0.6rem",
                      marginBottom: "0.2rem",
                      background: "#fef2f2",
                      border: "1px solid #fecaca",
                      borderRadius: "4px",
                      fontSize: "0.85rem",
                    }}>
                      <div className="flex-between" style={{ gap: "0.5rem", alignItems: "center" }}>
                        <Link to={`/versions/${t.dataset_id}`} style={{ flex: 1 }}>{t.dataset_title}</Link>
                        <span className="text-muted" style={{ fontSize: "0.8rem", whiteSpace: "nowrap" }}>
                          {formatRelative(t.completed_at)}
                        </span>
                        <button
                          onClick={() => handleRetryFailed(t.task_id, t.dataset_id, t.dataset_title)}
                          title="ניסיון חוזר"
                          style={{
                            background: "none",
                            border: "1px solid #166534",
                            color: "#166534",
                            cursor: "pointer",
                            fontSize: "0.7rem",
                            padding: "0.15rem 0.4rem",
                            borderRadius: "4px",
                            whiteSpace: "nowrap",
                          }}
                        >
                          ↻ נסה שוב
                        </button>
                        <button
                          onClick={() => handleDismissFailed(t.task_id, t.dataset_title)}
                          title="מחק מהיומן"
                          style={{
                            background: "none",
                            border: "1px solid #991b1b",
                            color: "#991b1b",
                            cursor: "pointer",
                            fontSize: "0.7rem",
                            padding: "0.15rem 0.4rem",
                            borderRadius: "4px",
                            whiteSpace: "nowrap",
                          }}
                        >
                          ✕ מחק
                        </button>
                      </div>
                      {workerLabel(t) && (
                        <div className="text-muted" style={{ marginTop: "0.2rem", fontSize: "0.75rem" }}>
                          מכונה: <strong>{workerLabel(t)}</strong>
                        </div>
                      )}
                      {t.error && (
                        <div style={{
                          marginTop: "0.2rem",
                          fontSize: "0.75rem",
                          color: "#991b1b",
                          wordBreak: "break-word",
                          whiteSpace: "pre-wrap",
                          fontFamily: "monospace",
                          maxHeight: "10rem",
                          overflowY: "auto",
                          padding: "0.25rem 0.4rem",
                          background: "#fff",
                          border: "1px solid #fecaca",
                          borderRadius: "3px",
                        }}>
                          {t.error}
                        </div>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}
      </section>
      </>)}

      {tab === "schedule" && (<>
      {/* Schedule preview — when does each active dataset's next poll fire */}
      <section className="card mb-2">
        <div className="page-header flex-between" style={{ flexWrap: "wrap", gap: "0.75rem" }}>
          <h2 style={{ fontSize: "1.25rem", fontWeight: 700 }}>
            תזמון משימות עתידיות
            {schedule && (
              <span className="text-muted" style={{ fontSize: "0.75rem", fontWeight: 400, marginInlineStart: "0.5rem" }}>
                ({schedule.jobs.length} מאגרים פעילים
                {!schedule.scheduler_running && " — ⚠ scheduler לא רץ"}
                )
              </span>
            )}
          </h2>
          <button onClick={loadSchedule} className="btn-secondary" style={{ fontSize: "0.75rem", padding: "0.25rem 0.6rem" }}>
            רענן ↻
          </button>
        </div>

        {!schedule ? (
          <div className="text-muted" style={{ fontSize: "0.85rem", padding: "0.75rem" }}>טוען...</div>
        ) : schedule.jobs.length === 0 ? (
          <div className="empty-state" style={{ padding: "1rem" }}>אין מאגרים פעילים מתוזמנים.</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
              <thead>
                <tr style={{ background: "#f9fafb", textAlign: "right" }}>
                  <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>מאגר</th>
                  <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>תדירות</th>
                  <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>נדגם לאחרונה</th>
                  <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>הפעלה הבאה</th>
                  <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>בעוד</th>
                </tr>
              </thead>
              <tbody>
                {schedule.jobs.map((j) => {
                  const overdue = j.seconds_until_next_run !== null && j.seconds_until_next_run < 0;
                  return (
                    <tr key={j.dataset_id} style={{
                      borderBottom: "1px solid #f0f0f0",
                      background: !j.scheduled ? "#fef2f2" : overdue ? "#fef3c7" : undefined,
                    }}>
                      <td style={{ padding: "0.4rem 0.6rem" }}>
                        <Link to={`/versions/${j.dataset_id}`}>{j.title}</Link>
                        {!j.scheduled && (
                          <span className="text-muted" style={{ fontSize: "0.7rem", marginInlineStart: "0.4rem", color: "#991b1b" }}>
                            (לא מתוזמן)
                          </span>
                        )}
                      </td>
                      <td style={{ padding: "0.4rem 0.6rem" }} className="text-muted">
                        {formatIntervalLabel(j.poll_interval)}
                      </td>
                      <td style={{ padding: "0.4rem 0.6rem" }} className="text-muted">
                        {j.last_polled_at
                          ? new Date(j.last_polled_at).toLocaleString()
                          : "אף פעם"}
                      </td>
                      <td style={{ padding: "0.4rem 0.6rem" }} className="text-muted">
                        {j.next_run_at
                          ? new Date(j.next_run_at).toLocaleString()
                          : "—"}
                      </td>
                      <td style={{ padding: "0.4rem 0.6rem", whiteSpace: "nowrap" }}>
                        {j.seconds_until_next_run === null ? (
                          <span className="text-muted">—</span>
                        ) : overdue ? (
                          <span style={{ color: "#92400e", fontWeight: 600 }}>
                            פיגור: {formatDuration(-j.seconds_until_next_run)}
                          </span>
                        ) : (
                          <span>{formatDuration(j.seconds_until_next_run)}</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {schedule.orphan_jobs.length > 0 && (
              <div style={{ marginTop: "0.6rem", fontSize: "0.8rem", color: "#92400e" }}>
                ⚠ {schedule.orphan_jobs.length} jobs יתומים בלי dataset תואם — Restart Render כדי לנקות.
              </div>
            )}
          </div>
        )}
      </section>
      </>)}

      {tab === "push_jobs" && <DatastorePushJobsPanel />}

      {tab === "log" && <ActivityLogPanel />}

      {tab === "mcp" && <McpUsersPanel />}

      {tab === "content" && <PageContentPanel />}

      {tab === "requests" && (<>
      {/* Section 1: Pending Requests */}

      {requests.length === 0 ? (
        <div className="empty-state" style={{ padding: "1.5rem" }}>{t("admin.empty")}</div>
      ) : (
        <div className="grid grid-2 mb-2">
          {requests.map((req) => {
            const badge = sourceBadge(req.source_type, req.organization, req.ckan_id);
            return (
            <article key={req.id} className="card" style={{ borderRight: `4px solid ${badge.accent}` }}>
              <div className="flex-between" style={{ marginBottom: "0.5rem", gap: "0.5rem" }}>
                <input
                  type="text"
                  value={titleOverrides[req.id] ?? req.title}
                  onChange={(e) => setTitleOverrides((prev) => ({
                    ...prev, [req.id]: e.target.value,
                  }))}
                  aria-label="שם המאגר"
                  style={{
                    flex: 1,
                    fontSize: "1rem",
                    fontWeight: 600,
                    padding: "0.3rem 0.5rem",
                    border: "1px solid var(--border)",
                    borderRadius: "4px",
                    background: "var(--surface)",
                  }}
                />
                <span style={{
                  display: "inline-block",
                  padding: "0.15rem 0.5rem",
                  borderRadius: "9999px",
                  fontSize: "0.65rem",
                  fontWeight: 600,
                  background: badge.bg,
                  color: badge.fg,
                  flexShrink: 0,
                }}>
                  {badge.label}
                </span>
              </div>
              {!isCkanLike(req.source_type) && req.source_url && (
                <p className="text-sm text-muted" style={{ wordBreak: "break-all", direction: "ltr" }}>
                  <a href={req.source_url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                    {req.source_url}
                  </a>
                </p>
              )}
              {req.organization && isCkanLike(req.source_type) && <p className="text-sm text-muted">{req.organization}</p>}
              <div className="text-sm mb-1">
                <div>{t("admin.requester")}: {req.requester_name} ({req.requester_email})</div>
                <div>{t("admin.requested_at")}: {new Date(req.created_at).toLocaleString()}</div>
                <div>{t("tracked.poll_interval")}: {formatIntervalLabel(req.poll_interval)}</div>
              </div>
              <div className="text-sm mb-1">
                <label style={{ fontSize: "0.85rem" }}>
                  שנה תדירות:{" "}
                  <select
                    value={intervalOverrides[req.id] ?? ""}
                    onChange={(e) => setIntervalOverrides((prev) => ({
                      ...prev, [req.id]: e.target.value ? Number(e.target.value) : undefined!,
                    }))}
                    style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem" }}
                  >
                    <option value="">לפי הבקשה</option>
                    {INTERVAL_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                  </select>
                </label>
              </div>
              <div className="text-sm mb-1">
                <label style={{ fontSize: "0.85rem" }}>
                  {t("organizations.admin_column")}:{" "}
                  <select
                    value={orgOverrides[req.id] ?? req.organization_id ?? ""}
                    onChange={(e) => setOrgOverrides((prev) => ({
                      ...prev, [req.id]: e.target.value,
                    }))}
                    style={{ width: "auto", maxWidth: "100%", padding: "0.2rem 0.4rem", fontSize: "0.8rem" }}
                  >
                    <option value="">{t("organizations.select_placeholder")}</option>
                    {orgs.map((o) => <option key={o.id} value={o.id}>{o.title}</option>)}
                  </select>
                </label>
              </div>
              {isCkanLike(req.source_type) && (
                <div className="text-sm mb-1" style={{ display: "flex", alignItems: "center", gap: "0.4rem", flexWrap: "wrap" }}>
                  <span style={{ fontWeight: 500 }}>קבצים נבחרים:</span>
                  <span className="badge" style={{ fontSize: "0.7rem" }}>
                    {req.resource_ids && req.resource_ids.length > 0
                      ? `${req.resource_ids.length} נבחרו`
                      : req.resource_id
                        ? "1 נבחר (legacy)"
                        : "טרם נבחרו"}
                  </span>
                  <button
                    className="btn-secondary"
                    onClick={() => setResourcePickerFor({ kind: "pending", req })}
                    style={{ fontSize: "0.75rem", padding: "0.15rem 0.5rem" }}
                  >
                    בחר קבצים
                  </button>
                </div>
              )}
              <div className="text-sm mb-1" style={{ display: "flex", alignItems: "center", gap: "0.4rem", flexWrap: "wrap" }}>
                <span style={{ fontWeight: 500 }}>{t("admin.storage_target") || "יעד אחסון"}:</span>
                <select
                  value={storageOverrides[req.id] ?? req.storage_target ?? "r2"}
                  onChange={(e) => setStorageOverrides((prev) => ({
                    ...prev, [req.id]: e.target.value as StorageTarget,
                  }))}
                  style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem" }}
                >
                  {storageTargetOptions(req.neon_eligible ?? isCkanLike(req.source_type)).map((o) => (
                    <option key={o.value} value={o.value} disabled={o.disabled}>{o.label}</option>
                  ))}
                </select>
              </div>
              <div className="flex mt-1">
                <button className="btn-primary" onClick={() => handleApprove(req.id)} disabled={processing.has(req.id)}>
                  {processing.has(req.id) ? "..." : t("admin.approve")}
                </button>
                <button className="btn-danger" onClick={() => handleReject(req.id)} disabled={processing.has(req.id)}>
                  {processing.has(req.id) ? "..." : t("admin.reject")}
                </button>
              </div>
            </article>
            );
          })}
        </div>
      )}

      </>)}

      {tab === "datasets" && (<>
      {/* Section 2: Active Datasets Management */}
      <div className="page-header">
        <h2 style={{ fontSize: "1rem", fontWeight: 600, color: "var(--text-muted)" }}>{activeDatasets.length} מאגרים</h2>
      </div>

      {/* OVER full-version coverage audit (#4) */}
      <div className="card mb-2" style={{ padding: "0.75rem 1rem" }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem", flexWrap: "wrap" }}>
          <strong style={{ fontSize: "0.95rem" }}>כיסוי גרסה מלאה על OVER</strong>
          <button className="btn-secondary" onClick={runCoverageScan} disabled={coverageBusy}
            style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}>
            {coverageBusy ? "סורק…" : "סרוק"}
          </button>
          {coverage && coverage.missing.length > 0 && (
            <button className="btn-primary" onClick={runCoverageFix} disabled={coverageBusy}
              style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}>
              צור גרסה מלאה ל-{coverage.missing.length} החסרים
            </button>
          )}
        </div>
        {coverageMsg && <div className="text-sm" style={{ marginTop: "0.4rem", color: "#0369a1" }}>{coverageMsg}</div>}
        {coverage && (
          <div className="text-sm" style={{ marginTop: "0.5rem", lineHeight: 1.6 }}>
            <div>
              ✅ מכוסים: <strong>{coverage.covered}</strong> / {coverage.total_active} מאגרים פעילים
              {coverage.local_only.length > 0 && <> · 🗂️ מקומיים (מחוץ ל-OVER מבחירה): <strong>{coverage.local_only.length}</strong></>}
            </div>
            {coverage.missing.length === 0 ? (
              <div style={{ color: "#15803d" }}>כל המאגרים (שאינם מקומיים) כוללים לפחות גרסה מלאה אחת על OVER.</div>
            ) : (
              <div style={{ marginTop: "0.3rem" }}>
                <div style={{ color: "#b91c1c", fontWeight: 600 }}>⚠️ חסרה גרסה מלאה ({coverage.missing.length}):</div>
                <ul style={{ margin: "0.25rem 0 0", paddingInlineStart: "1.2rem" }}>
                  {coverage.missing.slice(0, 30).map((m) => (
                    <li key={m.id}>
                      <Link to={`/versions/${m.id}`}>{m.title}</Link>{" "}
                      <span className="text-muted" style={{ fontSize: "0.75rem" }}>({m.source_type} · {m.storage_target})</span>
                    </li>
                  ))}
                  {coverage.missing.length > 30 && <li className="text-muted">…ועוד {coverage.missing.length - 30}</li>}
                </ul>
              </div>
            )}
          </div>
        )}
      </div>

      {activeDatasets.length === 0 ? (
        <div className="empty-state">אין מאגרים פעילים</div>
      ) : (
        <div>
          <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
              {activeDatasets.map((ds) => (
                <div key={ds.id} className="card" style={{ padding: "0.85rem 1rem", boxShadow: "var(--shadow-sm)" }}>
                  {/* ── Name / status / source ── */}
                  <div style={{ marginBottom: "0.6rem" }}>
                    {editingTitleFor === ds.id ? (
                      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center" }}>
                        <input
                          type="text"
                          value={editingTitleValue}
                          onChange={(e) => setEditingTitleValue(e.target.value)}
                          onKeyDown={(e) => {
                            if (e.key === "Enter") saveEditTitle(ds.id);
                            if (e.key === "Escape") cancelEditTitle();
                          }}
                          autoFocus
                          aria-label="שם המאגר"
                          style={{
                            flex: 1,
                            padding: "0.25rem 0.4rem",
                            fontSize: "0.85rem",
                            border: "1px solid var(--primary)",
                            borderRadius: "4px",
                          }}
                        />
                        <button
                          className="btn-primary"
                          style={{ padding: "0.2rem 0.5rem", fontSize: "0.7rem" }}
                          onClick={() => saveEditTitle(ds.id)}
                          disabled={savingTitle}
                          title="שמור"
                        >
                          ✓
                        </button>
                        <button
                          className="btn-secondary"
                          style={{ padding: "0.2rem 0.5rem", fontSize: "0.7rem" }}
                          onClick={cancelEditTitle}
                          disabled={savingTitle}
                          title="בטל"
                        >
                          ✕
                        </button>
                      </div>
                    ) : (
                      <div style={{ display: "flex", gap: "0.3rem", alignItems: "center", flexWrap: "wrap" }}>
                        <Link to={`/versions/${ds.id}`} style={{ fontWeight: 500 }}>
                          {ds.title}
                        </Link>
                        {(() => {
                          const s = datasetStatusMap.get(ds.id);
                          if (!s) return null;
                          const config = {
                            running: { icon: "🔄", color: "#16a34a", label: "בעבודה" },
                            pending: { icon: "🕐", color: "#92400e", label: "בתור" },
                            failed:  { icon: "⚠️", color: "#991b1b", label: "נכשל" },
                          }[s.kind];
                          return (
                            <span
                              title={s.tooltip ? `${config.label} — ${s.tooltip}` : config.label}
                              style={{ fontSize: "0.9rem", color: config.color }}
                            >
                              {config.icon}
                            </span>
                          );
                        })()}
                        <button
                          onClick={() => startEditTitle(ds.id, ds.title)}
                          aria-label="ערוך שם"
                          title="ערוך שם"
                          style={{
                            background: "none",
                            border: "none",
                            cursor: "pointer",
                            fontSize: "0.85rem",
                            padding: "0.1rem 0.3rem",
                            color: "var(--text-muted)",
                            lineHeight: 1,
                          }}
                        >
                          ✏
                        </button>
                        {(() => {
                          const b = sourceBadge(ds.source_type, ds.organization, ds.ckan_id);
                          return (
                            <span style={{
                              display: "inline-block",
                              padding: "0.1rem 0.45rem",
                              borderRadius: "9999px",
                              fontSize: "0.65rem",
                              fontWeight: 600,
                              background: b.bg,
                              color: b.fg,
                            }}>
                              {b.label}
                            </span>
                          );
                        })()}
                      </div>
                    )}
                    {!isCkanLike(ds.source_type) && ds.source_url && (
                      <div style={{ fontSize: "0.75rem", marginTop: "0.2rem", direction: "ltr" }}>
                        <a href={ds.source_url} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>
                          {ds.source_url}
                        </a>
                      </div>
                    )}
                    {ds.last_error && (
                      <div
                        title={ds.last_error}
                        style={{
                          fontSize: "0.7rem",
                          marginTop: "0.25rem",
                          padding: "0.2rem 0.4rem",
                          background: "#fee2e2",
                          color: "#991b1b",
                          borderRadius: "4px",
                          maxWidth: "28rem",
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                          cursor: "help",
                        }}
                      >
                        ⚠ {ds.last_error}
                      </div>
                    )}
                    {isCkanLike(ds.source_type) && (
                      <div style={{ marginTop: "0.25rem", display: "flex", alignItems: "center", gap: "0.3rem", flexWrap: "wrap", fontSize: "0.7rem" }}>
                        <span className="badge" style={{ fontSize: "0.65rem" }}>
                          {ds.resource_ids && ds.resource_ids.length > 0
                            ? `${ds.resource_ids.length} קבצים`
                            : ds.resource_id
                              ? "קובץ אחד (legacy)"
                              : "כל הקבצים"}
                        </span>
                        <button
                          onClick={() => setResourcePickerFor({ kind: "active", ds })}
                          style={{
                            background: "none",
                            border: "1px solid var(--border)",
                            borderRadius: "4px",
                            padding: "0.05rem 0.4rem",
                            fontSize: "0.7rem",
                            cursor: "pointer",
                            color: "var(--text-muted)",
                          }}
                        >
                          ערוך קבצים
                        </button>
                        {ds.new_resources_at_source && ds.new_resources_at_source.length > 0 && (
                          <>
                            <button
                              onClick={() => setResourcePickerFor({ kind: "active", ds })}
                              title={ds.new_resources_at_source.map((r) => r.name || r.id).join(", ")}
                              style={{
                                background: "#fef3c7",
                                color: "#92400e",
                                border: "1px solid #f59e0b",
                                borderRadius: "9999px",
                                padding: "0.05rem 0.5rem",
                                fontSize: "0.7rem",
                                fontWeight: 600,
                                cursor: "pointer",
                              }}
                            >
                              ✨ {ds.new_resources_at_source.length} קבצים חדשים זוהו
                            </button>
                            <button
                              onClick={() => handleDismissNewResources(ds.id)}
                              title="התעלם מהקבצים החדשים"
                              style={{
                                background: "none",
                                border: "none",
                                fontSize: "0.7rem",
                                color: "var(--text-muted)",
                                cursor: "pointer",
                              }}
                            >
                              ✕ התעלם
                            </button>
                          </>
                        )}
                      </div>
                    )}
                  </div>
                  {/* ── Controls (labeled, wrapping) ── */}
                  <div style={{ display: "flex", flexWrap: "wrap", gap: "0.85rem", alignItems: "flex-start" }}>
                    <div style={{ flex: "1 1 12rem", minWidth: "10rem" }}>
                      <div style={admFieldLabel}>{t("organizations.admin_column")}</div>
                    <select
                      value={ds.organization_id ?? ""}
                      onChange={(e) => handleChangeOrg(ds.id, e.target.value)}
                      style={{
                        width: "100%",
                        padding: "0.2rem 0.4rem",
                        fontSize: "0.8rem",
                        border: "1px solid var(--border)",
                        borderRadius: "4px",
                      }}
                    >
                      <option value="">{t("organizations.select_placeholder")}</option>
                      {orgs.map((o) => <option key={o.id} value={o.id}>{o.title}</option>)}
                    </select>
                    {ds.organization && !ds.organization_id && (
                      <div className="text-muted" style={{ fontSize: "0.7rem", marginTop: "0.2rem" }}>
                        {ds.organization}
                      </div>
                    )}
                    </div>
                    <div style={{ flex: "1 1 12rem", minWidth: "10rem" }}>
                      <div style={admFieldLabel}>תגיות</div>
                    <TagPicker
                      value={ds.tags || []}
                      available={availableTags}
                      onChange={(next) => handleSetDatasetTags(ds.id, next)}
                      onCreate={handleCreateTag}
                    />
                    </div>
                    <div style={{ flex: "0 1 9rem", minWidth: "8rem" }}>
                      <div style={admFieldLabel}>תדירות</div>
                    <select
                      value={ds.poll_interval}
                      onChange={(e) => handleUpdateInterval(ds.id, Number(e.target.value))}
                      style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem", border: "1px solid var(--border)", borderRadius: "4px" }}
                    >
                      {INTERVAL_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                    </select>
                    </div>
                    <div style={{ flex: "1 1 14rem", minWidth: "12rem" }}>
                      <div style={admFieldLabel}>{t("admin.storage_mode") || "אחסון"}</div>
                    <div style={{ display: "flex", flexDirection: "column", gap: "0.25rem" }}>
                      <select
                        value={ds.storage_mode || "full_snapshot"}
                        onChange={(e) =>
                          handleUpdateStorageMode(
                            ds.id,
                            e.target.value as "full_snapshot" | "append_only",
                          )
                        }
                        style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem", border: "1px solid var(--border)", borderRadius: "4px" }}
                      >
                        <option value="full_snapshot">{t("admin.storage_full") || "שמירה מלאה"}</option>
                        <option value="append_only">{t("admin.storage_append") || "תוספת בלבד"}</option>
                      </select>
                      {ds.storage_mode === "append_only" && (
                        <>
                        <input
                          type="text"
                          defaultValue={ds.append_key || ""}
                          placeholder={t("admin.append_key_placeholder") || "מפתח (אופציונלי)"}
                          onBlur={(e) => {
                            if ((e.target.value || "") !== (ds.append_key || "")) {
                              handleUpdateAppendKey(ds.id, e.target.value);
                            }
                          }}
                          style={{ width: "10rem", padding: "0.2rem 0.4rem", fontSize: "0.75rem", border: "1px solid var(--border)", borderRadius: "4px" }}
                        />
                        <label
                          title="מצב DIFF: לוכד גם שינויים בשורות קיימות (לא רק שורות חדשות), עם מיגרציה חד-פעמית כבדה. שמור למקרים קיצוניים בלבד (כמו מאגר הרכב)."
                          style={{ display: "flex", alignItems: "center", gap: "0.3rem", fontSize: "0.72rem", whiteSpace: "nowrap", color: ds.capture_changes ? "#b45309" : "var(--text-muted)", fontWeight: ds.capture_changes ? 600 : 400 }}
                        >
                          <input
                            type="checkbox"
                            checked={!!ds.capture_changes}
                            onChange={(e) => handleUpdateCaptureChanges(ds.id, e.target.checked)}
                          />
                          ⚠ DIFF (לכידת שינויים)
                        </label>
                        </>
                      )}
                      <select
                        value={ds.storage_target || "r2"}
                        onChange={(e) =>
                          handleUpdateStorageTarget(ds.id, e.target.value as StorageTarget)
                        }
                        title="תוכנית האחסון: היכן נשמרים הקבצים (מקומי / R2 / ODATA) והאם השורות הטבלאיות נשמרות גם ב-NEON (DB לתשאול). NEON זמין למאגרי data.gov.il טבלאיים בלבד."
                        style={{ width: "auto", padding: "0.2rem 0.4rem", fontSize: "0.8rem", border: "1px solid var(--border)", borderRadius: "4px", color: ds.storage_target === "local" ? "#b91c1c" : (ds.storage_target?.includes("neon") ? "#0369a1" : undefined) }}
                      >
                        {storageTargetOptions(ds.neon_eligible ?? (ds.source_type === "ckan")).map((o) => (
                          <option key={o.value} value={o.value} disabled={o.disabled}>{o.label}</option>
                        ))}
                      </select>
                    </div>
                    </div>
                    <div style={{ flex: "1 1 11rem", minWidth: "9rem" }} className="text-sm">
                      <div style={admFieldLabel}>גרסאות / בדיקה</div>
                    <div>
                      <Link to={`/versions/${ds.id}`}>{ds.version_count}</Link>
                      <span className="text-muted"> גרסאות</span>
                    </div>
                    <div className="text-muted" style={{ fontSize: "0.7rem", marginTop: "0.2rem" }}>
                      {ds.last_polled_at ? new Date(ds.last_polled_at).toLocaleString() : "—"}
                    </div>
                    <div className="text-muted" style={{ fontSize: "0.7rem", marginTop: "0.2rem" }}>
                      {sizes
                        ? `סך גודל: ${formatBytes(sizeByDsId.get(ds.id))}`
                        : sizesLoading
                          ? "סך גודל: ..."
                          : ""}
                    </div>
                    {suggestDeltaByDsId.has(ds.id) && (
                      <div
                        title="המאגר נשמר כעת רק כ-stub של metadata + 200 שורות sample כי הוא מעל 50k שורות. שנה את 'אופן שמירה' ל'תוספת בלבד' והגדר את שם עמודת המפתח (למשל מספר רישוי) — אז כל גרסה תכלול רק שורות חדשות, באמצעות הזרמה דרך datastore_search ב-32k שורות לעמוד."
                        style={{
                          marginTop: "0.25rem",
                          fontSize: "0.7rem",
                          color: "#92400e",
                          background: "#fef3c7",
                          border: "1px solid #fcd34d",
                          borderRadius: "4px",
                          padding: "0.2rem 0.4rem",
                          cursor: "help",
                          display: "inline-block",
                        }}
                      >
                        💡 שקול ארכוב delta (append_only + מפתח)
                      </div>
                    )}
                    </div>
                  </div>
                  {/* ── Actions ── */}
                  <div style={{ marginTop: "0.75rem", paddingTop: "0.6rem", borderTop: "1px solid var(--border)", display: "flex", gap: "0.4rem", alignItems: "center" }}>
                    <div className="flex" style={{ gap: "0.4rem" }}>
                      <div style={{ display: "flex", flexDirection: "column", gap: "0.2rem" }}>
                        <button
                          className="btn-secondary"
                          style={{ padding: "0.3rem 1rem", fontSize: "0.8rem" }}
                          onClick={() => handlePoll(ds.id)}
                          disabled={processing.has(ds.id)}
                        >
                          {processing.has(ds.id) ? "..." : "דגום"}
                        </button>
                        {pollToast?.id === ds.id && (
                          <span style={{
                            fontSize: "0.7rem",
                            padding: "0.15rem 0.4rem",
                            borderRadius: "4px",
                            background: pollToast.ok ? "#dcfce7" : "#fee2e2",
                            color: pollToast.ok ? "#166534" : "#991b1b",
                            whiteSpace: "nowrap",
                          }}>
                            {pollToast.msg}
                          </span>
                        )}
                      </div>
                      <button
                        className="btn-danger"
                        style={{ padding: "0.3rem 1rem", fontSize: "0.8rem" }}
                        onClick={() => handleDelete(ds.id, ds.title)}
                        disabled={processing.has(ds.id)}
                      >
                        מחק
                      </button>
                    </div>
                  </div>
                </div>
              ))}
          </div>
        </div>
      )}

      </>)}

      {tab === "orgs" && (<>
      {/* Section 3: Organizations hierarchy */}
      <div className="page-header">
        <h2 style={{ fontSize: "1rem", fontWeight: 600, color: "var(--text-muted)" }}>
          {orgs.length} ארגונים
        </h2>
      </div>
      {orgs.length === 0 ? (
        <div className="empty-state">אין ארגונים. סנכרן קודם מ-data.gov.il או gov.il.</div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", background: "var(--surface)", borderRadius: "var(--radius)", overflow: "hidden", boxShadow: "var(--shadow-sm)" }}>
            <thead>
              <tr style={{ background: "var(--primary-50)", borderBottom: "2px solid var(--border)" }}>
                <th style={thStyle}>ארגון</th>
                <th style={thStyle}>מקור</th>
                <th style={thStyle}>הורה</th>
                <th style={thStyle}>מאגרים</th>
                <th style={thStyle}>תת-יחידות</th>
              </tr>
            </thead>
            <tbody>
              {[...orgs]
                .sort((a, b) => (a.parent_title || "").localeCompare(b.parent_title || "") || a.title.localeCompare(b.title))
                .map((o) => (
                <tr key={o.id} style={{ borderBottom: "1px solid var(--border)" }}>
                  <td style={tdStyle}>
                    <Link to={`/organizations/${o.id}`} style={{ fontWeight: 500 }}>
                      {o.title}
                    </Link>
                  </td>
                  <td style={tdStyle}>
                    <div style={{ display: "flex", gap: "0.3rem", flexWrap: "wrap" }}>
                      {o.data_gov_il_id && (
                        <span style={{
                          display: "inline-block",
                          padding: "0.1rem 0.4rem",
                          borderRadius: "9999px",
                          fontSize: "0.65rem",
                          fontWeight: 600,
                          background: "#ccfbf1",
                          color: "#0f766e",
                        }}>DATA.GOV.IL</span>
                      )}
                      {o.gov_il_url_name && (
                        <span style={{
                          display: "inline-block",
                          padding: "0.1rem 0.4rem",
                          borderRadius: "9999px",
                          fontSize: "0.65rem",
                          fontWeight: 600,
                          background: "#fef3c7",
                          color: "#92400e",
                        }}>GOV.IL</span>
                      )}
                    </div>
                  </td>
                  <td style={tdStyle}>
                    <select
                      value={o.parent_id ?? ""}
                      onChange={(e) => handleChangeParentOrg(o.id, e.target.value)}
                      style={{
                        width: "100%",
                        maxWidth: 220,
                        padding: "0.2rem 0.4rem",
                        fontSize: "0.8rem",
                        border: "1px solid var(--border)",
                        borderRadius: "4px",
                      }}
                    >
                      <option value="">— ללא הורה —</option>
                      {orgs
                        .filter((p) => p.id !== o.id)
                        .sort((a, b) => a.title.localeCompare(b.title))
                        .map((p) => (
                          <option key={p.id} value={p.id}>{p.title}</option>
                        ))}
                    </select>
                  </td>
                  <td style={tdStyle} className="text-sm">{o.dataset_count}</td>
                  <td style={tdStyle} className="text-sm">{o.children_count || 0}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      </>)}

      {tab === "tags" && (<>
      {/* Section 4: Tags management */}
      <div className="page-header">
        <h2 style={{ fontSize: "1rem", fontWeight: 600, color: "var(--text-muted)" }}>
          {availableTags.length} תגיות
        </h2>
      </div>
      {availableTags.length === 0 ? (
        <div className="empty-state">
          עדיין אין תגיות. ניתן להוסיף תגיות לכל מאגר ברשימה למעלה.
        </div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", background: "var(--surface)", borderRadius: "var(--radius)", overflow: "hidden", boxShadow: "var(--shadow-sm)" }}>
            <thead>
              <tr style={{ background: "var(--primary-50)", borderBottom: "2px solid var(--border)" }}>
                <th style={thStyle}>תגית</th>
                <th style={thStyle}>מאגרים</th>
                <th style={thStyle}>פעולות</th>
              </tr>
            </thead>
            <tbody>
              {[...availableTags]
                .sort((a, b) => a.name.localeCompare(b.name, "he"))
                .map((tag) => (
                  <tr key={tag.id} style={{ borderBottom: "1px solid var(--border)" }}>
                    <td style={tdStyle}>
                      <Link to={`/tags/${tag.id}`} style={{ fontWeight: 500 }}>
                        {tag.name}
                      </Link>
                    </td>
                    <td style={tdStyle} className="text-sm">{tag.dataset_count}</td>
                    <td style={tdStyle}>
                      <button
                        className="btn-danger"
                        style={{ padding: "0.25rem 0.75rem", fontSize: "0.75rem" }}
                        onClick={() => handleDeleteTag(tag)}
                      >
                        מחק
                      </button>
                    </td>
                  </tr>
                ))}
            </tbody>
          </table>
        </div>
      )}

      </>)}

      </main>

      {resourcePickerFor && resourcePickerFor.kind === "active" && (
        <ResourcePickerModal
          ckanId={resourcePickerFor.ds.ckan_name || resourcePickerFor.ds.ckan_id}
          datasetTitle={resourcePickerFor.ds.title}
          initialSelected={
            resourcePickerFor.ds.resource_ids && resourcePickerFor.ds.resource_ids.length > 0
              ? resourcePickerFor.ds.resource_ids
              : resourcePickerFor.ds.resource_id
                ? [resourcePickerFor.ds.resource_id]
                : []
          }
          newResourceIds={(resourcePickerFor.ds.new_resources_at_source || []).map((r) => r.id)}
          onClose={() => setResourcePickerFor(null)}
          onSave={async (ids) => {
            await handleSaveResourceIds(resourcePickerFor.ds.id, ids, "active");
          }}
        />
      )}

      {resourcePickerFor && resourcePickerFor.kind === "pending" && (
        <ResourcePickerModal
          ckanId={resourcePickerFor.req.ckan_name || resourcePickerFor.req.ckan_id}
          datasetTitle={resourcePickerFor.req.title}
          initialSelected={
            resourcePickerFor.req.resource_ids && resourcePickerFor.req.resource_ids.length > 0
              ? resourcePickerFor.req.resource_ids
              : resourcePickerFor.req.resource_id
                ? [resourcePickerFor.req.resource_id]
                : []
          }
          onClose={() => setResourcePickerFor(null)}
          onSave={async (ids) => {
            await handleSaveResourceIds(resourcePickerFor.req.id, ids, "pending");
          }}
        />
      )}
    </div>
  );
}

const thStyle: React.CSSProperties = {
  textAlign: "start",
  padding: "0.5rem 0.5rem",
  fontSize: "0.8rem",
  fontWeight: 600,
  color: "var(--text)",
  overflowWrap: "anywhere",
  wordBreak: "break-word",
};

const tdStyle: React.CSSProperties = {
  padding: "0.5rem 0.5rem",
  verticalAlign: "top",
  fontSize: "0.85rem",
  overflowWrap: "anywhere",
  wordBreak: "break-word",
};

/**
 * Admin panel for the durable datastore-push queue.
 *
 * Lives at /admin#push_jobs. Replaces a frequent prod head-scratcher
 * ("dataset shows N rows but Download returns 404") with a clear
 * "this push failed, here's the error, click Retry" surface. Refresh
 * is manual + automatic on a 15s interval so running pushes show
 * progressing row counts without the admin reloading.
 */
function DatastorePushJobsPanel() {
  const [jobs, setJobs] = useState<import("../api/client").DatastorePushJob[]>([]);
  const [filter, setFilter] = useState<string>("");
  const [loading, setLoading] = useState(true);
  const [retrying, setRetrying] = useState<Set<string>>(new Set());
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const load = async () => {
    setLoading(true);
    setErrorMsg(null);
    try {
      const rows = await adminApi.datastoreJobs(filter || undefined);
      setJobs(rows);
    } catch (e) {
      setErrorMsg((e as Error)?.message ?? String(e));
    } finally {
      setLoading(false);
    }
  };
  useEffect(() => {
    load();
    const t = setInterval(load, 15000);
    return () => clearInterval(t);
    // load is stable enough; we recreate it via filter dep
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filter]);

  const handleRetry = async (id: string) => {
    setRetrying((s) => new Set(s).add(id));
    try {
      await adminApi.retryDatastoreJob(id);
      await load();
    } catch (e) {
      alert("ניסיון חוזר נכשל: " + ((e as Error)?.message ?? String(e)));
    } finally {
      setRetrying((s) => {
        const n = new Set(s);
        n.delete(id);
        return n;
      });
    }
  };

  // Compact status summary at the top — one number per state. Useful
  // for spotting trends without reading the whole table.
  const counts: Record<string, number> = { pending: 0, running: 0, success: 0, failed: 0 };
  for (const j of jobs) counts[j.status] = (counts[j.status] ?? 0) + 1;

  return (
    <section className="card mb-2">
      <div
        className="page-header flex-between"
        style={{ flexWrap: "wrap", gap: "0.75rem", alignItems: "center" }}
      >
        <h2 style={{ fontSize: "1.25rem", fontWeight: 700, margin: 0 }}>
          תור Datastore Push
          <span
            className="text-muted"
            style={{ fontSize: "0.75rem", fontWeight: 400, marginInlineStart: "0.5rem" }}
          >
            (queue עמיד שמחליף את FastAPI BackgroundTasks)
          </span>
        </h2>
        <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap" }}>
          {(["", "pending", "running", "success", "failed"] as const).map((f) => (
            <button
              key={f || "all"}
              onClick={() => setFilter(f)}
              className={filter === f ? "btn-primary" : "btn-secondary"}
              style={{ fontSize: "0.75rem", padding: "0.25rem 0.6rem" }}
            >
              {f === "" ? "הכל" : f}
              {f && counts[f] !== undefined && (
                <span style={{ marginInlineStart: "0.3rem", opacity: 0.7 }}>
                  ({counts[f]})
                </span>
              )}
            </button>
          ))}
          <button
            onClick={load}
            className="btn-secondary"
            style={{ fontSize: "0.75rem", padding: "0.25rem 0.6rem" }}
          >
            רענן ↻
          </button>
        </div>
      </div>

      {/* Inline status counters — visible even when no filter is active */}
      <div
        style={{
          display: "flex",
          gap: "1.5rem",
          padding: "0.25rem 0.6rem 0.75rem",
          fontSize: "0.8rem",
          flexWrap: "wrap",
          color: "var(--text-muted)",
        }}
      >
        <span>⏳ ממתינות: <strong>{counts.pending}</strong></span>
        <span>▶ רצות עכשיו: <strong>{counts.running}</strong></span>
        <span style={{ color: "#15803d" }}>✓ הצליחו: <strong>{counts.success}</strong></span>
        <span style={{ color: "#991b1b" }}>✗ נכשלו: <strong>{counts.failed}</strong></span>
      </div>

      {errorMsg && (
        <div role="alert" style={{ color: "#991b1b", padding: "0.5rem 0.6rem" }}>
          {errorMsg}
        </div>
      )}

      {loading && jobs.length === 0 ? (
        <div className="text-muted" style={{ fontSize: "0.85rem", padding: "0.75rem" }}>
          טוען...
        </div>
      ) : jobs.length === 0 ? (
        <div className="empty-state" style={{ padding: "1rem" }}>
          אין משימות בתור.
        </div>
      ) : (
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
            <thead>
              <tr style={{ background: "#f9fafb", textAlign: "right" }}>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>סטטוס</th>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>מאגר</th>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>resource_id</th>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>שורות</th>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>ניסיונות</th>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>נוצר</th>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>heartbeat</th>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>שגיאה</th>
                <th style={{ padding: "0.4rem 0.6rem", borderBottom: "1px solid var(--border)" }}>פעולות</th>
              </tr>
            </thead>
            <tbody>
              {jobs.map((j) => {
                const bg =
                  j.status === "failed"
                    ? "#fef2f2"
                    : j.status === "running"
                    ? "#fffbeb"
                    : j.status === "success"
                    ? "#f0fdf4"
                    : undefined;
                const fmtRows =
                  j.total_rows != null
                    ? `${j.rows_pushed.toLocaleString()} / ${j.total_rows.toLocaleString()}`
                    : j.rows_pushed.toLocaleString();
                return (
                  <tr key={j.id} style={{ borderBottom: "1px solid #f0f0f0", background: bg }}>
                    <td style={tdStyle}>
                      <span
                        style={{
                          fontSize: "0.7rem",
                          padding: "0.1rem 0.4rem",
                          borderRadius: 999,
                          background:
                            j.status === "failed" ? "#fecaca"
                            : j.status === "running" ? "#fde68a"
                            : j.status === "success" ? "#bbf7d0"
                            : "#e5e7eb",
                          color:
                            j.status === "failed" ? "#991b1b"
                            : j.status === "running" ? "#92400e"
                            : j.status === "success" ? "#166534"
                            : "#374151",
                          fontWeight: 600,
                        }}
                      >
                        {j.status}
                      </span>
                    </td>
                    <td style={tdStyle}>
                      {j.tracked_dataset_id ? (
                        <Link to={`/versions/${j.tracked_dataset_id}`}>
                          {j.tracked_dataset_title || j.tracked_dataset_id}
                        </Link>
                      ) : (
                        <span className="text-muted">—</span>
                      )}
                    </td>
                    <td style={{ ...tdStyle, fontFamily: "monospace", fontSize: "0.75rem" }}>
                      {j.resource_id.slice(0, 8)}…
                    </td>
                    <td style={tdStyle}>{fmtRows}</td>
                    <td style={tdStyle}>{j.attempts}</td>
                    <td style={{ ...tdStyle, whiteSpace: "nowrap" }} className="text-muted">
                      {formatRelative(j.created_at)}
                    </td>
                    <td style={{ ...tdStyle, whiteSpace: "nowrap" }} className="text-muted">
                      {formatRelative(j.updated_at)}
                    </td>
                    <td style={{ ...tdStyle, fontSize: "0.75rem", color: "#991b1b", maxWidth: 360 }}>
                      {j.error || ""}
                    </td>
                    <td style={tdStyle}>
                      {(j.status === "failed" || j.status === "success") && (
                        <button
                          onClick={() => handleRetry(j.id)}
                          disabled={retrying.has(j.id)}
                          className="btn-secondary"
                          style={{
                            fontSize: "0.75rem",
                            padding: "0.2rem 0.55rem",
                            cursor: retrying.has(j.id) ? "wait" : "pointer",
                            opacity: retrying.has(j.id) ? 0.6 : 1,
                          }}
                        >
                          {retrying.has(j.id) ? "..." : j.status === "success" ? "הרץ שוב" : "נסה שוב"}
                        </button>
                      )}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
