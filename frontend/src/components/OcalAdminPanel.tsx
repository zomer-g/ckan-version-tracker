import { useCallback, useEffect, useState } from "react";
import {
  ocalAdmin, OcalAdminSource, OcalAdminPerson, OcalAdminOrg,
  OcalCandidate, OcalException, OcalEntity, OcalAutoImportLog, OcalDashboard,
} from "../api/client";

type Section = "dashboard" | "sources" | "candidates" | "automation" | "people" | "orgs" | "entities" | "content" | "exceptions";
const SECTIONS: [Section, string][] = [
  ["dashboard", "סקירה"],
  ["sources", "יומנים"],
  ["candidates", "מועמדים חדשים"],
  ["automation", "אוטומציה"],
  ["exceptions", "נדחו"],
  ["entities", "ישויות"],
  ["people", "אנשים"],
  ["orgs", "ארגונים"],
  ["content", "טקסטים"],
];

const btn: React.CSSProperties = {
  padding: "0.25rem 0.6rem", fontSize: "0.78rem", borderRadius: 4,
  border: "1px solid var(--border, var(--border))", background: "none", cursor: "pointer",
};
const th: React.CSSProperties = {
  textAlign: "start", padding: "0.4rem 0.55rem", borderBottom: "2px solid var(--border, var(--border))",
  fontSize: "0.78rem", position: "sticky", top: 0, background: "var(--surface-2)",
};
const td: React.CSSProperties = { padding: "0.35rem 0.55rem", fontSize: "0.82rem", verticalAlign: "top" };
const inp: React.CSSProperties = {
  padding: "0.35rem 0.5rem", border: "1px solid var(--border, var(--border))", borderRadius: 4, fontSize: "0.85rem",
};

// CKAN resource last-modified/upload timestamp → readable he-IL date (or —).
function fmtDate(s: string | null | undefined): string {
  if (!s) return "—";
  const d = new Date(s);
  return isNaN(d.getTime()) ? "—" : d.toLocaleDateString("he-IL", { year: "numeric", month: "2-digit", day: "2-digit" });
}

function useMsg() {
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const ok = (m: string) => { setMsg(m); setErr(null); };
  const fail = (e: unknown) => { setErr((e as Error)?.message || "שגיאה"); setMsg(null); };
  const node = (
    <>
      {err && <div style={{ color: "var(--danger, #992C2C)", margin: "0.4rem 0" }}>{err}</div>}
      {msg && <div style={{ color: "var(--primary)", margin: "0.4rem 0" }}>{msg}</div>}
    </>
  );
  return { node, ok, fail };
}

// ── Dashboard (overview) ────────────────────────────────────────────────────
function DashboardSection({ onNav }: { onNav: (s: Section) => void }) {
  const { node, fail } = useMsg();
  const [d, setD] = useState<OcalDashboard | null>(null);
  useEffect(() => { ocalAdmin.dashboard().then(setD).catch(fail); }, []); // eslint-disable-line
  const tile = (label: string, val: number, sec?: Section) => {
    // A div with onClick is not a control. Where the tile navigates it
    // becomes a button; where it only reports a number it stays a div
    // and takes no focus at all (WCAG 2.1.1, 4.1.2).
    const Tag = sec ? "button" : "div";
    return (
      <Tag
        type={sec ? "button" : undefined}
        onClick={sec ? () => onNav(sec) : undefined}
        aria-label={sec ? `${label}: ${(val || 0).toLocaleString()} — מעבר לפירוט` : undefined}
        style={{
          padding: "0.8rem 1.1rem", background: "var(--surface-2)", borderRadius: 8,
          textAlign: "center", minWidth: 104, border: 0, font: "inherit",
          color: "inherit", cursor: sec ? "pointer" : "default",
        }}
      >
        <div style={{ fontSize: "1.55rem", fontWeight: 700 }}>{(val || 0).toLocaleString()}</div>
        <div className="text-sm text-muted">{label}</div>
      </Tag>
    );
  };
  const dt = (s: string) => { const x = new Date(s); return isNaN(x.getTime()) ? "—" : x.toLocaleDateString("he-IL"); };
  return (
    <div>
      {node}
      {!d ? <div className="text-sm text-muted">טוען…</div> : (<>
        <div style={{ display: "flex", gap: "0.6rem", flexWrap: "wrap", marginBottom: "1rem" }}>
          {tile("אירועים", d.counts.events)}
          {tile("יומנים", d.counts.sources, "sources")}
          {tile("פעילים", d.counts.enabled_sources, "sources")}
          {tile("ישויות", d.counts.entities, "entities")}
          {tile("אנשים", d.counts.people, "people")}
          {tile("ארגונים", d.counts.organizations, "orgs")}
          {tile("נדחו", d.counts.rejected, "exceptions")}
        </div>
        <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginBottom: "1rem" }}>
          <button className="btn-primary" onClick={() => onNav("candidates")}>מועמדים חדשים</button>
          <button style={btn} onClick={() => onNav("automation")}>אוטומציה</button>
          <button style={btn} onClick={() => onNav("sources")}>נהל יומנים</button>
        </div>
        <h4 style={{ margin: "0.4rem 0" }}>יומנים שנוספו לאחרונה</h4>
        <div tabIndex={0} role="region" aria-label="טבלת נתונים" className="scroll-region" style={{ overflowX: "auto", border: "1px solid var(--border)", borderRadius: 6 }}>
          <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 520 }}>
            <thead><tr><th scope="col" style={th}>יומן</th><th scope="col" style={th}>בעלים</th><th scope="col" style={{ ...th, textAlign: "end" }}>אירועים</th><th scope="col" style={th}>נוסף</th></tr></thead>
            <tbody>
              {d.recent_sources.map((s) => (
                <tr key={s.id} style={{ borderBottom: "1px solid var(--border)", opacity: s.is_enabled ? 1 : 0.55 }}>
                  <td style={td}><span aria-hidden style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: s.color || "#3B82F6", marginInlineEnd: 6 }} />{s.name}</td>
                  <td style={{ ...td, color: "var(--text-muted)" }}>{s.person_name || "—"}</td>
                  <td style={{ ...td, textAlign: "end" }}>{(s.total_events || 0).toLocaleString()}</td>
                  <td style={{ ...td, color: "var(--text-muted)" }}>{dt(s.created_at)}</td>
                </tr>
              ))}
              {d.recent_sources.length === 0 && <tr><td style={td} colSpan={4}>אין יומנים עדיין.</td></tr>}
            </tbody>
          </table>
        </div>
      </>)}
    </div>
  );
}

// ── Sources ───────────────────────────────────────────────────────────────
function SourcesSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcalAdminSource[]>([]);
  const [total, setTotal] = useState(0);
  const [q, setQ] = useState("");
  const [filter, setFilter] = useState<"all" | "enabled" | "disabled" | "unreviewed">("all");
  const [busy, setBusy] = useState<string | null>(null);
  const [ner, setNer] = useState<{ available: boolean; provider: string | null } | null>(null);
  const [people, setPeople] = useState<OcalAdminPerson[]>([]);

  useEffect(() => {
    ocalAdmin.aiNerStatus().then(setNer).catch(() => setNer(null));
    ocalAdmin.people().then((r) => setPeople(r.people)).catch(() => {});
  }, []);

  const load = useCallback(() => {
    const params: Record<string, unknown> = { q, limit: 300 };
    if (filter === "enabled") params.enabled = true;
    if (filter === "disabled") params.enabled = false;
    if (filter === "unreviewed") params.reviewed = false;
    ocalAdmin.sources(params).then((r) => { setRows(r.sources); setTotal(r.total); }).catch(fail);
  }, [q, filter]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => { load(); }, [load]);

  const act = async (id: string, fn: () => Promise<unknown>, label: string) => {
    setBusy(id);
    try { await fn(); ok(label); load(); } catch (e) { fail(e); } finally { setBusy(null); }
  };

  return (
    <div>
      <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.6rem", alignItems: "center" }}>
        <input aria-label="חיפוש יומן / בעלים…" style={{ ...inp, flex: "1 1 220px" }} placeholder="חיפוש יומן / בעלים…" value={q}
          onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => e.key === "Enter" && load()} />
        <select style={inp} aria-label="סינון לפי מצב" value={filter} onChange={(e) => setFilter(e.target.value as typeof filter)}>
          <option value="all">הכל</option>
          <option value="enabled">פעילים</option>
          <option value="disabled">מושבתים</option>
          <option value="unreviewed">לא נסקרו</option>
        </select>
        <button style={btn} onClick={load}>רענן</button>
        <span className="text-sm text-muted">{rows.length} מתוך {total.toLocaleString()}</span>
      </div>
      {node}
      <div tabIndex={0} role="region" aria-label="טבלת נתונים" className="scroll-region" style={{ overflowX: "auto", maxHeight: 560, border: "1px solid var(--border)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 780 }}>
          <thead><tr>
            <th scope="col" style={th}>יומן</th><th scope="col" style={th}>בעלים</th><th scope="col" style={{ ...th, textAlign: "end" }}>אירועים</th>
            <th scope="col" style={th}>סטטוס</th><th scope="col" style={th}>פעולות</th>
          </tr></thead>
          <tbody>
            {rows.map((s) => (
              <tr key={s.id} style={{ borderBottom: "1px solid var(--border)", opacity: s.is_enabled ? 1 : 0.55 }}>
                <td style={td}>
                  <span style={{ display: "inline-flex", alignItems: "center", gap: "0.35rem" }}>
                    <input type="color" value={s.color || "#3B82F6"} disabled={busy === s.id} title="צבע היומן"
                      style={{ width: 20, height: 18, padding: 0, border: "none", background: "none", cursor: "pointer" }}
                      onChange={(e) => act(s.id, () => ocalAdmin.patchSource(s.id, { color: e.target.value }), "צבע עודכן")} />
                    {s.name}
                  </span>
                  {s.reviewed_at && <span title="נסקר" style={{ marginInlineStart: 6, color: "var(--primary)" }}>✓</span>}
                </td>
                <td style={td}>
                  <select style={{ ...inp, fontSize: "0.76rem", maxWidth: 150, padding: "0.2rem 0.3rem" }} value={s.person_id || ""} disabled={busy === s.id}
                    title="בעלים" onChange={(e) => act(s.id, () => ocalAdmin.patchSource(s.id, { person_id: e.target.value }), "בעלים עודכן")}>
                    <option value="">{s.organization_name ? `(${s.organization_name})` : "— ללא בעלים —"}</option>
                    {people.map((p) => <option key={p.id} value={p.id}>{p.name}</option>)}
                  </select>
                </td>
                <td style={{ ...td, textAlign: "end" }}>{(s.total_events || 0).toLocaleString()}</td>
                <td style={{ ...td, color: "var(--text-muted)", fontSize: "0.75rem" }}>{s.sync_status}</td>
                <td style={td}>
                  <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
                    <button style={btn} disabled={busy === s.id}
                      onClick={() => act(s.id, () => ocalAdmin.patchSource(s.id, { is_enabled: !s.is_enabled }), s.is_enabled ? "הושבת" : "הופעל")}>
                      {s.is_enabled ? "השבת" : "הפעל"}
                    </button>
                    <button style={btn} disabled={busy === s.id}
                      onClick={() => act(s.id, () => s.reviewed_at ? ocalAdmin.unreviewSource(s.id) : ocalAdmin.reviewSource(s.id), "עודכן")}>
                      {s.reviewed_at ? "בטל סקירה" : "סמן נסקר"}
                    </button>
                    {s.resource_id && (
                      <button style={btn} disabled={busy === s.id}
                        onClick={() => act(s.id, () => ocalAdmin.reimportSource(s.id, false), "יובא מחדש")}>ייבא מחדש</button>
                    )}
                    {s.resource_id && (
                      <button style={btn} disabled={busy === s.id}
                        onClick={() => { if (confirm(`לנקות את כל האירועים של "${s.name}" ולייבא מחדש מאפס?`)) act(s.id, () => ocalAdmin.reimportSource(s.id, true), "נוקה ויובא מחדש"); }}>ייבא (נקה)</button>
                    )}
                    <button style={btn} disabled={busy === s.id}
                      onClick={() => act(s.id, () => ocalAdmin.enrichSource(s.id), "הועשר")}>העשר</button>
                    <button style={btn} disabled={busy === s.id} onClick={async () => {
                      setBusy(s.id);
                      try { const r = await ocalAdmin.deduplicateSource(s.id); ok(`הוסרו ${r.deleted} אירועים כפולים`); load(); }
                      catch (e) { fail(e); } finally { setBusy(null); }
                    }}>נקה כפולים</button>
                    <button style={btn} disabled={busy === s.id} onClick={async () => {
                      setBusy(s.id);
                      try { const r = await ocalAdmin.findMatchesSource(s.id); ok(`התאמות בין-יומנים: ${r.created ?? 0} חדשות, ${r.joined ?? 0} צורפו`); load(); }
                      catch (e) { fail(e); } finally { setBusy(null); }
                    }}>מצא התאמות</button>
                    {ner?.available && (
                      <button style={{ ...btn, color: "var(--tint-violet-fg)", borderColor: "var(--tint-violet-bd)" }} disabled={busy === s.id}
                        title={`חילוץ ישויות עם LLM (${ner.provider}) — כרוך בעלות`}
                        onClick={async () => {
                          if (!confirm(`להריץ חילוץ ישויות AI על "${s.name}"? הפעולה כרוכה בעלות LLM.`)) return;
                          setBusy(s.id);
                          try {
                            const r = await ocalAdmin.enrichSource(s.id, true) as { ai_ner?: { inserted?: number; provider?: string } };
                            ok(`חילוץ AI הושלם — ${r.ai_ner?.inserted ?? 0} ישויות (${r.ai_ner?.provider ?? ner.provider})`);
                            load();
                          } catch (e) { fail(e); } finally { setBusy(null); }
                        }}>חילוץ AI</button>
                    )}
                    <button style={{ ...btn, color: "var(--danger)", borderColor: "var(--tint-bad-bd)" }} disabled={busy === s.id}
                      onClick={() => { if (confirm(`למחוק את "${s.name}" וכל האירועים שלו?`)) act(s.id, () => ocalAdmin.deleteSource(s.id), "נמחק"); }}>מחק</button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Candidates ────────────────────────────────────────────────────────────
function CandidatesSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcalCandidate[]>([]);
  const [loading, setLoading] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    ocalAdmin.candidates(80).then((r) => setRows(r.candidates)).catch(fail).finally(() => setLoading(false));
  }, []); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => { load(); }, [load]);

  return (
    <div>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.6rem", alignItems: "center", flexWrap: "wrap" }}>
        <button style={btn} onClick={load} disabled={loading}>{loading ? "טוען…" : "רענן מועמדים"}</button>
        <button className="btn-primary" onClick={async () => {
          setBusy("scan");
          try { const r = await ocalAdmin.scan(5); ok(r.message || "הסריקה החלה ברקע — רענן בעוד דקה."); }
          catch (e) { fail(e); } finally { setBusy(null); }
        }} disabled={busy === "scan"}>{busy === "scan" ? "מתחיל…" : "סרוק וייבא (עד 5)"}</button>
        <span className="text-sm text-muted">{rows.length} מועמדים חדשים (לא יובאו / נדחו)</span>
      </div>
      {node}
      <div tabIndex={0} role="region" aria-label="טבלת נתונים" className="scroll-region" style={{ overflowX: "auto", maxHeight: 560, border: "1px solid var(--border)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 680 }}>
          <thead><tr><th scope="col" style={th}>יומן (dataset)</th><th scope="col" style={th}>פורמט</th><th scope="col" style={th}>הועלה / עודכן</th><th scope="col" style={th}>גוף</th><th scope="col" style={th}></th></tr></thead>
          <tbody>
            {rows.map((c) => (
              <tr key={c.resource_id} style={{ borderBottom: "1px solid var(--border)" }}>
                <td style={td}>{c.dataset_title || c.resource_name || c.resource_id}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{c.format || "—"}</td>
                <td style={{ ...td, color: "var(--text-muted)", whiteSpace: "nowrap" }} title={c.last_modified || ""}>{fmtDate(c.last_modified)}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{c.organization || "—"}</td>
                <td style={td}>
                  <button style={btn} disabled={busy === c.resource_id} onClick={async () => {
                    setBusy(c.resource_id);
                    try { const r = await ocalAdmin.importOne(c.resource_id); ok(`יובא: ${r.events_upserted} אירועים`); load(); }
                    catch (e) { fail(e); } finally { setBusy(null); }
                  }}>ייבא</button>
                </td>
              </tr>
            ))}
            {!loading && rows.length === 0 && <tr><td style={td} colSpan={5}>אין מועמדים חדשים.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Exceptions ────────────────────────────────────────────────────────────
function ExceptionsSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcalException[]>([]);
  const load = useCallback(() => { ocalAdmin.exceptions(300).then((r) => setRows(r.exceptions)).catch(fail); }, []); // eslint-disable-line
  useEffect(() => { load(); }, [load]);
  return (
    <div>
      <button style={btn} onClick={load}>רענן</button>
      <span className="text-sm text-muted" style={{ marginInlineStart: 8 }}>{rows.length} משאבים שנדחו אוטומטית</span>
      {node}
      <div tabIndex={0} role="region" aria-label="טבלת נתונים" className="scroll-region" style={{ overflowX: "auto", maxHeight: 560, border: "1px solid var(--border)", borderRadius: 6, marginTop: "0.5rem" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 620 }}>
          <thead><tr><th scope="col" style={th}>dataset</th><th scope="col" style={th}>פורמט</th><th scope="col" style={th}>סיבה</th><th scope="col" style={th}></th></tr></thead>
          <tbody>
            {rows.map((e) => (
              <tr key={e.resource_id} style={{ borderBottom: "1px solid var(--border)" }}>
                <td style={td}>{e.dataset_title || e.resource_name || e.resource_id}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{e.resource_format || "—"}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{e.exception_reason}</td>
                <td style={td}>
                  <button style={btn} onClick={async () => {
                    try { await ocalAdmin.clearException(e.resource_id); ok("שוחרר — ייבחן שוב בסריקה הבאה"); load(); } catch (x) { fail(x); }
                  }}>נסה שוב</button>
                </td>
              </tr>
            ))}
            {rows.length === 0 && <tr><td style={td} colSpan={4}>אין דחיות.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── People ────────────────────────────────────────────────────────────────
const EMPTY_PERSON = { id: "", name: "", organization_id: "", wikipedia_link: "", notes: "" };
function PeopleSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcalAdminPerson[]>([]);
  const [orgs, setOrgs] = useState<OcalAdminOrg[]>([]);
  const [q, setQ] = useState("");
  const [form, setForm] = useState({ ...EMPTY_PERSON });
  const [sel, setSel] = useState<Set<string>>(new Set());
  const [csv, setCsv] = useState("");
  const [showCsv, setShowCsv] = useState(false);
  const load = useCallback(() => {
    ocalAdmin.people(q || undefined).then((r) => setRows(r.people)).catch(fail);
    ocalAdmin.organizations().then((r) => setOrgs(r.organizations)).catch(() => {});
  }, [q]); // eslint-disable-line

  useEffect(() => { load(); }, [load]);

  const save = async () => {
    const name = form.name.trim();
    if (!name) return;
    const body = { name, organization_id: form.organization_id || undefined, wikipedia_link: form.wikipedia_link || undefined, notes: form.notes || undefined };
    try {
      if (form.id) { await ocalAdmin.patchPerson(form.id, body); ok("עודכן"); }
      else { await ocalAdmin.createPerson(body); ok("נוסף"); }
      setForm({ ...EMPTY_PERSON }); load();
    } catch (e) { fail(e); }
  };
  const toggle = (id: string) => setSel((s) => { const n = new Set(s); if (n.has(id)) n.delete(id); else n.add(id); return n; });
  const merge = async () => {
    const ids = [...sel];
    if (ids.length < 2) return;
    const target = ids[0];
    const tname = rows.find((r) => r.id === target)?.name;
    if (!confirm(`למזג ${ids.length - 1} אנשים לתוך "${tname}"? הפעולה בלתי הפיכה.`)) return;
    try { const r = await ocalAdmin.mergePeople(ids.slice(1), target); ok(`מוזגו ${r.merged} → ${tname}`); setSel(new Set()); load(); } catch (e) { fail(e); }
  };
  const importCsv = async () => {
    const parsed = csv.split(/\r?\n/).map((l) => l.trim()).filter(Boolean).map((l) => {
      const [name, organization_name, wikipedia_link, notes] = l.split(/\t|,/).map((s) => (s || "").trim());
      return { name, organization_name, wikipedia_link, notes };
    }).filter((r) => r.name);
    if (!parsed.length) { fail(new Error("לא נמצאו שורות תקינות (שם[,ארגון[,ויקיפדיה[,הערות]]])")); return; }
    try { const r = await ocalAdmin.bulkImportPeople(parsed); ok(`יבוא: נוצרו ${r.created}, עודכנו ${r.updated}, דולגו ${r.skipped}`); setCsv(""); setShowCsv(false); load(); } catch (e) { fail(e); }
  };

  return (
    <div>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.6rem", flexWrap: "wrap", alignItems: "center" }}>
        <input aria-label="חיפוש שם…" style={{ ...inp, flex: "1 1 200px" }} placeholder="חיפוש שם…" value={q}
          onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => e.key === "Enter" && load()} />
        <button style={btn} onClick={load}>חפש</button>
        <button style={btn} onClick={() => setShowCsv((v) => !v)}>יבוא CSV</button>
        {sel.size >= 2 && <button className="btn-primary" onClick={merge}>מזג {sel.size} → הראשון</button>}
      </div>

      {/* add / edit form */}
      <div style={{ display: "flex", gap: "0.4rem", marginBottom: "0.6rem", flexWrap: "wrap", alignItems: "center", padding: "0.5rem", background: "var(--surface-2)", borderRadius: 6 }}>
        <input aria-label="שם" style={{ ...inp, flex: "1 1 160px" }} placeholder="שם" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
        <select style={inp} aria-label="ארגון" value={form.organization_id} onChange={(e) => setForm({ ...form, organization_id: e.target.value })}>
          <option value="">— ללא ארגון —</option>
          {orgs.map((o) => <option key={o.id} value={o.id}>{o.name}</option>)}
        </select>
        <input aria-label="קישור ויקיפדיה" style={{ ...inp, flex: "1 1 160px" }} placeholder="קישור ויקיפדיה" value={form.wikipedia_link} onChange={(e) => setForm({ ...form, wikipedia_link: e.target.value })} />
        <input aria-label="הערות" style={{ ...inp, flex: "1 1 160px" }} placeholder="הערות" value={form.notes} onChange={(e) => setForm({ ...form, notes: e.target.value })} />
        <button className="btn-primary" disabled={!form.name.trim()} onClick={save}>{form.id ? "עדכן" : "הוסף"}</button>
        {form.id && <button style={btn} onClick={() => setForm({ ...EMPTY_PERSON })}>ביטול</button>}
      </div>

      {showCsv && (
        <div style={{ marginBottom: "0.6rem" }}>
          <textarea aria-label="שורה לכל אדם: שם,ארגון,קישור ויקיפדיה,הערות" style={{ ...inp, width: "100%", minHeight: 90, fontFamily: "monospace", fontSize: "0.8rem" }} dir="ltr"
            placeholder="שורה לכל אדם: שם,ארגון,קישור ויקיפדיה,הערות" value={csv} onChange={(e) => setCsv(e.target.value)} />
          <button className="btn-primary" style={{ marginTop: 4 }} disabled={!csv.trim()} onClick={importCsv}>ייבא</button>
        </div>
      )}
      {node}
      <div tabIndex={0} role="region" aria-label="טבלת נתונים" className="scroll-region" style={{ overflowX: "auto", maxHeight: 520, border: "1px solid var(--border)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 640 }}>
          <thead><tr><th scope="col" style={th}></th><th scope="col" style={th}>שם</th><th scope="col" style={th}>ארגון</th><th scope="col" style={th}>ויקיפדיה</th><th scope="col" style={{ ...th, textAlign: "end" }}>יומנים</th><th scope="col" style={th}></th></tr></thead>
          <tbody>
            {rows.map((p) => (
              <tr key={p.id} style={{ borderBottom: "1px solid var(--border)", background: sel.has(p.id) ? "var(--surface-2)" : undefined }}>
                <td style={td}><input type="checkbox" checked={sel.has(p.id)} onChange={() => toggle(p.id)} title="בחר למיזוג" /></td>
                <td style={td}>{p.name}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{p.organization_name || "—"}</td>
                <td style={td}>{p.wikipedia_link ? <a href={p.wikipedia_link} target="_blank" rel="noreferrer">↗<span className="sr-only"> (נפתח בחלון חדש)</span></a> : "—"}</td>
                <td style={{ ...td, textAlign: "end" }}>{p.source_count}</td>
                <td style={td}>
                  <button style={btn} onClick={() => setForm({ id: p.id, name: p.name, organization_id: p.organization_id || "", wikipedia_link: p.wikipedia_link || "", notes: p.notes || "" })}>ערוך</button>
                  <button style={{ ...btn, color: "var(--danger)", borderColor: "var(--tint-bad-bd)", marginInlineStart: 4 }} onClick={async () => {
                    if (!confirm(`למחוק את ${p.name}?`)) return;
                    try { await ocalAdmin.deletePerson(p.id); ok("נמחק"); load(); } catch (e) { fail(e); }
                  }}>מחק</button>
                </td>
              </tr>
            ))}
            {rows.length === 0 && <tr><td style={td} colSpan={6}>אין אנשים.</td></tr>}
          </tbody>
        </table>
      </div>
      <p className="text-sm text-muted" style={{ marginTop: 6 }}>סמן ≥2 כדי למזג — הראשון שנבחר הוא היעד. מיזוג מפנה יומנים/ישויות/הצלבות ואז מוחק את השאר.</p>
    </div>
  );
}

// ── Organizations ─────────────────────────────────────────────────────────
const EMPTY_ORG = { id: "", name: "", website: "", description: "" };
function OrgsSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcalAdminOrg[]>([]);
  const [q, setQ] = useState("");
  const [form, setForm] = useState({ ...EMPTY_ORG });
  const load = useCallback(() => { ocalAdmin.organizations(q || undefined).then((r) => setRows(r.organizations)).catch(fail); }, [q]); // eslint-disable-line
  useEffect(() => { load(); }, [load]);

  const save = async () => {
    const name = form.name.trim();
    if (!name) return;
    const body = { name, website: form.website || undefined, description: form.description || undefined };
    try {
      if (form.id) { await ocalAdmin.patchOrg(form.id, body); ok("עודכן"); }
      else { await ocalAdmin.createOrg(body); ok("נוסף"); }
      setForm({ ...EMPTY_ORG }); load();
    } catch (e) { fail(e); }
  };

  return (
    <div>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.6rem", flexWrap: "wrap", alignItems: "center" }}>
        <input aria-label="חיפוש ארגון…" style={{ ...inp, flex: "1 1 200px" }} placeholder="חיפוש ארגון…" value={q}
          onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => e.key === "Enter" && load()} />
        <button style={btn} onClick={load}>חפש</button>
      </div>
      <div style={{ display: "flex", gap: "0.4rem", marginBottom: "0.6rem", flexWrap: "wrap", alignItems: "center", padding: "0.5rem", background: "var(--surface-2)", borderRadius: 6 }}>
        <input aria-label="שם ארגון" style={{ ...inp, flex: "1 1 160px" }} placeholder="שם ארגון" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
        <input aria-label="אתר" style={{ ...inp, flex: "1 1 160px" }} placeholder="אתר" value={form.website} onChange={(e) => setForm({ ...form, website: e.target.value })} />
        <input aria-label="תיאור" style={{ ...inp, flex: "1 1 200px" }} placeholder="תיאור" value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} />
        <button className="btn-primary" disabled={!form.name.trim()} onClick={save}>{form.id ? "עדכן" : "הוסף"}</button>
        {form.id && <button style={btn} onClick={() => setForm({ ...EMPTY_ORG })}>ביטול</button>}
      </div>
      {node}
      <div tabIndex={0} role="region" aria-label="טבלת נתונים" className="scroll-region" style={{ overflowX: "auto", maxHeight: 520, border: "1px solid var(--border)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 560 }}>
          <thead><tr><th scope="col" style={th}>שם</th><th scope="col" style={th}>אתר</th><th scope="col" style={th}>תיאור</th><th scope="col" style={th}></th></tr></thead>
          <tbody>
            {rows.map((o) => (
              <tr key={o.id} style={{ borderBottom: "1px solid var(--border)" }}>
                <td style={td}>{o.name}</td>
                <td style={td}>{o.website ? <a href={o.website} target="_blank" rel="noreferrer" dir="ltr">{o.website}<span className="sr-only"> (נפתח בחלון חדש)</span></a> : "—"}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{o.description || "—"}</td>
                <td style={td}>
                  <button style={btn} onClick={() => setForm({ id: o.id, name: o.name, website: o.website || "", description: o.description || "" })}>ערוך</button>
                  <button style={{ ...btn, color: "var(--danger)", borderColor: "var(--tint-bad-bd)", marginInlineStart: 4 }} onClick={async () => {
                    if (!confirm(`למחוק את ${o.name}?`)) return;
                    try { await ocalAdmin.deleteOrg(o.id); ok("נמחק"); load(); } catch (e) { fail(e); }
                  }}>מחק</button>
                </td>
              </tr>
            ))}
            {rows.length === 0 && <tr><td style={td} colSpan={4}>אין ארגונים.</td></tr>}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Automation (settings + scan logs) ──────────────────────────────────────
function AutomationSection() {
  const { node, ok, fail } = useMsg();
  const [st, setSt] = useState<{ scheduler_interval_hours: number; per_tick: number; scan_running: boolean; last_run: OcalAutoImportLog | null } | null>(null);
  const [logs, setLogs] = useState<OcalAutoImportLog[]>([]);
  const [form, setForm] = useState<{ auto_scan_enabled: boolean; confidence: number; min_rows: number; interval_hours: number } | null>(null);
  const [busy, setBusy] = useState(false);
  const load = useCallback(() => {
    ocalAdmin.automationStatus().then((s) => {
      setSt(s);
      setForm({ auto_scan_enabled: s.settings.auto_scan_enabled, confidence: s.settings.confidence, min_rows: s.settings.min_rows, interval_hours: s.settings.interval_hours });
    }).catch(fail);
    ocalAdmin.automationLogs(50).then((r) => setLogs(r.logs)).catch(() => {});
  }, []); // eslint-disable-line
  useEffect(() => { load(); }, [load]);

  const save = async () => {
    if (!form) return;
    setBusy(true);
    try { await ocalAdmin.updateAutomationSettings(form); ok("הגדרות נשמרו"); load(); } catch (e) { fail(e); } finally { setBusy(false); }
  };
  const scanNow = async () => {
    setBusy(true);
    try { const r = await ocalAdmin.scan(5); ok(r.message); setTimeout(load, 2000); } catch (e) { fail(e); } finally { setBusy(false); }
  };
  const dt = (s: string) => { const d = new Date(s); return isNaN(d.getTime()) ? "—" : d.toLocaleString("he-IL"); };
  const dur = (l: OcalAutoImportLog) => l.finished_at ? `${Math.max(0, Math.round((new Date(l.finished_at).getTime() - new Date(l.started_at).getTime()) / 1000))}s` : "רץ…";

  return (
    <div>
      {node}
      {form && (
        <div style={{ padding: "0.7rem", background: "var(--surface-2)", borderRadius: 6, marginBottom: "0.8rem" }}>
          <label style={{ display: "flex", alignItems: "center", gap: 6, marginBottom: 8, fontWeight: 600 }}>
            <input type="checkbox" checked={form.auto_scan_enabled} onChange={(e) => setForm({ ...form, auto_scan_enabled: e.target.checked })} />
            ייבוא אוטומטי פעיל (סריקה כל {st?.scheduler_interval_hours}ש׳, עד {st?.per_tick} בכל ריצה)
          </label>
          <div style={{ display: "flex", gap: "1rem", flexWrap: "wrap", alignItems: "center" }}>
            <label style={{ fontSize: "0.85rem" }}>סף ביטחון: <input type="number" step="0.05" min="0" max="1" style={{ ...inp, width: 80 }} value={form.confidence} onChange={(e) => setForm({ ...form, confidence: parseFloat(e.target.value) || 0 })} /></label>
            <label style={{ fontSize: "0.85rem" }}>מינ׳ שורות: <input type="number" min="1" style={{ ...inp, width: 80 }} value={form.min_rows} onChange={(e) => setForm({ ...form, min_rows: parseInt(e.target.value) || 1 })} /></label>
            <label style={{ fontSize: "0.85rem" }}>מרווח (שעות, בעליית שרת): <input type="number" step="0.5" min="0.5" style={{ ...inp, width: 80 }} value={form.interval_hours} onChange={(e) => setForm({ ...form, interval_hours: parseFloat(e.target.value) || 6 })} /></label>
            <button className="btn-primary" disabled={busy} onClick={save}>שמור</button>
            <button style={btn} disabled={busy} onClick={scanNow}>סרוק עכשיו</button>
          </div>
          <p className="text-sm text-muted" style={{ marginTop: 6 }}>שער הייבוא: כותרת + תאריך ממופים, ביטחון ≥ הסף, ≥ מינ׳ שורות ← מיובא אוטומטית; אחרת ← "נדחו". שינוי המרווח חל בעליית השרת הבאה.</p>
        </div>
      )}
      {st && (
        <div className="text-sm text-muted" style={{ marginBottom: 8 }}>
          {st.scan_running ? "🟢 סריקה רצה כעת…" : "⚪ אין סריקה פעילה"}
          {st.last_run && ` · אחרונה: ${dt(st.last_run.started_at)} (${st.last_run.trigger}) — יובאו ${st.last_run.imported}, נדחו ${st.last_run.skipped}`}
        </div>
      )}
      <h4 style={{ margin: "0.5rem 0" }}>לוג סריקות</h4>
      <div tabIndex={0} role="region" aria-label="טבלת נתונים" className="scroll-region" style={{ overflowX: "auto", maxHeight: 420, border: "1px solid var(--border)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 640 }}>
          <thead><tr><th scope="col" style={th}>התחיל</th><th scope="col" style={th}>טריגר</th><th scope="col" style={{ ...th, textAlign: "end" }}>מועמדים</th><th scope="col" style={{ ...th, textAlign: "end" }}>יובאו</th><th scope="col" style={{ ...th, textAlign: "end" }}>נדחו</th><th scope="col" style={{ ...th, textAlign: "end" }}>שגיאות</th><th scope="col" style={th}>משך</th></tr></thead>
          <tbody>
            {logs.map((l, i) => (
              <tr key={l.id || i} style={{ borderBottom: "1px solid var(--border)" }}>
                <td style={td}>{dt(l.started_at)}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{l.trigger}</td>
                <td style={{ ...td, textAlign: "end" }}>{l.candidates}</td>
                <td style={{ ...td, textAlign: "end", color: l.imported ? "var(--primary)" : undefined }}>{l.imported}</td>
                <td style={{ ...td, textAlign: "end" }}>{l.skipped}</td>
                <td style={{ ...td, textAlign: "end", color: l.errors ? "var(--danger)" : undefined }}>{l.errors}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{dur(l)}</td>
              </tr>
            ))}
            {logs.length === 0 && <tr><td style={td} colSpan={7}>אין ריצות עדיין — לחץ "סרוק עכשיו" או המתן ל-scheduler.</td></tr>}
          </tbody>
        </table>
      </div>
      <button style={{ ...btn, marginTop: 8 }} onClick={load}>רענן</button>
    </div>
  );
}

// ── Entities (extracted event_entities) ────────────────────────────────────
const ETYPES: [string, string][] = [["", "הכל"], ["person", "אנשים"], ["organization", "ארגונים"], ["place", "מקומות"]];
const ETYPE_HE: Record<string, string> = { person: "אדם", organization: "ארגון", place: "מקום" };
function EntitiesSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcalEntity[]>([]);
  const [stats, setStats] = useState<{ total_unique: number; person_count: number; org_count: number; place_count: number } | null>(null);
  const [total, setTotal] = useState(0);
  const [type, setType] = useState("");
  const [q, setQ] = useState("");
  const [offset, setOffset] = useState(0);
  const [sel, setSel] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);
  const LIMIT = 100;
  const key = (e: OcalEntity) => `${e.entity_name} ${e.entity_type}`;

  const load = useCallback(() => {
    setBusy(true);
    ocalAdmin.entities({ type: type || undefined, q: q || undefined, limit: LIMIT, offset })
      .then((r) => { setRows(r.entities); setTotal(r.total); setStats(r.stats); })
      .catch(fail).finally(() => setBusy(false));
  }, [type, q, offset]); // eslint-disable-line
  useEffect(() => { load(); }, [load]);

  const tile = (label: string, val: number) => (
    <div style={{ padding: "0.4rem 0.9rem", background: "var(--surface-2)", borderRadius: 6, textAlign: "center", minWidth: 90 }}>
      <div style={{ fontSize: "1.25rem", fontWeight: 700 }}>{(val || 0).toLocaleString()}</div>
      <div className="text-sm text-muted">{label}</div>
    </div>
  );
  const toggle = (k: string) => setSel((s) => { const n = new Set(s); if (n.has(k)) n.delete(k); else n.add(k); return n; });
  const rename = async (e: OcalEntity) => {
    const nn = prompt(`שם חדש ל"${e.entity_name}":`, e.entity_name);
    if (!nn || nn.trim() === e.entity_name) return;
    try { await ocalAdmin.renameEntity(e.entity_name, nn.trim(), e.entity_type); ok("שונה שם"); load(); } catch (x) { fail(x); }
  };
  const del = async (e: OcalEntity) => {
    if (!confirm(`למחוק את כל ${e.event_count} השיוכים של "${e.entity_name}"?`)) return;
    try { const r = await ocalAdmin.deleteEntityByName(e.entity_name, e.entity_type); ok(`נמחקו ${r.deleted}`); load(); } catch (x) { fail(x); }
  };
  const merge = async () => {
    const chosen = rows.filter((e) => sel.has(key(e)));
    if (chosen.length < 2) return;
    if (new Set(chosen.map((e) => e.entity_type)).size > 1) { fail(new Error("אפשר למזג רק ישויות מאותו סוג")); return; }
    const target = chosen[0].entity_name;
    if (!confirm(`למזג ${chosen.length - 1} ישויות לתוך "${target}"?`)) return;
    try { await ocalAdmin.mergeEntities(chosen.slice(1).map((e) => e.entity_name), target, chosen[0].entity_type); ok(`מוזגו → ${target}`); setSel(new Set()); load(); } catch (x) { fail(x); }
  };

  return (
    <div>
      {stats && <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", marginBottom: "0.7rem" }}>
        {tile("סה\"כ ייחודיות", stats.total_unique)}{tile("אנשים", stats.person_count)}{tile("ארגונים", stats.org_count)}{tile("מקומות", stats.place_count)}
      </div>}
      <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", alignItems: "center", marginBottom: "0.6rem" }}>
        <input aria-label="חיפוש ישות…" style={{ ...inp, flex: "1 1 200px" }} placeholder="חיפוש ישות…" value={q}
          onChange={(e) => { setOffset(0); setQ(e.target.value); }} onKeyDown={(e) => e.key === "Enter" && load()} />
        <select style={inp} aria-label="סוג הישות" value={type} onChange={(e) => { setOffset(0); setType(e.target.value); }}>
          {ETYPES.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        <button style={btn} onClick={load}>רענן</button>
        {sel.size >= 2 && <button className="btn-primary" onClick={merge}>מזג {sel.size} → הראשון</button>}
        <span className="text-sm text-muted">{total.toLocaleString()} ישויות</span>
      </div>
      {node}
      <div tabIndex={0} role="region" aria-label="טבלת נתונים" className="scroll-region" style={{ overflowX: "auto", maxHeight: 500, border: "1px solid var(--border)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 640 }}>
          <thead><tr><th scope="col" style={th}></th><th scope="col" style={th}>ישות</th><th scope="col" style={th}>סוג</th><th scope="col" style={{ ...th, textAlign: "end" }}>אירועים</th><th scope="col" style={th}>מקושר</th><th scope="col" style={th}></th></tr></thead>
          <tbody>
            {rows.map((e) => (
              <tr key={key(e)} style={{ borderBottom: "1px solid var(--border)", background: sel.has(key(e)) ? "var(--surface-2)" : undefined }}>
                <td style={td}><input type="checkbox" aria-label={`בחירת ${e.entity_name}`} checked={sel.has(key(e))} onChange={() => toggle(key(e))} /></td>
                <td style={td}>{e.entity_name}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{ETYPE_HE[e.entity_type] || e.entity_type}</td>
                <td style={{ ...td, textAlign: "end" }}>{e.event_count.toLocaleString()}</td>
                <td style={td} title="מקושר לרשומת אדם/ארגון">{e.matched ? "✓" : "—"}</td>
                <td style={td}>
                  <button style={btn} onClick={() => rename(e)}>שנה שם</button>
                  <button style={{ ...btn, color: "var(--danger)", borderColor: "var(--tint-bad-bd)", marginInlineStart: 4 }} onClick={() => del(e)}>מחק</button>
                </td>
              </tr>
            ))}
            {!busy && rows.length === 0 && <tr><td style={td} colSpan={6}>אין ישויות עדיין — הרץ "חילוץ AI" או "העשר" על יומן בלשונית "יומנים".</td></tr>}
          </tbody>
        </table>
      </div>
      <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.5rem", alignItems: "center" }}>
        <button style={btn} disabled={offset === 0} onClick={() => setOffset(Math.max(0, offset - LIMIT))}>הקודם</button>
        <button style={btn} disabled={offset + LIMIT >= total} onClick={() => setOffset(offset + LIMIT)}>הבא</button>
        <span className="text-sm text-muted">{total ? `${offset + 1}–${Math.min(offset + LIMIT, total)}` : "0"}</span>
      </div>
      <p className="text-sm text-muted" style={{ marginTop: 6 }}>סמן ≥2 (מאותו סוג) כדי למזג — הראשון הוא היעד. שינוי‑שם/מיזוג מאחדים את השיוכים בכל היומנים.</p>
    </div>
  );
}

// ── Content ───────────────────────────────────────────────────────────────
function ContentSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<{ key: string; value: string }[]>([]);
  const [draft, setDraft] = useState<Record<string, string>>({});
  const load = useCallback(() => {
    ocalAdmin.content().then((r) => {
      setRows(r.content);
      setDraft(Object.fromEntries(r.content.map((c) => [c.key, c.value])));
    }).catch(fail);
  }, []); // eslint-disable-line
  useEffect(() => { load(); }, [load]);
  return (
    <div>
      {node}
      {rows.map((c) => (
        <div key={c.key} style={{ marginBottom: "0.9rem" }}>
          <label style={{ fontWeight: 600, fontSize: "0.85rem" }} htmlFor={`ocal-content-${c.key}`}>{c.key}</label>
          <textarea id={`ocal-content-${c.key}`} style={{ ...inp, width: "100%", minHeight: 70, marginTop: 4, fontFamily: "inherit" }}
            value={draft[c.key] ?? ""} onChange={(e) => setDraft({ ...draft, [c.key]: e.target.value })} />
          <button style={{ ...btn, marginTop: 4 }} onClick={async () => {
            try { await ocalAdmin.putContent(c.key, draft[c.key] ?? ""); ok(`"${c.key}" נשמר`); } catch (e) { fail(e); }
          }}>שמור</button>
        </div>
      ))}
      {rows.length === 0 && <div className="text-sm text-muted">אין טקסטים.</div>}
    </div>
  );
}

export default function OcalAdminPanel() {
  const [sec, setSec] = useState<Section>("dashboard");
  return (
    <div>
      <p className="text-sm text-muted" style={{ marginTop: 0, lineHeight: 1.6 }}>
        ניהול <strong>יומן לעם</strong> — יומני נבחרי הציבור שהוגרו ל-OVER. ניהול היומנים (מקורות),
        ייבוא אוטומטי של יומנים חדשים מ-odata.org.il, וקוריקציה של אנשים/ארגונים/טקסטים.
      </p>
      <div className="flex" style={{ gap: "0.3rem", borderBottom: "2px solid var(--border)", marginBottom: "1rem", flexWrap: "wrap" }}>
        {SECTIONS.map(([id, label]) => (
          <button key={id} type="button" onClick={() => setSec(id)}
            style={{
              padding: "0.4rem 0.9rem", border: "none", cursor: "pointer", background: "none",
              fontSize: "0.9rem", fontWeight: sec === id ? 700 : 500,
              color: sec === id ? "var(--primary)" : "var(--text-muted)",
              borderBottom: sec === id ? "3px solid var(--primary)" : "3px solid transparent", marginBottom: -2,
            }}>{label}</button>
        ))}
      </div>
      {sec === "dashboard" && <DashboardSection onNav={setSec} />}
      {sec === "sources" && <SourcesSection />}
      {sec === "candidates" && <CandidatesSection />}
      {sec === "automation" && <AutomationSection />}
      {sec === "exceptions" && <ExceptionsSection />}
      {sec === "people" && <PeopleSection />}
      {sec === "orgs" && <OrgsSection />}
      {sec === "entities" && <EntitiesSection />}
      {sec === "content" && <ContentSection />}
    </div>
  );
}
