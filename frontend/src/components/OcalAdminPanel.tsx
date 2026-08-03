import { useCallback, useEffect, useState } from "react";
import {
  ocalAdmin, OcalAdminSource, OcalAdminPerson, OcalAdminOrg,
  OcalCandidate, OcalException,
} from "../api/client";

type Section = "sources" | "candidates" | "people" | "orgs" | "content" | "exceptions";
const SECTIONS: [Section, string][] = [
  ["sources", "יומנים"],
  ["candidates", "מועמדים חדשים"],
  ["exceptions", "נדחו"],
  ["people", "אנשים"],
  ["orgs", "ארגונים"],
  ["content", "טקסטים"],
];

const btn: React.CSSProperties = {
  padding: "0.25rem 0.6rem", fontSize: "0.78rem", borderRadius: 4,
  border: "1px solid var(--border, #d1d5db)", background: "none", cursor: "pointer",
};
const th: React.CSSProperties = {
  textAlign: "start", padding: "0.4rem 0.55rem", borderBottom: "2px solid var(--border, #cbd5e1)",
  fontSize: "0.78rem", position: "sticky", top: 0, background: "var(--bg-muted, #eef2f5)",
};
const td: React.CSSProperties = { padding: "0.35rem 0.55rem", fontSize: "0.82rem", verticalAlign: "top" };
const inp: React.CSSProperties = {
  padding: "0.35rem 0.5rem", border: "1px solid var(--border, #d1d5db)", borderRadius: 4, fontSize: "0.85rem",
};

function useMsg() {
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const ok = (m: string) => { setMsg(m); setErr(null); };
  const fail = (e: unknown) => { setErr((e as Error)?.message || "שגיאה"); setMsg(null); };
  const node = (
    <>
      {err && <div style={{ color: "var(--danger, #dc2626)", margin: "0.4rem 0" }}>{err}</div>}
      {msg && <div style={{ color: "var(--primary)", margin: "0.4rem 0" }}>{msg}</div>}
    </>
  );
  return { node, ok, fail };
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

  useEffect(() => { ocalAdmin.aiNerStatus().then(setNer).catch(() => setNer(null)); }, []);

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
        <input style={{ ...inp, flex: "1 1 220px" }} placeholder="חיפוש יומן / בעלים…" value={q}
          onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => e.key === "Enter" && load()} />
        <select style={inp} value={filter} onChange={(e) => setFilter(e.target.value as typeof filter)}>
          <option value="all">הכל</option>
          <option value="enabled">פעילים</option>
          <option value="disabled">מושבתים</option>
          <option value="unreviewed">לא נסקרו</option>
        </select>
        <button style={btn} onClick={load}>רענן</button>
        <span className="text-sm text-muted">{rows.length} מתוך {total.toLocaleString()}</span>
      </div>
      {node}
      <div style={{ overflowX: "auto", maxHeight: 560, border: "1px solid var(--border,#e2e8f0)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 780 }}>
          <thead><tr>
            <th style={th}>יומן</th><th style={th}>בעלים</th><th style={{ ...th, textAlign: "end" }}>אירועים</th>
            <th style={th}>סטטוס</th><th style={th}>פעולות</th>
          </tr></thead>
          <tbody>
            {rows.map((s) => (
              <tr key={s.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)", opacity: s.is_enabled ? 1 : 0.55 }}>
                <td style={td}>
                  <span style={{ display: "inline-flex", alignItems: "center", gap: "0.35rem" }}>
                    <span aria-hidden style={{ width: 8, height: 8, borderRadius: "50%", background: s.color || "#3B82F6" }} />
                    {s.name}
                  </span>
                  {s.reviewed_at && <span title="נסקר" style={{ marginInlineStart: 6, color: "var(--primary)" }}>✓</span>}
                </td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{s.person_name || s.organization_name || "—"}</td>
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
                    <button style={btn} disabled={busy === s.id}
                      onClick={() => act(s.id, () => ocalAdmin.enrichSource(s.id), "הועשר")}>העשר</button>
                    {ner?.available && (
                      <button style={{ ...btn, color: "#6d28d9", borderColor: "#c4b5fd" }} disabled={busy === s.id}
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
                    <button style={{ ...btn, color: "#b91c1c", borderColor: "#fca5a5" }} disabled={busy === s.id}
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
      <div style={{ overflowX: "auto", maxHeight: 560, border: "1px solid var(--border,#e2e8f0)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 680 }}>
          <thead><tr><th style={th}>יומן (dataset)</th><th style={th}>פורמט</th><th style={th}>גוף</th><th style={th}></th></tr></thead>
          <tbody>
            {rows.map((c) => (
              <tr key={c.resource_id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td}>{c.dataset_title || c.resource_name || c.resource_id}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{c.format || "—"}</td>
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
            {!loading && rows.length === 0 && <tr><td style={td} colSpan={4}>אין מועמדים חדשים.</td></tr>}
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
      <div style={{ overflowX: "auto", maxHeight: 560, border: "1px solid var(--border,#e2e8f0)", borderRadius: 6, marginTop: "0.5rem" }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 620 }}>
          <thead><tr><th style={th}>dataset</th><th style={th}>פורמט</th><th style={th}>סיבה</th><th style={th}></th></tr></thead>
          <tbody>
            {rows.map((e) => (
              <tr key={e.resource_id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
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
function PeopleSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcalAdminPerson[]>([]);
  const [q, setQ] = useState("");
  const [name, setName] = useState("");
  const load = useCallback(() => { ocalAdmin.people(q || undefined).then((r) => setRows(r.people)).catch(fail); }, [q]); // eslint-disable-line
  useEffect(() => { load(); }, [load]);
  return (
    <div>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.6rem", flexWrap: "wrap" }}>
        <input style={{ ...inp, flex: "1 1 200px" }} placeholder="חיפוש שם…" value={q}
          onChange={(e) => setQ(e.target.value)} onKeyDown={(e) => e.key === "Enter" && load()} />
        <input style={inp} placeholder="שם איש חדש" value={name} onChange={(e) => setName(e.target.value)} />
        <button className="btn-primary" disabled={!name.trim()} onClick={async () => {
          try { await ocalAdmin.createPerson({ name: name.trim() }); setName(""); ok("נוסף"); load(); } catch (e) { fail(e); }
        }}>הוסף</button>
      </div>
      {node}
      <div style={{ overflowX: "auto", maxHeight: 560, border: "1px solid var(--border,#e2e8f0)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 560 }}>
          <thead><tr><th style={th}>שם</th><th style={th}>ארגון</th><th style={{ ...th, textAlign: "end" }}>יומנים</th><th style={th}></th></tr></thead>
          <tbody>
            {rows.map((p) => (
              <tr key={p.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td}>{p.name}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{p.organization_name || "—"}</td>
                <td style={{ ...td, textAlign: "end" }}>{p.source_count}</td>
                <td style={td}>
                  <button style={{ ...btn, color: "#b91c1c", borderColor: "#fca5a5" }} onClick={async () => {
                    if (!confirm(`למחוק את ${p.name}?`)) return;
                    try { await ocalAdmin.deletePerson(p.id); ok("נמחק"); load(); } catch (e) { fail(e); }
                  }}>מחק</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Organizations ─────────────────────────────────────────────────────────
function OrgsSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcalAdminOrg[]>([]);
  const [name, setName] = useState("");
  const load = useCallback(() => { ocalAdmin.organizations().then((r) => setRows(r.organizations)).catch(fail); }, []); // eslint-disable-line
  useEffect(() => { load(); }, [load]);
  return (
    <div>
      <div style={{ display: "flex", gap: "0.5rem", marginBottom: "0.6rem", flexWrap: "wrap" }}>
        <input style={inp} placeholder="שם ארגון חדש" value={name} onChange={(e) => setName(e.target.value)} />
        <button className="btn-primary" disabled={!name.trim()} onClick={async () => {
          try { await ocalAdmin.createOrg({ name: name.trim() }); setName(""); ok("נוסף"); load(); } catch (e) { fail(e); }
        }}>הוסף</button>
      </div>
      {node}
      <div style={{ overflowX: "auto", maxHeight: 560, border: "1px solid var(--border,#e2e8f0)", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 480 }}>
          <thead><tr><th style={th}>שם</th><th style={th}>אתר</th><th style={th}></th></tr></thead>
          <tbody>
            {rows.map((o) => (
              <tr key={o.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td}>{o.name}</td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{o.website || "—"}</td>
                <td style={td}>
                  <button style={{ ...btn, color: "#b91c1c", borderColor: "#fca5a5" }} onClick={async () => {
                    if (!confirm(`למחוק את ${o.name}?`)) return;
                    try { await ocalAdmin.deleteOrg(o.id); ok("נמחק"); load(); } catch (e) { fail(e); }
                  }}>מחק</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
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
          <label style={{ fontWeight: 600, fontSize: "0.85rem" }}>{c.key}</label>
          <textarea style={{ ...inp, width: "100%", minHeight: 70, marginTop: 4, fontFamily: "inherit" }}
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
  const [sec, setSec] = useState<Section>("sources");
  return (
    <div>
      <p className="text-sm text-muted" style={{ marginTop: 0, lineHeight: 1.6 }}>
        ניהול <strong>יומן לעם</strong> — יומני נבחרי הציבור שהוגרו ל-OVER. ניהול היומנים (מקורות),
        ייבוא אוטומטי של יומנים חדשים מ-odata.org.il, וקוריקציה של אנשים/ארגונים/טקסטים.
      </p>
      <div className="flex" style={{ gap: "0.3rem", borderBottom: "2px solid var(--border,#e2e8f0)", marginBottom: "1rem", flexWrap: "wrap" }}>
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
      {sec === "sources" && <SourcesSection />}
      {sec === "candidates" && <CandidatesSection />}
      {sec === "exceptions" && <ExceptionsSection />}
      {sec === "people" && <PeopleSection />}
      {sec === "orgs" && <OrgsSection />}
      {sec === "content" && <ContentSection />}
    </div>
  );
}
