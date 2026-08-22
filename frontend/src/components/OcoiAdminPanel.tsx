import { useCallback, useEffect, useState } from "react";
import {
  ocoiAdmin, OcoiStats, OcoiAdminDoc, OcoiAdminDocDetail, OcoiAdminEntity,
  OcoiAdminRel, OcoiProposal, OcoiCluster, OcoiJob, OcoiRegistrySource,
  OcoiRegistryRecord, OcoiIgnored, OcoiSuggestion, OcoiAudit,
} from "../api/client";

type Section =
  | "dashboard" | "documents" | "entities" | "relationships" | "duplicates"
  | "registry" | "ignored" | "suggestions" | "content" | "jobs" | "audit";

const SECTIONS: [Section, string][] = [
  ["dashboard", "סקירה"],
  ["documents", "מסמכים"],
  ["entities", "ישויות"],
  ["relationships", "קשרים"],
  ["duplicates", "כפילויות"],
  ["registry", "מאגרי רישום"],
  ["ignored", "התעלמות"],
  ["suggestions", "הצעות ציבור"],
  ["content", "טקסטים"],
  ["jobs", "משימות רקע"],
  ["audit", "בדיקת תקינות"],
];

const ENTITY_TYPES: [string, string][] = [
  ["person", "אנשים"], ["company", "חברות"],
  ["association", "עמותות"], ["domain", "תחומים"],
];

const btn: React.CSSProperties = {
  padding: "0.25rem 0.6rem", fontSize: "0.78rem", borderRadius: 4,
  border: "1px solid var(--border, #d1d5db)", background: "none", cursor: "pointer",
};
const th: React.CSSProperties = {
  textAlign: "start", padding: "0.4rem 0.55rem",
  borderBottom: "2px solid var(--border, #cbd5e1)", fontSize: "0.78rem",
  position: "sticky", top: 0, background: "var(--bg-muted, #eef2f5)",
};
const td: React.CSSProperties = {
  padding: "0.35rem 0.55rem", fontSize: "0.82rem", verticalAlign: "top",
};
const inp: React.CSSProperties = {
  padding: "0.35rem 0.5rem", border: "1px solid var(--border, #d1d5db)",
  borderRadius: 4, fontSize: "0.85rem",
};
const tableWrap: React.CSSProperties = {
  overflowX: "auto", border: "1px solid var(--border,#e2e8f0)", borderRadius: 6,
  maxHeight: "62vh", overflowY: "auto",
};

function fmtDate(s: string | null | undefined): string {
  if (!s) return "—";
  const d = new Date(s);
  return isNaN(d.getTime()) ? "—" : d.toLocaleDateString("he-IL",
    { year: "numeric", month: "2-digit", day: "2-digit" });
}
function fmtSize(n: number | null | undefined): string {
  if (!n) return "—";
  return n > 1024 * 1024 ? `${(n / 1024 / 1024).toFixed(1)}MB`
    : `${Math.round(n / 1024)}KB`;
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

/** Shared paging footer. `total` may be a floor when the endpoint capped it. */
function Pager({ total, limit, offset, capped, onGo }: {
  total: number; limit: number; offset: number; capped?: boolean;
  onGo: (o: number) => void;
}) {
  const from = total === 0 ? 0 : offset + 1;
  const to = Math.min(offset + limit, total);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginTop: "0.5rem" }}>
      <button style={btn} disabled={offset <= 0} onClick={() => onGo(Math.max(0, offset - limit))}>‹ הקודם</button>
      <span className="text-sm text-muted">
        {from.toLocaleString()}–{to.toLocaleString()} מתוך {total.toLocaleString()}{capped ? "+" : ""}
      </span>
      <button style={btn} disabled={offset + limit >= total} onClick={() => onGo(offset + limit)}>הבא ›</button>
    </div>
  );
}

// ── סקירה ────────────────────────────────────────────────────────────────────
function DashboardSection({ onNav }: { onNav: (s: Section) => void }) {
  const { node, fail } = useMsg();
  const [s, setS] = useState<OcoiStats | null>(null);
  useEffect(() => { ocoiAdmin.stats().then((r) => setS(r.data)).catch(fail); }, []); // eslint-disable-line
  const tile = (label: string, val: number, sec?: Section, tone?: "warn") => (
    <div
      key={label}
      onClick={sec ? () => onNav(sec) : undefined}
      style={{
        padding: "0.8rem 1.1rem", borderRadius: 8, textAlign: "center", minWidth: 108,
        background: tone === "warn" && val > 0 ? "#fef3c7" : "var(--bg-muted,#f1f5f9)",
        cursor: sec ? "pointer" : "default",
      }}
    >
      <div style={{ fontSize: "1.55rem", fontWeight: 700 }}>{(val || 0).toLocaleString()}</div>
      <div className="text-sm text-muted">{label}</div>
    </div>
  );
  return (
    <div>
      {node}
      {!s ? <div className="text-sm text-muted">טוען…</div> : (
        <>
          <div style={{ display: "flex", gap: "0.6rem", flexWrap: "wrap", marginBottom: "1rem" }}>
            {tile("מסמכים", s.documents, "documents")}
            {tile("עם קובץ", s.with_file, "documents")}
            {tile("אומתו", s.verified, "documents")}
            {tile("קשרים", s.relationships, "relationships")}
            {tile("אנשים", s.persons, "entities")}
            {tile("חברות", s.companies, "entities")}
            {tile("עמותות", s.associations, "entities")}
            {tile("תחומים", s.domains, "entities")}
          </div>
          <div style={{ display: "flex", gap: "0.6rem", flexWrap: "wrap", marginBottom: "1rem" }}>
            {tile("חילוץ ממתין", s.extraction_pending, "documents", "warn")}
            {tile("חילוץ נכשל", s.extraction_failed, "documents", "warn")}
            {tile("ללא טקסט", s.no_text, "documents", "warn")}
            {tile("כפילויות ממתינות", s.proposals_pending, "duplicates", "warn")}
            {tile("הצעות ציבור", s.suggestions_pending, "suggestions", "warn")}
            {tile("ברשימת התעלמות", s.ignored_resources, "ignored")}
            {tile("רשומות רישום", s.registry_records, "registry")}
          </div>
          <p className="text-sm text-muted" style={{ maxWidth: "44rem" }}>
            הקבצים עצמם יושבים ב-R2 והטבלאות ב-NEON. הורדת ה-PDF וחילוץ הטקסט רצים על
            צי ה-worker הביתי — odata.org.il חוסם את כתובת ה-IP של Render, ולכן השרת
            עצמו לא מסוגל להוריד את הקבצים.
          </p>
        </>
      )}
    </div>
  );
}

// ── מסמכים ───────────────────────────────────────────────────────────────────
function DocumentsSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcoiAdminDoc[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [busy, setBusy] = useState(false);
  const [q, setQ] = useState("");
  const [extraction, setExtraction] = useState("");
  const [conversion, setConversion] = useState("");
  const [detail, setDetail] = useState<OcoiAdminDocDetail | null>(null);
  const [sel, setSel] = useState<Set<string>>(new Set());
  const limit = 50;

  const load = useCallback((o = offset) => {
    setBusy(true);
    ocoiAdmin.documents({ q, extraction, conversion, limit, offset: o })
      .then((r) => { setRows(r.data); setTotal(r.total || 0); setOffset(o); setSel(new Set()); })
      .catch(fail).finally(() => setBusy(false));
  }, [q, extraction, conversion, offset]); // eslint-disable-line
  useEffect(() => { load(0); }, [extraction, conversion]); // eslint-disable-line

  const toggle = (id: string) => setSel((p) => {
    const n = new Set(p); n.has(id) ? n.delete(id) : n.add(id); return n;
  });

  const requeue = async () => {
    if (!sel.size) return;
    try {
      const r = await ocoiAdmin.resetDocumentStatus({
        ids: [...sel], field: "extraction_status", value: "pending" });
      ok(`הוחזרו לתור: ${JSON.stringify(r.data)}`);
      load();
    } catch (e) { fail(e); }
  };

  return (
    <div>
      {node}
      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
        <input style={{ ...inp, minWidth: 220 }} placeholder="חיפוש בכותרת / כתובת"
               value={q} onChange={(e) => setQ(e.target.value)}
               onKeyDown={(e) => e.key === "Enter" && load(0)} />
        <select style={inp} value={extraction} onChange={(e) => setExtraction(e.target.value)}>
          <option value="">חילוץ: הכל</option>
          <option value="pending">ממתין</option>
          <option value="completed">הושלם</option>
          <option value="failed">נכשל</option>
        </select>
        <select style={inp} value={conversion} onChange={(e) => setConversion(e.target.value)}>
          <option value="">המרה: הכל</option>
          <option value="converted">הומר</option>
          <option value="no_text">ללא טקסט</option>
          <option value="pending">ממתין</option>
          <option value="failed">נכשל</option>
        </select>
        <button style={btn} onClick={() => load(0)} disabled={busy}>חפש</button>
        {sel.size > 0 && (
          <button className="btn-primary" onClick={requeue}>
            החזר {sel.size} לתור חילוץ
          </button>
        )}
      </div>

      <div style={tableWrap}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 780 }}>
          <thead>
            <tr>
              <th style={{ ...th, width: 28 }} />
              <th style={th}>מסמך</th>
              <th style={th}>מקור</th>
              <th style={th}>המרה</th>
              <th style={th}>חילוץ</th>
              <th style={{ ...th, textAlign: "end" }}>גודל</th>
              <th style={th}>נוסף</th>
              <th style={th} />
            </tr>
          </thead>
          <tbody>
            {rows.map((d) => (
              <tr key={d.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td}>
                  <input type="checkbox" checked={sel.has(d.id)} onChange={() => toggle(d.id)} />
                </td>
                <td style={td}>
                  <div style={{ fontWeight: 500 }}>{d.title || "(ללא כותרת)"}</div>
                  <a href={d.file_url} target="_blank" rel="noreferrer"
                     className="text-sm text-muted" style={{ wordBreak: "break-all" }}>
                    {d.file_url.slice(0, 68)}…
                  </a>
                </td>
                <td style={{ ...td, maxWidth: 180 }}>{d.source_title || "—"}</td>
                <td style={td}>
                  {d.conversion_status}
                  {d.has_text ? <span className="text-sm text-muted"> ({d.text_length.toLocaleString()} תווים)</span> : null}
                </td>
                <td style={td}>{d.extraction_status}{d.verified ? " ✓" : ""}</td>
                <td style={{ ...td, textAlign: "end" }}>{fmtSize(d.file_size)}</td>
                <td style={td}>{fmtDate(d.created_at)}</td>
                <td style={td}>
                  <button style={btn} onClick={() =>
                    ocoiAdmin.document(d.id).then((r) => setDetail(r.data)).catch(fail)}>פרטים</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pager total={total} limit={limit} offset={offset} onGo={load} />

      {detail && (
        <div style={{ marginTop: "1rem", padding: "0.8rem", border: "1px solid var(--border,#e2e8f0)", borderRadius: 6 }}>
          <div style={{ display: "flex", justifyContent: "space-between", gap: "0.5rem" }}>
            <h4 style={{ margin: 0 }}>{detail.title || detail.id}</h4>
            <button style={btn} onClick={() => setDetail(null)}>סגור</button>
          </div>
          <div className="text-sm text-muted" style={{ margin: "0.4rem 0" }}>
            {detail.source_title} · {detail.file_format} · {fmtSize(detail.file_size)} ·
            טקסט {detail.markdown_length.toLocaleString()} תווים · נוסף {fmtDate(detail.created_at)}
          </div>
          <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", margin: "0.5rem 0" }}>
            <button style={btn} onClick={() => ocoiAdmin.verifyDocument(detail.id, !detail.verified)
              .then(() => { ok(detail.verified ? "בוטל האימות" : "אומת"); load(); setDetail(null); }).catch(fail)}>
              {detail.verified ? "בטל אימות" : "סמן כמאומת"}
            </button>
            <button style={btn} onClick={() => ocoiAdmin.reextractDocument(detail.id)
              .then(() => { ok("הוחזר לתור חילוץ"); load(); setDetail(null); }).catch(fail)}>
              חלץ מחדש
            </button>
            {detail.has_file && (
              <a style={{ ...btn, textDecoration: "none" }} target="_blank" rel="noreferrer"
                 href={`/api/v1/ocoi/documents/${detail.id}/file`}>פתח PDF</a>
            )}
          </div>
          {detail.extraction_runs?.length > 0 && (
            <>
              <h5 style={{ margin: "0.6rem 0 0.3rem" }}>הרצות חילוץ</h5>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead><tr><th style={th}>מודל</th><th style={th}>ישויות</th><th style={th}>קשרים</th><th style={th}>מתי</th></tr></thead>
                <tbody>
                  {detail.extraction_runs.map((r) => (
                    <tr key={r.id}>
                      <td style={td}>{r.model_version || r.extractor_type}</td>
                      <td style={td}>{r.entities_found ?? "—"}</td>
                      <td style={td}>{r.relationships_found ?? "—"}</td>
                      <td style={td}>{fmtDate(r.created_at)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </div>
      )}
    </div>
  );
}

// ── ישויות ───────────────────────────────────────────────────────────────────
function EntitiesSection() {
  const { node, ok, fail } = useMsg();
  const [etype, setEtype] = useState("person");
  const [rows, setRows] = useState<OcoiAdminEntity[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [q, setQ] = useState("");
  const [unmatched, setUnmatched] = useState(false);
  const [sel, setSel] = useState<Set<string>>(new Set());
  const [editing, setEditing] = useState<OcoiAdminEntity | null>(null);
  const limit = 50;

  const load = useCallback((o = offset, t = etype) => {
    ocoiAdmin.entities(t, { q, unmatched: unmatched || undefined, limit, offset: o })
      .then((r) => { setRows(r.data); setTotal(r.total || 0); setOffset(o); setSel(new Set()); })
      .catch(fail);
  }, [q, etype, unmatched, offset]); // eslint-disable-line
  useEffect(() => { load(0, etype); }, [etype, unmatched]); // eslint-disable-line

  const toggle = (id: string) => setSel((p) => {
    const n = new Set(p); n.has(id) ? n.delete(id) : n.add(id); return n;
  });

  /** The kept entity is the one with the most edges — merging into a stub
      would strand the graph on a name nobody links to. */
  const merge = async () => {
    if (sel.size < 2) return;
    const chosen = rows.filter((r) => sel.has(r.id))
      .sort((a, b) => b.connections - a.connections);
    const keep = chosen[0];
    if (!window.confirm(
      `למזג ${sel.size} ישויות אל «${keep.name_hebrew}» (${keep.connections} קשרים)?\n` +
      `השאר יימחקו והשמות שלהן יישמרו ככינויים.`)) return;
    try {
      const r = await ocoiAdmin.mergeEntities({
        entity_type: etype, keep_id: keep.id, drop_ids: chosen.slice(1).map((c) => c.id) });
      ok(`מוזגו: ${JSON.stringify(r.data)}`);
      load();
    } catch (e) { fail(e); }
  };

  const isCompanyish = etype === "company" || etype === "association";

  return (
    <div>
      {node}
      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
        <select style={inp} value={etype} onChange={(e) => setEtype(e.target.value)}>
          {ENTITY_TYPES.map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        <input style={{ ...inp, minWidth: 200 }} placeholder="חיפוש בשם / כינוי"
               value={q} onChange={(e) => setQ(e.target.value)}
               onKeyDown={(e) => e.key === "Enter" && load(0)} />
        <button style={btn} onClick={() => load(0)}>חפש</button>
        {isCompanyish && (
          <label className="text-sm" style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
            <input type="checkbox" checked={unmatched} onChange={(e) => setUnmatched(e.target.checked)} />
            רק ללא מספר רישום
          </label>
        )}
        {sel.size >= 2 && <button className="btn-primary" onClick={merge}>מזג {sel.size} ישויות</button>}
      </div>

      <div style={tableWrap}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 700 }}>
          <thead>
            <tr>
              <th style={{ ...th, width: 28 }} />
              <th style={th}>שם</th>
              <th style={th}>כינויים</th>
              {isCompanyish && <th style={th}>מספר רישום</th>}
              {etype === "person" && <th style={th}>תפקיד</th>}
              <th style={{ ...th, textAlign: "end" }}>קשרים</th>
              <th style={th} />
            </tr>
          </thead>
          <tbody>
            {rows.map((e) => (
              <tr key={e.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)", opacity: e.hidden ? 0.5 : 1 }}>
                <td style={td}><input type="checkbox" checked={sel.has(e.id)} onChange={() => toggle(e.id)} /></td>
                <td style={td}>{e.name_hebrew || "—"}{e.hidden ? " (מוסתר)" : ""}</td>
                <td style={{ ...td, maxWidth: 230 }} className="text-sm text-muted">
                  {e.aliases?.length ? e.aliases.join(" · ") : "—"}
                </td>
                {isCompanyish && (
                  <td style={td}>
                    {e.registration_number || "—"}
                    {e.match_confidence != null && e.match_confidence < 1 && (
                      <span className="text-sm text-muted"> ({e.match_confidence.toFixed(2)})</span>
                    )}
                  </td>
                )}
                {etype === "person" && <td style={td} className="text-sm">{e.position || e.title || "—"}</td>}
                <td style={{ ...td, textAlign: "end" }}>{e.connections}</td>
                <td style={td}><button style={btn} onClick={() => setEditing(e)}>ערוך</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pager total={total} limit={limit} offset={offset} onGo={load} />

      {editing && (
        <EntityEditor
          etype={etype} entity={editing}
          onClose={() => setEditing(null)}
          onSaved={(m) => { ok(m); setEditing(null); load(); }}
          onError={fail}
        />
      )}
    </div>
  );
}

function EntityEditor({ etype, entity, onClose, onSaved, onError }: {
  etype: string; entity: OcoiAdminEntity; onClose: () => void;
  onSaved: (m: string) => void; onError: (e: unknown) => void;
}) {
  const [name, setName] = useState(entity.name_hebrew || "");
  const [english, setEnglish] = useState(entity.name_english || "");
  const [extra, setExtra] = useState(
    etype === "person" ? entity.position || "" : entity.registration_number || "");
  const [hidden, setHidden] = useState(entity.hidden);
  const [keepAlias, setKeepAlias] = useState(true);

  const save = async () => {
    const body: Record<string, unknown> = {
      name_hebrew: name, name_english: english || null, hidden };
    if (etype === "person") body.position = extra || null;
    else if (etype !== "domain") body.registration_number = extra || null;
    try {
      await ocoiAdmin.patchEntity(etype, entity.id, body, keepAlias);
      onSaved("נשמר");
    } catch (e) { onError(e); }
  };

  return (
    <div style={{ marginTop: "1rem", padding: "0.8rem", border: "1px solid var(--border,#e2e8f0)", borderRadius: 6 }}>
      <div style={{ display: "flex", justifyContent: "space-between" }}>
        <h4 style={{ margin: 0 }}>עריכת ישות</h4>
        <button style={btn} onClick={onClose}>סגור</button>
      </div>
      <div style={{ display: "flex", gap: "0.5rem", flexWrap: "wrap", margin: "0.6rem 0" }}>
        <input style={{ ...inp, minWidth: 220 }} value={name} placeholder="שם בעברית"
               onChange={(e) => setName(e.target.value)} />
        <input style={{ ...inp, minWidth: 180 }} value={english} placeholder="שם באנגלית"
               onChange={(e) => setEnglish(e.target.value)} />
        {etype !== "domain" && (
          <input style={{ ...inp, minWidth: 160 }} value={extra}
                 placeholder={etype === "person" ? "תפקיד" : "מספר רישום"}
                 onChange={(e) => setExtra(e.target.value)} />
        )}
      </div>
      <div style={{ display: "flex", gap: "1rem", flexWrap: "wrap", alignItems: "center" }}>
        <label className="text-sm" style={{ display: "flex", gap: "0.3rem" }}>
          <input type="checkbox" checked={hidden} onChange={(e) => setHidden(e.target.checked)} />
          מוסתר מהאתר הציבורי
        </label>
        <label className="text-sm" style={{ display: "flex", gap: "0.3rem" }}>
          <input type="checkbox" checked={keepAlias} onChange={(e) => setKeepAlias(e.target.checked)} />
          שמור את השם הקודם ככינוי
        </label>
        <button className="btn-primary" onClick={save}>שמור</button>
      </div>
      <p className="text-sm text-muted" style={{ margin: "0.5rem 0 0" }}>
        שינוי שם בלי לשמור כינוי מנתק את הישות מהמסמכים שבהם היא מופיעה בשמה הישן.
      </p>
    </div>
  );
}

// ── קשרים ────────────────────────────────────────────────────────────────────
function RelationshipsSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcoiAdminRel[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [q, setQ] = useState("");
  const [verified, setVerified] = useState("");
  const [sel, setSel] = useState<Set<string>>(new Set());
  const limit = 50;

  const load = useCallback((o = offset) => {
    ocoiAdmin.relationships({
      q, verified: verified === "" ? undefined : verified === "yes", limit, offset: o })
      .then((r) => { setRows(r.data); setTotal(r.total || 0); setOffset(o); setSel(new Set()); })
      .catch(fail);
  }, [q, verified, offset]); // eslint-disable-line
  useEffect(() => { load(0); }, [verified]); // eslint-disable-line

  const toggle = (id: string) => setSel((p) => {
    const n = new Set(p); n.has(id) ? n.delete(id) : n.add(id); return n;
  });

  const del = async () => {
    if (!sel.size || !window.confirm(`למחוק ${sel.size} קשרים? הפעולה אינה הפיכה.`)) return;
    try {
      await ocoiAdmin.bulkDeleteRelationships([...sel]);
      ok(`נמחקו ${sel.size} קשרים`);
      load();
    } catch (e) { fail(e); }
  };

  return (
    <div>
      {node}
      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
        <input style={{ ...inp, minWidth: 220 }} placeholder="חיפוש בשמות / פירוט"
               value={q} onChange={(e) => setQ(e.target.value)}
               onKeyDown={(e) => e.key === "Enter" && load(0)} />
        <select style={inp} value={verified} onChange={(e) => setVerified(e.target.value)}>
          <option value="">אימות: הכל</option>
          <option value="yes">מאומת</option>
          <option value="no">לא מאומת</option>
        </select>
        <button style={btn} onClick={() => load(0)}>חפש</button>
        {sel.size > 0 && (
          <button style={{ ...btn, color: "var(--danger,#992C2C)" }} onClick={del}>
            מחק {sel.size}
          </button>
        )}
      </div>
      <div style={tableWrap}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 820 }}>
          <thead>
            <tr>
              <th style={{ ...th, width: 28 }} />
              <th style={th}>מקור</th>
              <th style={th}>סוג קשר</th>
              <th style={th}>יעד</th>
              <th style={th}>פירוט</th>
              <th style={th}>מסמך</th>
              <th style={{ ...th, textAlign: "end" }}>ודאות</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td}><input type="checkbox" checked={sel.has(r.id)} onChange={() => toggle(r.id)} /></td>
                <td style={td}>{r.source_name || r.source_entity_id.slice(0, 8)}</td>
                <td style={td} className="text-sm">{r.relationship_type}</td>
                <td style={td}>{r.target_name || r.target_entity_id.slice(0, 8)}</td>
                <td style={{ ...td, maxWidth: 220 }} className="text-sm text-muted">{r.details || "—"}</td>
                <td style={{ ...td, maxWidth: 160 }} className="text-sm text-muted">{r.document_title || "—"}</td>
                <td style={{ ...td, textAlign: "end" }}>{r.confidence?.toFixed(2) ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pager total={total} limit={limit} offset={offset} onGo={load} />
    </div>
  );
}

// ── כפילויות ─────────────────────────────────────────────────────────────────
function DuplicatesSection() {
  const { node, ok, fail } = useMsg();
  const [view, setView] = useState<"pairs" | "clusters">("pairs");
  const [pairs, setPairs] = useState<OcoiProposal[]>([]);
  const [clusters, setClusters] = useState<OcoiCluster[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [etype, setEtype] = useState("");
  const [minScore, setMinScore] = useState("");
  const limit = 25;

  const load = useCallback((o = offset) => {
    if (view === "pairs") {
      ocoiAdmin.proposals({
        status: "pending", entity_type: etype || undefined,
        min_score: minScore ? Number(minScore) : undefined, limit, offset: o })
        .then((r) => { setPairs(r.data); setTotal(r.total || 0); setOffset(o); }).catch(fail);
    } else {
      ocoiAdmin.clusters({
        entity_type: etype || undefined,
        min_score: minScore ? Number(minScore) : undefined, limit: 40 })
        .then((r) => { setClusters(r.data); setTotal(r.total || 0); }).catch(fail);
    }
  }, [view, etype, minScore, offset]); // eslint-disable-line
  useEffect(() => { load(0); }, [view, etype, minScore]); // eslint-disable-line

  const review = async (id: string, action: string) => {
    try { await ocoiAdmin.reviewProposal(id, action); ok(action === "approve" ? "מוזג" : "נדחה"); load(); }
    catch (e) { fail(e); }
  };

  const mergeCluster = async (c: OcoiCluster) => {
    const keep = c.members.find((m) => m.id === c.canonical_id) || c.members[0];
    if (!window.confirm(
      `למזג ${c.size} ישויות אל «${keep.name}» (${keep.connections} קשרים)?`)) return;
    try {
      await ocoiAdmin.mergeCluster({
        entity_type: c.entity_type, keep_id: keep.id,
        drop_ids: c.members.filter((m) => m.id !== keep.id).map((m) => m.id) });
      ok(`מוזגו ${c.size} ישויות`);
      load();
    } catch (e) { fail(e); }
  };

  return (
    <div>
      {node}
      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
        <button style={{ ...btn, fontWeight: view === "pairs" ? 700 : 400 }}
                onClick={() => setView("pairs")}>זוגות</button>
        <button style={{ ...btn, fontWeight: view === "clusters" ? 700 : 400 }}
                onClick={() => setView("clusters")}>אשכולות</button>
        <select style={inp} value={etype} onChange={(e) => setEtype(e.target.value)}>
          <option value="">כל הסוגים</option>
          {ENTITY_TYPES.filter(([v]) => v !== "domain").map(([v, l]) => <option key={v} value={v}>{l}</option>)}
        </select>
        <select style={inp} value={minScore} onChange={(e) => setMinScore(e.target.value)}>
          <option value="">כל הציונים</option>
          <option value="0.9">0.90 ומעלה</option>
          <option value="0.95">0.95 ומעלה</option>
          <option value="0.99">0.99 ומעלה</option>
        </select>
        <button style={btn} onClick={() => ocoiAdmin.scanDuplicates()
          .then(() => ok("סריקת כפילויות התחילה — ראה «משימות רקע»")).catch(fail)}>
          סרוק כפילויות
        </button>
      </div>

      {view === "pairs" ? (
        <>
          <div style={tableWrap}>
            <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 720 }}>
              <thead>
                <tr>
                  <th style={{ ...th, textAlign: "end", width: 60 }}>ציון</th>
                  <th style={th}>א׳</th>
                  <th style={th}>ב׳</th>
                  <th style={th}>סיבות</th>
                  <th style={th} />
                </tr>
              </thead>
              <tbody>
                {pairs.map((p) => (
                  <tr key={p.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                    <td style={{ ...td, textAlign: "end", fontWeight: 600 }}>{p.score.toFixed(2)}</td>
                    <td style={td}>{p.left.name}</td>
                    <td style={td}>{p.right.name}</td>
                    <td style={td} className="text-sm text-muted">{(p.reasons || []).join(", ")}</td>
                    <td style={{ ...td, whiteSpace: "nowrap" }}>
                      <button className="btn-primary" style={{ padding: "0.2rem 0.55rem", fontSize: "0.78rem" }}
                              onClick={() => review(p.id, "approve")}>מזג</button>{" "}
                      <button style={btn} onClick={() => review(p.id, "reject")}>דחה</button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <Pager total={total} limit={limit} offset={offset} onGo={load} />
        </>
      ) : (
        <div>
          <div className="text-sm text-muted" style={{ marginBottom: "0.5rem" }}>
            {total.toLocaleString()} אשכולות. הישות שנשמרת היא זו עם הכי הרבה קשרים.
          </div>
          {clusters.map((c) => (
            <div key={c.canonical_id} style={{ border: "1px solid var(--border,#e2e8f0)", borderRadius: 6, padding: "0.6rem", marginBottom: "0.5rem" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", gap: "0.5rem" }}>
                <strong>{c.members.find((m) => m.id === c.canonical_id)?.name}</strong>
                <span className="text-sm text-muted">{c.size} ישויות</span>
                <button className="btn-primary" onClick={() => mergeCluster(c)}>מזג הכל</button>
              </div>
              <div className="text-sm text-muted" style={{ marginTop: "0.35rem" }}>
                {c.members.map((m) => `${m.name} (${m.connections})`).join(" · ")}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ── מאגרי רישום ──────────────────────────────────────────────────────────────
function RegistrySection() {
  const { node, ok, fail } = useMsg();
  const [srcs, setSrcs] = useState<OcoiRegistrySource[]>([]);
  const [recs, setRecs] = useState<OcoiRegistryRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [capped, setCapped] = useState(false);
  const [offset, setOffset] = useState(0);
  const [q, setQ] = useState("");
  const [source, setSource] = useState("");
  const limit = 50;

  const loadSrcs = () => ocoiAdmin.registrySources().then((r) => setSrcs(r.data)).catch(fail);
  useEffect(() => { loadSrcs(); }, []); // eslint-disable-line

  const loadRecs = useCallback((o = offset) => {
    ocoiAdmin.registryRecords({ q, source: source || undefined, limit, offset: o })
      .then((r) => { setRecs(r.data); setTotal(r.total || 0); setCapped(!!r.total_capped); setOffset(o); })
      .catch(fail);
  }, [q, source, offset]); // eslint-disable-line
  useEffect(() => { loadRecs(0); }, [source]); // eslint-disable-line

  return (
    <div>
      {node}
      <div style={tableWrap}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 640 }}>
          <thead>
            <tr>
              <th style={th}>מאגר</th>
              <th style={{ ...th, textAlign: "end" }}>רשומות</th>
              <th style={th}>סטטוס</th>
              <th style={th}>סונכרן</th>
              <th style={th} />
            </tr>
          </thead>
          <tbody>
            {srcs.map((s) => (
              <tr key={s.key} style={{ borderBottom: "1px solid var(--border,#f1f5f9)", opacity: s.enabled ? 1 : 0.6 }}>
                <td style={td}>
                  {s.label}
                  {!s.enabled && <div className="text-sm text-muted">מושבת — {s.note}</div>}
                  {s.error_message && <div className="text-sm" style={{ color: "var(--danger,#992C2C)" }}>{s.error_message}</div>}
                </td>
                <td style={{ ...td, textAlign: "end" }}>{s.rows_held.toLocaleString()}</td>
                <td style={td}>{s.sync_status}</td>
                <td style={td}>{fmtDate(s.last_synced_at)}</td>
                <td style={td}>
                  <button style={btn} disabled={!s.enabled}
                          onClick={() => ocoiAdmin.syncRegistry([s.key])
                            .then(() => ok(`סנכרון ${s.label} התחיל`)).catch(fail)}>סנכרן</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", margin: "0.8rem 0 0.6rem" }}>
        <button className="btn-primary" onClick={() => ocoiAdmin.matchRegistry()
          .then(() => ok("התאמת חברות ועמותות למאגר התחילה — ראה «משימות רקע»")).catch(fail)}>
          התאם ישויות למאגר
        </button>
        <button style={btn} onClick={() => ocoiAdmin.syncRegistry()
          .then(() => ok("סנכרון כל המאגרים הפעילים התחיל")).catch(fail)}>סנכרן הכל</button>
        <button style={btn} onClick={loadSrcs}>רענן</button>
      </div>

      <h4 style={{ margin: "0.6rem 0 0.4rem" }}>חיפוש במאגר</h4>
      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.5rem" }}>
        <input style={{ ...inp, minWidth: 220 }} placeholder="שם חברה / עמותה"
               value={q} onChange={(e) => setQ(e.target.value)}
               onKeyDown={(e) => e.key === "Enter" && loadRecs(0)} />
        <select style={inp} value={source} onChange={(e) => setSource(e.target.value)}>
          <option value="">כל המאגרים</option>
          {srcs.filter((s) => s.enabled).map((s) => <option key={s.key} value={s.key}>{s.label}</option>)}
        </select>
        <button style={btn} onClick={() => loadRecs(0)}>חפש</button>
      </div>
      <div style={tableWrap}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 560 }}>
          <thead>
            <tr><th style={th}>שם</th><th style={th}>מספר</th><th style={th}>מאגר</th><th style={th}>סטטוס</th></tr>
          </thead>
          <tbody>
            {recs.map((r) => (
              <tr key={r.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td}>{r.name}</td>
                <td style={td}>{r.registration_number || "—"}</td>
                <td style={td} className="text-sm text-muted">{r.source_type}</td>
                <td style={td} className="text-sm">{r.status || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pager total={total} limit={limit} offset={offset} capped={capped} onGo={loadRecs} />
    </div>
  );
}

// ── רשימת התעלמות ────────────────────────────────────────────────────────────
function IgnoredSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcoiIgnored[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [q, setQ] = useState("");
  const [adding, setAdding] = useState("");
  const limit = 50;

  const load = useCallback((o = offset) => {
    ocoiAdmin.ignored({ q, limit, offset: o })
      .then((r) => { setRows(r.data); setTotal(r.total || 0); setOffset(o); }).catch(fail);
  }, [q, offset]); // eslint-disable-line
  useEffect(() => { load(0); }, []); // eslint-disable-line

  return (
    <div>
      {node}
      <p className="text-sm text-muted" style={{ maxWidth: "44rem", marginTop: 0 }}>
        כתובות שהגילוי האוטומטי לא יציע שוב. הרשימה מתמלאת גם לבד: קובץ שנדחף
        ותוכנו זהה למסמך קיים נרשם כאן — זה מה שמונע מהוורקר להוריד את אותה
        הצהרה שוב ושוב.
      </p>
      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
        <input style={{ ...inp, minWidth: 220 }} placeholder="חיפוש"
               value={q} onChange={(e) => setQ(e.target.value)}
               onKeyDown={(e) => e.key === "Enter" && load(0)} />
        <button style={btn} onClick={() => load(0)}>חפש</button>
        <input style={{ ...inp, minWidth: 300 }} placeholder="הוסף כתובת להתעלמות"
               value={adding} onChange={(e) => setAdding(e.target.value)} />
        <button style={btn} disabled={!adding.trim()}
                onClick={() => ocoiAdmin.addIgnored([adding.trim()])
                  .then(() => { ok("נוסף"); setAdding(""); load(0); }).catch(fail)}>הוסף</button>
      </div>
      <div style={tableWrap}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 620 }}>
          <thead>
            <tr><th style={th}>כותרת</th><th style={th}>כתובת</th><th style={th}>מקור</th><th style={th}>נוסף</th><th style={th} /></tr>
          </thead>
          <tbody>
            {rows.map((r) => (
              <tr key={r.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td}>{r.title || "—"}</td>
                <td style={{ ...td, maxWidth: 320, wordBreak: "break-all" }} className="text-sm text-muted">
                  {r.file_url}
                </td>
                <td style={td} className="text-sm">{r.source_type}</td>
                <td style={td}>{fmtDate(r.created_at)}</td>
                <td style={td}>
                  <button style={btn} onClick={() => ocoiAdmin.removeIgnored([r.file_url])
                    .then(() => { ok("הוסר — הגילוי יוכל להציע שוב"); load(); }).catch(fail)}>הסר</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pager total={total} limit={limit} offset={offset} onGo={load} />
    </div>
  );
}

// ── הצעות ציבור ──────────────────────────────────────────────────────────────
function SuggestionsSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcoiSuggestion[]>([]);
  const [total, setTotal] = useState(0);
  const [offset, setOffset] = useState(0);
  const [status, setStatus] = useState("pending");
  const limit = 50;

  const load = useCallback((o = offset) => {
    ocoiAdmin.suggestions({ status, limit, offset: o })
      .then((r) => { setRows(r.data); setTotal(r.total || 0); setOffset(o); }).catch(fail);
  }, [status, offset]); // eslint-disable-line
  useEffect(() => { load(0); }, [status]); // eslint-disable-line

  const review = (id: string, s: string) =>
    ocoiAdmin.reviewSuggestion(id, s).then(() => { ok("נרשם"); load(); }).catch(fail);

  return (
    <div>
      {node}
      <p className="text-sm text-muted" style={{ maxWidth: "44rem", marginTop: 0 }}>
        אישור מסמן שההצעה נבדקה — הוא לא מחיל את השינוי. הצעה היא טענה, לא עובדה;
        את התיקון עצמו מבצעים בלשונית «ישויות» או «קשרים».
      </p>
      <div style={{ display: "flex", gap: "0.4rem", marginBottom: "0.6rem" }}>
        <select style={inp} value={status} onChange={(e) => setStatus(e.target.value)}>
          <option value="pending">ממתינות</option>
          <option value="approved">אושרו</option>
          <option value="rejected">נדחו</option>
          <option value="all">הכל</option>
        </select>
      </div>
      <div style={tableWrap}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 720 }}>
          <thead>
            <tr>
              <th style={th}>יעד</th><th style={th}>שדה</th><th style={th}>נוכחי</th>
              <th style={th}>מוצע</th><th style={th}>הערה</th><th style={th}>נשלח</th><th style={th} />
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 && (
              <tr><td style={td} colSpan={7} className="text-sm text-muted">אין הצעות</td></tr>
            )}
            {rows.map((s) => (
              <tr key={s.id} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td} className="text-sm">{s.target_kind}</td>
                <td style={td} className="text-sm">{s.field_name}</td>
                <td style={{ ...td, maxWidth: 180 }}>{s.current_value || "—"}</td>
                <td style={{ ...td, maxWidth: 180, fontWeight: 500 }}>{s.proposed_value || "—"}</td>
                <td style={{ ...td, maxWidth: 200 }} className="text-sm text-muted">{s.comment || "—"}</td>
                <td style={td}>{fmtDate(s.created_at)}</td>
                <td style={{ ...td, whiteSpace: "nowrap" }}>
                  {s.status === "pending" ? (
                    <>
                      <button style={btn} onClick={() => review(s.id, "approved")}>אשר</button>{" "}
                      <button style={btn} onClick={() => review(s.id, "rejected")}>דחה</button>
                    </>
                  ) : <span className="text-sm text-muted">{s.status}</span>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Pager total={total} limit={limit} offset={offset} onGo={load} />
    </div>
  );
}

// ── טקסטים + פרומפט החילוץ ───────────────────────────────────────────────────
const CONTENT_KEYS: [string, string, string][] = [
  ["extraction_prompt", "פרומפט חילוץ",
   "ההנחיה שה-worker שולח ל-LLM כשהוא מחלץ ישויות וקשרים מהצהרה. ריק = הפרומפט המובנה בוורקר."],
  ["about_content", "אודות", "הטקסט בעמוד «אודות» של ניגוד עניינים לעם."],
  ["header_links", "קישורי כותרת", "קישורים בראש העמוד (JSON)."],
  ["footer_text", "טקסט תחתית", "הטקסט בתחתית העמוד."],
];

function ContentSection() {
  const { node, ok, fail } = useMsg();
  const [key, setKey] = useState("extraction_prompt");
  const [value, setValue] = useState("");
  const [updated, setUpdated] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const load = useCallback((k: string) => {
    setLoading(true);
    ocoiAdmin.content(k)
      .then((r) => { setValue(r.data.value || ""); setUpdated(r.data.updated_at); })
      .catch(fail).finally(() => setLoading(false));
  }, []); // eslint-disable-line
  useEffect(() => { load(key); }, [key]); // eslint-disable-line

  const meta = CONTENT_KEYS.find(([k]) => k === key);

  return (
    <div>
      {node}
      <div style={{ display: "flex", gap: "0.4rem", flexWrap: "wrap", marginBottom: "0.6rem" }}>
        {CONTENT_KEYS.map(([k, label]) => (
          <button key={k} style={{ ...btn, fontWeight: key === k ? 700 : 400 }}
                  onClick={() => setKey(k)}>{label}</button>
        ))}
      </div>
      <p className="text-sm text-muted" style={{ maxWidth: "46rem", marginTop: 0 }}>
        {meta?.[2]}
        {key === "extraction_prompt" && (
          <> בגרסה העצמאית הפרומפט ישב בקובץ על דיסק אפמרי, כך שכל עריכה נמחקה
          בפריסה הבאה; כאן הוא יושב בבסיס הנתונים והוורקר מושך אותו דרך
          <code style={{ margin: "0 0.25rem" }}>/api/worker/ocoi-config</code>.</>
        )}
      </p>
      <textarea
        style={{ ...inp, width: "100%", minHeight: key === "extraction_prompt" ? 320 : 180,
                 fontFamily: key === "extraction_prompt" ? "monospace" : undefined,
                 fontSize: "0.82rem", lineHeight: 1.5 }}
        value={value} onChange={(e) => setValue(e.target.value)}
        disabled={loading} dir="auto"
      />
      <div style={{ display: "flex", gap: "0.5rem", alignItems: "center", marginTop: "0.5rem" }}>
        <button className="btn-primary" disabled={loading}
                onClick={() => ocoiAdmin.saveContent(key, value)
                  .then(() => { ok("נשמר"); load(key); }).catch(fail)}>שמור</button>
        <button style={btn} onClick={() => load(key)}>בטל שינויים</button>
        <span className="text-sm text-muted">
          {value.length.toLocaleString()} תווים · עודכן {fmtDate(updated)}
        </span>
      </div>
    </div>
  );
}

// ── משימות רקע ───────────────────────────────────────────────────────────────
const JOB_LABELS: Record<string, string> = {
  duplicate_scan: "סריקת כפילויות",
  registry_sync: "סנכרון מאגרי רישום",
  registry_match: "התאמה למאגר",
};

function JobsSection() {
  const { node, ok, fail } = useMsg();
  const [rows, setRows] = useState<OcoiJob[]>([]);
  const load = useCallback(() => {
    ocoiAdmin.jobs().then((r) => setRows(r.data)).catch(fail);
  }, []); // eslint-disable-line
  useEffect(() => {
    load();
    const t = setInterval(load, 5000);
    return () => clearInterval(t);
  }, [load]);

  const fmtTime = (s: string | null) => {
    if (!s) return "—";
    const d = new Date(s);
    return isNaN(d.getTime()) ? "—" : d.toLocaleString("he-IL");
  };

  return (
    <div>
      {node}
      <p className="text-sm text-muted" style={{ marginTop: 0 }}>
        מתרענן כל 5 שניות. משימה שנתקעה במצב «running» משתחררת לבד אחרי 30 דקות.
      </p>
      <div style={tableWrap}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 640 }}>
          <thead>
            <tr><th style={th}>משימה</th><th style={th}>מצב</th><th style={th}>התקדמות</th>
                <th style={th}>התחילה</th><th style={th}>הסתיימה</th><th style={th} /></tr>
          </thead>
          <tbody>
            {rows.map((j) => (
              <tr key={j.kind} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                <td style={td}>{JOB_LABELS[j.kind] || j.kind}</td>
                <td style={td}>
                  {j.status}
                  {j.error && <div className="text-sm" style={{ color: "var(--danger,#992C2C)" }}>{j.error}</div>}
                </td>
                <td style={{ ...td, maxWidth: 300 }} className="text-sm text-muted">
                  {Object.keys(j.progress || {}).length
                    ? Object.entries(j.progress).map(([k, v]) => `${k}=${v}`).join(" · ")
                    : "—"}
                </td>
                <td style={td} className="text-sm">{fmtTime(j.started_at)}</td>
                <td style={td} className="text-sm">{fmtTime(j.finished_at)}</td>
                <td style={td}>
                  <button style={btn} disabled={j.status !== "running"}
                          onClick={() => ocoiAdmin.resetJob(j.kind)
                            .then(() => { ok("שוחררה"); load(); }).catch(fail)}>שחרר</button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── בדיקת תקינות ─────────────────────────────────────────────────────────────
function AuditSection() {
  const { node, ok, fail } = useMsg();
  const [a, setA] = useState<OcoiAudit | null>(null);
  const [busy, setBusy] = useState(false);
  const load = useCallback(() => {
    ocoiAdmin.audit().then((r) => setA(r.data)).catch(fail);
  }, []); // eslint-disable-line
  useEffect(() => { load(); }, [load]);

  const clean = async (dry: boolean) => {
    setBusy(true);
    try {
      const r = await ocoiAdmin.auditCleanup({ dry_run: dry });
      ok(dry ? `הרצה יבשה: ${JSON.stringify(r.data)}` : `נוקה: ${JSON.stringify(r.data)}`);
      if (!dry) load();
    } catch (e) { fail(e); } finally { setBusy(false); }
  };

  return (
    <div>
      {node}
      <p className="text-sm text-muted" style={{ maxWidth: "44rem", marginTop: 0 }}>
        ישויות-רפאים הן שמות שה-LLM החזיר במקום ערך אמיתי — «null», «***», מקפים.
        קשרים יתומים מצביעים על ישות שכבר לא קיימת.
      </p>
      {!a ? <div className="text-sm text-muted">טוען…</div> : (
        <>
          <h4 style={{ margin: "0.5rem 0 0.3rem" }}>ישויות-רפאים</h4>
          <div style={tableWrap}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr><th style={th}>סוג</th><th style={{ ...th, textAlign: "end" }}>כמות</th><th style={th}>דוגמאות</th></tr></thead>
              <tbody>
                {Object.entries(a.placeholder_entities).map(([k, v]) => (
                  <tr key={k} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                    <td style={td}>{ENTITY_TYPES.find(([t]) => t === k)?.[1] || k}</td>
                    <td style={{ ...td, textAlign: "end" }}>{v.count}</td>
                    <td style={td} className="text-sm text-muted">
                      {v.items.map((i) => i.name).join(" · ") || "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <h4 style={{ margin: "0.8rem 0 0.3rem" }}>קשרים יתומים</h4>
          <div style={tableWrap}>
            <table style={{ width: "100%", borderCollapse: "collapse" }}>
              <thead><tr><th style={th}>סוג</th><th style={{ ...th, textAlign: "end" }}>קשרים</th></tr></thead>
              <tbody>
                {Object.entries(a.orphan_relationships).map(([k, v]) => (
                  <tr key={k} style={{ borderBottom: "1px solid var(--border,#f1f5f9)" }}>
                    <td style={td}>{ENTITY_TYPES.find(([t]) => t === k)?.[1] || k}</td>
                    <td style={{ ...td, textAlign: "end" }}>{v.relationships}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.8rem" }}>
            <button style={btn} disabled={busy} onClick={() => clean(true)}>הרצה יבשה</button>
            <button style={{ ...btn, color: "var(--danger,#992C2C)" }} disabled={busy}
                    onClick={() => window.confirm("למחוק את ישויות-הרפאים והקשרים היתומים?") && clean(false)}>
              נקה
            </button>
          </div>
        </>
      )}
    </div>
  );
}

// ── shell ────────────────────────────────────────────────────────────────────
export default function OcoiAdminPanel() {
  const [section, setSection] = useState<Section>("dashboard");
  return (
    <div>
      <div style={{ display: "flex", gap: "0.35rem", flexWrap: "wrap", marginBottom: "0.9rem" }}>
        {SECTIONS.map(([id, label]) => (
          <button
            key={id}
            onClick={() => setSection(id)}
            style={{
              ...btn,
              fontWeight: section === id ? 700 : 400,
              background: section === id ? "var(--bg-muted,#e2e8f0)" : "none",
            }}
          >
            {label}
          </button>
        ))}
      </div>
      {section === "dashboard" && <DashboardSection onNav={setSection} />}
      {section === "documents" && <DocumentsSection />}
      {section === "entities" && <EntitiesSection />}
      {section === "relationships" && <RelationshipsSection />}
      {section === "duplicates" && <DuplicatesSection />}
      {section === "registry" && <RegistrySection />}
      {section === "ignored" && <IgnoredSection />}
      {section === "suggestions" && <SuggestionsSection />}
      {section === "content" && <ContentSection />}
      {section === "jobs" && <JobsSection />}
      {section === "audit" && <AuditSection />}
    </div>
  );
}
