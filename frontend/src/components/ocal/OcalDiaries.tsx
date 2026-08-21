import { useEffect, useMemo, useState } from "react";
import { ocal, OcalSource } from "../../api/client";
import { fmtDateHe } from "./ocalUtils";

function saveBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

export default function OcalDiaries() {
  const [sources, setSources] = useState<OcalSource[]>([]);
  const [filter, setFilter] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [bulking, setBulking] = useState(false);

  useEffect(() => {
    ocal
      .sources()
      .then((r) => setSources(r.data))
      .catch((e) => setError(e?.message || "שגיאה בטעינת היומנים"))
      .finally(() => setLoading(false));
  }, []);

  const shown = useMemo(() => {
    const f = filter.trim().toLowerCase();
    if (!f) return sources;
    return sources.filter(
      (s) =>
        s.name.toLowerCase().includes(f) ||
        (s.person_name || "").toLowerCase().includes(f) ||
        (s.organization_name || "").toLowerCase().includes(f),
    );
  }, [sources, filter]);

  const toggle = (id: string) =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });

  const allShownSelected = shown.length > 0 && shown.every((s) => selected.has(s.id));
  const toggleAll = () =>
    setSelected((prev) => {
      const next = new Set(prev);
      if (allShownSelected) shown.forEach((s) => next.delete(s.id));
      else shown.forEach((s) => next.add(s.id));
      return next;
    });

  const bulk = async (format: "csv" | "json") => {
    if (selected.size === 0) return;
    setBulking(true);
    setError(null);
    try {
      const blob = await ocal.downloadBulk([...selected], format);
      const name = selected.size === 1 ? `diary.${format}.zip` : `ocal-${selected.size}-diaries.zip`;
      saveBlob(blob, name);
    } catch (e) {
      setError((e as Error)?.message || "שגיאה בהורדה");
    } finally {
      setBulking(false);
    }
  };

  const th: React.CSSProperties = { textAlign: "start", padding: "0.45rem 0.6rem", borderBottom: "2px solid var(--border, var(--border))", fontSize: "0.82rem", position: "sticky", top: 0, background: "var(--surface-2)" };
  const td: React.CSSProperties = { padding: "0.4rem 0.6rem", fontSize: "0.85rem", verticalAlign: "top" };
  const dl: React.CSSProperties = { fontSize: "0.78rem", color: "var(--primary)", textDecoration: "underline" };

  return (
    <div style={{ paddingBottom: selected.size > 0 ? 64 : 0 }}>
      <div style={{ display: "flex", gap: "0.6rem", alignItems: "center", marginBottom: "0.7rem", flexWrap: "wrap" }}>
        <input
          type="search"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="סינון יומנים לפי שם / בעלים…"
          aria-label="סינון יומנים"
          style={{ flex: "1 1 280px", padding: "0.4rem 0.6rem", border: "1px solid var(--border, var(--border))", borderRadius: 4 }}
        />
        <span className="text-sm text-muted">
          {loading ? "טוען…" : `${shown.length.toLocaleString()} יומנים`}
          {selected.size > 0 ? ` · ${selected.size} נבחרו` : ""}
        </span>
      </div>

      {error && <div style={{ color: "var(--danger, #992C2C)", marginBottom: "0.6rem" }}>{error}</div>}

      <div tabIndex={0} role="region" aria-label="יומני הפגישות" className="scroll-region" style={{ overflowX: "auto", maxHeight: 620, border: "1px solid var(--border, var(--border))", borderRadius: 6 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", minWidth: 720 }}>
          <thead>
            <tr>
              <th scope="col" style={{ ...th, width: 34 }}>
                <input type="checkbox" checked={allShownSelected} onChange={toggleAll} aria-label="בחר הכל" />
              </th>
              <th scope="col" style={th}>יומן</th>
              <th scope="col" style={th}>בעלים</th>
              <th scope="col" style={{ ...th, textAlign: "end" }}>אירועים</th>
              <th scope="col" style={th}>טווח תאריכים</th>
              <th scope="col" style={th}>הורדה</th>
            </tr>
          </thead>
          <tbody>
            {shown.map((s) => (
              <tr key={s.id} style={{ borderBottom: "1px solid var(--border, var(--border))" }}>
                <td style={td}>
                  <input type="checkbox" checked={selected.has(s.id)} onChange={() => toggle(s.id)} aria-label={`בחר ${s.name}`} />
                </td>
                <td style={td}>
                  <span style={{ display: "inline-flex", alignItems: "center", gap: "0.4rem" }}>
                    <span aria-hidden style={{ width: 9, height: 9, borderRadius: "50%", background: s.color || "#3B82F6", flex: "0 0 auto" }} />
                    {s.name}
                  </span>
                </td>
                <td style={{ ...td, color: "var(--text-muted)" }}>{s.person_name || s.organization_name || "—"}</td>
                <td style={{ ...td, textAlign: "end" }}>{(s.total_events || 0).toLocaleString()}</td>
                <td style={{ ...td, color: "var(--text-muted)", whiteSpace: "nowrap" }}>
                  {s.first_event_date ? `${fmtDateHe(s.first_event_date)} – ${fmtDateHe(s.last_event_date)}` : "—"}
                </td>
                <td style={td}>
                  <a style={dl} href={ocal.downloadSourceUrl(s.id, { format: "csv" })}>CSV</a>
                  {" · "}
                  <a style={dl} href={ocal.downloadSourceUrl(s.id, { format: "json" })}>JSON</a>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
        {!loading && shown.length === 0 && (
          <div className="text-sm text-muted" style={{ padding: "1rem" }}>לא נמצאו יומנים.</div>
        )}
      </div>

      {selected.size > 0 && (
        <div style={{
          position: "sticky", bottom: 0, marginTop: "0.6rem", padding: "0.6rem 0.9rem",
          background: "var(--bg, #fff)", border: "1px solid var(--border, var(--border))", borderRadius: 6,
          display: "flex", gap: "0.6rem", alignItems: "center", flexWrap: "wrap",
          boxShadow: "0 -2px 8px rgba(0,0,0,0.06)",
        }}>
          <strong className="text-sm">{selected.size} יומנים נבחרו</strong>
          <button type="button" className="btn-primary" disabled={bulking} onClick={() => bulk("csv")}>
            {bulking ? "מכין…" : "⬇ הורד ZIP (CSV)"}
          </button>
          <button type="button" className="btn-secondary" disabled={bulking} onClick={() => bulk("json")}>
            <span aria-hidden="true">⬇</span> ZIP (JSON)
          </button>
          <button type="button" className="btn-secondary" onClick={() => setSelected(new Set())}>נקה בחירה</button>
        </div>
      )}
    </div>
  );
}
