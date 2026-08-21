import { useCallback, useEffect, useMemo, useState } from "react";
import { ocal, OcalEvent } from "../../api/client";
import { fmtTime, HE_DOW, HE_MONTHS, isoDate } from "./ocalUtils";

function parseDate(s: string): Date {
  const [y, m, d] = s.slice(0, 10).split("-").map(Number);
  return new Date(y, (m || 1) - 1, d || 1);
}

interface Cell {
  dateStr: string;
  day: number;
  inMonth: boolean;
  events: OcalEvent[];
}

export default function OcalCalendar() {
  const today = useMemo(() => new Date(), []);
  const [cursor, setCursor] = useState<Date>(new Date(today.getFullYear(), today.getMonth(), 1));
  const [events, setEvents] = useState<OcalEvent[]>([]);
  const [range, setRange] = useState<{ from: string; to: string } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  const load = useCallback(() => {
    setLoading(true);
    setError(null);
    ocal
      .calendar({ date: isoDate(cursor), view: "month" })
      .then((r) => { setEvents(r.events); setRange(r.date_range); })
      .catch((e) => { setError(e?.message || "שגיאה בטעינת לוח השנה"); setEvents([]); setRange(null); })
      .finally(() => setLoading(false));
  }, [cursor]);

  useEffect(() => { load(); }, [load]);

  const byDate = useMemo(() => {
    const m = new Map<string, OcalEvent[]>();
    for (const ev of events) {
      const k = String(ev.event_date).slice(0, 10);
      if (!m.has(k)) m.set(k, []);
      m.get(k)!.push(ev);
    }
    return m;
  }, [events]);

  const cells = useMemo<Cell[]>(() => {
    if (!range) return [];
    const out: Cell[] = [];
    const end = parseDate(range.to);
    const cur = parseDate(range.from);
    const month = cursor.getMonth();
    while (cur <= end) {
      const ds = isoDate(cur);
      out.push({ dateStr: ds, day: cur.getDate(), inMonth: cur.getMonth() === month, events: byDate.get(ds) || [] });
      cur.setDate(cur.getDate() + 1);
    }
    return out;
  }, [range, byDate, cursor]);

  const todayStr = isoDate(today);
  const shiftMonth = (delta: number) => {
    setSelected(null);
    setCursor((c) => new Date(c.getFullYear(), c.getMonth() + delta, 1));
  };

  const navBtn: React.CSSProperties = {
    padding: "0.35rem 0.8rem", borderRadius: 4, border: "1px solid var(--border)",
    background: "none", cursor: "pointer", fontSize: "0.85rem",
  };

  const selectedEvents = selected ? byDate.get(selected) || [] : [];

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: "0.6rem", marginBottom: "0.8rem", flexWrap: "wrap" }}>
        <button type="button" style={navBtn} onClick={() => shiftMonth(-1)}>→ קודם</button>
        <button type="button" style={navBtn} onClick={() => { setSelected(null); setCursor(new Date(today.getFullYear(), today.getMonth(), 1)); }}>היום</button>
        <button type="button" style={navBtn} onClick={() => shiftMonth(1)}>הבא ←</button>
        <strong style={{ fontSize: "1.05rem", marginInlineStart: "0.5rem" }}>
          {HE_MONTHS[cursor.getMonth()]} {cursor.getFullYear()}
        </strong>
        <span className="text-sm text-muted" style={{ marginInlineStart: "auto" }}>
          {loading ? "טוען…" : `${events.length.toLocaleString()} אירועים בחודש`}
        </span>
      </div>

      {error && <div style={{ color: "var(--danger)", marginBottom: "0.6rem" }}>{error}</div>}

      <div style={{ display: "grid", gridTemplateColumns: "repeat(7, 1fr)", gap: 1, background: "var(--border)", border: "1px solid var(--border)", borderRadius: 6, overflow: "hidden" }}>
        {HE_DOW.map((d) => (
          <div key={d} style={{ background: "var(--surface-2)", textAlign: "center", padding: "0.35rem 0", fontWeight: 600, fontSize: "0.8rem", color: "var(--text-muted)" }}>{d}</div>
        ))}
        {cells.map((c) => {
          const isToday = c.dateStr === todayStr;
          return (
            <button
              key={c.dateStr}
              type="button"
              onClick={() => setSelected(c.dateStr === selected ? null : c.dateStr)}
              style={{
                background: c.dateStr === selected ? "var(--surface-2)" : "var(--bg)",
                minHeight: 92, padding: "0.3rem", textAlign: "start", border: "none",
                cursor: "pointer", opacity: c.inMonth ? 1 : 0.4, display: "flex", flexDirection: "column", gap: 2,
              }}
            >
              <span style={{
                fontSize: "0.78rem", fontWeight: isToday ? 700 : 500, alignSelf: "flex-end",
                color: isToday ? "var(--primary)" : "var(--text-muted)",
                ...(isToday ? { background: "var(--fill-brand)", color: "var(--on-fill-brand)", borderRadius: "50%", width: 20, height: 20, display: "inline-flex", alignItems: "center", justifyContent: "center" } : {}),
              }}>
                {c.day}
              </span>
              {c.events.slice(0, 3).map((ev) => (
                <span key={ev.id} title={ev.title} style={{ display: "flex", alignItems: "center", gap: 3, fontSize: "0.68rem", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                  <span aria-hidden style={{ width: 6, height: 6, borderRadius: "50%", background: ev.source_color || "#3B82F6", flex: "0 0 auto" }} />
                  <span style={{ overflow: "hidden", textOverflow: "ellipsis" }}>{ev.title}</span>
                </span>
              ))}
              {c.events.length > 3 && (
                <span style={{ fontSize: "0.68rem", color: "var(--primary)" }}>+{c.events.length - 3} נוספים</span>
              )}
            </button>
          );
        })}
      </div>

      {selected && (
        <div className="card" style={{ marginTop: "1rem", padding: "0.9rem" }}>
          <strong>{selected.split("-").reverse().join(".")}</strong>{" "}
          <span className="text-sm text-muted">· {selectedEvents.length} אירועים</span>
          <ul style={{ margin: "0.6rem 0 0", padding: 0, listStyle: "none" }}>
            {selectedEvents.map((ev) => (
              <li key={ev.id} style={{ padding: "0.4rem 0", borderTop: "1px solid var(--border)", display: "flex", gap: "0.6rem", alignItems: "baseline", flexWrap: "wrap" }}>
                <span aria-hidden style={{ width: 9, height: 9, borderRadius: "50%", background: ev.source_color || "#3B82F6", flex: "0 0 auto" }} />
                <span className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>{[fmtTime(ev.start_time), fmtTime(ev.end_time)].filter(Boolean).join("–") || "—"}</span>
                <span style={{ fontWeight: 500 }}>
                  {ev.dataset_link ? <a href={ev.dataset_link} target="_blank" rel="noopener noreferrer" style={{ color: "var(--primary)" }}>{ev.title}<span className="sr-only"> (נפתח בחלון חדש)</span></a> : ev.title}
                </span>
                <span className="text-sm text-muted">· {ev.source_name}</span>
                {ev.location && <span className="text-sm text-muted">· 📍 {ev.location}</span>}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
