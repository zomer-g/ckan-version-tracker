import { useCallback, useEffect, useState } from "react";
import { admin } from "../api/client";

/**
 * Admin: materialise יומן לעם (ocal) into the console DB so it can be JOINed with
 * every other table in /data. ocal lives in a separate Neon DB; this copies its
 * tables into a local `ocal` schema. A scheduled job refreshes every 6h; this
 * panel forces a refresh now and shows what's currently materialised.
 */
type OcalTable = { table: string; title: string; rows: number | null };

export default function OcalSyncPanel() {
  const [tables, setTables] = useState<OcalTable[]>([]);
  const [configured, setConfigured] = useState(true);
  const [loading, setLoading] = useState(true);
  const [syncing, setSyncing] = useState(false);
  const [msg, setMsg] = useState<{ ok: boolean; text: string } | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const r = await admin.ocalStatus();
      setTables(r.tables || []);
      setConfigured(r.configured);
    } catch {
      setTables([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  const doSync = async () => {
    setSyncing(true);
    setMsg(null);
    try {
      const r = await admin.ocalSync(true); // inline so we can report the result
      setMsg({ ok: true, text: `סונכרן: ${r.synced ?? 0} טבלאות, ${(r.rows ?? 0).toLocaleString()} שורות${r.failed ? `, ${r.failed} נכשלו` : ""}` });
      await load();
    } catch (e) {
      setMsg({ ok: false, text: `סנכרון נכשל: ${(e as Error).message}` });
    } finally {
      setSyncing(false);
    }
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: "1rem" }}>
      <div style={{ fontSize: "0.88rem", lineHeight: 1.6, color: "#7c2d12", background: "#fffdf9", border: "1px solid #f59e0b", borderInlineStart: "4px solid #b45309", borderRadius: 6, padding: "0.75rem 0.9rem" }}>
        <strong>מידע מעובד.</strong> יומן לעם (ocal.org.il) יושב במסד נתונים נפרד. כאן
        מעתיקים את הטבלאות שלו לסכמה <code dir="ltr">ocal</code> בתוך מסד ה-SQL של
        הקונסולה — כך אפשר לתשאל ולעשות <strong>JOIN</strong> ביניהן לבין כל שאר
        הטבלאות (מידע לעם, הכנסת וכו'). רענון אוטומטי כל 6 שעות; הכפתור מרענן עכשיו.
      </div>

      {!configured && (
        <div role="alert" style={{ padding: "0.6rem 0.85rem", borderRadius: 6, background: "#fef2f2", color: "#991b1b", border: "1px solid #fecaca", fontSize: "0.88rem" }}>
          OCAL_DATABASE_URL אינו מוגדר — אי אפשר לסנכרן.
        </div>
      )}

      {msg && (
        <div role="status" style={{ padding: "0.6rem 0.85rem", borderRadius: 6, fontSize: "0.88rem", background: msg.ok ? "#ecfdf5" : "#fef2f2", color: msg.ok ? "#065f46" : "#991b1b", border: `1px solid ${msg.ok ? "#a7f3d0" : "#fecaca"}` }}>
          {msg.text}
        </div>
      )}

      <div>
        <button
          type="button"
          onClick={doSync}
          disabled={syncing || !configured}
          style={{ padding: "0.45rem 1.1rem", border: "none", borderRadius: 4, background: "#b45309", color: "#fff", fontWeight: 600, cursor: syncing ? "wait" : "pointer", opacity: syncing ? 0.7 : 1 }}
        >
          {syncing ? "מסנכרן…" : "⟳ סנכרן עכשיו"}
        </button>
      </div>

      <section>
        <h3 style={{ margin: "0 0 0.5rem", fontSize: "1.05rem" }}>
          טבלאות מסונכרנות ({tables.length})
        </h3>
        {loading ? (
          <div className="text-sm text-muted">טוען…</div>
        ) : tables.length === 0 ? (
          <div className="text-sm text-muted">עדיין לא סונכרן. לחצו "סנכרן עכשיו".</div>
        ) : (
          <div style={{ overflowX: "auto" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
              <thead>
                <tr style={{ borderBottom: "2px solid var(--border, #e5e7eb)" }}>
                  <th style={{ textAlign: "start", padding: "0.4rem 0.5rem" }}>כותרת</th>
                  <th style={{ textAlign: "start", padding: "0.4rem 0.5rem" }}>טבלה</th>
                  <th style={{ textAlign: "start", padding: "0.4rem 0.5rem" }}>שורות</th>
                </tr>
              </thead>
              <tbody>
                {tables.map((t) => (
                  <tr key={t.table} style={{ borderBottom: "1px solid var(--border, #f1f5f9)" }}>
                    <td style={{ padding: "0.4rem 0.5rem" }}>{t.title}</td>
                    <td style={{ padding: "0.4rem 0.5rem" }}>
                      <code dir="ltr" style={{ unicodeBidi: "isolate", fontSize: "0.78rem" }}>ocal.{t.table}</code>
                    </td>
                    <td style={{ padding: "0.4rem 0.5rem", fontVariantNumeric: "tabular-nums" }}>
                      {t.rows != null ? t.rows.toLocaleString() : "—"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
