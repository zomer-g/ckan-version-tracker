import { useEffect, useMemo, useState } from "react";
import {
  decisionAnalysis,
  DecisionAnalysisView,
  DecisionDoc,
  DecisionSection,
  DecisionTask,
  DecisionTaskStatus,
} from "../api/client";
import { invalidatePublishedDecisions } from "../hooks/usePublishedDecisions";

// Admin editor for the government-decision analysis page (/rationale/:key).
//
// The whole document is edited in local state and saved in one PUT — it is one
// hand-written essay with ordered sections, so per-field autosave would just
// mean 200 requests and a reordering problem. Publishing is deliberately a
// SEPARATE call from saving: content can be reworked for weeks while the public
// endpoint keeps 404ing, and the switch that changes that is one explicit,
// confirmable click.
//
// "אפס לברירת מחדל" drops the stored blob and the page falls back to the text
// bundled in app/data/decision_1933.py, so an edit is never destructive.

const DECISION_KEYS = ["1933"];

const STATUS_OPTIONS: { id: DecisionTaskStatus; label: string }[] = [
  { id: "done", label: "בוצע" },
  { id: "partial", label: "בוצע חלקית" },
  { id: "not_done", label: "לא בוצע" },
  { id: "unknown", label: "לא ידוע / לא דווח" },
];

// Captions the page renders, in the order they appear there.
const LABEL_FIELDS: { id: string; label: string }[] = [
  { id: "reveal_tasks", label: "כפתור 1 — חילוץ המשימות" },
  { id: "hide_tasks", label: "כפתור 1 — הסתרה" },
  { id: "reveal_analysis", label: "כפתור 2 — הצגת הניתוח" },
  { id: "hide_analysis", label: "כפתור 2 — הסתרה" },
  { id: "tasks_heading", label: "כותרת בלוק המשימות" },
  { id: "potential", label: 'כותרת עמודה — "הפוטנציאל"' },
  { id: "actual", label: 'כותרת עמודה — "בפועל"' },
  { id: "damage", label: 'כותרת עמודה — "הנזק"' },
  { id: "responsible", label: "תווית — אחריות" },
  { id: "due", label: "תווית — מועד" },
  { id: "status_done", label: "סטטוס — בוצע" },
  { id: "status_partial", label: "סטטוס — בוצע חלקית" },
  { id: "status_not_done", label: "סטטוס — לא בוצע" },
  { id: "status_unknown", label: "סטטוס — לא ידוע" },
];

const boxStyle: React.CSSProperties = {
  border: "1px solid var(--border)",
  borderRadius: "8px",
  padding: "0.75rem 0.9rem",
  background: "var(--surface)",
};

const labelStyle: React.CSSProperties = {
  display: "block",
  fontSize: "0.75rem",
  fontWeight: 600,
  color: "var(--text-muted)",
  marginBottom: "0.2rem",
};

const inputStyle: React.CSSProperties = {
  width: "100%",
  fontSize: "0.85rem",
  padding: "0.35rem 0.5rem",
};

const miniBtn: React.CSSProperties = { padding: "0.2rem 0.6rem", fontSize: "0.75rem" };

function Field({
  label,
  value,
  onChange,
  rows,
  dir,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  rows?: number;
  dir?: string;
}) {
  return (
    <div style={{ marginBottom: "0.6rem" }}>
      <label style={labelStyle}>{label}</label>
      {rows ? (
        <textarea
          value={value}
          rows={rows}
          dir={dir}
          onChange={(e) => onChange(e.target.value)}
          style={{ ...inputStyle, lineHeight: 1.6, resize: "vertical" }}
        />
      ) : (
        <input
          type="text"
          value={value}
          dir={dir}
          onChange={(e) => onChange(e.target.value)}
          style={inputStyle}
        />
      )}
    </div>
  );
}

function emptyTask(id: string): DecisionTask {
  return {
    id,
    title: "",
    obligation: "",
    responsible: "",
    due: "",
    status: "unknown",
    potential: "",
    actual: "",
    damage: "",
  };
}

function emptySection(id: string, part: string): DecisionSection {
  return { id, part, label: "", heading: "", text: "", tasks: [] };
}

// A fresh id that no section or task in the document already uses. Ids are
// stable React keys and in-page anchors, so a collision would break both.
function freshId(doc: DecisionDoc, prefix: string): string {
  const used = new Set<string>();
  for (const s of doc.sections) {
    used.add(s.id);
    for (const t of s.tasks) used.add(t.id);
  }
  let n = 1;
  while (used.has(`${prefix}-${n}`)) n += 1;
  return `${prefix}-${n}`;
}

function move<T>(arr: T[], from: number, to: number): T[] {
  if (to < 0 || to >= arr.length) return arr;
  const next = arr.slice();
  const [item] = next.splice(from, 1);
  next.splice(to, 0, item);
  return next;
}

export default function DecisionAnalysisPanel() {
  const [key, setKey] = useState(DECISION_KEYS[0]);
  const [view, setView] = useState<DecisionAnalysisView | null>(null);
  const [doc, setDoc] = useState<DecisionDoc | null>(null);
  const [dirty, setDirty] = useState(false);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [openSection, setOpenSection] = useState<string | null>(null);

  const load = async (k: string) => {
    setLoading(true);
    setErr(null);
    try {
      const data = await decisionAnalysis.getDraft(k);
      setView(data);
      setDoc(data.doc);
      setDirty(false);
      setOpenSection(null);
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    load(key);
  }, [key]);

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2500);
  };

  const taskCount = useMemo(
    () => (doc?.sections ?? []).reduce((n, s) => n + s.tasks.length, 0),
    [doc],
  );

  const patchDoc = (patch: Partial<DecisionDoc>) => {
    setDoc((prev) => (prev ? { ...prev, ...patch } : prev));
    setDirty(true);
  };

  const patchSections = (fn: (sections: DecisionSection[]) => DecisionSection[]) => {
    setDoc((prev) => (prev ? { ...prev, sections: fn(prev.sections) } : prev));
    setDirty(true);
  };

  const patchSection = (idx: number, patch: Partial<DecisionSection>) =>
    patchSections((sections) =>
      sections.map((s, i) => (i === idx ? { ...s, ...patch } : s)),
    );

  const patchTask = (sIdx: number, tIdx: number, patch: Partial<DecisionTask>) =>
    patchSections((sections) =>
      sections.map((s, i) =>
        i === sIdx
          ? { ...s, tasks: s.tasks.map((t, j) => (j === tIdx ? { ...t, ...patch } : t)) }
          : s,
      ),
    );

  const save = async () => {
    if (!doc) return;
    setBusy(true);
    setErr(null);
    try {
      await decisionAnalysis.save(key, { doc });
      setDirty(false);
      // The nav label comes from the document title, so a rename must not keep
      // serving the stale one from the cached index.
      invalidatePublishedDecisions();
      flash("נשמר");
      await load(key);
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setBusy(false);
    }
  };

  const togglePublished = async () => {
    if (!view) return;
    const next = !view.published;
    if (dirty && !confirm("יש שינויים שלא נשמרו. להמשיך בכל זאת בלי לשמור אותם?")) return;
    if (
      next &&
      !confirm("לפרסם את העמוד? מרגע זה הוא יוצג לכל המשתמשים ויופיע בתפריט.")
    )
      return;
    setBusy(true);
    setErr(null);
    try {
      await decisionAnalysis.save(key, { published: next });
      invalidatePublishedDecisions();
      flash(next ? "העמוד פורסם" : "העמוד הוסתר מהציבור");
      await load(key);
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setBusy(false);
    }
  };

  const revert = async () => {
    if (!confirm("לאפס את כל התוכן לנוסח שבקוד? כל העריכות שנשמרו יימחקו.")) return;
    setBusy(true);
    setErr(null);
    try {
      await decisionAnalysis.revert(key);
      invalidatePublishedDecisions();
      flash("אופס לנוסח שבקוד");
      await load(key);
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setBusy(false);
    }
  };

  if (loading) {
    return <div className="empty-state" style={{ padding: "1.5rem" }}>טוען…</div>;
  }

  if (!doc || !view) {
    return (
      <div className="text-sm" style={{ color: "#b91c1c" }}>
        {err ?? "לא נטען"}
      </div>
    );
  }

  return (
    <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          flexWrap: "wrap",
          gap: "0.5rem",
        }}
      >
        <h2 style={{ fontSize: "1.25rem", fontWeight: 700, margin: 0 }}>
          📑 ניתוח החלטת ממשלה
        </h2>
        <a
          href={`/rationale/${key}`}
          target="_blank"
          rel="noopener noreferrer"
          className="text-sm"
          style={{ color: "var(--primary, #2563eb)" }}
        >
          פתח את העמוד ↗
        </a>
      </div>

      <div
        className="text-sm"
        style={{ marginTop: "0.5rem", color: "var(--text-muted)", lineHeight: 1.6 }}
      >
        העמוד מציג את הנוסח המלא של ההחלטה; בלחיצה אחת נחשפות המשימות האופרטיביות של כל
        סעיף, ובלחיצה נוספת — הפוטנציאל, הביצוע בפועל והמחיר, לכל משימה. כל הטקסטים כאן
        ניתנים לעריכה, והם דורסים את הנוסח שבקוד. סעיף בלי משימות מוצג כטקסט בלבד.
      </div>

      {/* Decision selector — one entry today, ready for the next analysis. */}
      {DECISION_KEYS.length > 1 && (
        <div style={{ display: "flex", gap: "0.4rem", alignItems: "center", marginTop: "0.85rem" }}>
          <span className="text-sm" style={{ color: "var(--text-muted)" }}>החלטה:</span>
          {DECISION_KEYS.map((k) => (
            <button
              key={k}
              onClick={() => setKey(k)}
              className={key === k ? "btn-primary" : "btn-secondary"}
              style={{ padding: "0.3rem 0.9rem", fontSize: "0.85rem" }}
            >
              {k}
            </button>
          ))}
        </div>
      )}

      {/* Publish gate + save controls */}
      <div
        style={{
          ...boxStyle,
          marginTop: "0.85rem",
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          gap: "0.75rem",
          background: view.published ? "#ecfdf5" : "#fffbeb",
          borderColor: view.published ? "#6ee7b7" : "#fcd34d",
        }}
      >
        <span style={{ fontSize: "0.9rem", fontWeight: 700 }}>
          {view.published ? "🟢 מפורסם לציבור" : "🟡 טיוטה — מוסתר מהציבור"}
        </span>
        <span className="text-sm" style={{ color: "var(--text-muted)" }}>
          {view.published
            ? "העמוד גלוי לכולם ומופיע בתפריט תחת אודות."
            : "רק מנהלים מחוברים רואים את העמוד; הוא אינו מופיע בתפריט."}
        </span>
        <button
          onClick={togglePublished}
          disabled={busy}
          className={view.published ? "btn-secondary" : "btn-primary"}
          style={{ ...miniBtn, marginInlineStart: "auto" }}
        >
          {view.published ? "הסתר מהציבור" : "פרסם"}
        </button>
      </div>

      <div
        style={{
          display: "flex",
          flexWrap: "wrap",
          alignItems: "center",
          gap: "0.75rem",
          margin: "0.85rem 0",
        }}
      >
        <button onClick={save} disabled={busy || !dirty} className="btn-primary" style={miniBtn}>
          {dirty ? "שמור שינויים" : "אין שינויים לשמירה"}
        </button>
        <button onClick={() => load(key)} disabled={busy} className="btn-secondary" style={miniBtn}>
          בטל שינויים
        </button>
        <button onClick={revert} disabled={busy} className="btn-danger" style={miniBtn}>
          אפס לנוסח שבקוד
        </button>
        <span className="text-sm" style={{ color: "var(--text-muted)" }}>
          {doc.sections.length} סעיפים · {taskCount} משימות
          {view.is_customized
            ? ` · נערך${view.updated_by ? ` ע"י ${view.updated_by}` : ""}`
            : " · נוסח מקורי מהקוד"}
        </span>
      </div>

      {err && <div className="text-sm" style={{ color: "#b91c1c", marginBottom: "0.5rem" }}>{err}</div>}
      {toast && <div className="text-sm" style={{ color: "#065f46", marginBottom: "0.5rem" }}>{toast}</div>}

      {/* Document header */}
      <div style={{ ...boxStyle, marginBottom: "0.85rem" }}>
        <h3 style={{ fontSize: "0.95rem", fontWeight: 700, marginBottom: "0.6rem" }}>
          כותרות העמוד
        </h3>
        <Field label="כותרת" value={doc.title} onChange={(v) => patchDoc({ title: v })} />
        <Field
          label="תת-כותרת"
          value={doc.subtitle}
          onChange={(v) => patchDoc({ subtitle: v })}
          rows={2}
        />
        <Field label="פתיח" value={doc.intro} onChange={(v) => patchDoc({ intro: v })} rows={4} />
        <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
          <div style={{ flex: "1 1 8rem" }}>
            <Field
              label="מספר החלטה"
              value={doc.decision_number}
              onChange={(v) => patchDoc({ decision_number: v })}
            />
          </div>
          <div style={{ flex: "1 1 8rem" }}>
            <Field
              label="תאריך"
              value={doc.decision_date}
              onChange={(v) => patchDoc({ decision_date: v })}
            />
          </div>
          <div style={{ flex: "2 1 18rem" }}>
            <Field
              label="קישור למקור"
              value={doc.decision_url}
              dir="ltr"
              onChange={(v) => patchDoc({ decision_url: v })}
            />
          </div>
        </div>
      </div>

      {/* Button + column captions */}
      <details style={{ ...boxStyle, marginBottom: "0.85rem" }}>
        <summary style={{ cursor: "pointer", fontSize: "0.95rem", fontWeight: 700 }}>
          כיתובי הכפתורים והעמודות
        </summary>
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(auto-fill, minmax(15rem, 1fr))",
            gap: "0 0.75rem",
            marginTop: "0.75rem",
          }}
        >
          {LABEL_FIELDS.map((f) => (
            <Field
              key={f.id}
              label={f.label}
              value={doc.labels?.[f.id] ?? ""}
              onChange={(v) => patchDoc({ labels: { ...(doc.labels || {}), [f.id]: v } })}
            />
          ))}
        </div>
      </details>

      {/* Sections */}
      <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
        {doc.sections.map((section, sIdx) => {
          const open = openSection === section.id;
          return (
            <div key={section.id} style={boxStyle}>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "0.5rem",
                  flexWrap: "wrap",
                }}
              >
                <button
                  onClick={() => setOpenSection(open ? null : section.id)}
                  className="btn-secondary"
                  style={{ ...miniBtn, minWidth: "2rem" }}
                  aria-expanded={open}
                >
                  {open ? "−" : "+"}
                </button>
                <strong style={{ fontSize: "0.85rem" }}>{section.label || "(ללא מספר)"}</strong>
                <span style={{ fontSize: "0.85rem" }}>{section.heading || "(ללא כותרת)"}</span>
                <span
                  className="text-sm"
                  style={{ color: "var(--text-muted)", fontSize: "0.75rem" }}
                >
                  {section.tasks.length > 0 ? `${section.tasks.length} משימות` : "טקסט בלבד"}
                </span>
                <span style={{ marginInlineStart: "auto", display: "flex", gap: "0.25rem" }}>
                  <button
                    onClick={() => patchSections((s) => move(s, sIdx, sIdx - 1))}
                    disabled={sIdx === 0}
                    className="btn-secondary"
                    style={miniBtn}
                    title="הזז למעלה"
                  >
                    ↑
                  </button>
                  <button
                    onClick={() => patchSections((s) => move(s, sIdx, sIdx + 1))}
                    disabled={sIdx === doc.sections.length - 1}
                    className="btn-secondary"
                    style={miniBtn}
                    title="הזז למטה"
                  >
                    ↓
                  </button>
                  <button
                    onClick={() => {
                      if (!confirm(`למחוק את הסעיף "${section.heading || section.label}"?`)) return;
                      patchSections((s) => s.filter((_, i) => i !== sIdx));
                    }}
                    className="btn-danger"
                    style={miniBtn}
                    title="מחק סעיף"
                  >
                    ✕
                  </button>
                </span>
              </div>

              {open && (
                <div style={{ marginTop: "0.75rem" }}>
                  <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
                    <div style={{ flex: "2 1 14rem" }}>
                      <Field
                        label="חלק (קיבוץ הסעיפים בעמוד)"
                        value={section.part}
                        onChange={(v) => patchSection(sIdx, { part: v })}
                      />
                    </div>
                    <div style={{ flex: "1 1 6rem" }}>
                      <Field
                        label="מספר הסעיף"
                        value={section.label}
                        onChange={(v) => patchSection(sIdx, { label: v })}
                      />
                    </div>
                  </div>
                  <Field
                    label="כותרת הסעיף"
                    value={section.heading}
                    onChange={(v) => patchSection(sIdx, { heading: v })}
                  />
                  <Field
                    label="נוסח הסעיף (שורות ריקות נשמרות כפי שהן)"
                    value={section.text}
                    rows={10}
                    onChange={(v) => patchSection(sIdx, { text: v })}
                  />

                  <div
                    style={{
                      borderTop: "1px dashed var(--border)",
                      paddingTop: "0.75rem",
                      marginTop: "0.5rem",
                    }}
                  >
                    <div
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: "0.5rem",
                        marginBottom: "0.6rem",
                      }}
                    >
                      <strong style={{ fontSize: "0.85rem" }}>משימות אופרטיביות</strong>
                      <button
                        onClick={() =>
                          patchSections((sections) =>
                            sections.map((s, i) =>
                              i === sIdx
                                ? { ...s, tasks: [...s.tasks, emptyTask(freshId(doc, "task"))] }
                                : s,
                            ),
                          )
                        }
                        className="btn-secondary"
                        style={miniBtn}
                      >
                        + הוסף משימה
                      </button>
                    </div>

                    {section.tasks.length === 0 && (
                      <div className="text-sm" style={{ color: "var(--text-muted)" }}>
                        אין משימות — הסעיף יוצג כטקסט בלבד גם אחרי לחיצה על "חלצו את המשימות".
                      </div>
                    )}

                    {section.tasks.map((task, tIdx) => (
                      <div
                        key={task.id}
                        style={{
                          border: "1px solid var(--border)",
                          borderRadius: "8px",
                          padding: "0.6rem 0.75rem",
                          marginBottom: "0.5rem",
                          background: "var(--bg)",
                        }}
                      >
                        <div
                          style={{
                            display: "flex",
                            alignItems: "center",
                            gap: "0.35rem",
                            marginBottom: "0.5rem",
                          }}
                        >
                          <code dir="ltr" style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>
                            {task.id}
                          </code>
                          <span style={{ marginInlineStart: "auto", display: "flex", gap: "0.25rem" }}>
                            <button
                              onClick={() =>
                                patchSections((sections) =>
                                  sections.map((s, i) =>
                                    i === sIdx ? { ...s, tasks: move(s.tasks, tIdx, tIdx - 1) } : s,
                                  ),
                                )
                              }
                              disabled={tIdx === 0}
                              className="btn-secondary"
                              style={miniBtn}
                            >
                              ↑
                            </button>
                            <button
                              onClick={() =>
                                patchSections((sections) =>
                                  sections.map((s, i) =>
                                    i === sIdx ? { ...s, tasks: move(s.tasks, tIdx, tIdx + 1) } : s,
                                  ),
                                )
                              }
                              disabled={tIdx === section.tasks.length - 1}
                              className="btn-secondary"
                              style={miniBtn}
                            >
                              ↓
                            </button>
                            <button
                              onClick={() => {
                                if (!confirm(`למחוק את המשימה "${task.title}"?`)) return;
                                patchSections((sections) =>
                                  sections.map((s, i) =>
                                    i === sIdx
                                      ? { ...s, tasks: s.tasks.filter((_, j) => j !== tIdx) }
                                      : s,
                                  ),
                                );
                              }}
                              className="btn-danger"
                              style={miniBtn}
                            >
                              ✕
                            </button>
                          </span>
                        </div>

                        <Field
                          label="כותרת המשימה"
                          value={task.title}
                          onChange={(v) => patchTask(sIdx, tIdx, { title: v })}
                        />
                        <Field
                          label="מה הסעיף מחייב"
                          value={task.obligation}
                          rows={3}
                          onChange={(v) => patchTask(sIdx, tIdx, { obligation: v })}
                        />
                        <div style={{ display: "flex", gap: "0.75rem", flexWrap: "wrap" }}>
                          <div style={{ flex: "2 1 12rem" }}>
                            <Field
                              label="האחריות"
                              value={task.responsible}
                              onChange={(v) => patchTask(sIdx, tIdx, { responsible: v })}
                            />
                          </div>
                          <div style={{ flex: "1 1 8rem" }}>
                            <Field
                              label="המועד שנקבע"
                              value={task.due}
                              onChange={(v) => patchTask(sIdx, tIdx, { due: v })}
                            />
                          </div>
                          <div style={{ flex: "1 1 8rem", marginBottom: "0.6rem" }}>
                            <label style={labelStyle}>סטטוס</label>
                            <select
                              value={task.status}
                              onChange={(e) =>
                                patchTask(sIdx, tIdx, {
                                  status: e.target.value as DecisionTaskStatus,
                                })
                              }
                              style={inputStyle}
                            >
                              {STATUS_OPTIONS.map((o) => (
                                <option key={o.id} value={o.id}>
                                  {o.label}
                                </option>
                              ))}
                            </select>
                          </div>
                        </div>

                        <Field
                          label="הפוטנציאל שהיה"
                          value={task.potential}
                          rows={4}
                          onChange={(v) => patchTask(sIdx, tIdx, { potential: v })}
                        />
                        <Field
                          label="מה קרה בפועל"
                          value={task.actual}
                          rows={4}
                          onChange={(v) => patchTask(sIdx, tIdx, { actual: v })}
                        />
                        <Field
                          label="מה זה עלה לנו"
                          value={task.damage}
                          rows={4}
                          onChange={(v) => patchTask(sIdx, tIdx, { damage: v })}
                        />
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      <button
        onClick={() => {
          const id = freshId(doc, "sec");
          const part = doc.sections[doc.sections.length - 1]?.part ?? "";
          patchSections((s) => [...s, emptySection(id, part)]);
          setOpenSection(id);
        }}
        className="btn-secondary"
        style={{ ...miniBtn, marginTop: "0.75rem" }}
      >
        + הוסף סעיף
      </button>
    </section>
  );
}
