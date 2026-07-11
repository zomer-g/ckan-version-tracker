import { useEffect, useMemo, useState } from "react";
import { pageContent, PageContentOverrides } from "../api/client";
import heJson from "../i18n/he.json";
import enJson from "../i18n/en.json";

// Admin editor for the copy on the static About / Rationale pages.
//
// Defaults come from the bundled i18n JSON (imported here so they stay pristine,
// immune to the runtime addResourceBundle merge the public pages do). Overrides
// come from GET /api/page-content/{page}. Editing a field and saving writes an
// override row; "revert" deletes it and the string falls back to the bundled
// default. Values keep the <1>/<2>/<strong> inline tags the pages render through
// <Trans> — the note below reminds the editor not to break them.

const DEFAULTS: Record<string, Record<string, any>> = {
  he: heJson as Record<string, any>,
  en: enJson as Record<string, any>,
};

const PAGES: { id: string; label: string; path: string }[] = [
  { id: "about", label: "אודות", path: "/about" },
  { id: "rationale", label: "רציונל", path: "/rationale" },
];

const LANGS: { id: string; label: string }[] = [
  { id: "he", label: "עברית" },
  { id: "en", label: "English" },
];

// Keys under a page whose default value is a plain string (skip anything nested).
function editableKeys(page: string, lang: string): string[] {
  const ns = DEFAULTS[lang]?.[page] ?? {};
  return Object.keys(ns).filter((k) => typeof ns[k] === "string");
}

export default function PageContentPanel() {
  const [page, setPage] = useState("about");
  const [lang, setLang] = useState("he");
  const [overrides, setOverrides] = useState<PageContentOverrides>({});
  const [drafts, setDrafts] = useState<Record<string, string>>({});
  const [busyKey, setBusyKey] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const load = async (p: string) => {
    setLoading(true);
    setErr(null);
    try {
      setOverrides(await pageContent.get(p));
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setLoading(false);
    }
  };

  // Reload overrides when the page changes; clear any in-progress drafts.
  useEffect(() => {
    setDrafts({});
    load(page);
  }, [page]);

  const keys = useMemo(() => editableKeys(page, lang), [page, lang]);
  const pageDef = PAGES.find((p) => p.id === page)!;

  const draftKey = (k: string) => `${lang}:${k}`;
  const defaultVal = (k: string): string => DEFAULTS[lang]?.[page]?.[k] ?? "";
  const overrideVal = (k: string): string | undefined => overrides[lang]?.[k];
  const effective = (k: string): string => {
    const d = drafts[draftKey(k)];
    if (d !== undefined) return d;
    return overrideVal(k) ?? defaultVal(k);
  };
  const isOverridden = (k: string) => overrideVal(k) !== undefined;
  const isDirty = (k: string) => {
    const d = drafts[draftKey(k)];
    return d !== undefined && d !== (overrideVal(k) ?? defaultVal(k));
  };

  const onChange = (k: string, v: string) =>
    setDrafts((prev) => ({ ...prev, [draftKey(k)]: v }));

  const flash = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(null), 2500);
  };

  const save = async (k: string) => {
    setBusyKey(k);
    setErr(null);
    try {
      const value = effective(k);
      await pageContent.save(page, lang, k, value);
      setOverrides((prev) => ({
        ...prev,
        [lang]: { ...(prev[lang] || {}), [k]: value },
      }));
      setDrafts((prev) => {
        const next = { ...prev };
        delete next[draftKey(k)];
        return next;
      });
      flash(`נשמר: ${k}`);
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setBusyKey(null);
    }
  };

  const revert = async (k: string) => {
    if (!confirm(`לאפס את "${k}" לטקסט המקורי מהקוד?`)) return;
    setBusyKey(k);
    setErr(null);
    try {
      await pageContent.revert(page, lang, k);
      setOverrides((prev) => {
        const langMap = { ...(prev[lang] || {}) };
        delete langMap[k];
        return { ...prev, [lang]: langMap };
      });
      setDrafts((prev) => {
        const next = { ...prev };
        delete next[draftKey(k)];
        return next;
      });
      flash(`אופס לברירת מחדל: ${k}`);
    } catch (e) {
      setErr((e as Error)?.message ?? String(e));
    } finally {
      setBusyKey(null);
    }
  };

  const editedCount = Object.keys(overrides[lang] || {}).length;

  return (
    <section className="card mb-2" style={{ padding: "1rem 1.25rem" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", flexWrap: "wrap", gap: "0.5rem" }}>
        <h2 style={{ fontSize: "1.25rem", fontWeight: 700, margin: 0 }}>📝 טקסטים של עמודים</h2>
        <a href={pageDef.path} target="_blank" rel="noopener noreferrer" className="text-sm" style={{ color: "var(--primary, #2563eb)" }}>
          פתח את עמוד {pageDef.label} ↗
        </a>
      </div>

      <div className="text-sm" style={{ marginTop: "0.5rem", color: "var(--text-muted)", lineHeight: 1.6 }}>
        עריכה כאן דורסת את הטקסט שבקוד ומתעדכנת באתר מיד, בלי דיפלוי. שדה שלא נערך משתמש בברירת המחדל מהקוד.
        {" "}שמרו על תגיות כמו <code dir="ltr" style={{ background: "var(--surface)", padding: "0.05rem 0.3rem", borderRadius: "4px" }}>&lt;1&gt;…&lt;/1&gt;</code>,{" "}
        <code dir="ltr" style={{ background: "var(--surface)", padding: "0.05rem 0.3rem", borderRadius: "4px" }}>&lt;2&gt;…&lt;/2&gt;</code> ו-<code dir="ltr" style={{ background: "var(--surface)", padding: "0.05rem 0.3rem", borderRadius: "4px" }}>&lt;strong&gt;…&lt;/strong&gt;</code> כפי שהן, אחרת הקישורים/ההדגשות יישברו.
      </div>

      {/* Page + language selectors */}
      <div style={{ display: "flex", gap: "1.25rem", flexWrap: "wrap", margin: "0.85rem 0", alignItems: "center" }}>
        <div style={{ display: "flex", gap: "0.4rem", alignItems: "center" }}>
          <span className="text-sm" style={{ color: "var(--text-muted)" }}>עמוד:</span>
          {PAGES.map((p) => (
            <button
              key={p.id}
              onClick={() => setPage(p.id)}
              className={page === p.id ? "btn-primary" : "btn-secondary"}
              style={{ padding: "0.3rem 0.9rem", fontSize: "0.85rem" }}
            >
              {p.label}
            </button>
          ))}
        </div>
        <div style={{ display: "flex", gap: "0.4rem", alignItems: "center" }}>
          <span className="text-sm" style={{ color: "var(--text-muted)" }}>שפה:</span>
          {LANGS.map((l) => (
            <button
              key={l.id}
              onClick={() => setLang(l.id)}
              className={lang === l.id ? "btn-primary" : "btn-secondary"}
              style={{ padding: "0.3rem 0.9rem", fontSize: "0.85rem" }}
            >
              {l.label}
            </button>
          ))}
        </div>
        <span className="text-sm" style={{ color: "var(--text-muted)", marginInlineStart: "auto" }}>
          {editedCount > 0 ? `${editedCount} שדות נערכו` : "אין עריכות"}
        </span>
      </div>

      {err && <div className="text-sm" style={{ color: "#b91c1c", marginBottom: "0.5rem" }}>{err}</div>}
      {toast && <div className="text-sm" style={{ color: "#065f46", marginBottom: "0.5rem" }}>{toast}</div>}

      {loading ? (
        <div className="empty-state" style={{ padding: "1.5rem" }}>טוען…</div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: "0.85rem" }}>
          {keys.map((k) => {
            const overridden = isOverridden(k);
            const dirty = isDirty(k);
            const isEn = lang === "en";
            return (
              <div
                key={k}
                style={{
                  border: "1px solid var(--border)",
                  borderRadius: "8px",
                  padding: "0.6rem 0.75rem",
                  background: overridden ? "var(--surface, #f8fafc)" : "transparent",
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", marginBottom: "0.35rem", flexWrap: "wrap" }}>
                  <code dir="ltr" style={{ fontSize: "0.78rem", fontWeight: 600 }}>{k}</code>
                  {overridden && (
                    <span style={{ padding: "0.05rem 0.45rem", borderRadius: "999px", fontSize: "0.68rem", fontWeight: 600, background: "#eef2ff", color: "#3730a3" }}>
                      נערך
                    </span>
                  )}
                </div>
                <textarea
                  value={effective(k)}
                  onChange={(e) => onChange(k, e.target.value)}
                  dir={isEn ? "ltr" : "rtl"}
                  spellCheck={false}
                  style={{
                    width: "100%",
                    minHeight: "3.2rem",
                    padding: "0.45rem 0.6rem",
                    border: `1px solid ${dirty ? "#f59e0b" : "var(--border)"}`,
                    borderRadius: "6px",
                    fontSize: "0.85rem",
                    lineHeight: 1.6,
                    fontFamily: "inherit",
                    resize: "vertical",
                    boxSizing: "border-box",
                  }}
                />
                <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.4rem", alignItems: "center" }}>
                  <button
                    onClick={() => save(k)}
                    className="btn-primary"
                    disabled={!dirty || busyKey === k}
                    style={{ padding: "0.25rem 0.9rem", fontSize: "0.78rem", opacity: !dirty ? 0.5 : 1 }}
                  >
                    {busyKey === k ? "שומר…" : "שמור"}
                  </button>
                  <button
                    onClick={() => revert(k)}
                    className="btn-secondary"
                    disabled={!overridden || busyKey === k}
                    style={{ padding: "0.25rem 0.9rem", fontSize: "0.78rem", opacity: !overridden ? 0.5 : 1 }}
                  >
                    אפס לברירת מחדל
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </section>
  );
}
