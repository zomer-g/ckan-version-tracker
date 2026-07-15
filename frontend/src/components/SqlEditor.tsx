import { forwardRef, useCallback, useImperativeHandle, useRef, useState } from "react";

// Shared SQL editor used by every Neon-backed SQL console (the Knesset DB page
// and each append-archive page). Two jobs:
//   1. Autocomplete — type ≥2 chars of a table/column name and a suggestion box
//      offers matches; ↑/↓ to move, Enter/Tab to insert, Esc to close. The name
//      is inserted in its REAL casing, double-quoted automatically when it isn't
//      a plain lowercase identifier (so mixed-case English or Hebrew names, and
//      reserved words, are always referenced correctly).
//   2. Ctrl/⌘+Enter runs the query.
// The wrong-case pain point is thus fixed from both ends: the server corrects
// quoted-case on the way in (see append_store.normalize_quoted_case), and the
// editor inserts the exact name so it rarely comes up in the first place.

export interface SqlSuggestion {
  value: string;
  kind: "table" | "column";
  hint?: string;
}

// Reserved words that must be double-quoted to be used as an identifier. Not
// exhaustive — just the ones that actually appear as column names in these
// datasets (desc, date, order…) plus the common SQL keywords.
const RESERVED = new Set([
  "select", "from", "where", "group", "order", "by", "having", "limit", "offset",
  "join", "left", "right", "inner", "outer", "full", "on", "as", "and", "or",
  "not", "null", "is", "in", "like", "ilike", "between", "asc", "desc", "distinct",
  "union", "all", "case", "when", "then", "else", "end", "date", "time",
  "timestamp", "user", "table", "column", "with", "values", "default", "primary",
  "key", "using", "natural", "cross", "into", "over", "window", "returning",
]);

// A bare (unquoted) identifier resolves correctly only when it's lowercase snake
// and not reserved; anything else (uppercase, Hebrew, punctuation, a keyword)
// must be double-quoted to reference the real column.
function quoteIdent(name: string): string {
  const bare = /^[a-z_][a-z0-9_]*$/.test(name) && !RESERVED.has(name);
  return bare ? name : `"${name.replace(/"/g, '""')}"`;
}

// The identifier being typed just before the caret. Allows an optional leading
// quote (so completing inside `"Dec…` works) and Hebrew letters.
const TOKEN_RE = /"?[A-Za-z_֐-׿][A-Za-z0-9_֐-׿]*$/;
function tokenBeforeCaret(value: string, caret: number): { start: number; word: string } {
  const m = value.slice(0, caret).match(TOKEN_RE);
  if (!m) return { start: caret, word: "" };
  const raw = m[0];
  const word = raw.startsWith('"') ? raw.slice(1) : raw;
  return { start: caret - raw.length, word };
}

interface SqlEditorProps {
  value: string;
  onChange: (v: string) => void;
  onRun: () => void;
  suggestions: SqlSuggestion[];
  rows?: number;
  ariaLabel?: string;
}

export interface SqlEditorHandle {
  // Insert a table/column name at the caret, double-quoted when needed. Used by
  // the clickable SchemaReference below.
  insertIdentifier: (name: string) => void;
}

const SqlEditor = forwardRef<SqlEditorHandle, SqlEditorProps>(function SqlEditor({
  value, onChange, onRun, suggestions, rows = 6, ariaLabel = "שאילתת SQL",
}, ref) {
  const taRef = useRef<HTMLTextAreaElement>(null);
  const [items, setItems] = useState<SqlSuggestion[]>([]);
  const [index, setIndex] = useState(0);
  const [open, setOpen] = useState(false);

  const refresh = useCallback((val: string, caret: number) => {
    const { word } = tokenBeforeCaret(val, caret);
    if (word.length < 2) { setOpen(false); setItems([]); return; }
    const w = word.toLowerCase();
    const matches = suggestions
      .filter((s) => s.value.toLowerCase() !== w && s.value.toLowerCase().startsWith(w))
      .sort((a, b) =>
        (a.kind === b.kind ? 0 : a.kind === "table" ? -1 : 1) ||
        a.value.length - b.value.length ||
        a.value.localeCompare(b.value))
      .slice(0, 8);
    setItems(matches);
    setIndex(0);
    setOpen(matches.length > 0);
  }, [suggestions]);

  const accept = useCallback((sug: SqlSuggestion) => {
    const ta = taRef.current;
    if (!ta) return;
    const caret = ta.selectionStart ?? value.length;
    const { start } = tokenBeforeCaret(value, caret);
    const insert = quoteIdent(sug.value);
    const next = value.slice(0, start) + insert + value.slice(caret);
    const pos = start + insert.length;
    onChange(next);
    setOpen(false);
    requestAnimationFrame(() => { ta.focus(); ta.setSelectionRange(pos, pos); });
  }, [value, onChange]);

  useImperativeHandle(ref, () => ({
    insertIdentifier(name: string) {
      const insert = quoteIdent(name);
      const ta = taRef.current;
      if (!ta) { onChange(value + insert); return; }
      const caret = ta.selectionStart ?? value.length;
      const next = value.slice(0, caret) + insert + value.slice(caret);
      const pos = caret + insert.length;
      onChange(next);
      setOpen(false);
      requestAnimationFrame(() => { ta.focus(); ta.setSelectionRange(pos, pos); });
    },
  }), [value, onChange]);

  return (
    <div style={{ position: "relative" }}>
      <textarea
        ref={taRef}
        value={value}
        onChange={(e) => {
          onChange(e.target.value);
          refresh(e.target.value, e.target.selectionStart ?? e.target.value.length);
        }}
        onKeyDown={(e) => {
          if (open && items.length > 0) {
            if (e.key === "ArrowDown") { e.preventDefault(); setIndex((i) => (i + 1) % items.length); return; }
            if (e.key === "ArrowUp") { e.preventDefault(); setIndex((i) => (i - 1 + items.length) % items.length); return; }
            if (e.key === "Escape") { e.preventDefault(); setOpen(false); return; }
            // Ctrl/⌘+Enter always runs — even with the box open.
            if (e.key === "Enter" && (e.ctrlKey || e.metaKey)) { e.preventDefault(); onRun(); return; }
            if (e.key === "Enter" || e.key === "Tab") { e.preventDefault(); accept(items[index]); return; }
          }
          if ((e.ctrlKey || e.metaKey) && e.key === "Enter") onRun();
        }}
        onBlur={() => window.setTimeout(() => setOpen(false), 120)}
        spellCheck={false}
        dir="ltr"
        rows={rows}
        style={{
          width: "100%", fontFamily: "monospace", fontSize: "0.85rem", padding: "0.6rem",
          border: "1px solid var(--border, #d1d5db)", borderRadius: 4, resize: "vertical",
        }}
        aria-label={ariaLabel}
      />
      {open && items.length > 0 && (
        <ul
          dir="ltr"
          role="listbox"
          style={{
            position: "absolute", zIndex: 20, top: "calc(100% - 4px)", insetInlineStart: "0.6rem",
            margin: 0, padding: "0.2rem", listStyle: "none", minWidth: 260, maxWidth: 460,
            maxHeight: 240, overflowY: "auto", background: "var(--bg, #fff)",
            border: "1px solid var(--border, #cbd5e1)", borderRadius: 6,
            boxShadow: "0 6px 20px rgba(0,0,0,0.14)", fontSize: "0.82rem",
          }}
        >
          {items.map((s, i) => (
            <li
              key={`${s.kind}:${s.value}`}
              role="option"
              aria-selected={i === index}
              onMouseDown={(e) => { e.preventDefault(); accept(s); }}
              onMouseEnter={() => setIndex(i)}
              style={{
                display: "flex", alignItems: "center", gap: "0.5rem",
                padding: "0.3rem 0.5rem", borderRadius: 4, cursor: "pointer",
                background: i === index ? "var(--bg-muted, #eef2f5)" : "transparent",
              }}
            >
              <span
                aria-hidden
                style={{
                  fontSize: "0.62rem", fontWeight: 700, padding: "0.05rem 0.35rem",
                  borderRadius: 3, flex: "0 0 auto",
                  color: s.kind === "table" ? "#7c3aed" : "#0f766e",
                  background: s.kind === "table" ? "rgba(124,58,237,0.12)" : "rgba(15,118,110,0.12)",
                }}
              >
                {s.kind === "table" ? "טבלה" : "עמודה"}
              </span>
              <code style={{ fontWeight: 600 }}>{s.value}</code>
              {s.hint && (
                <span
                  dir="rtl"
                  style={{
                    marginInlineStart: "auto", fontSize: "0.72rem", color: "var(--text-muted)",
                    whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: 200,
                  }}
                >
                  {s.hint}
                </span>
              )}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
});

export default SqlEditor;

// ── Clickable tables & columns reference ─────────────────────────────────────
// A collapsible list of the queryable tables and their columns, shown next to
// the editor on every SQL console. Clicking a table or column inserts it (with
// correct quoting) at the caret via the editor's insertIdentifier handle. Two
// shapes: many tables (Knesset) → each expands to its columns; one table
// (append archive) → its columns are shown directly.
export interface SchemaTable {
  table: string;
  columns: string[];
  description?: string;
}

function ColumnChip({ name, onInsert }: { name: string; onInsert: (n: string) => void }) {
  return (
    <button
      type="button"
      onClick={() => onInsert(name)}
      title={`הכנס עמודה: ${name}`}
      style={{
        fontFamily: "monospace", fontSize: "0.76rem", padding: "0.12rem 0.45rem",
        border: "1px solid var(--border, #d1d5db)", borderRadius: 999,
        background: "var(--bg, #fff)", color: "var(--text, #111)", cursor: "pointer",
      }}
    >
      {name}
    </button>
  );
}

export function SchemaReference({
  tables, onInsert, defaultOpen = false,
}: {
  tables: SchemaTable[];
  onInsert: (name: string) => void;
  defaultOpen?: boolean;
}) {
  const single = tables.length === 1;
  const [open, setOpen] = useState(defaultOpen);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  if (tables.length === 0) return null;

  const toggle = (t: string) =>
    setExpanded((s) => {
      const n = new Set(s);
      if (n.has(t)) n.delete(t); else n.add(t);
      return n;
    });

  const header = single
    ? `עמודות הטבלה (${tables[0].columns.length})`
    : `טבלאות ועמודות (${tables.length})`;

  return (
    <div style={{ margin: "0.5rem 0" }}>
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        style={{
          background: "none", border: "none", color: "var(--primary)", cursor: "pointer",
          padding: 0, fontSize: "0.82rem", fontWeight: 600,
        }}
      >
        {open ? "▾" : "▸"} {header}
      </button>
      {open && (
        <div
          style={{
            marginTop: "0.4rem", padding: "0.5rem 0.6rem", maxHeight: 260, overflowY: "auto",
            background: "var(--bg-muted, #f8fafc)", border: "1px solid var(--border, #e2e8f0)",
            borderRadius: 6,
          }}
        >
          {single ? (
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.3rem" }}>
              {tables[0].columns.map((c) => (
                <ColumnChip key={c} name={c} onInsert={onInsert} />
              ))}
            </div>
          ) : (
            tables.map((t) => (
              <div key={t.table} style={{ marginBottom: "0.35rem" }}>
                <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
                  <button
                    type="button"
                    onClick={() => toggle(t.table)}
                    title={t.description || t.table}
                    style={{
                      background: "none", border: "none", cursor: "pointer", padding: "0.1rem 0",
                      fontSize: "0.8rem", color: "var(--text, #111)", display: "flex",
                      alignItems: "center", gap: "0.35rem",
                    }}
                  >
                    <span aria-hidden style={{ color: "var(--text-muted)", fontSize: "0.7rem" }}>
                      {expanded.has(t.table) ? "▾" : "▸"}
                    </span>
                    <code style={{ fontWeight: 600 }}>{t.table}</code>
                    <span className="text-muted" style={{ fontSize: "0.7rem", color: "var(--text-muted)" }}>
                      ({t.columns.length})
                    </span>
                  </button>
                  <button
                    type="button"
                    onClick={() => onInsert(t.table)}
                    title="הכנס שם טבלה"
                    style={{
                      fontSize: "0.72rem", padding: "0 0.35rem", borderRadius: 4, cursor: "pointer",
                      border: "1px solid var(--border, #d1d5db)", background: "var(--bg, #fff)",
                      color: "var(--primary)",
                    }}
                  >
                    ↧
                  </button>
                </div>
                {expanded.has(t.table) && (
                  <div style={{ display: "flex", flexWrap: "wrap", gap: "0.3rem", padding: "0.3rem 0 0.2rem 1rem" }}>
                    {t.columns.map((c) => (
                      <ColumnChip key={c} name={c} onInsert={onInsert} />
                    ))}
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}

// Collapsible "how casing works + autocomplete" help. `casing` tailors the
// wording: "lower" for the Knesset schema (everything stored lowercase),
// "preserve" for append archives (columns keep their source casing / Hebrew).
export function SqlHelpNote({ casing }: { casing: "lower" | "preserve" }) {
  const [open, setOpen] = useState(false);
  return (
    <div style={{ margin: "0 0 0.6rem" }}>
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        style={{
          background: "none", border: "none", color: "var(--primary)", cursor: "pointer",
          padding: 0, fontSize: "0.82rem", textDecoration: "underline",
        }}
      >
        {open ? "הסתר עזרה" : "ℹ️ עזרה: אותיות גדולות/קטנות והשלמה אוטומטית"}
      </button>
      {open && (
        <div
          className="text-sm"
          style={{
            marginTop: "0.4rem", padding: "0.6rem 0.8rem", lineHeight: 1.7,
            background: "var(--bg-muted, #eef2f5)", borderRadius: 6,
            border: "1px solid var(--border, #e2e8f0)",
          }}
        >
          <div style={{ fontWeight: 700, marginBottom: "0.25rem" }}>אותיות גדולות/קטנות בשמות עמודות</div>
          <ul style={{ margin: "0 0 0.5rem", paddingInlineStart: "1.2rem" }}>
            {casing === "lower" ? (
              <>
                <li>שמות הטבלאות והעמודות נשמרים ב<strong>אותיות קטנות</strong>: <code>kns_bill</code>, <code>knessetnum</code>.</li>
                <li>אפשר לכתוב בכל צורת אותיות <strong>בלי מרכאות</strong> — Postgres ממיר לבד: <code>KNS_Bill</code> = <code>kns_bill</code>.</li>
                <li>גם <strong>עם מרכאות כפולות</strong> וכל צורת אותיות עובד — <code>"KnessetNum"</code> יזוהה אוטומטית כ־<code>"knessetnum"</code>.</li>
              </>
            ) : (
              <>
                <li>שמות העמודות נשמרים <strong>כפי שהם במקור</strong> — כולל אותיות גדולות ושמות בעברית.</li>
                <li>שם עם אות גדולה או בעברית דורש <strong>מרכאות כפולות</strong>, למשל <code>"DecisionNum"</code> או <code>"כותרת"</code>. שם באותיות קטנות בלבד (כמו <code>first_seen</code>) אפשר בלי מרכאות.</li>
                <li>אם כתבתם במרכאות בצורת אותיות שגויה — המערכת מתקנת אוטומטית לשם האמיתי (<code>"decisionnum"</code> → <code>"DecisionNum"</code>).</li>
              </>
            )}
            <li>מילה שמורה כשם עמודה (<code>desc</code>, <code>date</code>, <code>order</code>) דורשת מרכאות כפולות: <code>"desc"</code>.</li>
            <li>מחרוזות עוטפים ב<strong>מרכאה בודדת</strong>: <code>WHERE ... ILIKE '%חינוך%'</code>.</li>
          </ul>
          <div style={{ fontWeight: 700, marginBottom: "0.25rem" }}>השלמה אוטומטית</div>
          <div>
            הקלידו את תחילת שם הטבלה או העמודה (2 אותיות ומעלה) ותופיע רשימת הצעות. הבחירה מכניסה את השם המדויק
            (עם מרכאות אם צריך). ניווט ב־<kbd>↑</kbd>/<kbd>↓</kbd>, הכנסה ב־<kbd>Enter</kbd> או <kbd>Tab</kbd>,
            סגירה ב־<kbd>Esc</kbd>. הרצה ב־<kbd>Ctrl/⌘</kbd>+<kbd>Enter</kbd>.
          </div>
        </div>
      )}
    </div>
  );
}
