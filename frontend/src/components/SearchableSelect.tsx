/**
 * A <select> you can type into. For lists too long to scroll (1,300 יישובים),
 * the input filters the options as you type, and the arrow keys, Enter and
 * Escape work as they do in a native select.
 *
 * Matching ignores the difference between ״/" and ׳/', and between a hyphen
 * and a space, so typing "תל אביב יפו" still finds "תל אביב-יפו".
 */
import { useEffect, useId, useMemo, useRef, useState } from "react";

export interface SearchableOption {
  value: string;
  label: string;
  /** Shown beside the label, muted — e.g. the row count. */
  hint?: string;
}

function norm(s: string): string {
  return s
    .toLowerCase()
    .replace(/[״"”“]/g, '"')
    .replace(/[׳'’]/g, "'")
    .replace(/[-־–—_]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

const MAX_SHOWN = 300;

export default function SearchableSelect({
  value, options, onChange, allLabel, placeholder = "הקלידו לחיפוש…", style, ariaLabel,
}: {
  value: string;
  options: SearchableOption[];
  onChange: (value: string) => void;
  /** The empty choice ("כל היישובים"); its value is "". */
  allLabel: string;
  placeholder?: string;
  style?: React.CSSProperties;
  ariaLabel?: string;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [active, setActive] = useState(0);
  const rootRef = useRef<HTMLDivElement>(null);
  const listRef = useRef<HTMLUListElement>(null);
  const listId = useId();

  const selected = options.find((o) => o.value === value);

  const filtered = useMemo(() => {
    const q = norm(query);
    const all: SearchableOption[] = [{ value: "", label: allLabel }, ...options];
    if (!q) return all;
    const starts: SearchableOption[] = [];
    const contains: SearchableOption[] = [];
    for (const o of options) {
      const n = norm(o.label);
      if (n.startsWith(q)) starts.push(o);
      else if (n.includes(q)) contains.push(o);
    }
    // Names that start with what was typed come first, then the rest that
    // contain it — each group keeps the server's order (most deals first).
    return [...starts, ...contains];
  }, [query, options, allLabel]);

  const shown = filtered.slice(0, MAX_SHOWN);

  useEffect(() => { setActive(0); }, [query]);

  useEffect(() => {
    if (!open) return;
    const onDown = (e: MouseEvent) => {
      if (rootRef.current && !rootRef.current.contains(e.target as Node)) close();
    };
    document.addEventListener("mousedown", onDown);
    return () => document.removeEventListener("mousedown", onDown);
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const el = listRef.current?.children[active] as HTMLElement | undefined;
    el?.scrollIntoView({ block: "nearest" });
  }, [active, open]);

  function close() {
    setOpen(false);
    setQuery("");
  }

  function pick(v: string) {
    onChange(v);
    close();
  }

  function onKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      if (!open) setOpen(true);
      else setActive((a) => Math.min(a + 1, shown.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((a) => Math.max(a - 1, 0));
    } else if (e.key === "Enter") {
      if (open && shown[active]) {
        e.preventDefault();
        pick(shown[active].value);
      }
    } else if (e.key === "Escape") {
      if (open) {
        e.preventDefault();
        close();
      }
    } else if (e.key === "Tab") {
      close();
    }
  }

  return (
    <div
      ref={rootRef}
      style={{ position: "relative", display: "inline-block", ...style }}
      // Inside a <label>, a click on an option would be forwarded to the input
      // and reopen the list it just closed.
      onClick={(e) => { if (e.target !== e.currentTarget.querySelector("input")) e.preventDefault(); }}
    >
      <input
        type="text"
        role="combobox"
        aria-label={ariaLabel}
        aria-expanded={open}
        aria-controls={listId}
        aria-autocomplete="list"
        aria-activedescendant={open && shown[active] ? `${listId}-${active}` : undefined}
        value={open ? query : (selected?.label ?? allLabel)}
        placeholder={open ? (selected?.label ?? placeholder) : undefined}
        onFocus={(e) => { setOpen(true); e.currentTarget.select(); }}
        onClick={() => setOpen(true)}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
        onKeyDown={onKeyDown}
        style={{
          width: "100%", padding: "0.35rem 0.5rem", paddingInlineEnd: "1.6rem",
          boxSizing: "border-box", cursor: open ? "text" : "pointer",
        }}
      />
      <span aria-hidden="true" style={{
        position: "absolute", insetInlineEnd: "0.5rem", top: "50%", transform: "translateY(-50%)",
        pointerEvents: "none", fontSize: "0.7rem", color: "var(--text-muted)",
      }}>▼</span>

      {open && (
        <ul
          id={listId}
          ref={listRef}
          role="listbox"
          style={{
            position: "absolute", zIndex: 50, insetInlineStart: 0, top: "100%", marginTop: 2,
            minWidth: "100%", maxHeight: 320, overflowY: "auto", listStyle: "none",
            padding: "0.2rem 0", margin: 0, background: "var(--surface)",
            border: "1px solid var(--border)", borderRadius: 6,
            boxShadow: "0 6px 18px rgba(0,0,0,0.15)",
          }}
        >
          {shown.length === 0 && (
            <li className="text-muted" style={{ padding: "0.35rem 0.6rem", fontSize: "0.85rem" }}>
              לא נמצאו תוצאות
            </li>
          )}
          {shown.map((o, i) => (
            <li
              key={o.value || "__all"}
              id={`${listId}-${i}`}
              role="option"
              aria-selected={o.value === value}
              onMouseDown={(e) => { e.preventDefault(); pick(o.value); }}
              onMouseEnter={() => setActive(i)}
              style={{
                padding: "0.3rem 0.6rem", fontSize: "0.86rem", cursor: "pointer",
                whiteSpace: "nowrap", display: "flex", justifyContent: "space-between", gap: "0.8rem",
                background: i === active ? "var(--primary)" : undefined,
                color: i === active ? "#fff" : undefined,
                fontWeight: o.value === value ? 700 : undefined,
              }}
            >
              <span>{o.label}</span>
              {o.hint && (
                <span style={{ opacity: 0.7, unicodeBidi: "isolate", direction: "ltr" }}>{o.hint}</span>
              )}
            </li>
          ))}
          {filtered.length > MAX_SHOWN && (
            <li className="text-muted" style={{ padding: "0.35rem 0.6rem", fontSize: "0.8rem" }}>
              ועוד {(filtered.length - MAX_SHOWN).toLocaleString("he-IL")}, המשיכו להקליד כדי לצמצם
            </li>
          )}
        </ul>
      )}
    </div>
  );
}
