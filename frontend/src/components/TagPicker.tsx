import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import type { Tag, TagWithCount } from "../api/client";

interface Props {
  value: Tag[];
  available: TagWithCount[];
  onChange: (next: Tag[]) => void | Promise<void>;
  onCreate: (name: string) => Promise<Tag>;
  disabled?: boolean;
}

export default function TagPicker({
  value,
  available,
  onChange,
  onCreate,
  disabled,
}: Props) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [busy, setBusy] = useState(false);
  const [highlight, setHighlight] = useState(0);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    const onDoc = (e: MouseEvent) => {
      if (wrapperRef.current && !wrapperRef.current.contains(e.target as Node)) {
        setOpen(false);
        setQuery("");
      }
    };
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, [open]);

  useEffect(() => {
    if (open) {
      // Focus the input when opened
      setTimeout(() => inputRef.current?.focus(), 0);
    }
  }, [open]);

  const selectedIds = useMemo(() => new Set(value.map((v) => v.id)), [value]);

  const q = query.trim().toLowerCase();
  const filtered = useMemo(
    () =>
      available
        .filter((tag) => !selectedIds.has(tag.id))
        .filter((tag) => !q || tag.name.toLowerCase().includes(q))
        .sort((a, b) => a.name.localeCompare(b.name, "he")),
    [available, selectedIds, q]
  );

  const exactMatch = useMemo(
    () =>
      available.find(
        (tag) => tag.name.trim().toLowerCase() === q && q.length > 0
      ),
    [available, q]
  );
  const showCreateOption = q.length > 0 && !exactMatch;

  const totalOptions = filtered.length + (showCreateOption ? 1 : 0);

  useEffect(() => {
    setHighlight(0);
  }, [query, open]);

  const removeTag = (id: string) => {
    if (disabled) return;
    onChange(value.filter((v) => v.id !== id));
  };

  const addTag = (tag: Tag) => {
    if (selectedIds.has(tag.id)) return;
    onChange([...value, tag]);
    setQuery("");
    setOpen(false);
  };

  const createAndAdd = async () => {
    const name = query.trim();
    if (!name || busy) return;
    setBusy(true);
    try {
      const tag = await onCreate(name);
      if (!selectedIds.has(tag.id)) {
        onChange([...value, tag]);
      }
      setQuery("");
      setOpen(false);
    } catch (e: any) {
      alert(e?.message || String(e));
    } finally {
      setBusy(false);
    }
  };

  const handleKey = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      setOpen(false);
      setQuery("");
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setHighlight((h) => Math.min(h + 1, Math.max(0, totalOptions - 1)));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setHighlight((h) => Math.max(h - 1, 0));
    } else if (e.key === "Enter") {
      e.preventDefault();
      if (highlight < filtered.length) {
        addTag(filtered[highlight]);
      } else if (showCreateOption) {
        createAndAdd();
      }
    }
  };

  return (
    <div className="tag-picker" ref={wrapperRef}>
      <div className="tag-picker-chips">
        {value.map((tag) => (
          <span key={tag.id} className="tag-chip tag-chip-removable">
            {tag.name}
            {!disabled && (
              <button
                type="button"
                className="tag-chip-remove"
                onClick={() => removeTag(tag.id)}
                aria-label={`Remove ${tag.name}`}
              >
                ×
              </button>
            )}
          </span>
        ))}
        {!disabled && !open && (
          <button
            type="button"
            className="tag-picker-add"
            onClick={() => setOpen(true)}
          >
            + {t("tags.picker_add", "הוסף תגית")}
          </button>
        )}
      </div>

      {open && (
        <div className="tag-picker-popover">
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKey}
            placeholder={t("tags.picker_search_or_create", "חפש או הקלד שם חדש...")}
            className="tag-picker-input"
            disabled={busy}
          />
          <div className="tag-picker-list">
            {filtered.map((tag, i) => (
              <button
                type="button"
                key={tag.id}
                className={
                  "tag-picker-option" + (i === highlight ? " is-highlight" : "")
                }
                onMouseEnter={() => setHighlight(i)}
                onClick={() => addTag(tag)}
              >
                <span>{tag.name}</span>
                {tag.dataset_count > 0 && (
                  <span className="tag-picker-count">
                    {tag.dataset_count}
                  </span>
                )}
              </button>
            ))}
            {filtered.length === 0 && !showCreateOption && (
              <div className="tag-picker-empty">
                {t("tags.picker_no_match", "אין תגיות תואמות")}
              </div>
            )}
            {showCreateOption && (
              <button
                type="button"
                className={
                  "tag-picker-option tag-picker-create" +
                  (highlight === filtered.length ? " is-highlight" : "")
                }
                onMouseEnter={() => setHighlight(filtered.length)}
                onClick={createAndAdd}
                disabled={busy}
              >
                +{" "}
                {t("tags.picker_create_new", 'צור תגית חדשה: "{{name}}"', {
                  name: query.trim(),
                })}
              </button>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
