import { useEffect, useState } from "react";
import { publicApi } from "../api/client";
import type { DatasetIdCard as Card } from "../api/client";

// The dataset's "ID card": a summary, topics and column descriptions that a
// LANGUAGE MODEL wrote from a sample of the rows (app/services/table_profiler).
//
// It is not the publisher's text and not part of the dataset, and it can be
// wrong — so it is framed as AI output, loudly: a dashed amber frame, a solid
// label bar ABOVE the content, and the word "מודל שפה" in the heading itself,
// so even a reader who skips the label (or a search snippet that quotes the
// heading) cannot take it for the data. The server-rendered copy of this page
// (app/services/seo.py::id_card_html) carries the same frame and label.
export default function DatasetIdCard({ datasetId }: { datasetId: string }) {
  const [cards, setCards] = useState<Card[]>([]);

  useEffect(() => {
    let alive = true;
    publicApi
      .datasetIdCard(datasetId)
      .then((r) => alive && setCards(r.cards || []))
      .catch(() => alive && setCards([]));
    return () => {
      alive = false;
    };
  }, [datasetId]);

  if (!cards.length) return null;

  return (
    <section
      aria-label="תוכן שנוצר על ידי מודל שפה"
      data-generated-by="llm"
      style={{
        border: "2px dashed var(--warning)",
        background: "color-mix(in srgb, var(--warning) 7%, var(--surface))",
        borderRadius: 8,
        padding: "1rem 1.25rem",
        margin: "1rem 0",
      }}
    >
      <div
        role="note"
        style={{
          background: "var(--warning)",
          color: "var(--surface)",
          fontWeight: 700,
          padding: "0.4rem 0.75rem",
          borderRadius: 6,
          marginBottom: "0.75rem",
          fontSize: "0.9rem",
        }}
      >
        {/* The emoji is decoration: a screen reader would announce "robot face"
            before the sentence that already says it is AI output. */}
        <span aria-hidden="true">🤖 </span>
        תוכן שנוצר אוטומטית על ידי מודל שפה (AI) — אינו חלק מהמאגר ואינו טקסט
        של הגוף המפרסם. נכתב מתוך דגימה של שורות המאגר ועלול להכיל טעויות.
      </div>
      <h2 style={{ marginTop: 0, fontSize: "1.1rem" }}>
        תעודת זהות של המאגר (נוצרה על ידי מודל שפה)
      </h2>
      {cards.map((c) => (
        <div key={c.table} style={{ marginBottom: "0.75rem" }}>
          {cards.length > 1 && c.title && <h3 style={{ fontSize: "1rem" }}>{c.title}</h3>}
          <p style={{ margin: "0.25rem 0" }}>{c.summary_he}</p>
          {c.tags.length > 0 && (
            <p className="text-sm text-muted" style={{ margin: "0.25rem 0" }}>
              נושאים (לפי המודל): {c.tags.join(" · ")}
            </p>
          )}
          {c.columns.length > 0 && (
            <details>
              <summary className="text-sm" style={{ cursor: "pointer", minHeight: "1.75rem", display: "list-item", paddingBlock: "0.2rem" }}>
                תיאור העמודות ({c.columns.length}) — לפי המודל
              </summary>
              <dl className="text-sm" style={{ margin: "0.5rem 0 0" }}>
                {c.columns.map((col) => (
                  <div key={col.name} style={{ marginBottom: "0.35rem" }}>
                    <dt style={{ fontWeight: 600 }}>{col.name}</dt>
                    <dd style={{ margin: 0 }}>{col.description_he}</dd>
                  </div>
                ))}
              </dl>
            </details>
          )}
        </div>
      ))}
    </section>
  );
}
