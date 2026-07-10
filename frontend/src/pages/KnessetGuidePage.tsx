import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { marked } from "marked";

// The guide's single source of truth is /knesset-sql-guide.md (a public static
// asset, generated from the Knesset's official ODATA manual + live $metadata).
// This page renders it; the download button serves the same file as-is.

const GUIDE_URL = "/knesset-sql-guide.md";

export default function KnessetGuidePage() {
  const [html, setHtml] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(GUIDE_URL)
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.text();
      })
      .then((md) => setHtml(marked.parse(md, { async: false }) as string))
      .catch(() => setError("שגיאה בטעינת המדריך"));
  }, []);

  return (
    <div className="container mt-3">
      <div className="flex-between" style={{ flexWrap: "wrap", gap: "0.75rem", marginBottom: "1rem" }}>
        <div className="flex" style={{ gap: "0.6rem", alignItems: "center" }}>
          <a
            href={GUIDE_URL}
            download="knesset-sql-guide.md"
            style={{
              fontSize: "0.85rem", padding: "0.4rem 0.9rem",
              background: "var(--primary, #0f766e)", color: "white",
              borderRadius: 4, textDecoration: "none", fontWeight: 500,
            }}
            title="הורדת המדריך כקובץ Markdown"
          >
            &#8595; הורדה (Markdown)
          </a>
          <Link to="/knesset" style={{ fontSize: "0.85rem", color: "var(--text-muted)", textDecoration: "none" }}>
            &rarr; לקונסולת התשאול
          </Link>
        </div>
      </div>

      {error && <div className="empty-state">{error}</div>}
      {!html && !error && <div className="loading" role="status">טוען את המדריך…</div>}
      {html && (
        <article
          className="card kns-guide"
          style={{ padding: "1.5rem 2rem", lineHeight: 1.75 }}
          // Our own generated markdown — no user content.
          dangerouslySetInnerHTML={{ __html: html }}
        />
      )}

      <style>{`
        .kns-guide h1 { font-size: 1.6rem; margin: 0 0 1rem; }
        .kns-guide h2 { font-size: 1.25rem; margin: 2rem 0 0.75rem; padding-top: 1rem; border-top: 1px solid var(--border, #e5e7eb); }
        .kns-guide h3 { font-size: 1.02rem; margin: 1.4rem 0 0.5rem; }
        .kns-guide p, .kns-guide li { font-size: 0.93rem; }
        .kns-guide a { color: var(--primary, #0f766e); }
        .kns-guide hr { border: none; border-top: 1px solid var(--border, #e5e7eb); margin: 1.5rem 0; }
        .kns-guide code {
          direction: ltr; unicode-bidi: embed;
          background: var(--bg-muted, #eef2f5); border-radius: 3px;
          padding: 0.08em 0.35em; font-size: 0.86em;
        }
        .kns-guide pre {
          direction: ltr; text-align: left;
          background: var(--bg-muted, #f6f8fa); border: 1px solid var(--border, #e5e7eb);
          border-radius: 6px; padding: 0.8rem 1rem; overflow-x: auto;
        }
        .kns-guide pre code { background: none; padding: 0; font-size: 0.84rem; }
        .kns-guide table {
          border-collapse: collapse; width: 100%; margin: 0.6rem 0 1rem;
          font-size: 0.86rem; display: block; overflow-x: auto;
        }
        .kns-guide th, .kns-guide td {
          border: 1px solid var(--border, #e5e7eb); padding: 0.35rem 0.6rem;
          text-align: start; vertical-align: top;
        }
        .kns-guide th { background: var(--bg-muted, #eef2f5); }
        .kns-guide blockquote {
          margin: 0.8rem 0; padding: 0.4rem 1rem;
          border-inline-start: 3px solid var(--primary, #0f766e);
          background: var(--bg-muted, #f8fafc); color: var(--text-muted);
        }
      `}</style>
    </div>
  );
}
