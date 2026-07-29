import { useEffect, useMemo, useState, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { useLocation, useNavigate, useParams, useSearchParams } from "react-router-dom";
import {
  resolve,
  lookupLinkFor,
  sourceUrlFromDirectPath,
  ResolveMatch,
} from "../api/client";
import ResolvedMatches from "../components/ResolvedMatches";

/**
 * ONE link that is correct before and after a source is onboarded, in two
 * spellings that mean the same thing:
 *
 *   /direct/<source url>   the shareable shape — a fixed prefix a tool can
 *                          concatenate onto any source URL (in production the
 *                          server 302s this before the SPA loads; this route
 *                          is the dev/safety-net twin)
 *   /lookup?url=<url>      the query-param shape, and where an ambiguous
 *                          source lands so the user can pick
 *
 * Either opens the layer's versions page if OVER tracks it, or the collection
 * request form if OVER doesn't. Nothing to update when the answer changes —
 * the same link starts resolving to versions the day the source is onboarded.
 *
 * With no URL at all it renders the generator: paste a source URL, get the link.
 */
export default function LookupPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const location = useLocation();
  const params = useParams();
  const [searchParams] = useSearchParams();
  // On /direct/* the query string belongs to the SOURCE url, not to us.
  const splat = params["*"] || "";
  const target = splat
    ? sourceUrlFromDirectPath(splat, location.search).trim()
    : (searchParams.get("url") || "").trim();

  const [status, setStatus] = useState<"idle" | "loading" | "done">("idle");
  const [matches, setMatches] = useState<ResolveMatch[]>([]);

  // Generator state (only used when there's no ?url= to resolve).
  const origin = typeof window !== "undefined" ? window.location.origin : "";
  const [draft, setDraft] = useState("");
  const [copied, setCopied] = useState(false);
  const generated = useMemo(
    () => (draft.trim() ? lookupLinkFor(draft) : ""),
    [draft],
  );

  useEffect(() => {
    if (!target) {
      setStatus("idle");
      return;
    }
    let cancelled = false;
    setStatus("loading");
    resolve
      .lookup(target)
      .then((res) => {
        if (cancelled) return;
        if (res.found && res.matches.length === 1) {
          // Tracked and unambiguous — this is the dataset the link meant.
          navigate(`/versions/${res.matches[0].id}`, { replace: true });
          return;
        }
        if (res.found) {
          setMatches(res.matches);
          setStatus("done");
          return;
        }
        // Not tracked → the home search box, which runs the source detectors
        // and opens the right collection request form for this URL. Replace
        // so Back returns to wherever the link was clicked, not here.
        navigate(`/?q=${encodeURIComponent(target)}`, { replace: true });
      })
      .catch(() => {
        if (cancelled) return;
        // The resolver is an optimisation, not a gate. If it's unreachable,
        // hand the URL to the search box anyway — it still recognises the
        // source and opens the right form. A shared link must never dead-end
        // on an error page.
        navigate(`/?q=${encodeURIComponent(target)}`, { replace: true });
      });
    return () => {
      cancelled = true;
    };
  }, [target, navigate]);

  const onGenerate = (e: FormEvent) => {
    e.preventDefault();
    if (draft.trim()) navigate(`/lookup?url=${encodeURIComponent(draft.trim())}`);
  };

  const copy = async () => {
    if (!generated) return;
    try {
      await navigator.clipboard.writeText(generated);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      /* clipboard blocked — the input below is selectable */
    }
  };

  if (target && status === "loading") {
    return (
      <div className="container mt-3">
        <div className="loading" role="status" aria-live="polite">
          {t("lookup.resolving")}
        </div>
      </div>
    );
  }

  if (target && status === "done") {
    return (
      <div className="container mt-3">
        <ResolvedMatches matches={matches} sourceUrl={target} />
      </div>
    );
  }

  // No ?url= — the generator.
  return (
    <div className="container mt-3">
      <h1 style={{ fontSize: "1.75rem", fontWeight: 700, marginBottom: "0.5rem" }}>
        {t("lookup.title")}
      </h1>
      <p className="text-muted mb-2" style={{ maxWidth: "56ch", lineHeight: 1.7 }}>
        {t("lookup.intro")}
      </p>

      {/* The pattern itself, spelled out — this page's other job is telling a
          tool author what to concatenate. */}
      <p className="text-sm mb-2">
        <strong>{t("lookup.pattern_label")}:</strong>{" "}
        <code dir="ltr" style={{ wordBreak: "break-all" }}>
          {origin}/direct/&lt;source-url&gt;
        </code>
        <br />
        <span className="text-muted">{t("lookup.pattern_hint")}</span>
      </p>

      <form onSubmit={onGenerate} className="card mb-2">
        <label
          htmlFor="lookup-url"
          style={{ display: "block", fontWeight: 600, marginBottom: "0.35rem" }}
        >
          {t("lookup.source_url_label")}
        </label>
        <input
          id="lookup-url"
          type="url"
          dir="ltr"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          placeholder="https://www.govmap.gov.il/?c=219143.61,618345.06&lay=11"
          style={{ width: "100%" }}
        />

        {generated && (
          <>
            <label
              htmlFor="lookup-generated"
              style={{ display: "block", fontWeight: 600, margin: "0.85rem 0 0.35rem" }}
            >
              {t("lookup.generated_label")}
            </label>
            <div className="flex" style={{ gap: "0.5rem", flexWrap: "wrap" }}>
              <input
                id="lookup-generated"
                readOnly
                dir="ltr"
                value={generated}
                onFocus={(e) => e.currentTarget.select()}
                style={{ flex: "1 1 22rem", minWidth: "16rem" }}
              />
              <button type="button" className="btn-secondary" onClick={copy}>
                {copied ? t("lookup.copied") : t("lookup.copy")}
              </button>
            </div>
          </>
        )}

        <div className="flex mt-2" style={{ gap: "0.75rem" }}>
          <button type="submit" className="btn-primary" disabled={!draft.trim()}>
            {t("lookup.open")}
          </button>
        </div>
      </form>

      <p className="text-sm text-muted" style={{ maxWidth: "60ch", lineHeight: 1.7 }}>
        {t("lookup.explainer")}
      </p>
    </div>
  );
}
