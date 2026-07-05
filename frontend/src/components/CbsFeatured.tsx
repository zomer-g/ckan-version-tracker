import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { cbs, formatBytes, CbsResult } from "../api/client";

// The three most-requested CBS series (per the דאטה-האב user-questions
// analysis), pinned as quick-access cards at the top of the CBS page. Each
// card is populated LIVE from the cbs_index search API — a curated query +
// title regex picks the series' pages out of the index and groups them by
// year, so a newly crawled year shows up here with no code change.
interface FeaturedSeries {
  id: string;
  title: string;
  titleEn: string;
  desc: string;
  descEn: string;
  query: string;
  titleRe: RegExp;
  excludeRe?: RegExp;
}

const SERIES: FeaturedSeries[] = [
  {
    id: "rashuyot",
    title: "קובץ הרשויות המקומיות",
    titleEn: "Local Authorities File",
    desc: "נתונים פיזיים וכספיים על כל רשות מקומית בישראל, שנה מול שנה",
    descEn: "Physical and financial data on every local authority in Israel, year by year",
    query: "הרשויות המקומיות בישראל",
    titleRe: /הרשויות המקומיות בישראל/,
    excludeRe: /הגדרות והסברים/,
  },
  {
    id: "yishuvim",
    title: "קובץ היישובים",
    titleEn: "Localities File",
    desc: "סמלי היישובים, אוכלוסייה ושיוכים גאוגרפיים לכל יישוב בישראל",
    descEn: "Locality codes, population and geographic classifications for every locality in Israel",
    query: "יישובים בישראל",
    titleRe: /^יישובים (בישראל|ואוכלוסייה בישראל)/,
    excludeRe: /הגדרות והסברים/,
  },
  {
    id: "socioeconomic",
    title: "מדד חברתי-כלכלי (אשכולות)",
    titleEn: "Socio-Economic Index (clusters)",
    desc: "אפיון וסיווג רשויות, יישובים ואזורים סטטיסטיים לפי הרמה החברתית-כלכלית",
    descEn: "Classification of authorities, localities and statistical areas by socio-economic level",
    query: "הרמה החברתית-כלכלית של האוכלוסייה",
    titleRe: /(אפיון.*חברתית-כלכלית|מדד חברתי[-–ה ]?כלכלי)/,
    excludeRe: /הגדרות והסברים/,
  },
];

// One year-group of a series: the best index record for that year.
interface YearEntry {
  yearLabel: string;
  sortYear: number;
  record: CbsResult;
}

interface SeriesData {
  series: FeaturedSeries;
  // Optional series landing page on cbs.gov.il (a wide year-range record with
  // no direct files, e.g. "קובצי נתונים לעיבוד 1999-2024") — linked separately
  // instead of competing with the yearly records for the "latest" slot.
  hub: CbsResult | null;
  latest: YearEntry;
  history: YearEntry[];
}

// Records that carry downloadable files win; among equals, media releases
// tend to hold the actual xlsx while publication pages in the index often
// have no file links (SummaryLinks is WAF-blocked at crawl time).
const SECTION_RANK: Record<string, number> = {
  mediarelease: 0,
  publications: 1,
  subjects: 2,
};

function pickBest(candidates: CbsResult[]): CbsResult {
  return [...candidates].sort((a, b) => {
    const aFiles = a.file_links?.length ? 1 : 0;
    const bFiles = b.file_links?.length ? 1 : 0;
    if (aFiles !== bFiles) return bFiles - aFiles;
    const aRank = SECTION_RANK[a.section ?? ""] ?? 9;
    const bRank = SECTION_RANK[b.section ?? ""] ?? 9;
    return aRank - bRank;
  })[0];
}

// A record spanning 5+ years with no downloadable files is a series landing
// page, not a yearly release — surface it as the "all years on cbs.gov.il"
// link rather than letting its high year_end win the "latest" slot.
function isHubPage(r: CbsResult): boolean {
  return (
    !!r.year_start &&
    !!r.year_end &&
    r.year_end - r.year_start >= 5 &&
    !(r.file_links && r.file_links.length > 0)
  );
}

function groupByYear(series: FeaturedSeries, results: CbsResult[]): SeriesData | null {
  const matches = results.filter(
    (r) =>
      r.title &&
      series.titleRe.test(r.title) &&
      !(series.excludeRe && series.excludeRe.test(r.title))
  );

  const hubs = matches.filter(isHubPage);
  const hub =
    hubs.length > 0
      ? hubs.reduce((a, b) => ((b.year_end ?? 0) > (a.year_end ?? 0) ? b : a))
      : null;

  const byYear = new Map<string, CbsResult[]>();
  for (const r of matches) {
    if (isHubPage(r)) continue;
    const y = r.year_end ?? r.year_start;
    if (!y) continue;
    const label =
      r.year_start && r.year_end && r.year_start !== r.year_end
        ? `${r.year_start}–${r.year_end}`
        : String(y);
    const bucket = byYear.get(label);
    if (bucket) bucket.push(r);
    else byYear.set(label, [r]);
  }
  if (byYear.size === 0) return null;

  const entries: YearEntry[] = [...byYear.entries()]
    .map(([yearLabel, candidates]) => {
      const record = pickBest(candidates);
      return {
        yearLabel,
        sortYear: record.year_end ?? record.year_start ?? 0,
        record,
      };
    })
    .sort((a, b) => b.sortYear - a.sortYear);

  return { series, hub, latest: entries[0], history: entries.slice(1) };
}

function FileLinks({ record, compact }: { record: CbsResult; compact?: boolean }) {
  if (!record.file_links || record.file_links.length === 0) return null;
  return (
    <div style={{ marginTop: compact ? "0.2rem" : "0.5rem" }}>
      {record.file_links.map((f, idx) => (
        <div
          key={`${f.href}-${idx}`}
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            gap: "0.5rem",
            padding: compact ? "0.2rem 0.45rem" : "0.35rem 0.55rem",
            marginBottom: "0.25rem",
            background: "var(--bg-secondary, #f8f9fa)",
            borderRadius: "4px",
            border: "1px solid var(--border, #e2e8f0)",
          }}
        >
          <a
            href={f.href}
            target="_blank"
            rel="noopener noreferrer"
            style={{
              fontSize: compact ? "0.78rem" : "0.82rem",
              color: "var(--primary)",
              wordBreak: "break-word",
            }}
          >
            {f.label && f.label !== ">>>" ? f.label : f.href.split("/").pop()}
            {f.ext && (
              <span
                className="badge"
                style={{ marginInlineStart: "0.4rem", fontSize: "0.65rem" }}
              >
                {f.ext.toUpperCase()}
              </span>
            )}
          </a>
          {f.size != null && (
            <span className="text-sm text-muted" style={{ whiteSpace: "nowrap" }}>
              {formatBytes(f.size)}
            </span>
          )}
        </div>
      ))}
    </div>
  );
}

function FeaturedCard({ data }: { data: SeriesData }) {
  const { t, i18n } = useTranslation();
  const he = i18n.language === "he";
  const [expanded, setExpanded] = useState(false);
  const { series, hub, latest, history } = data;

  return (
    <article className="card" style={{ borderRight: "4px solid #f59e0b" }}>
      <div className="flex-between mb-1">
        <h3 style={{ fontSize: "1rem", fontWeight: 700, margin: 0 }}>
          {he ? series.title : series.titleEn}
        </h3>
        <span
          style={{
            display: "inline-block",
            padding: "0.15rem 0.5rem",
            borderRadius: "9999px",
            fontSize: "0.65rem",
            fontWeight: 600,
            background: "#fef3c7",
            color: "#92400e",
            whiteSpace: "nowrap",
          }}
        >
          ★ {t("cbs.featured_badge", "מבוקש")}
        </span>
      </div>

      <p className="text-sm text-muted mb-1">{he ? series.desc : series.descEn}</p>

      <div style={{ fontSize: "0.85rem" }}>
        <span className="badge badge-info" style={{ marginInlineEnd: "0.4rem" }}>
          {latest.yearLabel}
        </span>
        <a
          href={latest.record.url}
          target="_blank"
          rel="noopener noreferrer"
          style={{ fontWeight: 600, color: "var(--text, inherit)" }}
        >
          {latest.record.title}
        </a>
      </div>
      <FileLinks record={latest.record} />

      {hub && (
        <div style={{ marginTop: "0.45rem", fontSize: "0.78rem" }}>
          <a href={hub.url} target="_blank" rel="noopener noreferrer">
            {t("cbs.featured_series_page", 'כל השנים בעמוד הלמ"ס')}
            {hub.year_start && hub.year_end
              ? ` (${hub.year_start}–${hub.year_end})`
              : ""}
            {" ↗"}
          </a>
        </div>
      )}

      {history.length > 0 && (
        <>
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setExpanded((v) => !v)}
            aria-expanded={expanded}
            style={{ fontSize: "0.78rem", padding: "0.3rem 0.6rem", marginTop: "0.6rem" }}
          >
            {expanded
              ? t("cbs.featured_hide_history", "הסתר שנים קודמות")
              : t("cbs.featured_show_history", {
                  count: history.length,
                  defaultValue: `שנים קודמות (${history.length})`,
                })}
            {" "}
            {expanded ? "▴" : "▾"}
          </button>

          {expanded && (
            <div style={{ marginTop: "0.5rem", maxHeight: "20rem", overflowY: "auto" }}>
              {history.map((h) => (
                <div
                  key={h.record.url}
                  style={{
                    padding: "0.4rem 0",
                    borderTop: "1px solid var(--border, #e2e8f0)",
                  }}
                >
                  <div style={{ fontSize: "0.82rem" }}>
                    <span
                      className="badge"
                      style={{
                        background: "#f1f5f9",
                        color: "#334155",
                        marginInlineEnd: "0.4rem",
                        fontSize: "0.68rem",
                      }}
                    >
                      {h.yearLabel}
                    </span>
                    <a
                      href={h.record.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{ color: "var(--text, inherit)" }}
                    >
                      {h.record.title}
                    </a>
                  </div>
                  <FileLinks record={h.record} compact />
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </article>
  );
}

export default function CbsFeatured() {
  const { t } = useTranslation();
  const [data, setData] = useState<SeriesData[]>([]);

  useEffect(() => {
    let cancelled = false;
    Promise.all(
      SERIES.map((s) =>
        cbs
          .search({ q: s.query, limit: 100 })
          .then((res) => groupByYear(s, res.results))
          .catch(() => null)
      )
    ).then((groups) => {
      if (!cancelled) setData(groups.filter((g): g is SeriesData => g !== null));
    });
    return () => {
      cancelled = true;
    };
  }, []);

  if (data.length === 0) return null;

  return (
    <section className="mb-2" aria-label={t("cbs.featured_title", "הקבצים המבוקשים ביותר")}>
      <h2 style={{ fontSize: "1.05rem", fontWeight: 700, margin: "0 0 0.6rem" }}>
        {t("cbs.featured_title", "הקבצים המבוקשים ביותר")}
      </h2>
      <div
        className="grid"
        style={{ gridTemplateColumns: "repeat(auto-fit, minmax(280px, 1fr))" }}
      >
        {data.map((g) => (
          <FeaturedCard key={g.series.id} data={g} />
        ))}
      </div>
    </section>
  );
}
