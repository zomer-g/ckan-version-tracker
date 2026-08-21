import { useTranslation } from "react-i18next";

/**
 * Page navigation for a long list.
 *
 * Windowed rather than exhaustive: 1,253 tracked datasets at 24 a page is 53
 * pages, and 53 buttons is not navigation. The strip always shows the first
 * page, the last page, and a run around the current one, with an ellipsis
 * standing in for the gap — so the two ends stay one click away no matter how
 * deep the reader is.
 *
 * The ellipsis is a <span aria-hidden>, not a disabled button: it is a
 * typographic gap, and announcing "ellipsis, dimmed button" between page
 * numbers is noise.
 */
export default function Pagination(props: {
  page: number;
  pageCount: number;
  onChange: (page: number) => void;
  /** Names the nav for screen readers — "עימוד <label>". */
  label: string;
  /** How many neighbours to show either side of the current page. */
  window?: number;
}) {
  const { page, pageCount, onChange, label } = props;
  const win = props.window ?? 1;
  const { t } = useTranslation();

  if (pageCount <= 1) return null;

  // Build the visible page numbers, then read gaps off the result rather than
  // trying to decide where the ellipses go while generating.
  const nums = new Set<number>([1, pageCount]);
  for (let p = page - win; p <= page + win; p++) {
    if (p >= 1 && p <= pageCount) nums.add(p);
  }
  // A gap of exactly one page is silly — show the page instead of an ellipsis.
  const sorted = [...nums].sort((a, b) => a - b);
  const filled: number[] = [];
  sorted.forEach((n, i) => {
    filled.push(n);
    const next = sorted[i + 1];
    if (next && next - n === 2) filled.push(n + 1);
  });

  const btn = (active: boolean): React.CSSProperties => ({
    minWidth: "var(--target-min)",
    minHeight: "var(--target-min)",
    padding: "0.35rem 0.7rem",
    borderRadius: "var(--radius)",
    border: `1px solid ${active ? "var(--fill-brand)" : "var(--border)"}`,
    background: active ? "var(--fill-brand)" : "var(--surface)",
    color: active ? "var(--on-fill)" : "var(--text)",
    fontWeight: active ? 700 : 500,
    fontSize: "0.88rem",
    fontVariantNumeric: "tabular-nums",
  });

  const step = (to: number, disabled: boolean, text: string, glyph: string) => (
    <button
      type="button"
      onClick={() => onChange(to)}
      disabled={disabled}
      style={{ ...btn(false), opacity: disabled ? 0.45 : 1, cursor: disabled ? "not-allowed" : "pointer" }}
    >
      {/* RTL: "previous" points right. The glyph is decoration — the word
          beside it is what gets announced. */}
      <span aria-hidden="true">{glyph}</span> {text}
    </button>
  );

  return (
    <nav
      aria-label={`${t("common.pagination", "עימוד")} ${label}`}
      style={{
        display: "flex",
        flexWrap: "wrap",
        gap: "0.4rem",
        alignItems: "center",
        justifyContent: "center",
        marginTop: "1.5rem",
      }}
    >
      {step(page - 1, page <= 1, t("common.prev_page", "הקודם"), "›")}

      {filled.map((n, i) => {
        const gapBefore = i > 0 && n - filled[i - 1] > 1;
        return (
          <span key={n} style={{ display: "inline-flex", alignItems: "center", gap: "0.4rem" }}>
            {gapBefore && (
              <span aria-hidden="true" style={{ color: "var(--text-muted)", padding: "0 0.15rem" }}>
                …
              </span>
            )}
            <button
              type="button"
              onClick={() => onChange(n)}
              aria-current={n === page ? "page" : undefined}
              aria-label={`${t("common.page", "עמוד")} ${n}`}
              style={btn(n === page)}
            >
              {n.toLocaleString("he-IL")}
            </button>
          </span>
        );
      })}

      {step(page + 1, page >= pageCount, t("common.next_page", "הבא"), "‹")}
    </nav>
  );
}
