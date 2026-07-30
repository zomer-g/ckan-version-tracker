/**
 * What a dataset's archive ACTUALLY holds, rendered.
 *
 * The storage PLAN (`storage_target`: r2 / neon / r2+neon …) is what an admin
 * configured. `dataset.archive` is what the latest version really contains,
 * derived server-side from its `resource_mappings` (app/services/archive_state.py).
 * They disagree often enough to matter: a CKAN dataset over 50k rows that wasn't
 * opted into NEON keeps a plan of "r2" while archiving only a metadata stub —
 * 16 of 67 CKAN datasets were in that state, ~13.4M rows behind them, and no
 * surface in the app said so.
 *
 * Two densities, one set of labels, so the admin view and the public view can
 * never describe the same dataset differently:
 *
 *   • `variant="admin"` — states the reality AND flags where it contradicts the
 *     plan. This is a working surface; it should be blunt.
 *   • `variant="public"` — one chip plus, only when there is genuinely less data
 *     than the page implies, a short factual note. Deliberately quiet: a visitor
 *     needs to know what they can download, not to read an internal audit.
 *
 * `fidelity: null` means the endpoint didn't compute it (the unpaginated public
 * catalog skips it on purpose — see ArchiveState in app/api/datasets.py). Null
 * renders nothing at all, which is different from "none", the measured claim
 * that the dataset holds no archived data.
 */
import type { ArchiveState } from "../api/client";

type Tone = "good" | "info" | "warn" | "bad";

const FIDELITY: Record<string, { label: string; tone: Tone; hint: string }> = {
  full: {
    label: "נתונים מלאים",
    tone: "good",
    hint: "הקבצים נשמרים וגם השורות נטענות למסד לתשאול",
  },
  rows: {
    label: "שורות לתשאול",
    tone: "good",
    hint: "השורות נשמרות במסד ואפשר לתשאל אותן ב-SQL; אין קובץ סנפשוט לגרסה",
  },
  files: {
    label: "קבצים שמורים",
    tone: "good",
    hint: "הקבצים נשמרים במלואם לכל גרסה",
  },
  sample: {
    label: "מטא־דאטה בלבד",
    tone: "warn",
    hint: "נשמרים ספירת שורות, שמות העמודות ושורות דגימה — לא הנתונים עצמם",
  },
  none: {
    label: "לא נשמרו נתונים",
    tone: "bad",
    hint: "המאגר במעקב אך אין לו גרסה שמחזיקה נתונים",
  },
};

const FILE_STORE: Record<string, string> = {
  r2: "R2",
  odata: "ODATA",
  mixed: "R2 + ODATA",
  none: "",
};

const MISMATCH: Record<string, string> = {
  no_version:
    "המאגר במעקב אך טרם נוצרה לו גרסה — אין שום נתון שמור.",
  sample_only:
    "תוכנית האחסון מבטיחה שמירת נתונים, ובפועל נשמר רק חוסך מטא־דאטה. " +
    "כדי לשמור את השורות עצמן יש להעביר את המאגר ל-NEON (‏r2+neon).",
  file_store:
    "הקבצים בגרסה האחרונה לא יושבים ביעד שמוגדר בתוכנית — גרסאות היסטוריות " +
    "נשארו באחסון הקודם.",
};

const TONE_STYLE: Record<Tone, { background: string; color: string }> = {
  good: { background: "#dcfce7", color: "#166534" },
  info: { background: "var(--primary-50)", color: "var(--primary)" },
  warn: { background: "#fef9c3", color: "#713f12" },
  bad: { background: "#fee2e2", color: "#991b1b" },
};

function Chip(props: {
  tone: Tone;
  children: React.ReactNode;
  title?: string;
}) {
  return (
    <span
      title={props.title}
      style={{
        display: "inline-block",
        padding: "0.1rem 0.45rem",
        borderRadius: "9999px",
        fontSize: "0.65rem",
        fontWeight: 600,
        whiteSpace: "nowrap",
        ...TONE_STYLE[props.tone],
      }}
    >
      {props.children}
    </span>
  );
}

/** Neutral outline chip for the storage location — a fact, not a verdict. */
function StoreChip(props: { children: React.ReactNode; title?: string }) {
  return (
    <span
      title={props.title}
      style={{
        display: "inline-block",
        padding: "0.1rem 0.45rem",
        borderRadius: "9999px",
        fontSize: "0.65rem",
        fontWeight: 600,
        whiteSpace: "nowrap",
        border: "1px solid var(--border)",
        color: "var(--text-muted)",
        direction: "ltr",
      }}
    >
      {props.children}
    </span>
  );
}

function rowsLabel(n: number | null | undefined): string | null {
  if (!n) return null;
  return `${n.toLocaleString("he-IL")} שורות במקור`;
}

export default function ArchiveChips(props: {
  archive: ArchiveState | undefined | null;
  variant: "admin" | "public";
  /** The configured plan, shown alongside the reality in the admin variant. */
  plan?: string;
}) {
  const a = props.archive;
  // Not computed by this endpoint (or an older cached response) — say nothing
  // rather than guess.
  if (!a || !a.fidelity) return null;
  const f = FIDELITY[a.fidelity] ?? FIDELITY.none;
  const store = FILE_STORE[a.file_store] || "";
  const flags = a.mismatch ?? [];

  if (props.variant === "public") {
    // Gentle: the state, where it lives, and an explanation only when there is
    // less here than the download links suggest.
    const explain =
      a.fidelity === "sample" || a.fidelity === "none" ? f.hint : null;
    return (
      <div style={{ marginTop: "0.5rem" }}>
        <div
          style={{
            display: "flex",
            gap: "0.4rem",
            flexWrap: "wrap",
            alignItems: "center",
          }}
        >
          <span
            className="text-sm text-muted"
            style={{ fontSize: "0.75rem" }}
          >
            מה נשמר כאן:
          </span>
          <Chip tone={f.tone} title={f.hint}>
            {f.label}
          </Chip>
          {store && <StoreChip title="אחסון הקבצים">{store}</StoreChip>}
          {a.row_store === "neon" && (
            <StoreChip title="השורות ניתנות לתשאול ב-SQL">NEON</StoreChip>
          )}
          {a.sample_of != null && (
            <span className="text-muted" style={{ fontSize: "0.72rem" }}>
              {rowsLabel(a.sample_of)}
            </span>
          )}
        </div>
        {explain && (
          <div
            className="text-muted"
            style={{
              fontSize: "0.75rem",
              marginTop: "0.3rem",
              maxWidth: "44rem",
              lineHeight: 1.6,
            }}
          >
            {explain}.
          </div>
        )}
      </div>
    );
  }

  // Admin: reality first, then the plan it was measured against, then the flags.
  return (
    <div
      style={{
        marginTop: "0.3rem",
        display: "flex",
        gap: "0.3rem",
        flexWrap: "wrap",
        alignItems: "center",
        fontSize: "0.7rem",
      }}
    >
      <span className="text-muted" style={{ fontSize: "0.65rem" }}>
        בפועל:
      </span>
      <Chip tone={f.tone} title={f.hint}>
        {f.label}
      </Chip>
      {store && <StoreChip title="אחסון הקבצים בגרסה האחרונה">{store}</StoreChip>}
      {a.row_store === "neon" && <StoreChip title="טבלת שורות ב-NEON">NEON</StoreChip>}
      {a.sample_of != null && (
        <span className="text-muted" style={{ fontSize: "0.65rem" }}>
          ({rowsLabel(a.sample_of)})
        </span>
      )}
      {props.plan && (
        <span className="text-muted" style={{ fontSize: "0.65rem" }}>
          · בתוכנית: <span style={{ direction: "ltr", display: "inline-block" }}>{props.plan}</span>
        </span>
      )}
      {flags.map((code) => (
        <span
          key={code}
          title={MISMATCH[code] ?? code}
          style={{
            display: "inline-block",
            padding: "0.1rem 0.45rem",
            borderRadius: "4px",
            fontSize: "0.65rem",
            fontWeight: 600,
            background: "#fef3c7",
            color: "#92400e",
            border: "1px solid #f59e0b",
            cursor: "help",
          }}
        >
          ⚠ {code === "no_version"
            ? "אין גרסאות"
            : code === "sample_only"
              ? "תוכנית ≠ מציאות"
              : "הקבצים לא ביעד"}
        </span>
      ))}
    </div>
  );
}
