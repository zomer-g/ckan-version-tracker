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
 *     plan, down to the bucket the bytes are in (R2 / NEON / ODATA), because
 *     that is the vocabulary the storage plan is configured in. This is a
 *     working surface; it should be blunt.
 *   • `variant="public"` — one chip plus, only when there is genuinely less data
 *     than the page implies, a short factual note. Deliberately quiet: a visitor
 *     needs to know what they can download, not to read an internal audit — and
 *     never the vendor names, which are meaningless outside this repo. The
 *     shared FIDELITY label carries the distinction that DOES matter to them:
 *     files kept per version vs. a queryable table of rows.
 *
 * `fidelity: null` means the endpoint didn't compute it (the unpaginated public
 * catalog skips it on purpose — see ArchiveState in app/api/datasets.py). Null
 * renders nothing at all, which is different from "none", the measured claim
 * that the dataset holds no archived data.
 */
import type { ArchiveState } from "../api/client";

type Tone = "good" | "info" | "warn" | "bad";

// The labels answer the visitor's actual question — "what do I get here?" —
// in the only two forms this system stores: whole FILES kept per version, and
// a queryable TABLE of rows. Vendor names (R2 / NEON / ODATA) say nothing to
// someone outside the project, so they appear in the admin variant only.
const FIDELITY: Record<string, { label: string; tone: Tone; hint: string }> = {
  full: {
    label: "קבצים + טבלה לתשאול",
    tone: "good",
    hint: "כל קובץ של כל גרסה נשמר להורדה, והשורות טעונות גם לטבלה שאפשר לסנן ולתשאל",
  },
  rows: {
    label: "טבלה לתשאול",
    tone: "good",
    hint: "השורות שמורות בטבלה שאפשר לסנן, לתשאל ב-SQL ולייצא; אין קובץ סנפשוט לכל גרסה",
  },
  files: {
    label: "קבצים שמורים",
    tone: "good",
    hint: "כל קובץ של כל גרסה נשמר במלואו וניתן להורדה",
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
    "הקבצים של הגרסה האחרונה עדיין יושבים בארכיון הלגסי (ODATA) ולא ביעד " +
    "שבתוכנית. הנתונים לא אבדו — הם פשוט טרם הועברו ל-R2.",
};

const TONE_STYLE: Record<Tone, { background: string; color: string }> = {
  good: { background: "var(--tint-good-bg)", color: "var(--success)" },
  info: { background: "var(--primary-50)", color: "var(--primary)" },
  warn: { background: "var(--tint-note-bg)", color: "var(--tint-note-fg)" },
  bad: { background: "var(--tint-bad-bg)", color: "var(--tint-bad-fg)" },
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
          {/* No store chip here on purpose. The label above already says which
              of the two forms exist; naming the bucket they sit in (R2 / NEON)
              only asks the visitor to learn our infrastructure. */}
          <Chip tone={f.tone} title={f.hint}>
            {f.label}
          </Chip>
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
            background: "var(--tint-warn-bg)",
            color: "var(--warning)",
            border: "1px solid var(--tint-warn-bd)",
            cursor: "help",
          }}
        >
          ⚠ {code === "no_version"
            ? "אין גרסאות"
            : code === "sample_only"
              ? "תוכנית ≠ מציאות"
              : "עוד ב-ODATA"}
        </span>
      ))}
    </div>
  );
}
