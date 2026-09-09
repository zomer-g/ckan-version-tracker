/**
 * The panel a SQL console shows when a query was refused for want of a signed-in
 * account.
 *
 * It is deliberately not styled as an error. Nothing went wrong and nothing is
 * being withheld: every table these consoles reach is public, and the machine
 * surfaces over the same data — `/api/append/{id}/sql`, the Looker connector,
 * the MCP servers — are untouched and still anonymous. Signing in attaches a
 * name and a per-account budget to a query, which is what lets the console stay
 * open to anyone rather than being rate-limited into uselessness by one abuser.
 *
 * Say that, and give the person the button. A red "401" would tell them they did
 * something wrong, which they did not.
 */
import { SIGN_IN_REQUIRED } from "../api/client";

export function isSignInRequired(error: string | null | undefined): boolean {
  return !!error && error === SIGN_IN_REQUIRED;
}

export function SqlSignInNotice({ message }: { message?: string }) {
  // Round-trip back to whatever the reader was doing, query string included, so
  // the SQL they typed is still there after Google sends them back.
  const next =
    typeof window !== "undefined"
      ? window.location.pathname + window.location.search
      : "/data";

  return (
    <div
      role="status"
      style={{
        marginTop: "0.6rem",
        padding: "0.9rem 1rem",
        border: "1px solid var(--border)",
        borderRadius: "0.5rem",
        background: "var(--surface-2, rgba(127,127,127,0.06))",
        fontSize: "0.9rem",
        lineHeight: 1.6,
        display: "flex",
        flexWrap: "wrap",
        alignItems: "center",
        gap: "0.75rem",
      }}
    >
      <span style={{ flex: "1 1 22rem", minWidth: 0 }}>
        {message || SIGN_IN_REQUIRED}
      </span>
      <a
        className="btn"
        href={`/api/auth/sso/google?next=${encodeURIComponent(next)}`}
        style={{ whiteSpace: "nowrap" }}
      >
        התחברות עם Google
      </a>
    </div>
  );
}
