import { Link } from "react-router-dom";

// Tab strip for the /data area: the SQL console and the healed-cross-reference
// guide. Each tab is its own route (dedicated URL) so it's linkable/shareable.
export default function DataTabs({ active }: { active: "console" | "explore" | "guide" | "normalize" }) {
  const tab = (to: string, label: React.ReactNode, key: string) => {
    const on = active === key;
    return (
      <Link
        to={to}
        className="data-tab"
        style={{
          padding: "0.5rem 1rem", textDecoration: "none", fontWeight: 600, fontSize: "0.9rem",
          borderRadius: "6px 6px 0 0", whiteSpace: "nowrap",
          color: on ? "var(--primary)" : "var(--text-muted)",
          background: on ? "var(--surface-2)" : "transparent",
          borderBottom: on ? "2px solid var(--primary)" : "2px solid transparent",
        }}
        aria-current={on ? "page" : undefined}
      >
        {label}
      </Link>
    );
  };
  return (
    // A <nav> with a name, not a bare div: this is the second-level navigation
    // for the whole /data area (WCAG 1.3.1). Icons are hidden from the
    // accessibility tree — they decorate the label, they are not the label.
    <nav
      aria-label="ניווט באזור הנתונים"
      style={{ display: "flex", gap: 4, borderBottom: "1px solid var(--border)", marginBottom: "1rem", flexWrap: "wrap" }}
    >
      {tab("/data", <><span aria-hidden="true">{"</>"}</span> קונסולת SQL</>, "console")}
      {tab("/data/explore", <><span aria-hidden="true">🔎</span> מצא נתונים</>, "explore")}
      {tab("/data/normalize", <><span aria-hidden="true">🧹</span> נרמול רשימת שמות</>, "normalize")}
      {tab("/data/guide", <><span aria-hidden="true">📖</span> מדריך — הצלבה מתוקנת</>, "guide")}
    </nav>
  );
}
