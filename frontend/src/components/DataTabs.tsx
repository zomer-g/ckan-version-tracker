import { Link } from "react-router-dom";

// Tab strip for the /data area: the SQL console and the healed-cross-reference
// guide. Each tab is its own route (dedicated URL) so it's linkable/shareable.
export default function DataTabs({ active }: { active: "console" | "explore" | "guide" | "normalize" }) {
  const tab = (to: string, label: string, key: string) => {
    const on = active === key;
    return (
      <Link
        to={to}
        style={{
          padding: "0.5rem 1rem", textDecoration: "none", fontWeight: 600, fontSize: "0.9rem",
          borderRadius: "6px 6px 0 0", whiteSpace: "nowrap",
          color: on ? "var(--primary, #0f766e)" : "var(--text-muted, #64748b)",
          background: on ? "var(--bg-muted, #eef2f5)" : "transparent",
          borderBottom: on ? "2px solid var(--primary, #0f766e)" : "2px solid transparent",
        }}
        aria-current={on ? "page" : undefined}
      >
        {label}
      </Link>
    );
  };
  return (
    <div style={{ display: "flex", gap: 4, borderBottom: "1px solid var(--border, #e5e7eb)", marginBottom: "1rem", flexWrap: "wrap" }}>
      {tab("/data/explore", "🔎 מצא נתונים", "explore")}
      {tab("/data", "</> קונסולת SQL", "console")}
      {tab("/data/normalize", "🧹 נרמול רשימת שמות", "normalize")}
      {tab("/data/guide", "📖 מדריך — הצלבה מתוקנת", "guide")}
    </div>
  );
}
