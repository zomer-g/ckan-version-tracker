import { NavLink, Outlet } from "react-router-dom";

/**
 * פרויקט 1933 — three pages side by side as tabs:
 *
 *   /projects/1933           הרציונל (RationalePage)
 *   /projects/1933/decision  הנוסח המלא, המשימות והחשבון (DecisionAnalysisPage, key 1933)
 *   /projects/1933/request   הבקשה הרוחבית (Project1933RequestTab)
 *
 * Each tab is its own URL, so a tab can be linked and the back button moves
 * between them. The old addresses (/rationale, /rationale/1933) redirect here
 * (see App.tsx). The tabs are navigation links, not an ARIA tablist: each one
 * loads a different page, and aria-current marks the one showing.
 */
export const PROJECT_1933_TABS: { to: string; label: string; end?: boolean }[] = [
  { to: "/projects/1933", label: "הרציונל", end: true },
  { to: "/projects/1933/decision", label: "הנוסח המלא, המשימות והחשבון" },
  { to: "/projects/1933/request", label: "הבקשה הרוחבית" },
];

/** Where a decision analysis lives: 1933 inside this project, others on their own. */
export function decisionPath(key: string): string {
  return key === "1933" ? "/projects/1933/decision" : `/rationale/${key}`;
}

export default function Project1933Layout() {
  return (
    <div>
      <div className="project1933-bar">
        <div className="container">
          <div className="project1933-eyebrow">פרויקט 1933 · החלטת הממשלה על הנגשת מאגרי המידע</div>
          <nav className="project1933-tabs" aria-label="עמודי פרויקט 1933">
            {PROJECT_1933_TABS.map((t) => (
              <NavLink
                key={t.to}
                to={t.to}
                end={t.end}
                className={({ isActive }) => "project1933-tab" + (isActive ? " is-active" : "")}
              >
                {t.label}
              </NavLink>
            ))}
          </nav>
        </div>
      </div>
      <Outlet />
    </div>
  );
}
