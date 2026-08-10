import { useEffect, useRef, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { datasets as datasetsApi } from "../api/client";
import { usePublishedDecisions } from "../hooks/usePublishedDecisions";

/**
 * Brand icon — inline SVG (no extra dep) shaped as a stacked-archive /
 * "versions" mark so it carries the same family weight as Ocoi's shield
 * and Ocal's calendar.
 */
function BrandIcon() {
  return (
    <svg
      className="brand-icon"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <rect x="3" y="4" width="18" height="4" rx="1" />
      <path d="M5 8v11a1 1 0 0 0 1 1h12a1 1 0 0 0 1-1V8" />
      <path d="M10 12h4" />
    </svg>
  );
}

function HamburgerIcon({ open }: { open: boolean }) {
  return (
    <svg
      className="hamburger-icon"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {open ? <path d="M18 6L6 18M6 6l12 12" /> : <path d="M3 6h18M3 12h18M3 18h18" />}
    </svg>
  );
}

function ChevronIcon() {
  return (
    <svg
      className="nav-chevron"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2.2"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M6 9l6 6 6-6" />
    </svg>
  );
}

// A single navigable link.
type NavLeaf = { to: string; label: string };
// A dropdown group of links, shown under one heading.
type NavGroup = { key: string; label: string; children: NavLeaf[] };
type NavEntry = NavLeaf | NavGroup;

const isGroup = (e: NavEntry): e is NavGroup =>
  (e as NavGroup).children !== undefined;

export default function Navbar() {
  const { t, i18n } = useTranslation();
  const location = useLocation();
  const [mobileOpen, setMobileOpen] = useState(false);
  // Which desktop dropdown is open (by group key), or null.
  const [openGroup, setOpenGroup] = useState<string | null>(null);
  const navDesktopRef = useRef<HTMLDivElement>(null);

  // Subtle public "requests waiting" dot: poll the pending count so the admin
  // (or anyone) notices a backlog on landing, without logging in. Best-effort
  // — a failed fetch just leaves the dot hidden.
  const [pendingCount, setPendingCount] = useState(0);
  useEffect(() => {
    let alive = true;
    const load = () =>
      datasetsApi
        .pendingCount()
        .then((r) => { if (alive) setPendingCount(r.count || 0); })
        .catch(() => {});
    load();
    const id = setInterval(load, 60000);
    return () => { alive = false; clearInterval(id); };
  }, []);

  const toggleLang = () => {
    const next = i18n.language === "he" ? "en" : "he";
    i18n.changeLanguage(next);
    localStorage.setItem("lang", next);
  };

  const langLabel =
    i18n.language === "he" ? "Switch to English" : "החלף לעברית";

  const isActive = (path: string) =>
    path === "/" ? location.pathname === "/" : location.pathname.startsWith(path);

  // Published decision analyses, if any — cached module-side, so this is one
  // request per session and not one per navigation.
  const publishedDecisions = usePublishedDecisions();

  // Two-level structure: a few flat entry points + grouped dropdowns.
  const navEntries: NavEntry[] = [
    { to: "/", label: t("nav.search") },
    // The catalog sits right next to search on purpose: both browse the same
    // corpus, so they belong side by side rather than at opposite ends.
    {
      key: "catalog",
      label: t("nav.catalog", "קטלוג"),
      children: [
        { to: "/organizations", label: t("nav.organizations", "ארגונים") },
        { to: "/tags", label: t("nav.tags", "תגיות") },
        { to: "/sources", label: t("nav.sources", "מקורות") },
      ],
    },
    { to: "/data", label: t("nav.data_sql", "SQL") },
    {
      key: "projects",
      label: t("nav.projects", "פרויקטים"),
      children: [
        { to: "/knesset", label: t("nav.knesset_db", "הכנסת") },
        { to: "/cbs", label: t("nav.cbs", 'למ"ס') },
        { to: "/projects/odata", label: t("nav.odata", "מידע לעם") },
        { to: "/projects/ocal", label: t("nav.ocal", "יומן לעם") },
        { to: "/projects/ocoi", label: t("nav.ocoi", "ניגוד עניינים לעם") },
        { to: "/projects/nadlan", label: t("nav.nadlan", 'נדל"ן לעם') },
      ],
    },
    {
      key: "about",
      label: t("nav.about"),
      children: [
        { to: "/about", label: t("nav.about") },
        { to: "/rationale", label: t("nav.rationale", "הרציונל") },
        // Decision analyses appear here only once published — an unpublished
        // draft returns nothing from the index endpoint and leaves no link.
        ...publishedDecisions.map((d) => ({
          to: `/rationale/${d.key}`,
          label: t("nav.decision", "החלטה") + " " + d.key,
        })),
        { to: "/api", label: t("nav.api", "API") },
      ],
    },
  ];

  const groupActive = (g: NavGroup) => g.children.some((c) => isActive(c.to));

  const closeMobile = () => setMobileOpen(false);
  const closeDesktop = () => setOpenGroup(null);

  // Close an open desktop dropdown on outside click or Escape.
  useEffect(() => {
    if (!openGroup) return;
    const onDown = (e: MouseEvent) => {
      if (!navDesktopRef.current?.contains(e.target as Node)) closeDesktop();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeDesktop();
    };
    document.addEventListener("mousedown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("mousedown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [openGroup]);

  // Any route change closes both menus.
  useEffect(() => {
    closeDesktop();
    closeMobile();
  }, [location.pathname]);

  return (
    <header>
      <nav className="navbar" role="navigation" aria-label={t("app_name")}>
        <div className="container navbar-inner">
          <Link
            to="/"
            className="brand"
            aria-label={t("app_name")}
            onClick={closeMobile}
          >
            <BrandIcon />
            <span className="brand-text">{t("app_name")}</span>
            {pendingCount > 0 && (
              <span
                className="pending-dot"
                role="status"
                aria-label={t("nav.pending_waiting", "יש בקשות ממתינות")}
                title={t("nav.pending_waiting", "יש בקשות ממתינות")}
              />
            )}
          </Link>

          {/* Desktop nav — visible at ≥640px */}
          <div className="navbar-nav-desktop" ref={navDesktopRef}>
            {navEntries.map((entry) =>
              isGroup(entry) ? (
                <div key={entry.key} className="nav-group">
                  <button
                    type="button"
                    className={`nav-link nav-group-btn${
                      groupActive(entry) ? " is-active" : ""
                    }${openGroup === entry.key ? " is-open" : ""}`}
                    aria-haspopup="true"
                    aria-expanded={openGroup === entry.key}
                    onClick={() =>
                      setOpenGroup((cur) => (cur === entry.key ? null : entry.key))
                    }
                  >
                    {entry.label}
                    <ChevronIcon />
                  </button>
                  {openGroup === entry.key && (
                    <div className="nav-dropdown" role="menu">
                      {entry.children.map((child) => (
                        <Link
                          key={child.to}
                          to={child.to}
                          role="menuitem"
                          className={`nav-dropdown-link${
                            isActive(child.to) ? " is-active" : ""
                          }`}
                          aria-current={isActive(child.to) ? "page" : undefined}
                          onClick={closeDesktop}
                        >
                          {child.label}
                        </Link>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <Link
                  key={entry.to}
                  to={entry.to}
                  className={`nav-link${isActive(entry.to) ? " is-active" : ""}`}
                  aria-current={isActive(entry.to) ? "page" : undefined}
                >
                  {entry.label}
                </Link>
              )
            )}
            <button
              className="btn-lang"
              onClick={toggleLang}
              aria-label={langLabel}
            >
              {i18n.language === "he" ? "EN" : "HE"}
            </button>
          </div>

          {/* Mobile hamburger — visible only <640px */}
          <button
            type="button"
            className="navbar-hamburger"
            aria-expanded={mobileOpen}
            aria-controls="navbar-mobile-panel"
            aria-label={t("nav.menu", "תפריט ניווט")}
            onClick={() => setMobileOpen((v) => !v)}
          >
            <HamburgerIcon open={mobileOpen} />
          </button>
        </div>

        {/* Mobile dropdown panel */}
        {mobileOpen && (
          <div
            id="navbar-mobile-panel"
            className="navbar-mobile-panel"
            role="region"
            aria-label={t("nav.menu", "תפריט ניווט")}
          >
            <div className="container">
              {navEntries.map((entry) =>
                isGroup(entry) ? (
                  <div key={entry.key} className="navbar-mobile-group">
                    <div className="navbar-mobile-group-title">{entry.label}</div>
                    {entry.children.map((child) => (
                      <Link
                        key={child.to}
                        to={child.to}
                        onClick={closeMobile}
                        className={`navbar-mobile-link navbar-mobile-sublink${
                          isActive(child.to) ? " is-active" : ""
                        }`}
                        aria-current={isActive(child.to) ? "page" : undefined}
                      >
                        {child.label}
                      </Link>
                    ))}
                  </div>
                ) : (
                  <Link
                    key={entry.to}
                    to={entry.to}
                    onClick={closeMobile}
                    className={`navbar-mobile-link${
                      isActive(entry.to) ? " is-active" : ""
                    }`}
                    aria-current={isActive(entry.to) ? "page" : undefined}
                  >
                    {entry.label}
                  </Link>
                )
              )}
              <button
                className="btn-lang navbar-mobile-lang"
                onClick={() => {
                  toggleLang();
                  closeMobile();
                }}
                aria-label={langLabel}
              >
                {langLabel}
              </button>
            </div>
          </div>
        )}
      </nav>
    </header>
  );
}
