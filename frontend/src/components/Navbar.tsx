import { useEffect, useState } from "react";
import { Link, useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { datasets as datasetsApi } from "../api/client";

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

export default function Navbar() {
  const { t, i18n } = useTranslation();
  const location = useLocation();
  const [mobileOpen, setMobileOpen] = useState(false);

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

  const navClass = (path: string) =>
    `nav-link${isActive(path) ? " is-active" : ""}`;

  const navItems: { to: string; label: string }[] = [
    { to: "/", label: t("nav.search") },
    { to: "/organizations", label: t("nav.organizations", "ארגונים") },
    { to: "/tags", label: t("nav.tags", "תגיות") },
    { to: "/api", label: t("nav.api", "API") },
    { to: "/about", label: t("nav.about") },
  ];

  const closeMobile = () => setMobileOpen(false);

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
          <div className="navbar-nav-desktop">
            {navItems.map((item) => (
              <Link
                key={item.to}
                to={item.to}
                className={navClass(item.to)}
                aria-current={isActive(item.to) ? "page" : undefined}
              >
                {item.label}
              </Link>
            ))}
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
              {navItems.map((item) => (
                <Link
                  key={item.to}
                  to={item.to}
                  onClick={closeMobile}
                  className={`navbar-mobile-link${isActive(item.to) ? " is-active" : ""}`}
                  aria-current={isActive(item.to) ? "page" : undefined}
                >
                  {item.label}
                </Link>
              ))}
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
