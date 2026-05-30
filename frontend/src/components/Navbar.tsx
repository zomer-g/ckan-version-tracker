import { Link, useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";

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

export default function Navbar() {
  const { t, i18n } = useTranslation();
  const location = useLocation();

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

  return (
    <header>
      <nav className="navbar" role="navigation" aria-label={t("app_name")}>
        <div className="container flex-between">
          <div className="flex">
            <Link to="/" className="brand" aria-label={t("app_name")}>
              <BrandIcon />
              <span>{t("app_name")}</span>
            </Link>
            <Link to="/" className={navClass("/")} aria-current={isActive("/") ? "page" : undefined}>
              {t("nav.search")}
            </Link>
            <Link
              to="/organizations"
              className={navClass("/organizations")}
              aria-current={isActive("/organizations") ? "page" : undefined}
            >
              {t("nav.organizations", "ארגונים")}
            </Link>
            <Link
              to="/tags"
              className={navClass("/tags")}
              aria-current={isActive("/tags") ? "page" : undefined}
            >
              {t("nav.tags", "תגיות")}
            </Link>
            <Link
              to="/api"
              className={navClass("/api")}
              aria-current={isActive("/api") ? "page" : undefined}
            >
              {t("nav.api", "API")}
            </Link>
            <Link
              to="/about"
              className={navClass("/about")}
              aria-current={isActive("/about") ? "page" : undefined}
            >
              {t("nav.about")}
            </Link>
          </div>
          <div className="flex">
            <button
              className="btn-lang"
              onClick={toggleLang}
              aria-label={langLabel}
            >
              {i18n.language === "he" ? "EN" : "HE"}
            </button>
          </div>
        </div>
      </nav>
    </header>
  );
}
