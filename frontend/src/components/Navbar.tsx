import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";

export default function Navbar() {
  const { t, i18n } = useTranslation();

  const toggleLang = () => {
    const next = i18n.language === "he" ? "en" : "he";
    i18n.changeLanguage(next);
    localStorage.setItem("lang", next);
  };

  const langLabel =
    i18n.language === "he" ? "Switch to English" : "החלף לעברית";

  return (
    <header>
      <nav className="navbar" role="navigation" aria-label={t("app_name")}>
        <div className="container flex-between">
          <div className="flex">
            <Link to="/" className="brand">
              {t("app_name")}
            </Link>
            <Link to="/" className="nav-link">{t("nav.search")}</Link>
            <Link to="/about" className="nav-link">{t("nav.about")}</Link>
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
