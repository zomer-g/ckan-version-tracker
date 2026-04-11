import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useAuth } from "../auth/AuthContext";

export default function Navbar() {
  const { t, i18n } = useTranslation();
  const { user, logout } = useAuth();

  const toggleLang = () => {
    const next = i18n.language === "he" ? "en" : "he";
    i18n.changeLanguage(next);
    localStorage.setItem("lang", next);
  };

  const langLabel =
    i18n.language === "he" ? "Switch to English" : "החלף לעברית";

  return (
    <header>
      <nav className="navbar" aria-label={t("app_name")}>
        <div className="container flex-between">
          <div className="flex">
            <Link to="/" className="brand">
              {t("app_name")}
            </Link>
            {user && (
              <>
                <Link to="/" className="nav-link">{t("nav.search")}</Link>
                <Link to="/tracked" className="nav-link">{t("nav.tracked")}</Link>
                {user.is_admin && <Link to="/admin" className="nav-link">{t("nav.admin")}</Link>}
              </>
            )}
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
            {user ? (
              <>
                <span className="user-name">{user.display_name}</span>
                <button className="btn-logout" onClick={logout}>
                  {t("nav.logout")}
                </button>
              </>
            ) : (
              <Link to="/login" className="nav-link">{t("nav.login")}</Link>
            )}
          </div>
        </div>
      </nav>
    </header>
  );
}
