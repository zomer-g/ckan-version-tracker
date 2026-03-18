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
      <nav
        aria-label={t("app_name")}
        style={{
          background: "var(--surface)",
          borderBottom: "1px solid var(--border)",
          padding: "0.75rem 0",
        }}
      >
        <div className="container flex-between">
          <div className="flex">
            <Link to="/" style={{ fontWeight: 700, fontSize: "1.1rem" }}>
              {t("app_name")}
            </Link>
            {user && (
              <>
                <Link to="/">{t("nav.search")}</Link>
                <Link to="/tracked">{t("nav.tracked")}</Link>
              </>
            )}
          </div>
          <div className="flex">
            <button
              className="btn-secondary"
              onClick={toggleLang}
              aria-label={langLabel}
            >
              {i18n.language === "he" ? "EN" : "HE"}
            </button>
            {user ? (
              <>
                <span className="text-sm text-muted">{user.display_name}</span>
                <button className="btn-secondary" onClick={logout}>
                  {t("nav.logout")}
                </button>
              </>
            ) : (
              <>
                <Link to="/login">{t("nav.login")}</Link>
                <Link to="/register">{t("nav.register")}</Link>
              </>
            )}
          </div>
        </div>
      </nav>
    </header>
  );
}
