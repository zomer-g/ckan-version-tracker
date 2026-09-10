import { Navigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useAuth } from "./AuthContext";

/**
 * `requireAdmin` is a display gate only. The server decides admin on every
 * request from the ADMIN_EMAILS secret and refuses everything under /api/admin
 * for anyone else. Without this, any Google account that signed in (which the
 * SQL consoles now ask people to do) was shown the admin menu, with every panel
 * failing on "Admin access required". Nothing leaked, but it must not render.
 */
export default function ProtectedRoute({
  children,
  requireAdmin = false,
}: {
  children: React.ReactNode;
  requireAdmin?: boolean;
}) {
  const { user, loading } = useAuth();
  const { t } = useTranslation();

  if (loading) return <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>;
  if (!user) return <Navigate to="/admin/login" replace />;
  if (requireAdmin && !user.is_admin) {
    // Not a redirect: /admin/login would bounce a signed-in user straight back.
    return (
      <div className="container mt-3" role="alert">
        <h1>{t("auth.adminOnlyTitle", "אין הרשאת ניהול")}</h1>
        <p>
          {t(
            "auth.adminOnlyBody",
            "החשבון שמחובר כרגע אינו חשבון ניהול. ההתחברות מאפשרת להריץ שאילתות במאגר הנתונים, ולא נותנת גישה לממשק הניהול.",
          )}
        </p>
      </div>
    );
  }

  return <>{children}</>;
}
