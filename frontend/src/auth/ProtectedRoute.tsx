import { Navigate } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useAuth } from "./AuthContext";

export default function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { user, loading } = useAuth();
  const { t } = useTranslation();

  if (loading) return <div className="loading" role="status" aria-live="polite">{t("common.loading")}</div>;
  if (!user) return <Navigate to="/admin/login" replace />;

  return <>{children}</>;
}
