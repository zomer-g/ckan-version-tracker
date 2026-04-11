import { useState, useRef, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { auth } from "../api/client";

export default function LoginPage() {
  const { t } = useTranslation();
  const { user } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [error, setError] = useState("");
  const [ssoProviders, setSsoProviders] = useState<{ google: boolean }>({
    google: false,
  });
  const errorRef = useRef<HTMLDivElement>(null);

  // If already logged in (e.g. SSO callback set token), redirect
  useEffect(() => {
    if (user) navigate("/");
  }, [user, navigate]);

  // Check for SSO error in URL
  useEffect(() => {
    const err = searchParams.get("error");
    if (err) {
      const errorMap: Record<string, string> = {
        google_denied: t("auth.sso_denied"),
        google_failed: t("auth.sso_failed"),
        no_email: t("auth.sso_no_email"),
      };
      setError(errorMap[err] || t("auth.sso_failed"));
    }
  }, [searchParams, t]);

  // Load SSO providers
  useEffect(() => {
    auth.ssoProviders().then(setSsoProviders).catch(() => {});
  }, []);

  return (
    <div style={{ maxWidth: 400, margin: "3rem auto" }}>
      <div className="card">
        <h1 style={{ fontSize: "1.5rem", fontWeight: 700 }} className="mb-2">
          {t("auth.login_title")}
        </h1>
        {error && (
          <div
            ref={errorRef}
            role="alert"
            className="badge badge-danger mb-2"
            style={{ display: "block" }}
            tabIndex={-1}
          >
            {error}
          </div>
        )}

        {ssoProviders.google && (
          <a
            href="/api/auth/sso/google"
            className="sso-btn"
            style={ssoButtonStyle}
          >
            <svg width="18" height="18" viewBox="0 0 24 24" style={{ marginInlineEnd: "0.5rem" }}>
              <path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92a5.06 5.06 0 01-2.2 3.32v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.1z"/>
              <path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/>
              <path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"/>
              <path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/>
            </svg>
            {t("auth.sso_google")}
          </a>
        )}
      </div>
    </div>
  );
}

const ssoButtonStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  width: "100%",
  padding: "0.75rem 1rem",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius)",
  background: "var(--surface)",
  color: "var(--text)",
  fontSize: "1rem",
  fontWeight: 500,
  textDecoration: "none",
  cursor: "pointer",
  transition: "background 0.2s",
};
