import { useState, useRef, useEffect, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { Link, useNavigate, useSearchParams } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";
import { auth } from "../api/client";

export default function LoginPage() {
  const { t } = useTranslation();
  const { user, login } = useAuth();
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [ssoProviders, setSsoProviders] = useState<{ google: boolean; github: boolean }>({
    google: false,
    github: false,
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
        github_denied: t("auth.sso_denied"),
        google_failed: t("auth.sso_failed"),
        github_failed: t("auth.sso_failed"),
        no_email: t("auth.sso_no_email"),
      };
      setError(errorMap[err] || t("auth.sso_failed"));
    }
  }, [searchParams, t]);

  // Load SSO providers
  useEffect(() => {
    auth.ssoProviders().then(setSsoProviders).catch(() => {});
  }, []);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError("");
    try {
      await login(email, password);
      navigate("/");
    } catch (err: any) {
      setError(err.message);
      setTimeout(() => errorRef.current?.focus(), 50);
    }
  };

  const hasSso = ssoProviders.google || ssoProviders.github;

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

        {/* SSO Buttons */}
        {hasSso && (
          <div className="mb-2">
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
            {ssoProviders.github && (
              <a
                href="/api/auth/sso/github"
                className="sso-btn"
                style={{ ...ssoButtonStyle, marginTop: "0.5rem" }}
              >
                <svg width="18" height="18" viewBox="0 0 24 24" style={{ marginInlineEnd: "0.5rem" }} fill="currentColor">
                  <path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/>
                </svg>
                {t("auth.sso_github")}
              </a>
            )}

            <div style={dividerStyle}>
              <span style={dividerTextStyle}>{t("auth.or")}</span>
            </div>
          </div>
        )}

        <form onSubmit={handleSubmit}>
          <div className="mb-1">
            <label htmlFor="login-email" className="text-sm">
              {t("auth.email")}
            </label>
            <input
              id="login-email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              autoComplete="email"
            />
          </div>
          <div className="mb-2">
            <label htmlFor="login-password" className="text-sm">
              {t("auth.password")}
            </label>
            <input
              id="login-password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              autoComplete="current-password"
            />
          </div>
          <button type="submit" className="btn-primary" style={{ width: "100%" }}>
            {t("auth.login_btn")}
          </button>
        </form>
        <p className="text-sm text-muted mt-2">
          {t("auth.no_account")} <Link to="/register">{t("nav.register")}</Link>
        </p>
      </div>
    </div>
  );
}

const ssoButtonStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  width: "100%",
  padding: "0.6rem 1rem",
  border: "1px solid var(--border)",
  borderRadius: "var(--radius)",
  background: "var(--surface)",
  color: "var(--text)",
  fontSize: "0.875rem",
  fontWeight: 500,
  textDecoration: "none",
  cursor: "pointer",
  transition: "background 0.2s",
};

const dividerStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  margin: "1rem 0 0",
  gap: "0.75rem",
};

const dividerTextStyle: React.CSSProperties = {
  flex: 1,
  textAlign: "center",
  fontSize: "0.8rem",
  color: "var(--text-muted)",
  position: "relative",
};
