import { useState, useRef, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";

export default function LoginPage() {
  const { t } = useTranslation();
  const { login } = useAuth();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const errorRef = useRef<HTMLDivElement>(null);

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
