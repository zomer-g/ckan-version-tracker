import { useState, useRef, FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { Link, useNavigate } from "react-router-dom";
import { useAuth } from "../auth/AuthContext";

export default function RegisterPage() {
  const { t } = useTranslation();
  const { register } = useAuth();
  const navigate = useNavigate();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [displayName, setDisplayName] = useState("");
  const [error, setError] = useState("");
  const errorRef = useRef<HTMLDivElement>(null);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError("");
    try {
      await register(email, password, displayName);
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
          {t("auth.register_title")}
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
            <label htmlFor="reg-name" className="text-sm">
              {t("auth.display_name")}
            </label>
            <input
              id="reg-name"
              type="text"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              required
              autoComplete="name"
            />
          </div>
          <div className="mb-1">
            <label htmlFor="reg-email" className="text-sm">
              {t("auth.email")}
            </label>
            <input
              id="reg-email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              autoComplete="email"
            />
          </div>
          <div className="mb-2">
            <label htmlFor="reg-password" className="text-sm">
              {t("auth.password")}
            </label>
            <input
              id="reg-password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={8}
              autoComplete="new-password"
              aria-describedby="password-hint"
            />
            <small id="password-hint" className="text-sm text-muted">
              {t("auth.password_hint", "Minimum 8 characters")}
            </small>
          </div>
          <button type="submit" className="btn-primary" style={{ width: "100%" }}>
            {t("auth.register_btn")}
          </button>
        </form>
        <p className="text-sm text-muted mt-2">
          {t("auth.has_account")} <Link to="/login">{t("nav.login")}</Link>
        </p>
      </div>
    </div>
  );
}
