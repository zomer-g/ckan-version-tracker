import { Trans, useTranslation } from "react-i18next";
import { Link } from "react-router-dom";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import { REPO_URL, SECURITY_EMAIL, SECURITY_LOG, SecurityEntry } from "../data/securityLog";

// Sits beside the privacy policy and the accessibility statement (/about):
// how to report a security problem, what to expect, and a public log of every
// report or finding with the commits that fixed it. The log itself lives in
// src/data/securityLog.ts so an entry is one commit to one file.

function ExtLink({ href, children }: { href: string; children?: React.ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
      <span className="sr-only"> (נפתח בחלון חדש)</span>
    </a>
  );
}

function fmtDate(iso: string, lang: string): string {
  return new Date(iso + "T00:00:00").toLocaleDateString(lang === "he" ? "he-IL" : "en-GB", {
    day: "numeric", month: "long", year: "numeric",
  });
}

function Entry({ e, lang }: { e: SecurityEntry; lang: "he" | "en" }) {
  const { t } = useTranslation();
  const dl: React.CSSProperties = { margin: "0.75rem 0 0", display: "grid", gap: "0.5rem" };
  const dt: React.CSSProperties = { fontWeight: 600, color: "var(--text-muted)", fontSize: "0.85rem" };
  const dd: React.CSSProperties = { margin: 0, lineHeight: 1.7 };
  return (
    <article className="about-card" aria-labelledby={`sec-${e.date}-${e.commits[0]?.sha}`}>
      <div style={{ display: "flex", flexWrap: "wrap", alignItems: "center", gap: "0.5rem 0.75rem" }}>
        <span
          className="badge"
          style={{
            fontSize: "0.72rem",
            background: e.kind === "report" ? "var(--tint-warn-bg, #fef3c7)" : "var(--surface-2, var(--surface))",
            border: "1px solid var(--border)",
          }}
        >
          {e.kind === "report" ? t("security.kind_report") : t("security.kind_internal")}
        </span>
        <time dateTime={e.date} className="text-sm text-muted">{fmtDate(e.date, lang)}</time>
        {e.fixed && (
          <span className="text-sm text-muted">
            · {t("security.fixed_on")} <time dateTime={e.fixed}>{fmtDate(e.fixed, lang)}</time>
          </span>
        )}
      </div>
      <h3 id={`sec-${e.date}-${e.commits[0]?.sha}`} style={{ fontSize: "1.1rem", fontWeight: 600, margin: "0.6rem 0 0" }}>
        {e.title[lang]}
      </h3>
      <dl style={dl}>
        <div>
          <dt style={dt}>{t("security.col_finding")}</dt>
          <dd style={dd}>{e.finding[lang]}</dd>
        </div>
        <div>
          <dt style={dt}>{t("security.col_exposure")}</dt>
          <dd style={dd}>{e.exposure[lang]}</dd>
        </div>
        <div>
          <dt style={dt}>{t("security.col_fix")}</dt>
          <dd style={dd}>{e.fix[lang]}</dd>
        </div>
        {e.credit && (
          <div>
            <dt style={dt}>{t("security.col_credit")}</dt>
            <dd style={dd}>{e.credit[lang]}</dd>
          </div>
        )}
        <div>
          <dt style={dt}>{t("security.col_commits")}</dt>
          <dd style={dd}>
            <ul style={{ margin: 0, paddingInlineStart: "1.2rem" }}>
              {e.commits.map((c) => (
                <li key={c.sha}>
                  <ExtLink href={`${REPO_URL}/commit/${c.sha}`}>
                    <code dir="ltr">{c.sha}</code>
                  </ExtLink>
                  {" "}{c.label[lang]}
                </li>
              ))}
            </ul>
          </dd>
        </div>
      </dl>
    </article>
  );
}

export default function SecurityPage() {
  const { t, i18n } = useTranslation();
  useDocumentTitle(t("security.title"));
  const lang: "he" | "en" = i18n.language === "en" ? "en" : "he";
  const mailto = `mailto:${SECURITY_EMAIL}?subject=${encodeURIComponent(t("security.mail_subject"))}`;
  const reports = SECURITY_LOG.filter((e) => e.kind === "report").length;

  return (
    <div>
      <div className="about-hero">
        <div className="container">
          <h1>{t("security.title")}</h1>
        </div>
      </div>

      <div className="about-section">
        <div className="about-card" id="report">
          <h2>{t("security.report_title")}</h2>
          <p>
            <Trans
              i18nKey="security.report_text"
              values={{ email: SECURITY_EMAIL }}
              components={{ 1: <a href={mailto} dir="ltr" /> }}
            />
          </p>
          <p>{t("security.report_what")}</p>
          <ul>
            <li>{t("security.report_item_url")}</li>
            <li>{t("security.report_item_steps")}</li>
            <li>{t("security.report_item_data")}</li>
          </ul>
          {/* Email is the ONLY channel, on purpose: GitHub's private
              vulnerability reporting notifies only through GitHub's own
              app/inbox, which the maintainer does not watch, so a report
              there could sit unseen. It is disabled on the repository. */}
          <p>{t("security.report_email_only")}</p>
          <p className="text-sm text-muted">
            <Trans
              i18nKey="security.report_txt"
              components={{ 1: <a href="/.well-known/security.txt" dir="ltr" /> }}
            />
          </p>
        </div>

        <div className="about-card" id="expect">
          <h2>{t("security.expect_title")}</h2>
          <ul>
            <li>{t("security.expect_ack")}</li>
            <li>{t("security.expect_fix")}</li>
            <li>{t("security.expect_log")}</li>
            <li>{t("security.expect_credit")}</li>
            <li>{t("security.expect_no_action")}</li>
          </ul>
        </div>

        <div className="about-card" id="practices">
          <h2>{t("security.practices_title")}</h2>
          <p>{t("security.practices_intro")}</p>
          <ul>
            <li>{t("security.practice_minimal")}</li>
            <li>{t("security.practice_readonly")}</li>
            <li>{t("security.practice_admin")}</li>
            <li>{t("security.practice_tokens")}</li>
            <li>{t("security.practice_limits")}</li>
            <li>
              <Trans
                i18nKey="security.practice_open"
                components={{ 1: <ExtLink href={REPO_URL} /> }}
              />
            </li>
          </ul>
          <p>
            <Trans
              i18nKey="security.see_privacy"
              components={{ 1: <Link to="/about#privacy" />, 2: <Link to="/about#accessibility" /> }}
            />
          </p>
        </div>

        <h2 id="log" style={{ fontSize: "1.4rem", fontWeight: 700, margin: "2rem 0 0.5rem" }}>
          {t("security.log_title")}
        </h2>
        <p className="text-muted" style={{ marginBottom: "1.25rem", lineHeight: 1.7 }}>
          {t("security.log_intro", { total: SECURITY_LOG.length, reports })}
        </p>
        {SECURITY_LOG.map((e) => (
          <Entry key={`${e.date}-${e.commits[0]?.sha}`} e={e} lang={lang} />
        ))}
      </div>
    </div>
  );
}
