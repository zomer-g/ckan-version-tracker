import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";

const SPONSOR_URL = "https://github.com/sponsors/zomer-g?utm_source=over&utm_medium=footer";
const SOURCE_URL = "https://github.com/zomer-g/ckan-version-tracker";
// Google's "preferred sources" deeplink: lets a reader star over.org.il so it
// shows up more in Top Stories / AI Overviews for them. A plain link, not
// Google's publisher.js button, so the footer loads no third-party script.
const PREFERRED_SOURCE_URL = "https://www.google.com/preferences/source?q=over.org.il";

function trackOutbound(name: string) {
  const gtag = (window as unknown as { gtag?: (...a: unknown[]) => void }).gtag;
  try {
    gtag?.("event", name, { transport_type: "beacon" });
  } catch {
    /* analytics must never cost a click */
  }
}

/**
 * Footer — short on purpose. The header already lists every project, so the
 * footer no longer repeats them: it carries the brand, two compact lines of
 * site links (catalog, about/legal), and a bottom bar with the copyright,
 * the source code, the "say thanks" link and the preferred-source link.
 */
export default function Footer() {
  const { t, i18n } = useTranslation();
  const he = i18n.language === "he";

  const groups: { title: string; links: { label: string; to: string }[] }[] = [
    {
      title: t("footer.group_catalog", "קטלוג"),
      links: [
        { label: t("nav.organizations", "ארגונים"), to: "/organizations" },
        { label: t("nav.sources", "מקורות"), to: "/sources" },
        { label: t("nav.tags", "תגיות"), to: "/tags" },
        { label: t("nav.data_sql", "SQL"), to: "/data" },
        { label: t("nav.api", "API"), to: "/api" },
      ],
    },
    {
      title: t("footer.group_about", "אודות"),
      links: [
        { label: t("nav.about"), to: "/about" },
        { label: t("about.privacy_title"), to: "/about#privacy" },
        { label: t("about.accessibility_title"), to: "/about#accessibility" },
        { label: t("nav.security", "אבטחת מידע"), to: "/security" },
      ],
    },
  ];

  const newWindow = <span className="sr-only"> {he ? "(נפתח בחלון חדש)" : "(opens in a new window)"}</span>;

  return (
    <footer
      className="footer"
      // Translated in full, so it names its own language (WCAG 3.1.2).
      lang={i18n.language}
      dir={he ? "rtl" : "ltr"}
    >
      <div className="container">
        <div className="footer-top">
          <div className="footer-brand-col">
            <div className="footer-brand">{t("app_name")}</div>
            <div className="footer-desc">{t("footer.description")}</div>
          </div>

          <div className="footer-links">
            {groups.map((group) => (
              <nav key={group.title} className="footer-links-row" aria-label={group.title}>
                <span className="footer-links-title">{group.title}</span>
                {group.links.map((link) => (
                  <Link key={link.to} to={link.to}>
                    {link.label}
                  </Link>
                ))}
              </nav>
            ))}
          </div>
        </div>

        <div className="footer-bottom">
          <span className="footer-copyright">{t("footer.copyright")}</span>
          <div className="footer-meta">
            <a href={SOURCE_URL} target="_blank" rel="noopener noreferrer">
              {t("footer.source_code_github", he ? "קוד מקור ב-GitHub" : "Source code on GitHub")}
              {newWindow}
            </a>
            <a
              href={PREFERRED_SOURCE_URL}
              target="_blank"
              rel="noopener noreferrer"
              onClick={() => trackOutbound("preferred_source_click")}
            >
              {he ? "הוספה כמקור מועדף ב-Google" : "Add as a preferred source on Google"}
              {newWindow}
            </a>
            {he ? (
              <p className="gz-support" dir="rtl" lang="he">
                הכלי הזה חינמי.{" "}
                <a
                  href={SPONSOR_URL}
                  target="_blank"
                  rel="noopener"
                  title="דרך GitHub Sponsors, נדרש חשבון GitHub"
                  onClick={() => trackOutbound("sponsor_click")}
                >
                  אפשר להגיד תודה{newWindow}
                </a>{" "}
                · ללא הטבה וללא התחייבות
              </p>
            ) : (
              <p className="gz-support" lang="en">
                This tool is free.{" "}
                <a
                  href={SPONSOR_URL}
                  target="_blank"
                  rel="noopener"
                  title="Via GitHub Sponsors, a GitHub account is required"
                  onClick={() => trackOutbound("sponsor_click")}
                >
                  Say thanks{newWindow}
                </a>{" "}
                · no perks, no commitment
              </p>
            )}
          </div>
        </div>
      </div>
    </footer>
  );
}
