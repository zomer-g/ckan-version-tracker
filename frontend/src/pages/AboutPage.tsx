import { Trans, useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { usePageContentOverrides } from "../hooks/usePageContentOverrides";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import { GLOSSARY } from "../components/a11y/Abbr";
// Small helper for external links so the Trans <1> placeholder stays terse.
function ExtLink({ href, children }: { href: string; children?: React.ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
    <span className="sr-only"> (נפתח בחלון חדש)</span></a>
  );
}

export default function AboutPage() {
  useDocumentTitle("אודות");
  const { t } = useTranslation();
  usePageContentOverrides("about");

  return (
    <div>
      <div className="about-hero">
        <div className="container">
          <h1>{t("about.title")}</h1>
        </div>
      </div>

      <div className="about-section">
        <div className="about-card">
          <h2>{t("about.what_title")}</h2>
          <p>
            <Trans
              i18nKey="about.what_text"
              components={{ 1: <ExtLink href="https://data.gov.il" /> }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.why_title")}</h2>
          <p>{t("about.why_text")}</p>
          <p>
            <Trans
              i18nKey="about.rationale_teaser"
              components={{ 1: <Link to="/rationale" /> }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.how_title")}</h2>
          <p>
            <Trans
              i18nKey="about.how_text"
              components={{
                1: <ExtLink href="https://data.gov.il" />,
                2: <ExtLink href="https://www.odata.org.il" />,
              }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.for_whom_title")}</h2>
          <ul>
            <li>{t("about.for_whom_item1")}</li>
            <li>{t("about.for_whom_item2")}</li>
            <li>{t("about.for_whom_item3")}</li>
          </ul>
        </div>

        <div className="about-card">
          <h2>{t("about.what_can_do_title")}</h2>
          <ul>
            <li>{t("about.what_can_do_item1")}</li>
            <li>{t("about.what_can_do_item2")}</li>
            <li>{t("about.what_can_do_item3")}</li>
          </ul>
        </div>

        <div className="about-card">
          <h2>{t("about.who_title")}</h2>
          <p>
            <Trans
              i18nKey="about.who_text"
              components={{
                strong: <strong />,
                1: <ExtLink href="https://www.z-g.co.il/projects" />,
              }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.family_title")}</h2>
          <p>{t("about.family_intro")}</p>
          <ul className="family-list">
            <li>
              <strong>
                <ExtLink href="https://www.odata.org.il">
                  {t("about.family_odata_name")}
                </ExtLink>
              </strong>
              {": "}
              {t("about.family_odata_desc")}
            </li>
            <li>
              <strong>
                <ExtLink href="https://www.ocoi.org.il">
                  {t("about.family_ocoi_name")}
                </ExtLink>
              </strong>
              {": "}
              {t("about.family_ocoi_desc")}
            </li>
            <li>
              <strong>
                <ExtLink href="https://ocal.org.il">
                  {t("about.family_ocal_name")}
                </ExtLink>
              </strong>
              {": "}
              {t("about.family_ocal_desc")}
            </li>
          </ul>
        </div>

        {/* WCAG 3.1.3 (Unusual Words) and 3.1.4 (Abbreviations) ask that a
            reader be able to look up jargon they cannot work out from context.
            The site is full of it — CKAN, ODATA, WFS, גוש/חלקה. One list, read
            from the same table the inline <Abbr> tooltips use, so the two
            cannot drift apart. */}
        <div className="about-card" id="glossary">
          <h2>מילון מונחים</h2>
          <p className="text-muted text-sm">
            מונחים שחוזרים באתר. כל מונח מסומן בקו מקווקו בטקסט — אפשר לעצור
            עליו כדי לראות את ההסבר, וכאן מופיעה הרשימה המלאה.
          </p>
          <dl style={{ display: "grid", gap: "0.7rem", marginTop: "1rem" }}>
            {Object.entries(GLOSSARY).map(([term, meaning]) => (
              <div key={term}>
                <dt style={{ fontWeight: 700 }} dir={/^[A-Za-z]/.test(term) ? "ltr" : "rtl"}>
                  {term}
                </dt>
                <dd style={{ margin: 0, color: "var(--text-muted)" }}>{meaning}</dd>
              </div>
            ))}
          </dl>
        </div>

        <div className="about-card">
          <h2>{t("about.contact_title")}</h2>
          <p>
            <Trans
              i18nKey="about.contact_text"
              components={{
                1: <a href="mailto:guy@z-g.co.il" />,
              }}
            />
          </p>
        </div>
      </div>
    </div>
  );
}
