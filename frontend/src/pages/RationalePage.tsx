import { Trans, useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import { usePageContentOverrides } from "../hooks/usePageContentOverrides";
import { usePublishedDecisions } from "../hooks/usePublishedDecisions";

const DECISION_URL = "https://www.gov.il/he/pages/2016_dec1933";
const REPORT_URL =
  "https://www.gov.il/BlobFolder/policy/report_inter_ministerial_improve_access_to_public_databases/he/DataBaseAccessibilityReport290716a.pdf";
const COMPTROLLER_URL =
  "https://library.mevaker.gov.il/sites/DigitalLibrary/Pages/Reports/5272-2.aspx";
const CALCALIST_URL = "https://www.calcalist.co.il/local_news/article/H15tV3iDu";
const JESTER_URL = "https://www.z-g.co.il/govscraper";

function ExtLink({ href, children }: { href: string; children?: React.ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  );
}

export default function RationalePage() {
  const { t } = useTranslation();
  usePageContentOverrides("rationale");
  // Empty until an analysis is published — the card below then appears on its
  // own, with no code change.
  const decisions = usePublishedDecisions();

  return (
    <div>
      <div className="about-hero">
        <div className="container">
          <h1>{t("rationale.title")}</h1>
          <p className="rationale-hero-sub">{t("rationale.subtitle")}</p>
        </div>
      </div>

      <div className="about-section">
        <div className="about-card">
          <h2>{t("rationale.promise_title")}</h2>
          <p>
            <Trans
              i18nKey="rationale.promise_text1"
              components={{
                1: <ExtLink href={DECISION_URL} />,
                2: <ExtLink href={REPORT_URL} />,
              }}
            />
          </p>
          <p>{t("rationale.promise_text2")}</p>
          <ul>
            <li>{t("rationale.promise_item1")}</li>
            <li>{t("rationale.promise_item2")}</li>
            <li>{t("rationale.promise_item3")}</li>
            <li>{t("rationale.promise_item4")}</li>
            <li>{t("rationale.promise_item5")}</li>
            <li>{t("rationale.promise_item6")}</li>
          </ul>
        </div>

        <blockquote className="rationale-principle">
          <p>{t("rationale.principle")}</p>
          <footer>{t("rationale.principle_source")}</footer>
        </blockquote>

        <div className="about-card">
          <h2>{t("rationale.standard_title")}</h2>
          <p>{t("rationale.standard_text1")}</p>
          <ul>
            <li>{t("rationale.standard_item1")}</li>
            <li>{t("rationale.standard_item2")}</li>
            <li>{t("rationale.standard_item3")}</li>
            <li>{t("rationale.standard_item4")}</li>
            <li>{t("rationale.standard_item5")}</li>
          </ul>
          <p>{t("rationale.standard_text2")}</p>
        </div>

        <div className="about-card">
          <h2>{t("rationale.exceptions_title")}</h2>
          <p>{t("rationale.exceptions_text1")}</p>
          <p>{t("rationale.exceptions_text2")}</p>
        </div>

        <div className="about-card">
          <h2>{t("rationale.reality_title")}</h2>
          <p>
            <Trans
              i18nKey="rationale.reality_text1"
              components={{ 1: <ExtLink href={COMPTROLLER_URL} /> }}
            />
          </p>
          <ul>
            <li>{t("rationale.reality_item1")}</li>
            <li>{t("rationale.reality_item2")}</li>
            <li>{t("rationale.reality_item3")}</li>
            <li>{t("rationale.reality_item4")}</li>
            <li>{t("rationale.reality_item5")}</li>
          </ul>
          <p>{t("rationale.reality_text2")}</p>
        </div>

        <div className="about-card">
          <h2>{t("rationale.fix_title")}</h2>
          <p>{t("rationale.fix_text1")}</p>
          <ul className="family-list">
            <li>
              <strong>
                <Link to="/">{t("rationale.fix_over_name")}</Link>
              </strong>
              {": "}
              {t("rationale.fix_over_desc")}
            </li>
            <li>
              <strong>
                <ExtLink href={JESTER_URL}>{t("rationale.fix_jester_name")}</ExtLink>
              </strong>
              {": "}
              {t("rationale.fix_jester_desc")}
            </li>
          </ul>
          <p>{t("rationale.fix_text2")}</p>
        </div>

        {decisions.length > 0 && (
          <div className="about-card">
            <h2>{t("decision.rationale_link_title", "ניתוח מלא של החלטה 1933")}</h2>
            <p>{t("decision.rationale_link_text")}</p>
            <ul className="family-list">
              {decisions.map((d) => (
                <li key={d.key}>
                  <strong>
                    <Link to={`/rationale/${d.key}`}>{d.title}</Link>
                  </strong>
                  {d.subtitle ? `: ${d.subtitle}` : ""}
                </li>
              ))}
            </ul>
          </div>
        )}

        <div className="about-card">
          <h2>{t("rationale.sources_title")}</h2>
          <ul>
            <li>
              <ExtLink href={DECISION_URL}>{t("rationale.source_decision")}</ExtLink>
            </li>
            <li>
              <ExtLink href={REPORT_URL}>{t("rationale.source_report")}</ExtLink>
            </li>
            <li>
              <ExtLink href={COMPTROLLER_URL}>{t("rationale.source_comptroller")}</ExtLink>
            </li>
            <li>
              <ExtLink href={CALCALIST_URL}>{t("rationale.source_calcalist")}</ExtLink>
            </li>
            <li>
              <ExtLink href={JESTER_URL}>{t("rationale.source_jester")}</ExtLink>
            </li>
            <li>
              <Link to="/about">{t("rationale.source_about")}</Link>
            </li>
          </ul>
        </div>
      </div>
    </div>
  );
}
