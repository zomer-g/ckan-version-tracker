import { useTranslation } from "react-i18next";

export default function AboutPage() {
  const { t } = useTranslation();

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
          <p>{t("about.what_text")}</p>
        </div>

        <div className="about-card">
          <h2>{t("about.why_title")}</h2>
          <p>{t("about.why_text")}</p>
        </div>

        <div className="about-card">
          <h2>{t("about.how_title")}</h2>
          <p>{t("about.how_text")}</p>
        </div>

        <div className="about-card">
          <h2>{t("about.projects_title")}</h2>
          <a
            href="https://www.odata.org.il"
            target="_blank"
            rel="noopener noreferrer"
            className="project-link"
          >
            {t("about.project_odata")}
          </a>
          <a
            href="https://www.ocoi.org.il"
            target="_blank"
            rel="noopener noreferrer"
            className="project-link"
          >
            {t("about.project_ocoi")}
          </a>
        </div>
      </div>
    </div>
  );
}
