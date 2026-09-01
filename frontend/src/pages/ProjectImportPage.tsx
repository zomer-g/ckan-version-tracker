import { Trans, useTranslation } from "react-i18next";
import OdataSearch from "../components/OdataSearch";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
/**
 * Landing page for a sibling "לעם" project. Three of these live under
 * /projects/*:
 *   odata → מידע לעם   (odata.org.il)
 *   ocal  → יומן לעם   (ocal.org.il)
 *
 * Every one surfaces PROCESSED / derived data, not original government
 * sources, hence the loud banner up top. odata now hosts a live CKAN-API
 * search over מידע לעם.
 *
 * ocoi is gone from here: ניגוד עניינים לעם is served by OcoiPage, and
 * ocoi.org.il is being retired, so a link out to it would be a link to
 * nothing. ocal keeps its entry only as long as ocal.org.il answers.
 */
type ProjectKey = "odata" | "ocal";

const PROJECTS: Record<ProjectKey, { url: string }> = {
  odata: { url: "https://www.odata.org.il/" },
  ocal: { url: "https://ocal.org.il/" },
};

export default function ProjectImportPage({ project }: { project: ProjectKey }) {
  useDocumentTitle("מידע לעם");
  const { t } = useTranslation();
  const { url } = PROJECTS[project];

  return (
    <div>
      {/* Prominent, page-wide processed-data notice */}
      <div className="processed-banner" role="note">
        <div className="container">
          <span className="processed-banner-badge">
            {t("projects.processed_badge")}
          </span>
          <span className="processed-banner-text">
            <Trans
              i18nKey="projects.processed_note"
              components={{ strong: <strong /> }}
            />
          </span>
        </div>
      </div>

      <div className="about-hero">
        <div className="container">
          <h1>{t(`projects.${project}_title`)}</h1>
          <p className="project-hero-desc">{t(`projects.${project}_desc`)}</p>
        </div>
      </div>

      <div className={project === "odata" ? "odata-section" : "about-section"}>
        {project === "odata" ? (
          <div className="about-card odata-card">
            <OdataSearch />
            <a
              className="odata-visit-link"
              href={url}
              target="_blank"
              rel="noopener noreferrer"
            >
              {t("projects.visit_site")}
            <span className="sr-only"> (נפתח בחלון חדש)</span></a>
          </div>
        ) : (
          <div className="about-card project-soon-card">
            <a
              className="btn-primary project-soon-link"
              href={url}
              target="_blank"
              rel="noopener noreferrer"
            >
              {t("projects.visit_site")}
            <span className="sr-only"> (נפתח בחלון חדש)</span></a>
          </div>
        )}
      </div>
    </div>
  );
}
