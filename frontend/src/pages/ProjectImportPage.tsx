import { Trans, useTranslation } from "react-i18next";
import OdataSearch from "../components/OdataSearch";

/**
 * Landing page for a sibling "לעם" project. Three of these live under
 * /projects/*:
 *   odata → מידע לעם   (odata.org.il)
 *   ocal  → יומן לעם   (ocal.org.il)
 *   ocoi  → ניגוד עניינים לעם (ocoi.org.il)
 *
 * Every one surfaces PROCESSED / derived data, not original government
 * sources, hence the loud banner up top. odata now hosts a live CKAN-API
 * search over מידע לעם; ocal/ocoi are still just a link out to the live
 * site until their interfaces are imported.
 */
type ProjectKey = "odata" | "ocal" | "ocoi";

const PROJECTS: Record<ProjectKey, { url: string }> = {
  odata: { url: "https://www.odata.org.il/" },
  ocal: { url: "https://ocal.org.il/" },
  ocoi: { url: "https://www.ocoi.org.il/" },
};

export default function ProjectImportPage({ project }: { project: ProjectKey }) {
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
            </a>
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
            </a>
          </div>
        )}
      </div>
    </div>
  );
}
