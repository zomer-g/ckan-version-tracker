import { Trans, useTranslation } from "react-i18next";

// Small helper for external links so the Trans <1> placeholder stays terse.
function ExtLink({ href, children }: { href: string; children?: React.ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
    </a>
  );
}

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
              {" — "}
              {t("about.family_odata_desc")}
            </li>
            <li>
              <strong>
                <ExtLink href="https://www.ocoi.org.il">
                  {t("about.family_ocoi_name")}
                </ExtLink>
              </strong>
              {" — "}
              {t("about.family_ocoi_desc")}
            </li>
            <li>
              <strong>
                <ExtLink href="https://ocal.org.il">
                  {t("about.family_ocal_name")}
                </ExtLink>
              </strong>
              {" — "}
              {t("about.family_ocal_desc")}
            </li>
          </ul>
        </div>

        <div className="about-card">
          <h2>{t("about.contact_title")}</h2>
          <p>
            <Trans
              i18nKey="about.contact_text"
              components={{
                1: <a href="mailto:zomer@octopus.org.il" />,
              }}
            />
          </p>
        </div>
      </div>
    </div>
  );
}
