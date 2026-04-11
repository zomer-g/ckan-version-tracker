import { useTranslation } from "react-i18next";

export default function Footer() {
  const { t } = useTranslation();

  return (
    <footer className="footer">
      <div className="container">
        <div className="footer-inner">
          <div>
            <div className="footer-brand">{t("app_name")}</div>
            <div className="footer-desc">{t("footer.description")}</div>
          </div>
          <div className="footer-links">
            <a href="/about">{t("nav.about")}</a>
            <a
              href="https://github.com/zomer-g/ckan-versions"
              target="_blank"
              rel="noopener noreferrer"
            >
              {t("footer.source_code")}
            </a>
            <a
              href="https://www.odata.org.il"
              target="_blank"
              rel="noopener noreferrer"
            >
              {t("footer.info_for_people")}
            </a>
          </div>
          <div className="footer-copy">
            <div>{t("footer.copyright")}</div>
            <div>over.org.il</div>
          </div>
        </div>
      </div>
    </footer>
  );
}
