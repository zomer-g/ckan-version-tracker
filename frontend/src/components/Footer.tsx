import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";

/**
 * Footer — mirrors the two-level header: a brand blurb plus three grouped
 * link columns (Projects / Catalog / About) and a copyright line.
 */
export default function Footer() {
  const { t } = useTranslation();

  const groups: {
    title: string;
    links: { label: string; to?: string; href?: string }[];
  }[] = [
    {
      title: t("footer.group_projects", "פרויקטים"),
      links: [
        { label: t("nav.questions", "שאלות לעם"), to: "/projects/questions" },
        { label: t("nav.knesset_db", "הכנסת"), to: "/knesset" },
        { label: t("nav.cbs", 'למ"ס'), to: "/cbs" },
        { label: t("nav.odata", "מידע לעם"), to: "/projects/odata" },
        { label: t("nav.ocal", "יומן לעם"), to: "/projects/ocal" },
        { label: t("nav.ocoi", "ניגוד עניינים לעם"), to: "/projects/ocoi" },
      ],
    },
    {
      title: t("footer.group_catalog", "קטלוג"),
      links: [
        { label: t("nav.search"), to: "/" },
        { label: t("nav.data_sql", "SQL"), to: "/data" },
        { label: t("nav.organizations", "ארגונים"), to: "/organizations" },
        { label: t("nav.tags", "תגיות"), to: "/tags" },
        { label: t("nav.sources", "מקורות"), to: "/sources" },
      ],
    },
    {
      title: t("footer.group_about", "אודות"),
      links: [
        { label: t("nav.about"), to: "/about" },
        { label: t("nav.rationale", "הרציונל"), to: "/rationale" },
        { label: t("nav.api", "API"), to: "/api" },
        {
          label: t("footer.source_code"),
          href: "https://github.com/zomer-g/ckan-version-tracker",
        },
      ],
    },
  ];

  return (
    <footer className="footer" role="contentinfo">
      <div className="container">
        <div className="footer-inner">
          <div className="footer-brand-col">
            <div className="footer-brand">{t("app_name")}</div>
            <div className="footer-desc">{t("footer.description")}</div>
          </div>

          <div className="footer-groups">
            {groups.map((group) => (
              <nav key={group.title} className="footer-group" aria-label={group.title}>
                <div className="footer-group-title">{group.title}</div>
                {group.links.map((link) =>
                  link.to ? (
                    <Link key={link.label} to={link.to}>
                      {link.label}
                    </Link>
                  ) : (
                    <a
                      key={link.label}
                      href={link.href}
                      target="_blank"
                      rel="noopener noreferrer"
                    >
                      {link.label}
                    </a>
                  )
                )}
              </nav>
            ))}
          </div>
        </div>

        <div className="footer-copy">
          <span>{t("footer.copyright")}</span>
          <span>over.org.il</span>
        </div>
      </div>
    </footer>
  );
}
