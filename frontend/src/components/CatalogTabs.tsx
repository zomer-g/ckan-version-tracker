import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";

/**
 * Tab strip for the catalog area: the search home page and the three ways the
 * same catalog is sliced (organizations / tags / sources). Same shape as
 * DataTabs — each tab is its own route, so it stays linkable — and it renders
 * on all four pages so moving between them is one click instead of a trip
 * through the navbar dropdown.
 */
export type CatalogTab = "search" | "organizations" | "tags" | "sources";

export default function CatalogTabs({ active }: { active: CatalogTab }) {
  const { t } = useTranslation();

  const tab = (to: string, label: string, key: CatalogTab) => {
    const on = active === key;
    return (
      <Link
        key={key}
        to={to}
        style={{
          padding: "0.5rem 1rem",
          textDecoration: "none",
          fontWeight: 600,
          fontSize: "0.9rem",
          borderRadius: "6px 6px 0 0",
          whiteSpace: "nowrap",
          color: on ? "var(--primary, #0f766e)" : "var(--text-muted, #64748b)",
          background: on ? "var(--bg-muted, #eef2f5)" : "transparent",
          borderBottom: on
            ? "2px solid var(--primary, #0f766e)"
            : "2px solid transparent",
        }}
        aria-current={on ? "page" : undefined}
      >
        {label}
      </Link>
    );
  };

  return (
    <nav
      aria-label={t("nav.catalog", "קטלוג")}
      style={{
        display: "flex",
        gap: 4,
        borderBottom: "1px solid var(--border, #e5e7eb)",
        marginBottom: "1rem",
        flexWrap: "wrap",
      }}
    >
      {tab("/", `🔍 ${t("nav.search", "חיפוש")}`, "search")}
      {tab("/organizations", `🏛 ${t("nav.organizations", "ארגונים")}`, "organizations")}
      {tab("/tags", `🏷 ${t("nav.tags", "תגיות")}`, "tags")}
      {tab("/sources", `🌐 ${t("nav.sources", "מקורות")}`, "sources")}
    </nav>
  );
}
