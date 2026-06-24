import { useState } from "react";
import { useTranslation } from "react-i18next";

/**
 * /api page — public API documentation + MCP card.
 *
 * Visual contract: matches the "API ציבורי" pages in Ocoi and Ocal so the
 * three לעם sites read as siblings. Key shared elements:
 *   • Hero: vertical gradient primary-800 → primary-700, white title,
 *     primary-100 subtitle.
 *   • MCP card: amber-tinted container, "ביתא"/"בפיתוח" pill badge,
 *     two white sub-cards, dark URL chip with copy button.
 *   • Base-URL card: primary-50 tint with primary-200 border.
 *   • Endpoint cards: bg-white, gray-200 border, GET pill badge.
 */

interface ApiParam {
  name: string;
  desc: string;
}

interface ApiEndpoint {
  path: string;
  description: string;
  params?: ApiParam[];
  example: string;
}

const ENDPOINTS: ApiEndpoint[] = [
  {
    path: "/api/v1/datasets",
    description:
      "רשימת כל המאגרים שבמעקב — סינון לפי ארגון, תגית או סטטוס, עם עימוד.",
    params: [
      { name: "organization_id", desc: "UUID של ארגון" },
      { name: "tag / tag_id", desc: "שם תגית או UUID — ניתן לחזור (AND)" },
      { name: "status", desc: "active | pending | all (ברירת מחדל: active)" },
      { name: "limit / offset", desc: "עימוד (limit 1-500, ברירת מחדל 100)" },
    ],
    example: "/api/v1/datasets?status=active&limit=10",
  },
  {
    path: "/api/v1/datasets/{id}",
    description: "פרטי מאגר בודד לפי UUID — כולל מקור, תגיות, וקישור ל-ODATA.",
    example: "/api/v1/datasets/00000000-0000-0000-0000-000000000000",
  },
  {
    path: "/api/v1/datasets/{id}/versions",
    description:
      "היסטוריית הגרסאות של מאגר (מהחדשה לישנה) — כל גרסה עם מספרה, תאריך זיהוי, סיכום שינויים ורשימת קבצים, כשלכל קובץ יש download_url ישיר.",
    example:
      "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions",
  },
  {
    path: "/api/v1/datasets/{id}/versions/latest",
    description:
      "הגרסה העדכנית ביותר של מאגר (מספר הגרסה הגבוה ביותר) — אותו מבנה כמו גרסה ברשימה, כולל download_url לכל קובץ. מחזיר 404 אם אין עדיין גרסאות.",
    example:
      "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions/latest",
  },
  {
    path: "/api/v1/datasets/{id}/versions/{number}",
    description:
      "גרסה ספציפית לפי מספרה (1-based, כפי שמופיע ברשימת הגרסאות). מחזיר 404 אם הגרסה לא קיימת.",
    example:
      "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions/1",
  },
  {
    path: "/api/v1/tags",
    description: "כל התגיות במערכת כולל מספר המאגרים תחת כל תגית.",
    example: "/api/v1/tags",
  },
  {
    path: "/api/v1/tags/{id}",
    description: "פרטי תגית בודדת + רשימת כל המאגרים תחתיה.",
    example: "/api/v1/tags/00000000-0000-0000-0000-000000000000",
  },
  {
    path: "/api/v1/organizations",
    description: "כל הארגונים הציבוריים כולל מספר המאגרים שמתחזק כל ארגון.",
    example: "/api/v1/organizations",
  },
  {
    path: "/api/v1/organizations/{id}",
    description: "פרטי ארגון בודד לפי UUID.",
    example:
      "/api/v1/organizations/00000000-0000-0000-0000-000000000000",
  },
];

function CopyUrlButton({ value }: { value: string }) {
  const [copied, setCopied] = useState(false);

  const onCopy = async () => {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      /* ignore — older browsers without clipboard API */
    }
  };

  return (
    <button
      type="button"
      onClick={onCopy}
      className="api-copy-btn"
      aria-label="העתק כתובת"
    >
      {copied ? "✓ הועתק" : "העתק"}
    </button>
  );
}

function McpCard() {
  const mcpUrl =
    typeof window !== "undefined"
      ? `${window.location.origin}/mcp`
      : "https://www.over.org.il/mcp";

  return (
    <section
      className="api-mcp-card"
      aria-labelledby="api-mcp-title"
    >
      <div className="api-mcp-header">
        <h2 id="api-mcp-title" className="api-mcp-title">
          MCP — חיבור ישיר ל-Claude / ChatGPT / Cursor / סוכני AI
        </h2>
        <span className="api-mcp-badge">בפיתוח</span>
      </div>

      <p className="api-mcp-lead">
        גישה מובנית לדאטה דרך Model Context Protocol — ה-LLM יכול לחפש
        מאגרים, להציג גרסאות ולהוריד נתונים מתוך השיחה, בלי לעבור דרך
        ה-API הציבורי. שרת ה-MCP של "גרסאות לעם" עדיין בפיתוח; אם
        אתם רוצים להיות בין המתחברים הראשונים — כתבו לנו במייל ונעדכן
        ברגע שזה זמין.
      </p>

      <div className="api-mcp-grid">
        <div className="api-mcp-subcard">
          <h3 className="api-mcp-subtitle">איך להירשם להשקה?</h3>
          <p>
            שלחו אימייל ל-
            <a href="mailto:guy@z-g.co.il">guy@z-g.co.il</a> עם כתובת
            ה-Google שאיתה תרצו להתחבר, ושורה-שתיים על השימוש המתוכנן
            (מחקר, עיתונאות, כלי משלכם וכו'). נעדכן ברגע שהשרת זמין.
          </p>
        </div>

        <div className="api-mcp-subcard">
          <h3 className="api-mcp-subtitle">איך מתחברים מ-Claude (בעתיד)</h3>
          <ol className="api-mcp-steps">
            <li>
              ב-Claude (אפליקציית הדסקטופ או claude.ai) פתחו{" "}
              <strong>Settings → Connectors</strong>.
            </li>
            <li>
              לחצו על <strong>Add custom connector</strong>.
            </li>
            <li>
              ב-<em>Name</em> כתבו שם שתזהו (למשל "גרסאות לעם").
              ב-<em>Server URL</em> הדביקו את הכתובת:
              <div className="api-mcp-url-row" dir="ltr">
                <code className="api-mcp-url">{mcpUrl}</code>
                <CopyUrlButton value={mcpUrl} />
              </div>
            </li>
            <li>
              לחצו <strong>Connect</strong> — ייפתח חלון Google. התחברו
              <strong> עם אותה כתובת מייל</strong> שעליה ביקשתם הזמנה.
            </li>
            <li>
              ב-connector יסומן Connected, ובסרגל הכלים של השיחה
              יופיעו פעולות חדשות (חיפוש מאגרים, גרסאות וכד').
            </li>
          </ol>
        </div>
      </div>
    </section>
  );
}

function EndpointCard({ ep }: { ep: ApiEndpoint }) {
  const baseUrl =
    typeof window !== "undefined" ? window.location.origin : "";

  return (
    <div className="api-endpoint">
      <div className="api-endpoint-head">
        <span className="api-method-badge">GET</span>
        <code className="api-endpoint-path" dir="ltr">
          {ep.path}
        </code>
      </div>
      <p className="api-endpoint-desc">{ep.description}</p>

      {ep.params && ep.params.length > 0 && (
        <ul className="api-param-list">
          {ep.params.map((p) => (
            <li key={p.name}>
              <code className="api-param-name">{p.name}</code>
              <span> — {p.desc}</span>
            </li>
          ))}
        </ul>
      )}

      <div className="api-endpoint-example">
        <span className="api-endpoint-example-label">דוגמה:</span>
        <a
          href={`${baseUrl}${ep.example}`}
          target="_blank"
          rel="noopener noreferrer"
          className="api-endpoint-example-link"
          dir="ltr"
        >
          {ep.example}
        </a>
      </div>
    </div>
  );
}

export default function ApiPage() {
  const { t } = useTranslation();
  const baseUrl =
    typeof window !== "undefined" ? `${window.location.origin}/api/v1` : "/api/v1";

  return (
    <>
      <section className="hero" style={{ textAlign: "center" }}>
        <div className="container">
          <h1 style={{ fontSize: "2rem", fontWeight: 700, marginBottom: "0.4rem" }}>
            {t("api.title", "API ציבורי")}
          </h1>
          <p style={{ color: "var(--primary-100)", fontSize: "0.95rem" }}>
            {t(
              "api.subtitle",
              "ממשק פתוח לקריאת נתונים — מאגרים, גרסאות, ארגונים ותגיות",
            )}
          </p>
        </div>
      </section>

      <div className="container" style={{ paddingTop: "1.5rem", paddingBottom: "3rem" }}>
        <div className="api-base-card">
          <p>
            <strong>{t("api.base_url", "כתובת בסיס:")} </strong>
            <code className="api-base-url" dir="ltr">{baseUrl}</code>
          </p>
          <p>
            {t(
              "api.intro",
              "כל ה-endpoints הם GET ציבוריים. אין צורך באימות, אין מפתח API.",
            )}
          </p>
          <p>
            {t(
              "api.intro_2",
              "השימוש פתוח לחוקרים, עיתונאים, פעילי שקיפות ופרויקטים אזרחיים. אם אתם בונים אינטגרציה רחבה — נא להימנע ממיליוני קריאות מקבילות ולשמור מטמון מקומי.",
            )}
          </p>
          <p>
            {t(
              "api.intro_versions",
              "כל גרסה מחזירה רשימת קבצים (resources); לכל קובץ יש download_url ישיר ושדה storage (odata או r2) שמציין היכן הקובץ מאוחסן. תיעוד אינטראקטיבי מלא (OpenAPI) זמין ב-",
            )}
            <a href="/docs" target="_blank" rel="noopener noreferrer" dir="ltr">/docs</a>
            {" · "}
            <a href="/redoc" target="_blank" rel="noopener noreferrer" dir="ltr">/redoc</a>
            .
          </p>
        </div>

        <McpCard />

        <h2 className="api-endpoints-heading">{t("api.endpoints", "נקודות קצה")}</h2>
        <div className="api-endpoint-list">
          {ENDPOINTS.map((ep) => (
            <EndpointCard key={ep.path} ep={ep} />
          ))}
        </div>
      </div>
    </>
  );
}
