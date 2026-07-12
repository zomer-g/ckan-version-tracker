import { useState } from "react";
import { useTranslation } from "react-i18next";

/**
 * /api page — public API documentation + MCP cards.
 *
 * Covers all three data sources OVER serves:
 *   • OVER   — tracked government datasets   (/api/v1, /api/append)
 *   • CBS    — הלמ״ס content index           (/api/cbs)
 *   • Knesset— committee protocols + ODATA   (/api/knesset-db, /api/knesset-protocols)
 * Each source also has a dedicated MCP server (see McpCard).
 *
 * Visual contract: matches the "API ציבורי" pages in Ocoi and Ocal so the
 * לעם sites read as siblings (amber MCP card, primary base-URL card, GET
 * endpoint cards).
 */

interface ApiParam {
  name: string;
  desc: string;
}

interface ApiEndpoint {
  path: string;
  method?: "GET" | "POST";
  description: string;
  params?: ApiParam[];
  example: string;
}

interface ApiGroup {
  id: string;
  title: string;
  note?: string;
  endpoints: ApiEndpoint[];
}

const ENDPOINT_GROUPS: ApiGroup[] = [
  {
    id: "over",
    title: "OVER — מאגרי מידע ממשלתיים",
    note: "מעקב גרסאות אחרי מאגרי data.gov.il ומקורות ממשלתיים נוספים. כתובת בסיס: /api/v1 (מטא-דאטה) ו-/api/append (תוכן השורות).",
    endpoints: [
      {
        path: "/api/v1/datasets",
        description:
          "רשימת כל המאגרים שבמעקב — כל פריט כולל id (ה-UUID של OVER לשאר הקריאות), וגם ckan_id + ckan_name (מזהה ה-dataset וה-slug ב-data.gov.il). סינון לפי ארגון, תגית, סטטוס או מקור, עם עימוד.",
        params: [
          { name: "organization_id", desc: "UUID של ארגון" },
          { name: "tag / tag_id", desc: "שם תגית או UUID — ניתן לחזור (AND)" },
          { name: "status", desc: "active | pending | all (ברירת מחדל: active)" },
          { name: "ckan_id", desc: "גישור מ-data.gov.il: מחזיר את מאגר ה-OVER שעוקב אחרי dataset נתון. מתאים ל-ckan_id (ה-UUID שב-data.gov.il/dataset/<id>) או ל-ckan_name (slug)." },
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
        example: "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions",
      },
      {
        path: "/api/v1/datasets/{id}/versions/latest",
        description:
          "הגרסה העדכנית ביותר של מאגר (מספר הגרסה הגבוה ביותר) — אותו מבנה כמו גרסה ברשימה, כולל download_url לכל קובץ. מחזיר 404 אם אין עדיין גרסאות.",
        example: "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions/latest",
      },
      {
        path: "/api/v1/datasets/{id}/versions/{number}",
        description:
          "גרסה ספציפית לפי מספרה (1-based, כפי שמופיע ברשימת הגרסאות). מחזיר 404 אם הגרסה לא קיימת.",
        example: "/api/v1/datasets/00000000-0000-0000-0000-000000000000/versions/1",
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
        example: "/api/v1/organizations/00000000-0000-0000-0000-000000000000",
      },
      {
        path: "/api/append/{id}/datastore_search",
        description:
          "תשאול תוכן מאגר (השורות עצמן, לא רק הקבצים) — בהשראת datastore_search של CKAN. זמין למאגרים שתוכנם נשמר ב-NEON (append). מחזיר עטיפת CKAN: {success, result:{fields:[{id,type}], records, total, _links}}.",
        params: [
          { name: "filters", desc: "אובייקט JSON של עמודה→ערך (התאמה מדויקת; ערך יכול להיות רשימה ל-IN)" },
          { name: "q", desc: "חיפוש מחרוזת בכל העמודות" },
          { name: "fields", desc: "רשימת עמודות מופרדת בפסיקים (projection)" },
          { name: "sort", desc: '"עמודה" או "עמודה desc, עמודה2 asc"' },
          { name: "limit / offset", desc: "עימוד (limit עד 500)" },
          { name: "distinct / include_total", desc: "boolean" },
        ],
        example:
          '/api/append/e437ab0b-c247-4d35-b2c4-79c2d19dbabd/datastore_search?limit=5&filters={"tozeret_nm":"קיה קוריאה"}',
      },
      {
        path: "/api/append/{id}/datastore_search_sql",
        description:
          "שאילתת SQL גולמית (SELECT/WITH בלבד) על תוכן המאגר — בהשראת datastore_search_sql של CKAN. רצה בטרנזקציית READ ONLY עם הגבלת זמן ושורות. שם הטבלה זמין ב-/schema.",
        params: [{ name: "sql", desc: "משפט SELECT/WITH יחיד" }],
        example:
          "/api/append/e437ab0b-c247-4d35-b2c4-79c2d19dbabd/datastore_search_sql?sql=SELECT tozeret_nm, count(*) FROM append_private_and_commercial_vehicles_e437ab0b GROUP BY 1 ORDER BY 2 DESC LIMIT 10",
      },
      {
        path: "/api/append/{id}/schema",
        description:
          "סכמת תוכן המאגר ב-NEON: שם הטבלה, מספר השורות, רשימת העמודות, ועמודת first_seen (זמן הוספת כל שורה).",
        example: "/api/append/e437ab0b-c247-4d35-b2c4-79c2d19dbabd/schema",
      },
    ],
  },
  {
    id: "cbs",
    title: 'למ״ס (CBS) — אינדקס פרסומי הלשכה המרכזית לסטטיסטיקה',
    note: "אינדקס תוכן (HEAD-only) של פרסומי cbs.gov.il: כותרות, נושאים, סוגי קבצים ושנות נתונים — לא בתי הקבצים עצמם. כתובת בסיס: /api/cbs.",
    endpoints: [
      {
        path: "/api/cbs/search",
        description: "חיפוש טקסט חופשי + פאסטים על אינדקס הלמ״ס.",
        params: [
          { name: "q", desc: "טקסט חופשי (עברית/אנגלית)" },
          { name: "subject / geo / file_type / section / item_type / lang", desc: "סינון פר-פאסט" },
          { name: "year_from / year_to", desc: "טווח שנת נתונים" },
          { name: "sort", desc: "relevance (ברירת מחדל) | chrono" },
          { name: "limit / offset", desc: "עימוד (limit 1-100, ברירת מחדל 30)" },
        ],
        example: "/api/cbs/search?q=אוכלוסייה&sort=chrono&limit=5",
      },
      {
        path: "/api/cbs/facets",
        description: "ספירת הפאסטים הזמינים (נושאים, אזורים גאוגרפיים, סוגי קבצים, מדורים ועוד) לבניית מסנני חיפוש.",
        example: "/api/cbs/facets",
      },
      {
        path: "/api/cbs/stats",
        description: "היקף האינדקס וטריות: מספר הפריטים ומתי סונכרן לאחרונה.",
        example: "/api/cbs/stats",
      },
      {
        path: "/api/cbs/featured",
        description: "פריטים נבחרים (מובלטים ידנית) מתוך האינדקס.",
        example: "/api/cbs/featured",
      },
    ],
  },
  {
    id: "knesset",
    title: "כנסת — מראה ה-ODATA + פרוטוקולי ועדות",
    note: "מראה מלא של 48 טבלאות ה-ODATA של הכנסת (קונסולת SQL), קטלוג מסמכי מרכז המחקר והמידע (ממ״מ), וחיפוש/הורדה בכמות של פרוטוקולי ועדות. כתובות בסיס: /api/knesset-db ו-/api/knesset-protocols.",
    endpoints: [
      {
        path: "/api/knesset-db/tables",
        description: "קטלוג טבלאות מראה הכנסת: שם SQL, תיאור בעברית, מספר שורות ורשימת עמודות — הבסיס לכתיבת שאילתות.",
        example: "/api/knesset-db/tables",
      },
      {
        path: "/api/knesset-db/sql",
        method: "POST",
        description: "שאילתת SQL גולמית (SELECT/WITH בלבד) על מראה הכנסת, READ ONLY עם הגבלת זמן ושורות. גוף הבקשה: JSON עם {\"sql\": \"...\"}.",
        params: [{ name: "sql (body)", desc: "משפט SELECT/WITH יחיד" }],
        example: "/api/knesset-db/tables",
      },
      {
        path: "/api/knesset-db/export.csv",
        description: "הרצת שאילתת SQL והזרמת התוצאה כקובץ CSV להורדה (UTF-8 עם BOM).",
        params: [{ name: "sql", desc: "משפט SELECT/WITH יחיד" }],
        example: "/api/knesset-db/export.csv?sql=SELECT * FROM knesset_committee LIMIT 100",
      },
      {
        path: "/api/knesset-db/mmm/search",
        description: "חיפוש בקטלוג מסמכי מרכז המחקר והמידע (ממ״מ) — מטא-דאטה של מחקרי הכנסת.",
        params: [
          { name: "q", desc: "טקסט חופשי בכותרת/תקציר" },
          { name: "author / doc_type", desc: "סינון לפי מחבר או סוג מסמך" },
          { name: "year_from / year_to", desc: "טווח שנים" },
          { name: "limit / offset", desc: "עימוד (ברירת מחדל 20)" },
        ],
        example: "/api/knesset-db/mmm/search?q=דיור&limit=5",
      },
      {
        path: "/api/knesset-protocols/search",
        description: "חיפוש פרוטוקולי ועדות (מסמך אחד לכל פרוטוקול) לפי טקסט חופשי, מספר כנסת ושם ועדה — כל תוצאה מקשרת לקובץ ב-fs.knesset.gov.il.",
        params: [
          { name: "q", desc: "טקסט חופשי במסמך/ועדה/ישיבה" },
          { name: "knesset", desc: "מספר כנסת (למשל 25)" },
          { name: "committee", desc: "שם ועדה (ILIKE)" },
          { name: "limit / offset", desc: "עימוד (limit 1-200, ברירת מחדל 50)" },
        ],
        example: "/api/knesset-protocols/search?q=תקציב&knesset=25&limit=5",
      },
      {
        path: "/api/knesset-db/protocols/batch.zip",
        description: "הורדה בכמות: אורז את כל קובצי הפרוטוקולים התואמים לפילטר ל-ZIP יחיד (עד תקרת קבצים; ראו /protocols/count).",
        params: [
          { name: "knesset_num / committee_id / q", desc: "אותם מסננים כמו החיפוש" },
        ],
        example: "/api/knesset-db/protocols/count?knesset_num=25",
      },
    ],
  },
];

const MCP_SERVERS: {
  key: string;
  label: string;
  path: string;
  purpose: string;
  tools: string[];
}[] = [
  {
    key: "over",
    label: "OVER — מאגרי מידע ממשלתיים",
    path: "/mcp",
    purpose: "חיפוש מאגרים שבמעקב, שליפת גרסאות וקבצים, תגיות וארגונים, ותשאול תוכן השורות (NEON).",
    tools: ["search_datasets", "get_dataset", "query_dataset_rows", "list_tags", "list_organizations", "get_stats"],
  },
  {
    key: "cbs",
    label: "למ״ס (CBS)",
    path: "/cbs/mcp",
    purpose: "חיפוש באינדקס פרסומי הלמ״ס, שליפת עמוד, פאסטים ופריטים נבחרים.",
    tools: ["search", "get_page", "facets", "list_featured", "get_stats"],
  },
  {
    key: "knesset",
    label: "כנסת — פרוטוקולי ועדות + ODATA",
    path: "/knesset/mcp",
    purpose: "חיפוש ועדות/ישיבות/פרוטוקולים, מסמכי ממ״מ, שליפת ישיבה, ו-SQL חופשי על מראה ה-ODATA של הכנסת.",
    tools: ["search_committees", "search_sessions", "search_protocols", "get_session", "search_mmm", "run_sql", "list_tables", "get_stats"],
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
    <button type="button" onClick={onCopy} className="api-copy-btn" aria-label="העתק כתובת">
      {copied ? "✓ הועתק" : "העתק"}
    </button>
  );
}

function McpCard() {
  const origin =
    typeof window !== "undefined" ? window.location.origin : "https://www.over.org.il";

  return (
    <section className="api-mcp-card" aria-labelledby="api-mcp-title">
      <div className="api-mcp-header">
        <h2 id="api-mcp-title" className="api-mcp-title">
          MCP — חיבור ישיר ל-Claude / ChatGPT / Cursor / סוכני AI
        </h2>
        <span className="api-mcp-badge">ביתא</span>
      </div>

      <p className="api-mcp-lead">
        גישה מובנית לדאטה דרך Model Context Protocol — ה-LLM מחפש ומושך נתונים
        מתוך השיחה, בלי לעבור דרך ה-API הציבורי. שלושה שרתי MCP <strong>חיים</strong>,
        אחד לכל מקור. הגישה בהזמנה (Google + רשימת מוזמנים) — לקבלת גישה שלחו
        אימייל ל-<a href="mailto:guy@z-g.co.il">guy@z-g.co.il</a> עם כתובת ה-Google
        שאיתה תתחברו ושורה על השימוש המתוכנן.
      </p>

      <div className="api-mcp-grid">
        {MCP_SERVERS.map((s) => {
          const url = `${origin}${s.path}`;
          return (
            <div className="api-mcp-subcard" key={s.key}>
              <h3 className="api-mcp-subtitle">{s.label}</h3>
              <p>{s.purpose}</p>
              <div className="api-mcp-url-row" dir="ltr">
                <code className="api-mcp-url">{url}</code>
                <CopyUrlButton value={url} />
              </div>
              <p style={{ marginTop: "0.5rem", fontSize: "0.8rem", opacity: 0.85 }} dir="ltr">
                {s.tools.join(" · ")}
              </p>
            </div>
          );
        })}
      </div>

      <div className="api-mcp-subcard" style={{ marginTop: "1rem" }}>
        <h3 className="api-mcp-subtitle">איך מתחברים מ-Claude</h3>
        <ol className="api-mcp-steps">
          <li>
            ב-Claude (דסקטופ או claude.ai) פתחו <strong>Settings → Connectors</strong> ולחצו{" "}
            <strong>Add custom connector</strong>.
          </li>
          <li>
            ב-<em>Name</em> כתבו שם שתזהו (למשל "כנסת — גרסאות לעם"), וב-<em>Server URL</em>{" "}
            הדביקו את כתובת השרת הרצוי מלמעלה (למשל <code dir="ltr">{origin}/knesset/mcp</code>).
          </li>
          <li>
            לחצו <strong>Connect</strong> — ייפתח חלון Google. התחברו{" "}
            <strong>עם אותה כתובת מייל</strong> שעליה ביקשתם הזמנה.
          </li>
          <li>
            ה-connector יסומן Connected, ובסרגל הכלים של השיחה יופיעו הפעולות של
            אותו שרת. אפשר לחבר את שלושת השרתים במקביל.
          </li>
        </ol>
      </div>
    </section>
  );
}

function EndpointCard({ ep }: { ep: ApiEndpoint }) {
  const baseUrl = typeof window !== "undefined" ? window.location.origin : "";
  const method = ep.method ?? "GET";

  return (
    <div className="api-endpoint">
      <div className="api-endpoint-head">
        <span className="api-method-badge">{method}</span>
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
        <span className="api-endpoint-example-label">
          {method === "POST" ? "נתיב (POST — עם גוף JSON):" : "דוגמה:"}
        </span>
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
              "ממשק פתוח לשלושה מקורות — מאגרי ממשלה (OVER), אינדקס הלמ״ס, ופרוטוקולי הכנסת — ב-REST וב-MCP",
            )}
          </p>
        </div>
      </section>

      <div className="container" style={{ paddingTop: "1.5rem", paddingBottom: "3rem" }}>
        <div className="api-base-card">
          <p>
            {t(
              "api.intro",
              "כל ה-endpoints הם ציבוריים (רובם GET) — אין צורך באימות, אין מפתח API. לכל מקור קידומת כתובת משלו: OVER תחת /api/v1 ו-/api/append, הלמ״ס תחת /api/cbs, והכנסת תחת /api/knesset-db ו-/api/knesset-protocols.",
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
              "תיעוד אינטראקטיבי מלא (OpenAPI) לכל נקודות הקצה של כל המקורות זמין ב-",
            )}
            <a href="/docs" target="_blank" rel="noopener noreferrer" dir="ltr">/docs</a>
            {" · "}
            <a href="/redoc" target="_blank" rel="noopener noreferrer" dir="ltr">/redoc</a>
            .
          </p>
        </div>

        <McpCard />

        <h2 className="api-endpoints-heading">{t("api.endpoints", "נקודות קצה")}</h2>
        {ENDPOINT_GROUPS.map((group) => (
          <section key={group.id} className="api-endpoint-group" aria-label={group.title}>
            <h3 className="api-endpoint-group-title">{group.title}</h3>
            {group.note && <p className="api-endpoint-group-note">{group.note}</p>}
            <div className="api-endpoint-list">
              {group.endpoints.map((ep) => (
                <EndpointCard key={`${ep.method ?? "GET"} ${ep.path}`} ep={ep} />
              ))}
            </div>
          </section>
        ))}
      </div>
    </>
  );
}
