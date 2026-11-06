import { Link } from "react-router-dom";
import { usePublishedDecisions } from "../hooks/usePublishedDecisions";
import { useDocumentTitle } from "../hooks/useDocumentTitle";
import {
  BUDGET_DATASET,
  Finding1933Status,
  ODATA_SEARCH_URL,
  REQUEST_1933,
  RESPONSES_1933,
} from "../data/project1933";

/**
 * פרויקט 1933 — the hub for government decision 1933 (2016).
 *
 * Three things in one place: the two pages that used to sit under "About"
 * (the rationale, and the clause-by-clause analysis of the decision), and what
 * מידע לעם holds about how the decision was carried out — chiefly the
 * ministries' answers to a cross-government FOI request asking what each had
 * done to implement it. The answers are listed from src/data/project1933.ts.
 */

const DECISION_URL = "https://www.gov.il/he/pages/2016_dec1933";

const STATUS_LABEL: Record<Finding1933Status, string> = {
  nothing: "לא נעשה דבר",
  partial: "יישום חלקי",
  acted: "יושם",
  refused: "לא נמסר מענה לגופו",
};

function ExtLink({ href, children }: { href: string; children?: React.ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
      <span className="sr-only"> (נפתח בחלון חדש)</span>
    </a>
  );
}

function formatDate(iso: string): string {
  const [y, m, d] = iso.split("-");
  return `${d}/${m}/${y}`;
}

/** How each status reads inside the summary sentence ("ב-3 ..."). */
const STATUS_PHRASE: Record<Finding1933Status, string> = {
  nothing: "נכתב שלא נעשה דבר או שלא נמצא שום תיעוד",
  partial: "תוארו צעדים חלקיים",
  acted: "תואר יישום ממשי",
  refused: "לא ניתנה תשובה לגופו של עניין",
};

/** One sentence built from the counts, so it can never drift from the table. */
function summarySentence(
  counts: Partial<Record<Finding1933Status, number>>,
  analysed: number,
): string {
  const parts = (["nothing", "partial", "acted", "refused"] as Finding1933Status[])
    .filter((s) => counts[s])
    .map((s) => `ב-${counts[s]} ${STATUS_PHRASE[s]}`);
  if (parts.length === 0) return "";
  const list =
    parts.length === 1 ? parts[0] : `${parts.slice(0, -1).join(", ")} ו${parts[parts.length - 1]}`;
  return `מתוך ${analysed} התשובות שסווגו, ${list}.`;
}

export default function Project1933Page() {
  useDocumentTitle("פרויקט 1933");
  const decisions = usePublishedDecisions();

  const analysed = RESPONSES_1933.filter((r) => r.finding);
  const counts = analysed.reduce<Partial<Record<Finding1933Status, number>>>((acc, r) => {
    const s = r.finding!.status;
    acc[s] = (acc[s] ?? 0) + 1;
    return acc;
  }, {});

  return (
    <div>
      <div className="about-hero">
        <div className="container">
          <h1>פרויקט 1933</h1>
          <p className="rationale-hero-sub">
            החלטת ממשלה 1933 משנת 2016 התחייבה לפתוח את מאגרי המידע הממשלתיים לציבור. כאן מרוכז מה
            שהיא קבעה, למה זה חשוב, ומה משרדי הממשלה עצמם השיבו כשנשאלו מה עשו כדי לקיים אותה.
          </p>
        </div>
      </div>

      <div className="about-section">
        <div className="about-card">
          <h2>העמודים בפרויקט</h2>
          <ul className="family-list">
            <li>
              <strong>
                <Link to="/rationale">הרציונל</Link>
              </strong>
              {": "}
              מה הובטח בהחלטה, מה התקן שנקבע, מה מצא מבקר המדינה, ולמה גרסאות לעם קיים.
            </li>
            {decisions.map((d) => (
              <li key={d.key}>
                <strong>
                  <Link to={`/rationale/${d.key}`}>{d.title}</Link>
                </strong>
                {d.subtitle ? `: ${d.subtitle}` : ""}
              </li>
            ))}
            <li>
              <strong>
                <ExtLink href={DECISION_URL}>נוסח ההחלטה באתר gov.il</ExtLink>
              </strong>
            </li>
          </ul>
        </div>

        <div className="about-card">
          <h2>הבקשה הרוחבית: מה עשיתם כדי ליישם את ההחלטה?</h2>
          <p>
            ב-{formatDate(REQUEST_1933.date)} שלחה עמותת הצלחה בקשת חופש מידע אחת לכל משרדי הממשלה,
            וביקשה את המסמכים שמראים מה כל משרד עשה כדי ליישם את החלטה 1933. התשובות פורסמו באתר{" "}
            <ExtLink href="https://www.odata.org.il">מידע לעם</ExtLink>, כל משרד בנפרד, ומרוכזות
            בטבלה שלמטה. זה נוסח הבקשה:
          </p>
          <blockquote className="project1933-request">
            <p>{REQUEST_1933.intro}</p>
            <ol>
              {REQUEST_1933.items.map((t) => (
                <li key={t}>{t}</li>
              ))}
            </ol>
            <footer className="text-sm text-muted">
              {REQUEST_1933.sender}. מקור:{" "}
              <ExtLink href={REQUEST_1933.source_url}>{REQUEST_1933.source_label}</ExtLink>.
            </footer>
          </blockquote>
          <p>
            נאספו {RESPONSES_1933.length} תשובות של משרדי ממשלה.
            {analysed.length > 0 && (
              <>
                {" "}
                מתוכן סווגו {analysed.length}:{" "}
                {(Object.keys(STATUS_LABEL) as Finding1933Status[])
                  .filter((s) => counts[s])
                  .map((s) => `${STATUS_LABEL[s]} — ${counts[s]}`)
                  .join(" · ")}
                .
              </>
            )}
          </p>
          {analysed.length > 0 && <p>{summarySentence(counts, analysed.length)}</p>}

          <div className="table-scroll" style={{ overflowX: "auto" }}>
            <table className="table project1933-table">
              <caption className="sr-only">תשובות משרדי הממשלה לבקשה על יישום החלטה 1933</caption>
              <thead>
                <tr>
                  <th scope="col">משרד</th>
                  <th scope="col">פורסם במידע לעם</th>
                  <th scope="col">מסמכים</th>
                  <th scope="col">מה עולה מהתשובה</th>
                </tr>
              </thead>
              <tbody>
                {RESPONSES_1933.map((r) => (
                  <tr key={r.url}>
                    <th scope="row">
                      <ExtLink href={r.url}>{r.ministry}</ExtLink>
                      {r.also?.map((u, i) => (
                        <span key={u} className="text-sm text-muted">
                          {" "}
                          (<ExtLink href={u}>{`העלאה נוספת${r.also!.length > 1 ? ` ${i + 1}` : ""}`}</ExtLink>)
                        </span>
                      ))}
                    </th>
                    <td>{formatDate(r.date)}</td>
                    <td>
                      <ul className="project1933-files">
                        {r.files.map((f, i) => (
                          <li key={`${f}-${i}`}>{f}</li>
                        ))}
                      </ul>
                    </td>
                    <td>
                      {r.finding ? (
                        <>
                          <strong>{STATUS_LABEL[r.finding.status]}</strong>
                          {r.finding.summary ? ` — ${r.finding.summary}` : ""}
                        </>
                      ) : r.findingNote ? (
                        <span className="text-muted">{r.findingNote}</span>
                      ) : (
                        <span className="text-muted">טרם סוכם — המסמכים זמינים בקישור</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="about-card">
          <h2>כמה כסף הוקצה</h2>
          <p>
            <strong>
              <ExtLink href={BUDGET_DATASET.url}>{BUDGET_DATASET.title}</ExtLink>
            </strong>
            {": "}
            {BUDGET_DATASET.summary} (פורסם במידע לעם ב-{formatDate(BUDGET_DATASET.date)}.)
          </p>
        </div>

        <div className="about-card">
          <h2>עוד במידע לעם</h2>
          <p>
            הרשימה כאן נלקחה מקטלוג מידע לעם. לחיפוש עדכני של כל מה שפורסם שם על ההחלטה:{" "}
            <ExtLink href={ODATA_SEARCH_URL}>החיפוש "1933" במידע לעם</ExtLink>, או{" "}
            <Link to="/projects/questions?q=1933">החיפוש הרוחבי בשאלות לעם</Link>, שמצליב גם עם
            פרוטוקולי הכנסת, דוחות המבקר והחלטות הממשלה.
          </p>
        </div>
      </div>
    </div>
  );
}
