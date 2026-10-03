/**
 * GovMap מול אתר הנדל״ן ורשות המיסים — the same ten parcels as the gaps report
 * (NadlanGaps), read a third time: in GovMap's deals layer (layer 16).
 *
 * The nadlan.gov.il and Tax Authority numbers are the ones collected for the
 * gaps report and were NOT collected again; only GovMap was read, on
 * 3.10.2026, from the version OVER archived that day (v3, 2,501,231 deals).
 * The note at the top says so, because the three columns are not from one day.
 */

const CELL: React.CSSProperties = { padding: "0.3rem 0.5rem", borderTop: "1px solid var(--border)" };
const NUM: React.CSSProperties = { ...CELL, textAlign: "end", whiteSpace: "nowrap" };
const HEAD: React.CSSProperties = { padding: "0.3rem 0.5rem", textAlign: "start", color: "var(--text-muted)", fontWeight: 600, verticalAlign: "bottom" };

function Ltr({ children }: { children: React.ReactNode }) {
  return <bdi dir="ltr" style={{ unicodeBidi: "isolate" }}>{children}</bdi>;
}

// parcel, town, nadlan table, nadlan history, GovMap, taxes deals,
// GovMap↔taxes matched (sub-parcel + date), amount identical
const PARCELS = [
  ["17457-63", "מגדל העמק", "20", "23", 44, 46, 43, 41],
  ["7151-316", "בת ים", "12", "5", 17, 18, 17, 16],
  ["12593-23", "נופית", "1", "0", 1, 1, 1, 1],
  ["7787-464", "תל מונד", "6", "0", 6, 5, 4, 4],
  ["10236-99", "קריית ביאליק", "39", "14", 53, 56, 53, 53],
  ["7136-208", "בת ים", "9", "5", 14, 14, 14, 12],
  ["17032-221", "כפר תבור", "2", "1", 3, 3, 3, 3],
  ["6982-34", "תל אביב‑יפו", "3", "4", 7, 8, 7, 5],
  ["5134-28", "שדמה / כפר מרדכי", "אין דף", "—", 3, 9, 1, 1],
  ["7369-15", "כוכב יאיר / צור יגאל", "1", "0", 1, 1, 1, 1],
] as const;

/** The nadlan.gov.il total: its table holds one row per property (the latest
 *  sale), and every earlier sale of that property sits in the row's history. */
function nadlanTotal(table: string, history: string): number | null {
  const t = Number(table), h = Number(history);
  return Number.isFinite(t) && Number.isFinite(h) ? t + h : null;
}

export default function GovmapGaps() {
  return (
    <article style={{ maxWidth: 900, lineHeight: 1.7 }}>
      <h2 style={{ fontSize: "1.35rem", lineHeight: 1.35, margin: "0 0 0.6rem" }}>
        GovMap מול אתר הנדל״ן ורשות המיסים: אותן עשר חלקות, מקור שלישי
      </h2>

      <div style={{ border: "1px solid var(--warning)", borderRadius: 8, padding: "0.6rem 0.8rem", marginBottom: "1rem", fontSize: "0.92rem" }}>
        <strong>מתי נבדק מה.</strong> הבדיקה מול GovMap בוצעה ב־<strong>3.10.2026</strong>, על שכבת עסקאות
        הנדל״ן של GovMap (שכבה 16) כפי שנשמרה ב־OVER באותו יום — גרסה 3, <Ltr>2,501,231</Ltr> עסקאות.
        הנתונים של <strong>אתר הנדל״ן ורשות המיסים הם אלה שנאספו בבדיקה הקודמת</strong> (
        <a href="/projects/deals?tab=gaps">פערים מול מיסוי מקרקעין</a>) ולא נאספו מחדש, ולכן ייתכן
        שחלק מהפערים נובעים משינויים במקורות בין שני מועדי הבדיקה.
      </div>

      <p>
        <strong>בקצרה:</strong> שכבת העסקאות של GovMap היא אותו מאגר שמציג אתר הנדל״ן — אבל ברשימה אחת.
        בשמונה מעשר החלקות מספר העסקאות ב־GovMap שווה בדיוק לסכום של שתי הרשימות באתר הנדל״ן: הטבלה
        והיסטוריית העסקאות שנפתחת מכל שורה. מול רשות המיסים, <strong>144 מתוך 159</strong> העסקאות השמורות ב־OVER נמצאות
        ב־GovMap באותה תת־חלקה ובאותו יום, ו־<strong>147</strong> אם סופרים גם עסקה באותו יום שתת־החלקה שלה
        מוספרה אחרת. בכל 137 מהן הסכום זהה.
      </p>

      <h3>עשר החלקות, מספר מול מספר</h3>
      <div tabIndex={0} role="region" aria-label="עשר החלקות" className="scroll-region" style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", fontSize: "0.88rem", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              <th style={HEAD}>חלקה</th><th style={HEAD}>יישוב</th>
              <th style={{ ...HEAD, textAlign: "end", background: "var(--bg-subtle, rgba(0,0,0,0.04))" }}>אתר הנדל״ן<br />עסקאות + היסטוריה</th>
              <th style={{ ...HEAD, textAlign: "end", background: "var(--bg-subtle, rgba(0,0,0,0.04))" }}>GovMap</th>
              <th style={{ ...HEAD, textAlign: "end" }}>מיסים<br />עסקאות</th>
              <th style={{ ...HEAD, textAlign: "end" }}>GovMap ↔ מיסים<br />מתלכדות</th>
              <th style={{ ...HEAD, textAlign: "end" }}>מהן<br />סכום זהה</th>
            </tr>
          </thead>
          <tbody>
            {PARCELS.map(([parcel, town, table, history, gm, taxes, match, same]) => (
              <tr key={parcel}>
                <td style={CELL}><Ltr>{parcel}</Ltr></td>
                <td style={CELL}>{town}</td>
                <td style={{ ...NUM, fontWeight: 700, background: "var(--bg-subtle, rgba(0,0,0,0.04))" }}>
                  {nadlanTotal(table, history) ?? (table === "אין דף" ? "אין דף" : "—")}
                </td>
                <td style={{ ...NUM, fontWeight: 700, background: "var(--bg-subtle, rgba(0,0,0,0.04))" }}>
                  {gm}{nadlanTotal(table, history) === gm && <span title="שווה בדיוק לסה״כ באתר הנדל״ן" style={{ color: "var(--success)" }}> ✓</span>}
                </td>
                <td style={NUM}>{taxes}</td>
                <td style={NUM}>{match}</td>
                <td style={NUM}>{same}</td>
              </tr>
            ))}
            <tr style={{ fontWeight: 700 }}>
              <td style={CELL}>סה״כ</td><td style={CELL}>—</td>
              <td style={NUM}>145</td>
              <td style={NUM}>149</td><td style={NUM}>161</td>
              <td style={NUM}>144</td><td style={NUM}>137</td>
            </tr>
          </tbody>
        </table>
      </div>
      <p className="text-sm" style={{ marginTop: "0.5rem" }}>
        <strong>איך לקרוא את הטבלה:</strong> הטבלה באתר הנדל״ן מציגה <strong>שורה אחת לכל נכס</strong> — המכירה
        האחרונה שלו — וכל מכירה קודמת של אותו נכס מוסתרת בהיסטוריה שנפתחת מהשורה. העמודה של אתר הנדל״ן סופרת
        את שתיהן יחד, כמו GovMap, שמציג כל מכירה כשורה. ✓ — שווה בדיוק.
      </p>
      <p className="text-sm text-muted">
        ״מיסים · עסקאות״ סופר מכירות כפי שבדוח הקודם: שורות של אותה תת־חלקה ואותו יום מכירה הן עסקה
        אחת. ״מתלכדות״ — עסקה של GovMap שיש לה עסקה ברשות המיסים באותה תת־חלקה ובאותו יום; בחלקה שאינה
        מחולקת (תת־חלקה 000 ברשות המיסים) — באותו יום. ״סכום זהה״ — השווי ב־GovMap שווה לשווי ברשות
        המיסים, או לשווי חלקי החלק הנמכר, בעיגול לאלף. עמודת ״מיסים · עסקאות״ היא הספירה של הדוח
        הקודם (161); ההתאמה עסקה מול עסקה נעשתה מול שורות רשות המיסים שכבר שמורות ב־OVER, שבהן הספירה
        באותה שיטה נותנת 159 — הפער של שתיים, בתל מונד ובתל אביב, הוא שורות שהדוח הקודם מנה כעסקאות נפרדות.
      </p>

      <h3>הממצאים</h3>
      <ol>
        <li style={{ marginBottom: "0.6rem" }}>
          <strong>GovMap הוא אתר הנדל״ן ברשימה אחת.</strong> בשמונה חלקות GovMap = טבלה + היסטוריה בדיוק
          (למשל קריית ביאליק 10236-99: <Ltr>53 = 39 + 14</Ltr>; בת ים 7151-316: <Ltr>17 = 12 + 5</Ltr>).
          במגדל העמק 17457-63 יש ב־GovMap 44 מול 43. מה שבאתר הנדל״ן מוסתר בהיסטוריה של כל שורה, ב־GovMap
          מופיע כשורה.
        </li>
        <li style={{ marginBottom: "0.6rem" }}>
          <strong>חלקה שאין לה דף באתר הנדל״ן — יש לה עסקאות ב־GovMap.</strong> גוש 5134 חלקה 28 מחזירה באתר
          הנדל״ן ״המיקום לא נמצא״, וב־GovMap יש לה שלוש עסקאות. שלושתן קיימות ברשות המיסים באותם ימים
          (<Ltr>21/11/2006</Ltr>, <Ltr>05/02/2011</Ltr>, <Ltr>24/01/2013</Ltr>), אבל GovMap ממספר את תתי־החלקה
          מחדש (1, 2, 3 במקום 15, 12, 000). שש מתשע העסקאות של רשות המיסים בחלקה אינן ב־GovMap.
        </li>
        <li style={{ marginBottom: "0.6rem" }}>
          <strong>אותו מספור תתי־חלקה של אתר הנדל״ן.</strong> בחלקה שאינה מחולקת — תל מונד 7787-464 וכוכב יאיר
          7369-15 — GovMap ממספר את העסקאות 1, 2, 3… במקום ה־000 של רשות המיסים, בדיוק כמו אתר הנדל״ן.
        </li>
        <li style={{ marginBottom: "0.6rem" }}>
          <strong>אותה עסקה כפולה של אתר הנדל״ן.</strong> בתל מונד מופיעות ב־GovMap שתי עסקאות של
          <Ltr> 2,700,000 </Ltr>ש״ח ביומיים רצופים (<Ltr>18/01/2017</Ltr> ו־<Ltr>19/01/2017</Ltr>), כפי שמצא
          הדוח הקודם באתר הנדל״ן; ברשות המיסים יש אחת. שם גם עסקה של <Ltr>29/06/2014</Ltr> מופיעה ב־GovMap
          פעמיים, על תת־חלקה 1 ו־2.
        </li>
        <li style={{ marginBottom: "0.6rem" }}>
          <strong>הסכומים כמעט תמיד מתלכדים.</strong> מתוך 144 עסקאות מתלכדות, ב־137 הסכום זהה. רוב השאר הם
          עסקה אחת ב־GovMap מול כמה שורות ברשות המיסים שסכומן הוא סכום העסקה (תל אביב 6982-34, <Ltr>11/09/2017</Ltr>:
          <Ltr> 2,136,400 + 113,600 = 2,250,000</Ltr>), או חלק נמכר מעוגל לשלוש ספרות (בת ים 7151-316:
          <Ltr> 633,333 ÷ 0.333</Ltr> מול <Ltr>1,900,000</Ltr>). שתי עסקאות ישנות במגדל העמק נותרו לא מוסברות:
          <Ltr> 270,000</Ltr> מול <Ltr>166,470</Ltr> (<Ltr>04/12/2003</Ltr>) ו־<Ltr>286,000</Ltr> מול
          <Ltr> 45,880</Ltr> (<Ltr>01/07/2001</Ltr>).
        </li>
        <li style={{ marginBottom: "0.6rem" }}>
          <strong>מה שחסר ב־GovMap.</strong> 15 עסקאות של רשות המיסים לא נמצאו ב־GovMap: שש בחלקה 5134-28,
          שלוש ישנות בקריית ביאליק 10236-99 (2002–2003), שלוש במגדל העמק 17457-63 (אחת מהן, <Ltr>29/01/2017</Ltr>,
          כנראה אותה עסקה שמופיעה ב־GovMap על תת־חלקה 33 במקום 944), ואחת בבת ים 7151-316 (<Ltr>14/12/2006</Ltr>).
        </li>
      </ol>

      <h3>על המקור</h3>
      <ul className="text-sm">
        <li>שכבת העסקאות של GovMap מסמנת את כל העסקאות של בניין על נקודה אחת. היא נאספת במלואה — 2,501,231
          עסקאות בגרסה הנבדקת — למעט 27 נקודות (חלקות ללא כתובת) שבהן GovMap מחזיר רק 500 עסקאות; אף אחת
          מעשר החלקות איננה ביניהן.</li>
        <li>תאריכי העסקה בשכבה נעים בין 1900 ל־2048 — ערכים כפי שפורסמו במקור.</li>
        <li>את השכבה כולה אפשר לחפש בלשונית <a href="/projects/deals?tab=govmap">עסקאות GovMap</a>.</li>
      </ul>
    </article>
  );
}
