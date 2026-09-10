/**
 * "פערים מול מיסוי מקרקעין" — the fifth tab of נדל"ן לעם.
 *
 * Ten random parcels were read page by page on both nadlan.gov.il and
 * nadlan.taxes.gov.il on 9 and 10 September 2026 and compared row against row,
 * including every "עסקאות קודמות לנכס" window behind a nadlan.gov.il row. The
 * headline is that the nadlan.gov.il table shows one row per property and hides
 * the earlier sales behind a click: 93 rows, 145 sales once every row is opened,
 * 140 of which match the tax authority. This component is the write-up: the
 * findings, the numbers behind each one, and the 68 screenshots they were read
 * off.
 *
 * It lives behind a lazy import because none of it, least of all the appendix,
 * is needed by the four lookup tabs. The screenshots are static files under
 * public/nadlan-gaps/, and the grid shows small crops rather than the originals
 * so opening the tab does not pull every full-size screenshot.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { GAP_FIGURES, GapFigure, figureUrl, thumbUrl } from "./nadlanGapsFigures";

/* ── evidence references ────────────────────────────────────────────────────
   Each finding cites the figures it was read off. Clicking one opens that
   screenshot straight in the lightbox, which is what a reader wants from a
   citation; the appendix below is for reading the whole thing in order. */

const BY_N = new Map(GAP_FIGURES.map((f) => [f.n, f]));

/**
 * An LTR island inside the RTL page.
 *
 * A cell like "+132,712 · 18%" is all bidi-neutral characters, so an RTL
 * paragraph reorders it into "18% · 132,712+" and a leading minus lands after
 * the digits. Isolating the run fixes the order without touching how the cell
 * itself is aligned.
 */
function Ltr({ children }: { children: React.ReactNode }) {
  return <span className="ngap-ltr">{children}</span>;
}

function Refs({ ns, onOpen }: { ns: number[]; onOpen: (n: number) => void }) {
  return (
    <p className="ngap-refs">
      <b>ראיות:</b>
      {ns.map((n) => {
        const f = BY_N.get(n);
        if (!f) return null;
        return (
          <button
            key={n}
            type="button"
            className={`ngap-ref${f.src === "tax" ? " is-tax" : ""}`}
            onClick={() => onOpen(n)}
            title={`${f.host} · ${f.place}`}
          >
            {n}
          </button>
        );
      })}
      <span className="ngap-key">
        <i style={{ background: "var(--ngap-tax)" }} />
        מיסוי מקרקעין ·{" "}
        <i style={{ background: "var(--ngap-nadlan)" }} />
        נדל״ן
      </span>
    </p>
  );
}

/* ── lightbox ───────────────────────────────────────────────────────────── */

function Lightbox({
  index,
  onClose,
  onMove,
}: {
  index: number;
  onClose: () => void;
  onMove: (delta: number) => void;
}) {
  const fig = GAP_FIGURES[index];
  const closeRef = useRef<HTMLButtonElement>(null);

  // Escape closes, arrows page. In RTL the visually-previous figure is the one
  // to the right, so the arrow keys are swapped against their LTR meaning.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") { e.preventDefault(); onClose(); }
      else if (e.key === "ArrowLeft") { e.preventDefault(); onMove(1); }
      else if (e.key === "ArrowRight") { e.preventDefault(); onMove(-1); }
    };
    document.addEventListener("keydown", onKey);
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    closeRef.current?.focus();
    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
    };
  }, [onClose, onMove]);

  return (
    <div
      className="ngap-lb"
      role="dialog"
      aria-modal="true"
      aria-label={`איור ${fig.n} מתוך ${GAP_FIGURES.length}`}
    >
      <div className="ngap-lb-bar">
        <span className="ngap-lb-n">איור {fig.n} / {GAP_FIGURES.length}</span>
        <span className="ngap-lb-place">{fig.place}</span>
        <span className="ngap-lb-host" dir="ltr">{fig.host}</span>
        <span className="ngap-lb-spacer" />
        <button type="button" onClick={() => onMove(-1)} disabled={index === 0}>
          ‹ הקודם
        </button>
        <button
          type="button"
          onClick={() => onMove(1)}
          disabled={index === GAP_FIGURES.length - 1}
        >
          הבא ›
        </button>
        <a href={figureUrl(fig.n)} target="_blank" rel="noreferrer">
          פתח בכרטיסייה חדשה ↗
        </a>
        <button ref={closeRef} type="button" onClick={onClose}>
          סגור ✕
        </button>
      </div>
      <div className="ngap-lb-stage">
        <img src={figureUrl(fig.n)} alt={`${fig.place}. ${fig.cap}`} />
      </div>
      <p className="ngap-lb-cap">{fig.cap}</p>
    </div>
  );
}

/* ── the appendix, grouped parcel by parcel ─────────────────────────────── */

type Group = {
  place: string;
  tax: GapFigure[];
  /** Pages of the nadlan.gov.il deals table. */
  nadlanPages: GapFigure[];
  /** One shot per row with its "עסקאות קודמות לנכס" window open. */
  nadlanHistory: GapFigure[];
};

function groupFigures(): Group[] {
  const out: Group[] = [];
  for (const f of GAP_FIGURES) {
    let g = out[out.length - 1];
    if (!g || g.place !== f.place) {
      g = { place: f.place, tax: [], nadlanPages: [], nadlanHistory: [] };
      out.push(g);
    }
    if (f.src === "tax") g.tax.push(f);
    else if (f.kind === "history") g.nadlanHistory.push(f);
    else g.nadlanPages.push(f);
  }
  return out;
}

function Thumbs({
  figs,
  onOpen,
}: {
  figs: GapFigure[];
  onOpen: (n: number) => void;
}) {
  if (figs.length === 0) return null;
  return (
    <div className="ngap-figrow">
      {figs.map((f) => (
        <figure
          key={f.n}
          id={`ngap-fig-${f.n}`}
          className="ngap-thumb"
          role="button"
          tabIndex={0}
          onClick={() => onOpen(f.n)}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onOpen(f.n); }
          }}
          aria-label={`הגדל איור ${f.n}: ${f.cap}`}
        >
          <img src={thumbUrl(f.n)} alt="" loading="lazy" decoding="async" />
          <figcaption><b>{f.n}.</b> {f.cap}</figcaption>
        </figure>
      ))}
    </div>
  );
}

/* ── the report ─────────────────────────────────────────────────────────── */

export default function NadlanGaps() {
  const [openIdx, setOpenIdx] = useState<number | null>(null);
  const groups = useMemo(groupFigures, []);

  const openByNumber = useCallback((n: number) => {
    const i = GAP_FIGURES.findIndex((f) => f.n === n);
    if (i >= 0) setOpenIdx(i);
  }, []);

  const move = useCallback((delta: number) => {
    setOpenIdx((i) => {
      if (i == null) return i;
      const next = i + delta;
      return next >= 0 && next < GAP_FIGURES.length ? next : i;
    });
  }, []);

  const close = useCallback(() => setOpenIdx(null), []);

  return (
    <div className="ngap">
      <header>
        <div className="ngap-kicker">בדיקה השוואתית · ספטמבר 2026</div>
        <h2 style={{ fontSize: "1.35rem", lineHeight: 1.35, margin: "0 0 0.6rem" }}>
          פערים בין אתר הנדל״ן הממשלתי לבין מאגר מיסוי מקרקעין
        </h2>
        <p className="ngap-lede">
          שני אתרים ממשלתיים מפרסמים את אותן עסקאות נדל״ן. עשר חלקות אקראיות ברחבי הארץ
          נבדקו בשניהם, עמוד אחר עמוד, והושוו זו מול זו.
        </p>
        <p className="ngap-lede">
          הממצא המרכזי הוא ש<strong>טבלת העסקאות של אתר הנדל״ן מציגה שורה אחת לכל נכס</strong>,
          המכירה האחרונה בו, ואת המכירות הקודמות של אותו נכס היא מציגה רק בחלון שנפתח בלחיצה
          על השורה. מי שקורא את הטבלה רואה 93 עסקאות מול 161 אצל רשות המיסים; מי שפותח כל שורה
          מגיע ל־145, ו־140 מהן מתלכדות עם רשות המיסים על התאריך ועל הסכום. מה שנותר אחרי ההסבר
          הוא שבע מכירות שאינן מופיעות באתר הנדל״ן בשום מקום, סכומים שונים בין האתרים, חלקה
          שלמה שאין לה דף, שטח שנבדל בין המקורות עד כדי פי עשרה, ועובדה שאינה מסומנת בשום
          מקום: לכל עסקה יש שני סכומים רשמיים, ואתר הנדל״ן מפרסם רק אחד מהם.
        </p>
        <div className="ngap-legend">
          <span className="is-nadlan">
            <i />
            <span dir="ltr">nadlan.gov.il</span>, אתר הנדל״ן הממשלתי
          </span>
          <span className="is-tax">
            <i />
            <span dir="ltr">nadlan.taxes.gov.il</span>, מיסוי מקרקעין
          </span>
        </div>
      </header>

      <div className="ngap-stats">
        <div className="ngap-stat is-nadlan">
          <b>145</b><span>מכירות באתר הנדל״ן, 93 בטבלה ו־52 בחלונות ההיסטוריה</span>
        </div>
        <div className="ngap-stat is-tax">
          <b>161</b><span>עסקאות אצל רשות המיסים (188 שורות)</span>
        </div>
        <div className="ngap-stat is-good">
          <b>140</b><span>מתלכדות בין המקורות על תאריך וסכום</span>
        </div>
        <div className="ngap-stat is-bad">
          <b>7</b><span>מכירות שאינן מופיעות באתר הנדל״ן כלל</span>
        </div>
      </div>

      {/* ═══ part א: the structural gap ═══ */}
      <div className="ngap-section">חלק א׳ · הפער המבני</div>

      <div className="ngap-card">
        <h3>
          <span className="ngap-num">1</span>
          טבלת אתר הנדל״ן מציגה שורה לכל נכס, וההיסטוריה מאחורי חץ
        </h3>
        <p className="ngap-p">
          זה ההסבר לרוב הפער. טבלת העסקאות של אתר הנדל״ן מציגה שורה אחת לכל נכס, ובה המכירה
          האחרונה שבוצעה בו. המכירות הקודמות של אותו נכס קיימות באתר, אך מוצגות רק כשפותחים את
          השורה בלחיצה על החץ שבקצה, בחלון שכותרתו <strong>״עסקאות קודמות לנכס״</strong>. רשות
          המיסים, לעומת זאת, מציגה כל הצהרה בשורה משלה.
        </p>
        <div className="ngap-evidence">
          <span className="ngap-cap">
            גוש 17457 חלקה 63 · תת־חלקה 027, חמש מכירות אצל רשות המיסים, וכולן באתר הנדל״ן
          </span>
          {`07/10/2008    160,000 ₪   `}<em>← בחלון ההיסטוריה</em>{`
07/10/2010    200,000 ₪   `}<em>← בחלון ההיסטוריה</em>{`
12/12/2011    315,000 ₪   `}<em>← בחלון ההיסטוריה</em>{`
15/01/2014    356,000 ₪   `}<em>← בחלון ההיסטוריה</em>{`
04/04/2021    510,000 ₪   `}<em>← בשורת הטבלה</em>
        </div>
        <p className="ngap-p">
          הכלל נבדק על כל עשר החלקות: בטבלאות מוצגות 93 שורות, ובחלונות ההיסטוריה שמאחוריהן
          עוד 52 מכירות, <strong>145 בסך הכול</strong>, מול 161 עסקאות אצל רשות המיסים.{" "}
          <strong>140 מהן מתלכדות</strong> על התאריך ועל הסכום, 120 עד השקל ו־20 בעיגול לאלף.
          מבין 21 העסקאות של רשות המיסים שאין להן התאמה, תשע בחלקה שאין לה דף באתר הנדל״ן
          (סעיף 7), שבע אינן מופיעות בו כלל (סעיף 3), שלוש מופיעות בסכום אחר (סעיף 4), ושתיים,
          מכירה אחת בתל אביב מ־11/09/2017, רשומות אצל רשות המיסים כשתי שורות של 2,136,400
          ו־113,600 ש״ח, ובאתר הנדל״ן כשורה אחת של 2,250,000.
        </p>
        <Refs
          ns={[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 59, 60, 61, 62, 63, 64]}
          onOpen={openByNumber}
        />
        <p className="ngap-p">
          שום דבר בשורה עצמה אינו אומר שיש מאחוריה היסטוריה, מלבד סימן עקיף אחד: חיווי השינוי
          במחיר (למשל ״137.2% ב־4.4 שנים״) מופיע רק בשורות שיש להן מכירה קודמת. גם הספירה
          ״נמצאו 9 עסקאות״ שבתחתית הטבלה סופרת נכסים ולא מכירות: בגוש 7136 חלקה 208 בבת ים
          מסתתרות בחלונות עוד חמש מכירות מעבר לתשע.
        </p>
        <Refs ns={[55, 56, 57, 58]} onOpen={openByNumber} />
        <p className="ngap-p">
          דיוק אחד: היחידה של אתר הנדל״ן היא <strong>נכס במספור שלו</strong>. בבניין מחולק
          היא מתלכדת עם תת־החלקה של רשות המיסים; בחלקה שאינה מחולקת, כמו גוש 7787 חלקה 464
          בתל מונד, שכל חמש הרשומות שלה אצל רשות המיסים תחת תת־חלקה 000, אתר הנדל״ן ממספר
          את הנכסים בעצמו ומציג שש שורות נפרדות.
        </p>
        <div className="ngap-takeaway">
          <b>המשמעות.</b> מי שסופר שורות בטבלת אתר הנדל״ן, או מסתמך על ״נמצאו N עסקאות״,
          יחסיר יותר משליש מהמכירות, 52 מתוך 145 בעשר החלקות, וזה אינו חוסר בנתונים אלא אופן
          ההצגה. היסטוריית המכירות של דירה מסוימת זמינה בשני האתרים; באתר הנדל״ן, רק בפתיחת
          השורה, אחת אחת.
        </div>
      </div>

      <div className="ngap-card">
        <h3>
          <span className="ngap-num">2</span>
          מכירת חלקים: כל צד סופר אחרת, והכלל אינו עקבי
        </h3>
        <p className="ngap-p">
          כשנכס נמכר בחלקים, רשות המיסים רושמת שורה נפרדת לכל חלק, עם עמודת ״חלק נמכר״.
          אתר הנדל״ן מציג שורה אחת בלבד ואין בו עמודה כזו. השאלה היא איזה סכום מוצג,
          ובמדגם הזה אין לכך תשובה אחת.
        </p>
        <Refs ns={[1, 4, 18, 33, 53, 54, 62]} onOpen={openByNumber} />
        <div className="ngap-tablewrap">
          <table>
            <caption>תשעת המקרים במדגם שבהם החלק הנמכר קטן מ־1.000</caption>
            <thead>
              <tr>
                <th>חלקה</th>
                <th className="num">תאריך</th>
                <th>רשות המיסים</th>
                <th className="num">אתר הנדל״ן</th>
                <th>ההתנהגות</th>
              </tr>
            </thead>
            <tbody>
              {([
                ["17457-63", "20/06/2010", "105,000 ×2 @0.500", "210,000", "sum", ""],
                ["7787-464", "19/01/2017", "2,700,000 @0.500", "2,700,000", "raw", ""],
                ["7787-464", "24/07/2014", "1,200,000 @0.500", "1,200,000", "raw", ""],
                ["10236-99", "07/01/2025", "1,553,000 @0.500", "1,553,000", "raw", ""],
                ["17032-221", "23/10/2013", "1,472,500 @0.500", "1,472,000", "raw", ""],
                ["7151-316", "07/04/2021", "705,000 ×2 @0.500", "1,410,000", "sum", ""],
                ["10236-99", "11/04/2019", "797,500 ×2 @0.500", "1,595,000", "sum", ""],
                ["17457-63", "16/04/2024", "470,000 @0.666 + 235,000 @0.333", "705,000", "sum", ""],
                ["7136-208", "01/10/2023", "@0.998", "1,500,000", "sum", "שישה חלקים"],
              ] as const).map(([parcel, date, tax, nadlanValue, kind, prefix], i) => (
                <tr key={i}>
                  <td className="ngap-id">{parcel}</td>
                  <td className="num">{date}</td>
                  <td className="ngap-id">
                    {prefix ? `${prefix} · ` : null}
                    <Ltr>{tax}</Ltr>
                  </td>
                  <td className="num ngap-vn">{nadlanValue}</td>
                  <td>
                    <span className={`ngap-tag${kind === "raw" ? " is-nadlan" : ""}`}>
                      {kind === "raw" ? "לא גולם" : "סכום החלקים"}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="ngap-p">
          כלומר, ההנחה שאתר הנדל״ן מגלם את הסכום לשווי הנכס המלא אינה נכונה באף אחד{" "}
          <strong>מתשעת המקרים</strong>. הכלל אחיד: מוצג סכום החלקים כפי שהוצהרו, בלי גילום.
          מכיוון שהאתר אינו מציין שמדובר במכירה חלקית, אין דרך לדעת מתוכו לבד אם המספר
          שמוצג הוא שווי הנכס או שווי חלק ממנו.
        </p>
      </div>

      <div className="ngap-card">
        <h3>
          <span className="ngap-num">3</span>
          שבע מכירות שאינן מופיעות באתר הנדל״ן בשום מקום
        </h3>
        <p className="ngap-p">
          גם אחרי שפותחים את כל חלונות ההיסטוריה, שבע מכירות של רשות המיסים, בחלקות שיש להן
          דף, אינן מופיעות באתר הנדל״ן: לא בשורת הטבלה ולא בהיסטוריה של הנכס.
        </p>
        <Refs
          ns={[1, 3, 6, 7, 16, 17, 18, 19, 20, 29, 30, 31, 32, 33, 34, 35, 36, 37, 49]}
          onOpen={openByNumber}
        />
        <div className="ngap-tablewrap">
          <table>
            <thead>
              <tr>
                <th>חלקה</th><th>תת־חלקה</th>
                <th className="num">תאריך</th>
                <th className="num">סכום</th>
                <th className="num">שטח רשום</th>
                <th>באתר הנדל״ן</th>
              </tr>
            </thead>
            <tbody>
              {([
                ["17457-63", "018", "01/07/2001", "45,880", "70", "אין שורה לנכס"],
                ["17457-63", "024", "15/09/2019", "48,100", "70", "אין שורה לנכס"],
                ["17457-63", "030", "02/02/2017", "460,000", "0", "חסרה בהיסטוריה: 2011 ו־2020 מוצגות"],
                ["7151-316", "004", "14/12/2006", "557,763", "0", "הנכס מוצג רק עם מכירת 2024"],
                ["10236-99", "012", "30/10/2002", "900,000", "0", "אין שורה לנכס"],
                ["10236-99", "035", "07/08/2002", "915,000", "0", "חסרה בהיסטוריה: 2007 מוצגת"],
                ["10236-99", "038", "19/02/2003", "805,000", "0", "הנכס מוצג רק עם מכירת 2008"],
              ] as const).map(([parcel, sub, date, amount, area, onNadlan], i) => (
                <tr key={i}>
                  <td className="ngap-id">{parcel}</td>
                  <td className="ngap-id">{sub}</td>
                  <td className="num">{date}</td>
                  <td className="num ngap-vt">{amount}</td>
                  <td className={area === "0" ? "num ngap-vc" : "num"}>{area}</td>
                  <td>{onNadlan}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="ngap-p">
          המכנה המשותף בולט: חמש מהשבע רשומות אצל רשות המיסים בשטח 0, ושתיים בסכום של פחות
          מ־50,000 ש״ח. אבל זה אינו כלל: שש שורות אחרות בשטח 0 כן מוצגות באתר הנדל״ן. כך או
          כך, האתר אינו מסמן שהושמטה מכירה, ובהיסטוריה של הנכס נוצר חור שאינו נראה.
        </p>
      </div>

      {/* ═══ part ב: the data itself ═══ */}
      <div className="ngap-section">חלק ב׳ · פערים בנתון עצמו</div>

      <div className="ngap-card is-severe">
        <h3>
          <span className="ngap-num">4</span>
          שישה סכומים שאינם מתיישבים על אותה עסקה
        </h3>
        <p className="ngap-p">
          אותה חלקה, אותה תת־חלקה, אותו יום מכירה, ושני מספרים שונים. ארבעה מהם פער עיגול
          לאלף, ושלושה לא.
        </p>
        <Refs ns={[17, 19, 27, 28, 29, 32, 35, 37, 53, 55]} onOpen={openByNumber} />
        <div className="ngap-tablewrap">
          <table>
            <thead>
              <tr>
                <th>חלקה</th>
                <th className="num">תאריך</th>
                <th className="num">אתר הנדל״ן</th>
                <th className="num">רשות המיסים</th>
                <th className="num">הפרש</th>
                <th>אופי הפער</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="ngap-id">10236-99</td><td className="num">25/02/2004</td>
                <td className="num ngap-vn">870,000</td><td className="num ngap-vt">737,288</td>
                <td className="num ngap-vc"><Ltr>+132,712 · 18%</Ltr></td><td>ערך שונה לחלוטין</td>
              </tr>
              <tr>
                <td className="ngap-id">7136-208</td><td className="num">09/05/2022</td>
                <td className="num ngap-vn">1,541,600</td><td className="num ngap-vt">1,479,998</td>
                <td className="num ngap-vc"><Ltr>+61,602 · 4.2%</Ltr></td>
                <td>סכום ארבעת החלקים אינו מגיע לסכום המוצג</td>
              </tr>
              <tr>
                <td className="ngap-id">7151-316</td><td className="num">27/09/2006</td>
                <td className="num ngap-vn">276,000</td><td className="num ngap-vt">279,600</td>
                <td className="num ngap-vc"><Ltr>−3,600 · 1.3%</Ltr></td>
                <td>גם השטח נבדל: 85 מ״ר מול 97</td>
              </tr>
              <tr>
                <td className="ngap-id">17457-63</td><td className="num">29/01/2017</td>
                <td className="num ngap-vn">170,000</td><td className="num ngap-vt">170,800</td>
                <td className="num"><Ltr>−800</Ltr></td>
                <td><span className="ngap-tag">עיגול לאלף</span></td>
              </tr>
              <tr>
                <td className="ngap-id">17032-221</td><td className="num">23/10/2013</td>
                <td className="num ngap-vn">1,472,000</td><td className="num ngap-vt">1,472,500</td>
                <td className="num"><Ltr>−500</Ltr></td>
                <td><span className="ngap-tag">עיגול לאלף</span></td>
              </tr>
              <tr>
                <td className="ngap-id">7787-464</td><td className="num">29/06/2014</td>
                <td className="num ngap-vn"><Ltr>869,000 · 816,000</Ltr></td>
                <td className="num ngap-vt"><Ltr>869,186 · 816,872</Ltr></td>
                <td className="num"><Ltr>−186 · −872</Ltr></td>
                <td><span className="ngap-tag">עיגול לאלף</span></td>
              </tr>
              <tr>
                <td className="ngap-id">10236-99</td><td className="num">16/11/1999</td>
                <td className="num ngap-vn">3,102,000</td><td className="num ngap-vt">3,102,841</td>
                <td className="num"><Ltr>−841</Ltr></td>
                <td><span className="ngap-tag">עיגול לאלף</span></td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="ngap-p">
          המקרה הראשון הוא היחיד שאי אפשר לתלות בעיגול או במכירת חלקים: פער של 18% על דירה
          בקריית ביאליק, ‎870,000‎ ש״ח מול ‎737,288‎ ש״ח. שני האתרים מציגים היום מספרים
          סותרים על אותה מכירה, ואין באף אחד מהם סימן לכך שהערך שנוי במחלוקת.
        </p>
        <p className="ngap-p">
          העיגול אינו מוגבל לשורת הטבלה: גם הסכומים בחלון ההיסטוריה מעוגלים לאלף, ולא תמיד
          לאלף הקרוב. בגוש 17457 חלקה 63 מופיעה מכירה מ־19/08/2010 כ־234,000 מול 235,000 אצל
          רשות המיסים, ומכירה מ־04/04/2014 כ־369,000 מול 370,000: סכומים עגולים שהאתר מציג
          נמוכים באלף.
        </p>
        <Refs ns={[1, 2, 3, 4, 5, 8, 12]} onOpen={openByNumber} />
      </div>

      <div className="ngap-card is-severe">
        <h3>
          <span className="ngap-num">5</span>
          לכל עסקה שני סכומים, ואתר הנדל״ן מפרסם רק אחד מהם
        </h3>
        <p className="ngap-p">
          רשות המיסים מציגה שתי עמודות כסף: <strong>תמורה מוצהרת</strong>, מה שהצדדים
          הצהירו, ו<strong>שווי מכירה</strong>, השווי שנקבע בסופו של דבר. ברוב השורות הן
          זהות, אבל לא תמיד, ואז הפער אינו זניח. אתר הנדל״ן מפרסם תמיד את שווי המכירה, בלי
          לציין שקיים מספר שני.
        </p>
        <Refs ns={[17, 27, 29, 32]} onOpen={openByNumber} />
        <div className="ngap-tablewrap">
          <table>
            <caption>השורות במדגם שבהן שתי העמודות נבדלות</caption>
            <thead>
              <tr>
                <th>חלקה</th><th>תת־חלקה</th>
                <th className="num">תאריך</th>
                <th className="num">תמורה מוצהרת</th>
                <th className="num">שווי מכירה</th>
                <th className="num">פער</th>
              </tr>
            </thead>
            <tbody>
              {([
                ["7151-316", "010", "27/09/2006", "122,700", "279,600", "×2.3", true],
                ["10236-99", "014", "19/02/2004", "724,576", "855,000", "+18%", true],
                ["10236-99", "035", "01/08/2010", "850,000", "1,050,000", "+24%", true],
                ["10236-99", "030", "31/07/2003", "117,071", "99,212", "−15%", true],
                ["7787-464", "000", "29/06/2014", "838,980", "869,186", "+3.6%", false],
                ["7787-464", "000", "29/06/2014", "786,666", "816,872", "+3.8%", false],
              ] as const).map(([parcel, sub, date, declared, value, gap, big], i) => (
                <tr key={i}>
                  <td className="ngap-id">{parcel}</td>
                  <td className="ngap-id">{sub}</td>
                  <td className="num">{date}</td>
                  <td className="num">{declared}</td>
                  <td className="num ngap-vt">{value}</td>
                  <td className={big ? "num ngap-vc" : "num"}><Ltr>{gap}</Ltr></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="ngap-p">
          שימו לב לשורה הראשונה: אותה דירה בבת ים הוצהרה ב־122,700 ש״ח ונקבע לה שווי מכירה
          של 279,600, פי 2.3. באתר הנדל״ן מופיע רק ‎276,000‎, בלי שום רמז לכך ששני המספרים
          האחרים קיימים.
        </p>
        <div className="ngap-takeaway is-bad">
          <b>המשמעות.</b> ״מחיר העסקה״ באתר הנדל״ן אינו המחיר שהצדדים דיווחו עליו אלא השווי
          שנקבע. לרוב אין הבדל; במקרים שבהם יש, ההבדל גדול, והאתר אינו מסמן אותם.
        </div>
      </div>

      <div className="ngap-card is-severe">
        <h3>
          <span className="ngap-num">6</span>
          גם השטח אינו מתלכד, ובמקרה אחד פי עשרה
        </h3>
        <p className="ngap-p">
          השטח הרשום לאותו נכס נבדל בין שני האתרים בארבעה מקרים במדגם, ובאחד מהם ההפרש הוא
          סדר גודל שלם.
        </p>
        <Refs ns={[17, 19, 29, 32, 35, 59, 60]} onOpen={openByNumber} />
        <div className="ngap-tablewrap">
          <table>
            <thead>
              <tr>
                <th>חלקה</th><th>תת־חלקה</th>
                <th className="num">תאריך</th>
                <th className="num">אתר הנדל״ן (מ״ר)</th>
                <th className="num">רשות המיסים (מ״ר)</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="ngap-id">10236-99</td><td className="ngap-id">017</td>
                <td className="num">10/01/2001</td>
                <td className="num ngap-vn">125.2</td>
                <td className="num ngap-vc">1,250</td>
              </tr>
              <tr>
                <td className="ngap-id">17032-221</td><td className="ngap-id">000</td>
                <td className="num">23/10/2013</td>
                <td className="num ngap-vn">749</td>
                <td className="num ngap-vt">169</td>
              </tr>
              <tr>
                <td className="ngap-id">10236-99</td><td className="ngap-id">001</td>
                <td className="num">25/02/2004</td>
                <td className="num ngap-vn">137.4</td>
                <td className="num ngap-vt">100</td>
              </tr>
              <tr>
                <td className="ngap-id">7151-316</td><td className="ngap-id">010</td>
                <td className="num">27/09/2006</td>
                <td className="num ngap-vn">85</td>
                <td className="num ngap-vt">97</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="ngap-p">
          בשתי החלקות הראשונות ההפרש גדול מכדי להיות עיגול או הבדל הגדרה: 125.2 מול 1,250,
          ו־749 מול 169. מחיר למ״ר שיחושב מהאתר הלא־נכון יהיה שגוי בהתאם.
        </p>
      </div>

      <div className="ngap-card is-severe">
        <h3>
          <span className="ngap-num">7</span>
          חלקה שלמה שאין לה דף באתר הנדל״ן
        </h3>
        <p className="ngap-p">
          גוש 5134 חלקה 28 מחזירה באתר הנדל״ן את הודעת{" "}
          <strong>״המיקום לא נמצא״</strong>, אותה תשובה בדיוק שהיה מקבל מי שיחפש גוש שאינו
          קיים.
        </p>
        <Refs ns={[65, 66]} onOpen={openByNumber} />
        <p className="ngap-p">
          אצל רשות המיסים יש לאותה חלקה <strong>תשע עסקאות בשמונה תתי־חלקה</strong>, בין
          2006 ל־2013, כולל מכירת קוטג׳ ב־3.6 מיליון ש״ח בינואר 2013. כלומר היעדר דף באתר
          הנדל״ן אינו ראיה להיעדר עסקאות, ואינו מלווה בשום הסבר או סימון.
        </p>
      </div>

      <div className="ngap-card">
        <h3>
          <span className="ngap-num">8</span>
          עסקה אחת שקיימת רק באתר הנדל״ן
        </h3>
        <p className="ngap-p">
          בגוש 7787 חלקה 464 בתל מונד מציג אתר הנדל״ן שתי עסקאות של ‎2,700,000‎ ש״ח בימים
          עוקבים, 18 ו־19 בינואר 2017, בשטחים שונים, 207 ו־250 מ״ר. אצל רשות המיסים קיימת
          רק זו של ה־19.
        </p>
        <Refs ns={[27, 28]} onOpen={openByNumber} />
        <p className="ngap-p">
          זהו המקרה היחיד בכל המדגם שבו לאתר הנדל״ן יש מכירה בתאריך שאין לו מקבילה אצל רשות
          המיסים. לכל 144 המכירות האחרות שהאתר מציג, בטבלה ובחלונות ההיסטוריה, יש רשומה באותו
          תאריך במאגר מיסוי מקרקעין.
        </p>
        <div className="ngap-takeaway">
          <b>הכיוון ברור.</b> מאגר מיסוי מקרקעין הוא כמעט תמיד קבוצה מכילה של אתר הנדל״ן.
          חריגה אחת מתוך 145 מספיקה כדי לומר שלא תמיד, אך לא כדי להצדיק התייחסות לאתר
          הנדל״ן כמקור עצמאי.
        </div>
      </div>

      <div className="ngap-card">
        <h3>
          <span className="ngap-num">9</span>
          שם היישוב ושנת הבנייה אינם מתלכדים
        </h3>
        <p className="ngap-p">
          שתיים מעשר החלקות נושאות שם יישוב שונה בשני האתרים. כל הצלבה בין המאגרים לפי שם
          בטקסט חופשי תיכשל עליהן בשקט: תחזיר תוצאה, ופשוט תפיל את השורות האלה.
        </p>
        <Refs ns={[65, 66, 67, 68]} onOpen={openByNumber} />
        <div className="ngap-tablewrap">
          <table>
            <thead>
              <tr><th>חלקה</th><th>אתר הנדל״ן</th><th>רשות המיסים</th></tr>
            </thead>
            <tbody>
              <tr>
                <td className="ngap-id">5134-28</td>
                <td className="ngap-vn">שדמה</td><td className="ngap-vt">כפר מרדכי</td>
              </tr>
              <tr>
                <td className="ngap-id">7369-15</td>
                <td className="ngap-vn">כוכב יאיר‑צור יגאל</td><td className="ngap-vt">צור יגאל</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="ngap-p">
          שנת הבנייה אצל רשות המיסים אינה שדה שאפשר לסמוך עליו: במדגם היא מקבלת את הערכים{" "}
          <code>0</code>, <code>1</code> ו־<code>1800</code>, ולעיתים משתנה בין שורות של
          אותה תת־חלקה: ‎1973‎ באחת ו־‎1980‎ באחרת, לאותה דירה.
        </p>
        <p className="ngap-p">
          ולבסוף, אי־עקביות בתוך מאגר מיסוי מקרקעין עצמו: בגוש 10236 חלקה 99, תת־חלקה 023,
          העסקה מ־09/01/2003 מופיעה בשתי שורות של מחצית כל אחת, ולשתי השורות{" "}
          <strong>תמורה מוצהרת שונה</strong> (405,000 באחת, 810,000 בשנייה) ושטח שונה
          (0 מול 110). שתי הצהרות על אותה מכירה, ואין באתר דרך לדעת איזו נכונה.
        </p>
      </div>

      <div className="ngap-card is-severe">
        <h3>
          <span className="ngap-num">10</span>
          אותו דף מחזיר לפעמים ״0 עסקאות״, ואין דרך להבחין
        </h3>
        <p className="ngap-p">
          במהלך הבדיקה נטענו דפי הגוש/חלקה עשרות פעמים. ב־<strong>כרבע מהטעינות</strong> הדף
          הסתיים בהודעה ״נמצאו 0 עסקאות״ עבור חלקה שיש בה עסקאות: טבלה ריקה, בלי שגיאה,
          בלי סימן שמשהו נכשל.
        </p>
        <Refs ns={[28]} onOpen={openByNumber} />
        <div className="ngap-evidence">
          <span className="ngap-cap">
            גוש 7787 חלקה 464, שבע טעינות רצופות של אותה כתובת
          </span>
          {`טעינה 1  ·  0 עסקאות
טעינה 2  ·  0 עסקאות
טעינה 3  ·  6 עסקאות
טעינה 4  ·  0 עסקאות
טעינה 5  ·  6 עסקאות
טעינה 6  ·  6 עסקאות
טעינה 7  ·  6 עסקאות`}
        </div>
        <p className="ngap-p">
          זה אינו עניין של סבלנות: טעינה שהמתינה 40 שניות החזירה 0, וטעינה שהמתינה 18
          שניות החזירה 6. באותם רגעים עצמם חלקות אחרות נטענו כרגיל, כך שאין מדובר בתקלה
          כללית באתר.
        </p>
        <div className="ngap-takeaway is-bad">
          <b>המשמעות.</b> ״0 עסקאות״ באתר הנדל״ן אינו קביעה שאפשר להסתמך עליה. מי שבודק
          חלקה ומקבל טבלה ריקה צריך לרענן את הדף לפני שיסיק שלא נמכר בה דבר.
        </div>
      </div>

      {/* ═══ the ten parcels, number against number ═══ */}
      <div className="ngap-section">נספח · עשר החלקות, מספר מול מספר</div>

      <div className="ngap-tablewrap">
        <table>
          <thead>
            <tr>
              <th>חלקה</th><th>יישוב</th>
              <th className="num">נדל״ן<br />בטבלה</th>
              <th className="num">נדל״ן<br />בהיסטוריה</th>
              <th className="num">מיסים<br />עסקאות</th>
              <th className="num">מיסים<br />שורות</th>
              <th className="num">מתלכדות</th>
            </tr>
          </thead>
          <tbody>
            {([
              ["17457-63", "מגדל העמק", "20", "23", "46", "49", "43"],
              ["7151-316", "בת ים", "12", "5", "18", "24", "16"],
              ["12593-23", "נופית", "1", "0", "1", "1", "1"],
              ["7787-464", "תל מונד", "6", "0", "5", "5", "5"],
              ["10236-99", "קריית ביאליק", "39", "14", "56", "58", "52"],
              ["7136-208", "בת ים", "9", "5", "14", "26", "13"],
              ["17032-221", "כפר תבור", "2", "1", "3", "3", "3"],
              ["6982-34", "תל אביב‑יפו", "3", "4", "8", "12", "6"],
              ["5134-28", "שדמה / כפר מרדכי", "אין דף", "—", "9", "9", "0"],
              ["7369-15", "כוכב יאיר / צור יגאל", "1", "0", "1", "1", "1"],
            ] as const).map(([parcel, town, table, history, deals, rows, match], i) => (
              <tr key={i}>
                <td className="ngap-id">{parcel}</td>
                <td>{town}</td>
                <td className={table === "אין דף" ? "num ngap-vc" : "num"}>{table}</td>
                <td className="num">{history}</td>
                <td className="num">{deals}</td>
                <td className="num">{rows}</td>
                <td className="num">{match}</td>
              </tr>
            ))}
            <tr className="ngap-sum">
              <td className="ngap-id">סה״כ</td><td>—</td>
              <td className="num">93</td><td className="num">52</td>
              <td className="num">161</td><td className="num">188</td>
              <td className="num">140</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="ngap-card">
        <h3>איך לקרוא את הנספח</h3>
        <p className="ngap-p">
          <strong>מיסים · עסקאות</strong> סופר מכירות: שורות של אותה תת־חלקה ואותו יום מכירה
          נספרות כמכירה אחת כשהחלקים שבהן מסתכמים לנכס שלם לכל היותר.{" "}
          <strong>מיסים · שורות</strong> סופר את השורות שהאתר מציג בפועל, וההפרש בין השניים,
          27 שורות, הוא מכירות החלקים. <strong>נדל״ן · בטבלה</strong>{" "}
          ו<strong>נדל״ן · בהיסטוריה</strong> מופרדים כי זה עיקר הממצא הראשון: רק סכומם
          מתקרב לעמודת העסקאות של רשות המיסים. <strong>מתלכדות</strong>: אותו תאריך ואותו
          סכום, בסטייה של עיגול לאלף לכל היותר.
        </p>
      </div>

      {/* ═══ what a data user should take from this ═══ */}
      <div className="ngap-section">מסקנות למי שמשתמש בנתונים</div>

      <div className="ngap-card">
        <ol className="ngap-actions">
          <li>
            <strong>שורות בטבלת אתר הנדל״ן הן נכסים, לא מכירות.</strong> המכירות הקודמות של
            כל נכס מוצגות רק בחלון שנפתח בלחיצה; מי שסופר שורות, או קורא את ״נמצאו N
            עסקאות״, יחסיר יותר משליש מהמכירות.
          </li>
          <li>
            <strong>גם פתיחת כל השורות אינה מבטיחה תמונה מלאה.</strong> שבע מכירות במדגם
            אינן מופיעות באתר הנדל״ן בשום מקום, וחמש מהן רשומות בשטח 0.
          </li>
          <li>
            <strong>אין להסיק מחיר נכס מלא ממספר שמוצג באתר הנדל״ן.</strong> ברוב מכירות
            החלקים מוצג שם סכום החלק שנמכר, בלי ציון שמדובר בחלק.
          </li>
          <li>
            <strong>אין להצליב בין המאגרים לפי שם יישוב.</strong> שתיים מעשר החלקות נושאות
            שני שמות שונים; הצלבה כזו נכשלת בלי להודיע.
          </li>
          <li>
            <strong>שנת בנייה ושטח אינם שדות שאפשר להסתמך עליהם</strong>: הם נבדלים בין
            המאגרים, ולעיתים בתוך אותו מאגר.
          </li>
          <li>
            <strong>היעדר דף באתר הנדל״ן אינו ראיה להיעדר עסקאות.</strong> חלקה עם תשע
            מכירות מתועדות מחזירה שם ״המיקום לא נמצא״.
          </li>
          <li>
            <strong>גם טבלה ריקה אינה ראיה.</strong> כרבע מהטעינות מחזירות ״0 עסקאות״
            לחלקה שיש בה עסקאות; יש לרענן לפני שמסיקים.
          </li>
          <li>
            <strong>״מחיר העסקה״ באתר הנדל״ן הוא שווי המכירה, לא התמורה שהוצהרה.</strong>{" "}
            השניים נבדלים לעיתים בעשרות אחוזים, ורק מאגר מיסוי מקרקעין מציג את שניהם.
          </li>
        </ol>
        <div className="ngap-takeaway is-good">
          <b>ולבסוף, הצד החיובי.</b> כשקוראים את אתר הנדל״ן במלואו, כולל חלונות ההיסטוריה,
          שני המקורות מסכימים: 140 מתוך 145 המכירות שהאתר מציג מתלכדות עם רשות המיסים על
          התאריך ועל הסכום, 120 מהן עד השקל. רק שלוש נושאות פער סכום שאינו עיגול לאלף,
          והגדול שבהם, 18%, בעסקה מפברואר 2004.
        </div>
      </div>

      {/* ═══ method ═══ */}
      <div className="ngap-section">שיטת הבדיקה</div>

      <div className="ngap-card">
        <p className="ngap-p">
          נבחרו עשר חלקות אקראיות שיש להן עסקאות בשני האתרים, בעשרה יישובים שונים,
          ממגדל העמק ונופית בצפון ועד באזור המרכז ותל אביב. הבחירה נעשתה בהגרלה, בלי סינון
          לפי גודל החלקה או מספר העסקאות בה.
        </p>
        <p className="ngap-p">
          כל חלקה נפתחה בשני האתרים ונקראה במלואה: באתר הנדל״ן דרך דף הגוש והחלקה, עם מעבר
          על כל עמודי טבלת העסקאות ופתיחת חלון ״עסקאות קודמות לנכס״ בכל שורה שיש לה היסטוריה;
          ובאתר רשות המיסים דרך חיפוש גוש/חלקה מלא, כולל מעבר על כל עמודי התוצאות. כל מספר
          בדוח הוא מה שהאתרים הציגו בפועל ב־9 וב־10 בספטמבר 2026, וניתן לשחזור בחיפוש חוזר של
          אותו גוש וחלקה.
        </p>
        <p className="ngap-p">
          בשל התופעה שמתוארת בסעיף 10, כל תוצאה של ״0 עסקאות״ נבדקה בטעינה חוזרת ולא נרשמה
          כממצא עד שהתקבלה פעמיים ברציפות. הבדיקה כולה בוצעה פעמיים, במרווח של שעות ומכתובת
          רשת שונה, והספירות בשתי הריצות היו זהות.
        </p>
        <p className="ngap-p">
          עשר חלקות אינן מספיקות כדי לאמוד שכיחות ארצית של כל תופעה. הן מספיקות כדי לזהות{" "}
          <strong>סוגי</strong> פערים ולכמת אותם בתוך המדגם, וזה מה שהדוח עושה.
        </p>
        <p className="ngap-p" style={{ marginBottom: 0 }}>
          הבדיקה בוצעה ב־9 וב־10 בספטמבר 2026 מול הגרסאות החיות של{" "}
          <span dir="ltr">nadlan.gov.il</span> ושל{" "}
          <span dir="ltr">nadlan.taxes.gov.il</span>.
        </p>
      </div>

      {/* ═══ the screenshots ═══ */}
      <div className="ngap-section">נספח תמונות · הראיות כפי שהוצגו באתרים</div>

      <p className="ngap-p">
        האיורים מסודרים חלקה אחר חלקה. לכל חלקה מוצגות תחילה הרשומות של{" "}
        <strong>מאגר מיסוי מקרקעין</strong> ואחריהן העסקאות שמציג{" "}
        <strong>אתר הנדל״ן</strong>: דפי הטבלה, ואחריהם כל שורה שיש לה היסטוריה כשחלון
        ״עסקאות קודמות לנכס״ פתוח. כך אפשר לעבור על התמונות לבדן ולראות את הפער. לחיצה על
        איור פותחת תצוגה מקדימה, וממנה אפשר לפתוח אותו בכרטיסייה נפרדת.
      </p>

      {groups.map((g) => (
        <section key={g.place}>
          <div className="ngap-figgroup">{g.place}</div>
          {g.tax.length > 0 && (
            <>
              <div className="ngap-figsub is-tax">
                מאגר מיסוי מקרקעין <span dir="ltr">nadlan.taxes.gov.il</span>
              </div>
              <Thumbs figs={g.tax} onOpen={openByNumber} />
            </>
          )}
          {(g.nadlanPages.length > 0 || g.nadlanHistory.length > 0) && (
            <>
              <div className="ngap-figsub is-nadlan">
                אתר הנדל״ן הממשלתי <span dir="ltr">nadlan.gov.il</span>
              </div>
              <Thumbs figs={g.nadlanPages} onOpen={openByNumber} />
              {g.nadlanHistory.length > 0 && (
                <>
                  <div className="ngap-figkind">
                    חלונות ״עסקאות קודמות לנכס״ ({g.nadlanHistory.length})
                  </div>
                  <Thumbs figs={g.nadlanHistory} onOpen={openByNumber} />
                </>
              )}
            </>
          )}
        </section>
      ))}

      {openIdx != null && (
        <Lightbox index={openIdx} onClose={close} onMove={move} />
      )}
    </div>
  );
}
