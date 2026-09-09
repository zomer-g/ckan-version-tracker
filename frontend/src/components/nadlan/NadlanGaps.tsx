/**
 * "פערים מול מיסוי מקרקעין" — the fifth tab of נדל"ן לעם.
 *
 * Ten random parcels were read page by page on both nadlan.gov.il and
 * nadlan.taxes.gov.il on 9 September 2026 and compared row against row. The
 * headline is that the two sites disagree by definition rather than by
 * coverage, and this component is the write-up: the findings, the numbers
 * behind each one, and the 36 screenshots they were read off.
 *
 * It lives behind a lazy import because none of it, least of all the appendix,
 * is needed by the four lookup tabs. The screenshots are static files under
 * public/nadlan-gaps/, and the grid shows small crops rather than the originals
 * so opening the tab costs 0.56 MB of pictures instead of 2.9 MB.
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

type Group = { place: string; tax: GapFigure[]; nadlan: GapFigure[] };

function groupFigures(): Group[] {
  const out: Group[] = [];
  for (const f of GAP_FIGURES) {
    let g = out[out.length - 1];
    if (!g || g.place !== f.place) {
      g = { place: f.place, tax: [], nadlan: [] };
      out.push(g);
    }
    (f.src === "tax" ? g.tax : g.nadlan).push(f);
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
          הממצא המרכזי הוא ש<strong>הפער אינו פער כיסוי אלא פער הגדרה</strong>: אתר הנדל״ן
          מפרסם עסקה אחת לכל תת־חלקה, האחרונה שבוצעה בה, בעוד רשות המיסים מפרסמת כל הצהרה.
          93 עסקאות מול 159, וכמעט כולן מוסברות בכך. מה שנותר אחרי ההסבר הוא שמונה עסקאות
          שסכומן שונה בין האתרים, חלקה שלמה שאין לה דף כלל באתר הנדל״ן, שטח שנבדל בין
          המקורות עד כדי פי עשרה, ועובדה שאינה מסומנת בשום מקום: לכל עסקה יש שני סכומים
          רשמיים, ואתר הנדל״ן מפרסם רק אחד מהם.
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
          <b>93</b><span>עסקאות באתר הנדל״ן, בעשר החלקות</span>
        </div>
        <div className="ngap-stat is-tax">
          <b>159</b><span>עסקאות אצל רשות המיסים (188 שורות)</span>
        </div>
        <div className="ngap-stat is-good">
          <b>77</b><span>מתלכדות במדויק בין המקורות</span>
        </div>
        <div className="ngap-stat is-bad">
          <b>8</b><span>עסקאות שסכומן שונה בין האתרים</span>
        </div>
      </div>

      {/* ═══ part א: the structural gap ═══ */}
      <div className="ngap-section">חלק א׳ · הפער המבני</div>

      <div className="ngap-card">
        <h3>
          <span className="ngap-num">1</span>
          אתר הנדל״ן מציג עסקה אחת לכל תת־חלקה, האחרונה בלבד
        </h3>
        <p className="ngap-p">
          זה ההסבר לרוב הפער. אתר הנדל״ן אינו היסטוריית עסקאות אלא תמונת מצב: לכל תת־חלקה
          רשומה מוצגת רק המכירה האחרונה שבוצעה בה. רשות המיסים, לעומת זאת, מציגה כל הצהרה
          שנרשמה אי־פעם.
        </p>
        <div className="ngap-evidence">
          <span className="ngap-cap">
            גוש 17457 חלקה 63 · תת־חלקה 027, חמש מכירות אצל רשות המיסים
          </span>
          {`07/10/2008    160,000 ₪
07/10/2010    200,000 ₪
12/12/2011    315,000 ₪
15/01/2014    356,000 ₪
04/04/2021    510,000 ₪   `}
          <em>← רק זו מוצגת באתר הנדל״ן</em>
        </div>
        <p className="ngap-p">
          הכלל נבדק על כל עשר החלקות: 93 העסקאות שמציג אתר הנדל״ן מול{" "}
          <strong>99 תתי־חלקה</strong> שיש להן עסקאות אצל רשות המיסים, ו־
          <strong>77 מהן מתלכדות בדיוק</strong> על התאריך ועל הסכום של העסקה האחרונה באותה
          תת־חלקה. השאר מוסברות בסעיפים 2–9.
        </p>
        <Refs ns={[1, 2, 3, 4, 5, 6, 7, 29, 30]} onOpen={openByNumber} />
        <p className="ngap-p">
          דיוק אחד: היחידה של אתר הנדל״ן היא <strong>נכס במספור שלו</strong>. בבניין מחולק
          היא מתלכדת עם תת־החלקה של רשות המיסים; בחלקה שאינה מחולקת, כמו גוש 7787 חלקה 464
          בתל מונד, שכל חמש הרשומות שלה אצל רשות המיסים תחת תת־חלקה 000, אתר הנדל״ן ממספר
          את הנכסים בעצמו ומציג שש שורות נפרדות.
        </p>
        <div className="ngap-takeaway">
          <b>המשמעות.</b> כל השוואה שסופרת עסקאות מול עסקאות תדווח על כ־40% ״חוסר״ באתר
          הנדל״ן שאינו חוסר. מי שזקוק להיסטוריית המכירות של דירה מסוימת חייב את מאגר רשות
          המיסים; מי שמסתפק במחיר העדכני של חלקה יכול להישאר באתר הנדל״ן.
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
        <Refs ns={[1, 4, 9, 20, 26, 27, 31]} onOpen={openByNumber} />
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
          סוגי עסקאות שאתר הנדל״ן אינו מציג כלל
        </h3>
        <p className="ngap-p">
          מעבר לכלל ״אחת לתת־חלקה״, יש תתי־חלקה שלמות שאינן מופיעות באתר הנדל״ן. במדגם הזה
          כולן חולקות מכנה משותף: סכום חריג בקטנותו או שטח רשום 0.
        </p>
        <Refs ns={[1, 3, 17]} onOpen={openByNumber} />
        <div className="ngap-tablewrap">
          <table>
            <thead>
              <tr>
                <th>חלקה</th><th>תת־חלקה</th>
                <th className="num">תאריך</th>
                <th className="num">סכום</th>
                <th className="num">שטח רשום</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="ngap-id">17457-63</td><td className="ngap-id">018</td>
                <td className="num">01/07/2001</td>
                <td className="num ngap-vt">45,880</td><td className="num">70</td>
              </tr>
              <tr>
                <td className="ngap-id">17457-63</td><td className="ngap-id">024</td>
                <td className="num">15/09/2019</td>
                <td className="num ngap-vt">48,100</td><td className="num">70</td>
              </tr>
              <tr>
                <td className="ngap-id">10236-99</td><td className="ngap-id">012</td>
                <td className="num">30/10/2002</td>
                <td className="num ngap-vt">900,000</td><td className="num ngap-vc">0</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="ngap-p">
          זו כנראה מדיניות סינון ולא תקלה, אך היא אומרת שספירת עסקאות מאתר הנדל״ן תיתן
          מספר נמוך מהאמת גם אחרי שמנטרלים את כלל תת־החלקה.
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
        <Refs ns={[8, 10, 14, 15, 16, 19, 22, 24, 26, 28]} onOpen={openByNumber} />
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
        <Refs ns={[8, 14, 16, 19]} onOpen={openByNumber} />
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
        <Refs ns={[8, 10, 16, 19, 22, 29, 30]} onOpen={openByNumber} />
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
        <Refs ns={[33, 34]} onOpen={openByNumber} />
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
        <Refs ns={[14, 15]} onOpen={openByNumber} />
        <p className="ngap-p">
          זהו המקרה היחיד בכל המדגם שבו לאתר הנדל״ן יש תאריך עסקה שאין לו מקבילה אצל רשות
          המיסים. בכל 92 העסקאות האחרות, כל מה שמופיע באתר הנדל״ן קיים גם במאגר מיסוי
          מקרקעין.
        </p>
        <div className="ngap-takeaway">
          <b>הכיוון ברור.</b> מאגר מיסוי מקרקעין הוא כמעט תמיד קבוצה מכילה של אתר הנדל״ן.
          חריגה אחת מתוך 93 מספיקה כדי לומר שלא תמיד, אך לא כדי להצדיק התייחסות לאתר
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
        <Refs ns={[33, 34, 35, 36]} onOpen={openByNumber} />
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
        <Refs ns={[15]} onOpen={openByNumber} />
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
              <th className="num">נדל״ן<br />עסקאות מוצגות</th>
              <th className="num">מיסים<br />תתי־חלקה</th>
              <th className="num">מיסים<br />עסקאות</th>
              <th className="num">מיסים<br />שורות</th>
            </tr>
          </thead>
          <tbody>
            {([
              ["17457-63", "מגדל העמק", "20", "22", "46", "49"],
              ["7151-316", "בת ים", "12", "12", "18", "24"],
              ["12593-23", "נופית", "1", "1", "1", "1"],
              ["7787-464", "תל מונד", "6", "1", "4", "5"],
              ["10236-99", "קריית ביאליק", "39", "40", "56", "58"],
              ["7136-208", "בת ים", "9", "9", "14", "26"],
              ["17032-221", "כפר תבור", "2", "2", "3", "3"],
              ["6982-34", "תל אביב‑יפו", "3", "3", "7", "12"],
              ["5134-28", "שדמה / כפר מרדכי", "אין דף", "8", "9", "9"],
              ["7369-15", "כוכב יאיר / צור יגאל", "1", "1", "1", "1"],
            ] as const).map(([parcel, town, shown, subs, deals, rows], i) => (
              <tr key={i}>
                <td className="ngap-id">{parcel}</td>
                <td>{town}</td>
                <td className={shown === "אין דף" ? "num ngap-vc" : "num"}>{shown}</td>
                <td className="num">{subs}</td>
                <td className="num">{deals}</td>
                <td className="num">{rows}</td>
              </tr>
            ))}
            <tr className="ngap-sum">
              <td className="ngap-id">סה״כ</td><td>—</td>
              <td className="num">93</td><td className="num">99</td>
              <td className="num">159</td><td className="num">188</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="ngap-card">
        <h3>איך לקרוא את הנספח</h3>
        <p className="ngap-p">
          <strong>מיסים · עסקאות</strong> סופר צירופים של תת־חלקה ויום מכירה.{" "}
          <strong>מיסים · שורות</strong> סופר את השורות שהאתר מציג בפועל, וההפרש בין
          השניים, 29 שורות, הוא בדיוק מכירות החלקים: כמה רוכשים שרכשו כל אחד חלק מאותו
          נכס באותו יום. <strong>נדל״ן · עסקאות מוצגות</strong> קרוב לעמודת תתי־החלקה ולא
          לעמודת העסקאות, וזו התמצית המספרית של הממצא הראשון.
        </p>
      </div>

      {/* ═══ what a data user should take from this ═══ */}
      <div className="ngap-section">מסקנות למי שמשתמש בנתונים</div>

      <div className="ngap-card">
        <ol className="ngap-actions">
          <li>
            <strong>ספירת עסקאות באתר הנדל״ן היא ספירת נכסים, לא ספירת מכירות.</strong>{" "}
            מי שמחשב נפח פעילות באזור יקבל מספר נמוך משמעותית מהאמת.
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
            <strong>שנת בנייה ושטח אינם שדות שאפשר להסתמך עליהם</strong>, הם נבדלים בין
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
          <b>ולבסוף, הצד החיובי.</b> אחרי שמנטרלים את הבדל היחידות, שני המקורות מסכימים:
          77 מתוך 93 העסקאות שאתר הנדל״ן מציג מתלכדות בדיוק עם רשות המיסים, ורק אחת מהן,
          עסקה מפברואר 2004, נושאת פער שאי אפשר לייחס לעיגול או למכירת חלקים.
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
          על כל עמודי טבלת העסקאות; ובאתר רשות המיסים דרך חיפוש גוש/חלקה מלא, כולל מעבר על
          כל עמודי התוצאות. כל מספר בדוח הוא מה שהאתרים הציגו בפועל ב־9 בספטמבר 2026, וניתן
          לשחזור בחיפוש חוזר של אותו גוש וחלקה.
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
          הבדיקה בוצעה ב־9 בספטמבר 2026 מול הגרסאות החיות של{" "}
          <span dir="ltr">nadlan.gov.il</span> ושל{" "}
          <span dir="ltr">nadlan.taxes.gov.il</span>.
        </p>
      </div>

      {/* ═══ the screenshots ═══ */}
      <div className="ngap-section">נספח תמונות · הראיות כפי שהוצגו באתרים</div>

      <p className="ngap-p">
        האיורים מסודרים חלקה אחר חלקה. לכל חלקה מוצגות תחילה הרשומות של{" "}
        <strong>מאגר מיסוי מקרקעין</strong> ואחריהן העסקאות שמציג{" "}
        <strong>אתר הנדל״ן</strong>, כך שאפשר לעבור על התמונות לבדן ולראות את הפער. לחיצה
        על איור פותחת תצוגה מקדימה, וממנה אפשר לפתוח אותו בכרטיסייה נפרדת.
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
          {g.nadlan.length > 0 && (
            <>
              <div className="ngap-figsub is-nadlan">
                אתר הנדל״ן הממשלתי <span dir="ltr">nadlan.gov.il</span>
              </div>
              <Thumbs figs={g.nadlan} onOpen={openByNumber} />
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
