import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { Link } from "react-router-dom";
import { useTranslation } from "react-i18next";
import Modal from "./a11y/Modal";

/**
 * The "!" in the header: a plain-language explanation of what the site is,
 * what scraping means, and why every number here deserves a second look at
 * the source before anyone relies on it.
 *
 * It opens by itself on a visitor's first visit (like a cookie notice) and is
 * remembered per browser afterwards; the button reopens it at any time.
 *
 * The button carries visible text beside the glyph (the "!" alone is not a
 * name, WCAG 2.5.3 Label in Name), and the dialog is portalled to <body> so
 * the shared Modal can make the rest of the page inert and so the header's
 * white-link styling does not leak into the dialog's text.
 */

const SEEN_KEY = "over_caveat_seen_v1";

function wasSeen(): boolean {
  try {
    return localStorage.getItem(SEEN_KEY) === "1";
  } catch {
    // Storage blocked (private mode, site data off): treat as seen rather
    // than greet the visitor with the dialog on every single page load.
    return true;
  }
}

function markSeen() {
  try {
    localStorage.setItem(SEEN_KEY, "1");
  } catch {
    /* nothing to remember it in */
  }
}

type Copy = {
  button: string;
  buttonLong: string;
  title: string;
  whatTitle: string;
  what: string[];
  scrapingTitle: string;
  scraping: string[];
  risksTitle: string;
  risksLead: string;
  risksLeadTail: string;
  risksWhy: string[];
  kindsLead: string;
  kinds: { name: string; text: string }[];
  sourceErrors: string;
  todoTitle: string;
  todoLead: string;
  todoLeadTail: string;
  todo: string[];
  more: string;
  about: string;
  sources: string;
  gotIt: string;
};

const COPY: Record<"he" | "en", Copy> = {
  he: {
    button: "חשוב לדעת",
    buttonLong: "חשוב לדעת: איך האתר עובד ומה צריך לבדוק לפני שמשתמשים בנתונים",
    title: "חשוב לדעת לפני שמשתמשים בנתונים",
    whatTitle: "מה האתר הזה עושה?",
    what: [
      "משרדי ממשלה, רשויות וגופים ציבוריים מפרסמים הרבה מידע באתרים שלהם. המידע הזה פומבי, אבל הוא מפוזר בהרבה מקומות, מתעדכן בלי הודעה, ולפעמים נמחק.",
      "האתר הזה אוסף את המידע בשבילכם. תוכנות קטנות שלנו נכנסות לאתרים האלה באופן קבוע, מורידות את המידע, שומרות כל גרסה שלו, ומסדרות אותו כך שיהיה קל לחפש בו, להשוות בין גרסאות ולחבר בין מאגרים שונים. לכל מאגר יש קישור למקור שממנו הוא נלקח.",
    ],
    scrapingTitle: "מה זה סקרייפינג?",
    scraping: [
      "סקרייפינג (Scraping) הוא איסוף אוטומטי של מידע מאתרי אינטרנט. במקום שאדם ייכנס לאתר, יעבור עמוד אחרי עמוד ויעתיק את מה שכתוב, תוכנה עושה את זה במקומו.",
      "זה חוסך הרבה עבודה ומאפשר לאסוף כמויות גדולות של מידע. אבל התוכנה לא \"מבינה\" את האתר כמו אדם. היא עושה רק מה שלימדו אותה לעשות, ואם האתר משתנה, היא עלולה לפספס דברים בלי לשים לב.",
    ],
    risksTitle: "למה צריך להיזהר?",
    risksLead: "תמיד קיים חשש שהאיסוף לא בוצע בצורה מלאה, ולכן חלק מהמידע חסר.",
    risksLeadTail: "זה יכול לקרות, למשל, כי:",
    risksWhy: [
      "אתר המקור היה איטי או לא זמין בזמן האיסוף.",
      "אתר המקור שינה את המבנה שלו, והתוכנה כבר לא מוצאת את כל הפרטים.",
      "אתר המקור מציג רק חלק מהתוצאות, או מסתיר חלק מהן מאחורי חיפוש או כפתור.",
      "היו שגיאות בזמן הקריאה או הסידור של הקבצים.",
    ],
    kindsLead: "המידע החסר יכול להיות משני סוגים:",
    kinds: [
      { name: "חוסרים אקראיים:", text: "פה ושם חסרה שורה או מסמך, בלי סיבה מיוחדת." },
      {
        name: "חוסרים שיוצרים הטיה:",
        text: "חסר דווקא סוג מסוים של מידע, למשל תקופה מסוימת, אזור מסוים או סוג מסוים של מסמכים. חוסר כזה מסוכן במיוחד, כי הוא יכול להוביל למסקנה שגויה שנראית משכנעת.",
      },
    ],
    sourceErrors:
      "בנוסף, גם המידע שבמקור עצמו יכול להכיל טעויות. אנחנו לא מתקנים את המידע, אלא מציגים אותו כפי שנאסף.",
    todoTitle: "מה לעשות?",
    todoLead: "התייחסו לנתונים כאן בחשדנות.",
    todoLeadTail: "הם נקודת פתיחה טובה, לא מילה אחרונה.",
    todo: [
      "לפני שמשתמשים בנתונים בפועל (בכתבה, במחקר, בבקשה לרשות או בבית משפט), פתחו את הקישור למקור ובדקו ידנית שהמידע שם זהה למה שמופיע כאן.",
      "אם משהו נראה חסר או מוזר, למשל מספר קטן מהצפוי או תקופה ריקה, אל תניחו שזה המצב האמיתי. בדקו במקור.",
      "שימו לב לתאריך שבו המידע נאסף. ייתכן שבמקור כבר יש גרסה חדשה יותר.",
    ],
    more: "רוצים לדעת עוד?",
    about: "אודות האתר",
    sources: "רשימת המקורות",
    gotIt: "הבנתי",
  },
  en: {
    button: "Important",
    buttonLong: "Important: how this site works and what to check before using the data",
    title: "Important to know before using the data",
    whatTitle: "What does this site do?",
    what: [
      "Government ministries, local authorities and public bodies publish a lot of information on their websites. This information is public, but it is spread across many places, changes without notice, and is sometimes deleted.",
      "This site collects that information for you. Small programs of ours visit those websites regularly, download the information, keep every version of it, and organize it so it is easy to search, to compare versions and to connect different datasets. Every dataset links back to the source it was taken from.",
    ],
    scrapingTitle: "What is scraping?",
    scraping: [
      "Scraping is the automatic collection of information from websites. Instead of a person visiting a website, going page by page and copying what it says, a program does it for them.",
      "This saves a lot of work and makes it possible to collect large amounts of information. But the program does not \"understand\" the website the way a person does. It only does what it was taught to do, and if the website changes, it may miss things without noticing.",
    ],
    risksTitle: "Why be careful?",
    risksLead: "There is always a risk that the collection was not complete, so some of the information is missing.",
    risksLeadTail: "This can happen, for example, because:",
    risksWhy: [
      "The source website was slow or unavailable at the time of collection.",
      "The source website changed its structure, and the program no longer finds all the details.",
      "The source website shows only part of the results, or hides some of them behind a search or a button.",
      "There were errors while reading or organizing the files.",
    ],
    kindsLead: "Missing information can be of two kinds:",
    kinds: [
      { name: "Random gaps:", text: "a row or a document is missing here and there, for no particular reason." },
      {
        name: "Gaps that create bias:",
        text: "a specific kind of information is missing, for example a certain period, a certain area or a certain type of document. Gaps like this are especially dangerous, because they can lead to a wrong conclusion that looks convincing.",
      },
    ],
    sourceErrors:
      "In addition, the information at the source itself may contain errors. We do not correct the information; we show it as it was collected.",
    todoTitle: "What should you do?",
    todoLead: "Treat the data here with suspicion.",
    todoLeadTail: "It is a good starting point, not the final word.",
    todo: [
      "Before actually using the data (in an article, a study, a request to an authority or in court), open the link to the source and check by hand that the information there matches what appears here.",
      "If something looks missing or odd, for example a number smaller than expected or an empty period, do not assume that is the real situation. Check the source.",
      "Pay attention to the date the information was collected. The source may already have a newer version.",
    ],
    more: "Want to know more?",
    about: "About this site",
    sources: "List of sources",
    gotIt: "Got it",
  },
};

export default function DataCaveatNotice() {
  const { i18n } = useTranslation();
  const lang: "he" | "en" = i18n.language === "en" ? "en" : "he";
  const c = COPY[lang];
  const [open, setOpen] = useState(false);
  const btnRef = useRef<HTMLButtonElement>(null);

  // First visit in this browser: open by itself, once.
  useEffect(() => {
    if (!wasSeen()) setOpen(true);
  }, []);

  const close = useCallback(() => {
    markSeen();
    setOpen(false);
    // After an automatic first-visit open there is no opener to return to, so
    // focus lands on the "!" button: it also shows where to find this again.
    requestAnimationFrame(() => btnRef.current?.focus());
  }, []);

  return (
    <>
      <button
        ref={btnRef}
        type="button"
        className="btn-caveat"
        onClick={() => setOpen(true)}
        aria-haspopup="dialog"
        aria-label={c.buttonLong}
      >
        <span className="btn-caveat-mark" aria-hidden="true">!</span>
        <span className="btn-caveat-text" aria-hidden="true">{c.button}</span>
      </button>

      {open &&
        createPortal(
          // Language and direction on the whole dialog (title, close, footer),
          // not just the body: the page itself stays RTL in English mode.
          <div lang={lang} dir={lang === "he" ? "rtl" : "ltr"}>
          <Modal
            title={c.title}
            onClose={close}
            width="40rem"
            closeLabel={lang === "he" ? "סגירת החלון" : "Close"}
            footer={
              <button type="button" className="btn btn-primary" onClick={close}>
                {c.gotIt}
              </button>
            }
          >
            <div className="caveat-body">
              <section aria-labelledby="caveat-what">
                <h3 id="caveat-what">{c.whatTitle}</h3>
                {c.what.map((p, i) => <p key={i}>{p}</p>)}
              </section>

              <section aria-labelledby="caveat-scraping">
                <h3 id="caveat-scraping">{c.scrapingTitle}</h3>
                {c.scraping.map((p, i) => <p key={i}>{p}</p>)}
              </section>

              <section aria-labelledby="caveat-risks" className="caveat-risks">
                <h3 id="caveat-risks">{c.risksTitle}</h3>
                <p>
                  <strong>{c.risksLead}</strong> {c.risksLeadTail}
                </p>
                <ul>
                  {c.risksWhy.map((li, i) => <li key={i}>{li}</li>)}
                </ul>
                <p>{c.kindsLead}</p>
                <ul>
                  {c.kinds.map((k, i) => (
                    <li key={i}>
                      <strong>{k.name}</strong> {k.text}
                    </li>
                  ))}
                </ul>
                <p>{c.sourceErrors}</p>
              </section>

              <section aria-labelledby="caveat-todo" className="caveat-todo">
                <h3 id="caveat-todo">{c.todoTitle}</h3>
                <p>
                  <strong>{c.todoLead}</strong> {c.todoLeadTail}
                </p>
                <ol>
                  {c.todo.map((li, i) => <li key={i}>{li}</li>)}
                </ol>
              </section>

              <p className="caveat-more">
                {c.more}{" "}
                <Link to="/about" onClick={close}>{c.about}</Link>
                {" · "}
                <Link to="/sources" onClick={close}>{c.sources}</Link>
              </p>
            </div>
          </Modal>
          </div>,
          document.body
        )}
    </>
  );
}
