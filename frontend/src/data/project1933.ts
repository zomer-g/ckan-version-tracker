/**
 * פרויקט 1933 — what מידע לעם (odata.org.il) holds about government decision
 * 1933 (2016, "improving government data transfer and opening government
 * databases to the public").
 *
 * The bulk of it is one cross-government FOI request filed by עמותת הצלחה on
 * 12.9.2024 to every ministry, asking for the documents behind each one's
 * implementation of the decision (meeting summaries, damage reviews, work
 * plans); each ministry's answer was published on מידע לעם as its own
 * dataset. The list below is the catalog of those answers, taken from
 * odata.org.il's CKAN API (package_search q=1933, cross-checked against every
 * הצלחה dataset since 9/2024 that mentions מאגרי מידע). Two ministries' answers
 * were uploaded twice (byte-identical files, a day or so apart) and are folded
 * into one entry with the duplicate under `also`.
 *
 * The answers themselves are kept in the repo under docs/project1933/.
 *
 * `finding` is the editorial summary of what the ministry's answer actually
 * says, read from the documents (not the file names). It stays null when the
 * answer fits none of the statuses; `findingNote` then says why.
 */

export type Finding1933Status =
  /** No steps at all: no forum, no meetings, no databases opened. */
  | "nothing"
  /** Some steps, short of what the decision required. */
  | "partial"
  /** Real implementation: a named owner, a database list, publications. */
  | "acted"
  /** Refused or did not answer the substance. */
  | "refused";

export interface Response1933 {
  ministry: string;
  /** When the answer was uploaded to מידע לעם (not the answer's own date). */
  date: string;
  url: string;
  /** Duplicate uploads of the same answer. */
  also?: string[];
  /** Attached documents, without the e-mail signature images. */
  files: string[];
  /** How many inline e-mail images (logos, signatures) were uploaded too. */
  images: number;
  finding: null | { status: Finding1933Status; summary: string };
  /** Why `finding` is null, shown in its place. */
  findingNote?: string;
}

export const ODATA_SEARCH_URL = "https://www.odata.org.il/dataset?q=1933";

/**
 * The request itself, as quoted in full under the משרד החקלאות answer (the
 * reply was written on top of the original e-mail of 12.9.2024).
 */
export const REQUEST_1933 = {
  date: "2024-09-12",
  sender: "עו\"ד אלעד מן, יועמ\"ש הצלחה",
  intro:
    "בהמשך להחלטה מספר 1933 של הממשלה מיום 30.08.2016 ובהתאם להוראות חוק חופש המידע, " +
    "נבקש לקבל את המידע הבא:",
  items: [
    "כלל סיכומי הדיונים ופירוט הישיבות שנערכו הנוגעים ליישום החלטת הממשלה האמורה ולפרסום מאגרי המידע.",
    "העתק כל תחקיר או סיכום שנערך ואשר עניינו בנזקים בפועל או אפשריים בשל פרסום מידע מהמאגרים כאמור.",
    "העתק תוכניות העבודה שנערכו בקשר עם מיפוי ופרסום המאגרים, מאז החלטת הממשלה, ומסמכים המתארים את ביצוע תוכניות העבודה, ככל וקיימים.",
  ],
  get text(): string {
    return [this.intro, ...this.items.map((t, i) => `${i + 1}. ${t}`)].join("\n");
  },
  source_url: "https://www.odata.org.il/dataset/6a2b33b0-f809-4a80-b2a8-9fda497b4573",
  source_label: "תשובת משרד החקלאות, שבסופה מצוטט המייל המקורי",
};

/** The budget-tracking file: how much each ministry was allotted for 1933. */
export const BUDGET_DATASET = {
  title: "תקצוב החלטת ממשלה 1933",
  date: "2021-12-08",
  url: "https://www.odata.org.il/dataset/64c698d7-02ef-4dd2-a6af-585aed4e4645",
  summary:
    "פירוט תקצוב משרדי הממשלה ויחידות הסמך לצורך פתיחת מאגרי המידע על פי ההחלטה. " +
    "התקצוב נפרס על פני השנים 2017 עד 2021 ומסתכם ביותר מ-40 מיליון שקל.",
  publisher: "גיא זומר",
};

/** The answers to the cross-government request, oldest upload first. */
export const RESPONSES_1933: Response1933[] = [
  {
    ministry: "משרד הנגב, הגליל והחוסן הלאומי",
    date: "2024-10-10",
    url: "https://www.odata.org.il/dataset/49400a24-da68-4955-b27d-addeaa43f457",
    files: ["מענה הצלחה החמ 1933.pdf"],
    images: 0,
    finding: {
      status: "refused",
      summary:
        "התשובה עוסקת בהעברת מידע לציבור לפי חוק חופש המידע ובהעברת מידע בין משרדים, ואינה מתייחסת לישיבות, לתוכניות עבודה או לפרסום מאגרים.",
    },
  },
  {
    ministry: "משרד האנרגיה",
    date: "2024-11-05",
    url: "https://www.odata.org.il/dataset/a3b471e2-dc0f-4870-b889-1cf636a73294",
    files: ["MH_87_2018.pdf", "פורמט לנספח - הנגשת מאגרי מידע והעברות מידע - משרד האנרגיה - 2019.pdf", "MH_92_2018.xlsx", "dov_549_2024.pdf"],
    images: 6,
    finding: {
      status: "acted",
      summary:
        "התקיים דיון ב-21.3.2018 שחילק משימות בין יחידות המשרד; צורפו מיפוי מאגרים מ-2018 ורשימת מאגרים להנגשה ב-2019. המשרד מפרסם מאגרים ב-data.gov.il ומפנה לשבעה מאגרים נוספים שהונגשו בדרך אחרת.",
    },
  },
  {
    ministry: "משרד התרבות והספורט",
    date: "2024-11-11",
    url: "https://www.odata.org.il/dataset/a35657c6-82dd-403b-9fa3-5dd6e0a1db45",
    files: ["מענה לפניה.pdf", "עותק של הנגשת מאגרים - משרד התרבות והספורט.xlsx"],
    images: 0,
    finding: {
      status: "acted",
      summary:
        "ב-2017 מופו מאגרי המשרד לפי תבנית של מערך הדיגיטל, ובקובץ המיפוי רשומה ממונה על יישום ההחלטה; לפי התשובה המאגרים \"מפורסמים באתר דאטה הממשלתי\". לא אותרו פרוטוקולים או תוכניות עבודה נוספות.",
    },
  },
  {
    ministry: "משרד החקלאות",
    date: "2024-11-20",
    url: "https://www.odata.org.il/dataset/6a2b33b0-f809-4a80-b2a8-9fda497b4573",
    files: ["החלטת ממשלה 1933 - משרד החקלאות.docx"],
    images: 11,
    finding: {
      status: "nothing",
      summary:
        "\"לא נערכו דיונים הנוגעים ליישום החלטת הממשלה\" ולא הוכנו תוכניות עבודה. מבדק אבטחת מידע על מאגרים נבחרים לא נמסר (סעיף 9(א)(1)); המשרד מתכוון לפרסם מאגר מצלמות ב-2025.",
    },
  },
  {
    ministry: "משרד התפוצות והמאבק באנטישמיות",
    date: "2024-11-27",
    url: "https://www.odata.org.il/dataset/3360eab5-415f-47cf-b918-1c989c38d32c",
    files: ["מענה לבקשת מאגרי מידע.pdf"],
    images: 1,
    finding: null,
    findingNote: "המשרד השיב במשפט אחד: \"אין ברשות משרד התפוצות והמאבק באנטישמיות מאגרי מידע\". התשובה לא מתאימה לאף אחת מהקטגוריות, ולכן לא סווגה.",
  },
  {
    ministry: "המשרד לשירותי דת",
    date: "2024-11-28",
    url: "https://www.odata.org.il/dataset/68c54698-ddbb-40bd-b868-4350e1868838",
    also: ["https://www.odata.org.il/dataset/15968372-cf3d-4725-a4b5-9b5ccaa60fe1"],
    files: ["מענה לבקשת חופש מידע בנוגע להחלטת ממשלה 1933 בעניין מאגרי מידע לציבור.pdf"],
    images: 4,
    finding: {
      status: "partial",
      summary:
        "השתתף בישיבת זום אחת לכלל משרדי הממשלה ב-19.9.2024; אין במשרד תוכניות עבודה או מסמכי ביצוע, ולדבריו האחריות ליישום היא של מערך הדיגיטל. צירף קישור למאגריו ב-data.gov.il.",
    },
  },
  {
    ministry: "משרד הבינוי והשיכון",
    date: "2024-12-05",
    url: "https://www.odata.org.il/dataset/db15d4be-06f4-466e-a6b1-215524d5013a",
    files: ["מענה לבקשת מידע.pdf"],
    images: 3,
    finding: {
      status: "partial",
      summary:
        "התקיים דיון ב-6.9.2016 עם רשות התקשוב, אבל את סיכומו המשרד לא מסר והפנה למשרד הכלכלה ולמערך הדיגיטל. המשרד מפרסם תשעה מאגרים ב-data.gov.il; תיעוד של תוכניות עבודה לא נמצא.",
    },
  },
  {
    ministry: "משרד ירושלים ומסורת ישראל",
    date: "2024-12-10",
    url: "https://www.odata.org.il/dataset/fbab09a7-ed41-432c-a518-25183930eb1b",
    files: ["מענה סופי - אלעד מן - החלטת הממשלה 1933 מאגרי מידע - 9.12.24.pdf"],
    images: 2,
    finding: {
      status: "nothing",
      summary:
        "\"במשרדנו לא קיימים מאגרי מידע ולא התקיימו דיונים בנושא החלטת הממשלה\".",
    },
  },
  {
    ministry: "משרד החינוך",
    date: "2024-12-25",
    url: "https://www.odata.org.il/dataset/1118a606-2cd9-4350-80c6-398d371f56ec",
    files: ["מענה למבקר המדינה -מענה פעם אחת.pdf", "מענה למבקר המדינה -מענה פעם אחת.pdf", "1933 (1)_241115_154137.pdf", "מבקר המידנה מדיניות פעם אחת.pdf", "אלעד מן - החלטת ממשלה 1933- מאגרי מידע.pdf"],
    images: 10,
    finding: {
      status: "refused",
      summary:
        "התשובה מתארת שירותים דיגיטליים להורים ולבוגרים ואת המעבר להזדהות הלאומית, ולא מאגרי מידע; ישיבות הסטטוס עם מערך הדיגיטל \"לאו דווקא קשורות להחלטת ממשלה זו\". צורפו דוחות חיצוניים על מדיניות \"פעם אחת\".",
    },
  },
  {
    ministry: "המשרד לשיתוף פעולה איזורי",
    date: "2024-12-25",
    url: "https://www.odata.org.il/dataset/8606a2c8-4a5a-45e0-811c-87fb31f1b78e",
    files: ["עמותת הצלחה - בקשת חופש מידע-החלטת הממשלה 1933 בעניין מאגרי מידע (003).pdf"],
    images: 0,
    finding: {
      status: "nothing",
      summary:
        "\"לא נמצאו תיעודיים או מסמכים\" על דיונים, ישיבות או תוכניות עבודה הנוגעים ליישום ההחלטה ולמיפוי ופרסום המאגרים.",
    },
  },
  {
    ministry: "משרד התיירות",
    date: "2025-01-01",
    url: "https://www.odata.org.il/dataset/1493e5a1-7f17-493b-8cde-b5348600a637",
    files: ["החלטת ממשלה 1933 מאגרי מידע -מענה לבקשתך באמצעות חוק חופש המידע (002).pdf", "אופן הוצאת נתונים ל OPEN DATA.pdf", "חשיבה מחדש על תכני אתר מידע למטייל - סיכום ישיבה - 2 נוסף.pdf", "אופן טיפול בעיות ב OPEN DATA 3.pdf", "דגשים להוצאת מאגרים ל OPEN DATA 4.pdf", "חשיבה מחדש על תכני אתר מידע למטייל - סיכום ישיבה 5.pdf", "נוהל הוועדה להעברת מידע בין גופים ציבוריים.pdf"],
    images: 0,
    finding: {
      status: "acted",
      summary:
        "צורפו נהלים פנימיים להוצאת נתונים ל-data.gov.il מ-2018, שלפיהם המאגרים מתעדכנים שם אוטומטית פעם בשבוע, הנחיות לפרסום מאגרים וסיכומי ישיבות מ-2021 על אתר \"מידע למטייל\".",
    },
  },
  {
    ministry: "משרד הכלכלה",
    date: "2025-01-16",
    url: "https://www.odata.org.il/dataset/8fdc8461-24da-457c-881e-000c98706dc5",
    files: ["מענה.pdf"],
    images: 8,
    finding: {
      status: "partial",
      summary:
        "השתתף בישיבת זום אחת לכלל משרדי הממשלה ב-19.9.2024; לא נערכו פגישות נוספות ואין במשרד תוכניות עבודה או מסמכי ביצוע. צירף קישור למאגריו ב-data.gov.il.",
    },
  },
  {
    ministry: "משרד החדשנות, המדע והטכנולוגיה",
    date: "2025-01-21",
    url: "https://www.odata.org.il/dataset/b2843348-0df4-42bd-b89d-229ec16134fb",
    also: ["https://www.odata.org.il/dataset/2210694a-9159-4fe7-8707-f8cbf311f3f4"],
    files: ["מענה בעניין החלטת ממשלה 1933.pdf", "הנגשת מאגרים - משרד המדע.xlsx"],
    images: 0,
    finding: {
      status: "partial",
      summary:
        "לפי התשובה, ב-2017 מופו מאגרי המשרד לפי תבנית של מערך הדיגיטל והם \"מפורסמים באתר דאטה הממשלתי\"; בקובץ המיפוי שצורף שדות האחראים על היישום ריקים. לא אותרו פרוטוקולים או תוכניות עבודה נוספות.",
    },
  },
  {
    ministry: "משרד הפנים",
    date: "2025-02-26",
    url: "https://www.odata.org.il/dataset/dcec1a5b-a8cb-45fc-9e5a-1c888aba3632",
    files: ["Untitled_20250226_133523.pdf"],
    images: 0,
    finding: {
      status: "nothing",
      summary:
        "\"לא נמצאו סיכומי דיונים או תוכניות עבודה בנושא\".",
    },
  },
  {
    ministry: "המשרד לשיוויון חברתי",
    date: "2025-03-03",
    url: "https://www.odata.org.il/dataset/74da2369-bb6a-4eb0-956a-2651a5682593",
    files: ["תשובה - החלטת ממשלה 1933 בעניין מאגרי מידע 31-24 -.pdf"],
    images: 0,
    finding: {
      status: "partial",
      summary:
        "אגף מערכות המידע \"סקר מאגרי מידע של המשרד\" ופרסם מאגרים ב-data.gov.il. שאר התשובה מתארת פעולות מידע אחרות (מחוון האלימות המגדרית, מאגר חירום לאזרחים ותיקים); לא נמסרו סיכומי ישיבות או תוכניות עבודה.",
    },
  },
  {
    ministry: "משרד הביטחון",
    date: "2025-09-02",
    url: "https://www.odata.org.il/dataset/18c6b812-22f9-42a1-b913-8e4c78ad0055",
    files: ["מענה חח''מ 239134.pdf", "נספח א.pdf", "נספח ב.pdf", "נספח ג.pdf"],
    images: 1,
    finding: {
      status: "partial",
      summary:
        "צורפו מכתב מ-2017, סיכום פגישה עם רשות התקשוב מ-4.2.2019 והתכתבות שבה הסכים המשרד לפרסם את רשימת מאגריו, בחלקם מושחרים (סעיף 9(א)(1)). התשובה אינה מפרטת מאגרים שפורסמו בפועל.",
    },
  },
  {
    ministry: "משרד ראש הממשלה",
    date: "2026-01-12",
    url: "https://www.odata.org.il/dataset/a9c6abab-d8db-45d9-be0d-96c0b03d5287",
    files: ["ועדת היגוי מאי 2021.pdf", "מענה החלטת ממשלה 1933 בעניין מאגרי מידע.pdf"],
    images: 3,
    finding: {
      status: "partial",
      summary:
        "נציגי המשרד השתתפו בוועדת ההיגוי שריכזה רשות התקשוב, והוועדה לא התכנסה מאז מאי 2021; נספח הנגשת מאגרים פורסם בספרי תוכניות העבודה של 2017–2019 בלבד. לסיכומי הדיונים הופנתה העמותה למערך הדיגיטל.",
    },
  },
];
