/**
 * פרויקט 1933 — what מידע לעם (odata.org.il) holds about government decision
 * 1933 (2016, "improving government data transfer and opening government
 * databases to the public").
 *
 * The bulk of it is one cross-government FOI request filed by עמותת הצלחה in
 * late 2024 to every ministry, asking what each had done to carry out the
 * decision; each ministry's answer was published on מידע לעם as its own
 * dataset. The list below is the catalog of those answers, taken from
 * odata.org.il's CKAN API (package_search q=1933). Two ministries' answers were
 * uploaded twice (same files, a day or so apart) and are folded into one entry
 * with the duplicate under `also`.
 *
 * `finding` is the editorial summary of what the ministry's answer actually
 * says. It is null until someone has read the answer — the page then says so
 * plainly rather than guessing from a file name.
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
}

export const ODATA_SEARCH_URL = "https://www.odata.org.il/dataset?q=1933";

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
    finding: null,
  },
  {
    ministry: "משרד האנרגיה",
    date: "2024-11-05",
    url: "https://www.odata.org.il/dataset/a3b471e2-dc0f-4870-b889-1cf636a73294",
    files: ["MH_87_2018.pdf", "פורמט לנספח - הנגשת מאגרי מידע והעברות מידע - משרד האנרגיה - 2019.pdf", "MH_92_2018.xlsx", "dov_549_2024.pdf"],
    images: 6,
    finding: null,
  },
  {
    ministry: "משרד התרבות והספורט",
    date: "2024-11-11",
    url: "https://www.odata.org.il/dataset/a35657c6-82dd-403b-9fa3-5dd6e0a1db45",
    files: ["מענה לפניה.pdf", "עותק של הנגשת מאגרים - משרד התרבות והספורט.xlsx"],
    images: 0,
    finding: null,
  },
  {
    ministry: "משרד החקלאות",
    date: "2024-11-20",
    url: "https://www.odata.org.il/dataset/6a2b33b0-f809-4a80-b2a8-9fda497b4573",
    files: ["החלטת ממשלה 1933 - משרד החקלאות.docx"],
    images: 11,
    finding: null,
  },
  {
    ministry: "משרד התפוצות והמאבק באנטישמיות",
    date: "2024-11-27",
    url: "https://www.odata.org.il/dataset/3360eab5-415f-47cf-b918-1c989c38d32c",
    files: ["מענה לבקשת מאגרי מידע.pdf"],
    images: 1,
    finding: null,
  },
  {
    ministry: "המשרד לשירותי דת",
    date: "2024-11-28",
    url: "https://www.odata.org.il/dataset/68c54698-ddbb-40bd-b868-4350e1868838",
    also: ["https://www.odata.org.il/dataset/15968372-cf3d-4725-a4b5-9b5ccaa60fe1"],
    files: ["מענה לבקשת חופש מידע בנוגע להחלטת ממשלה 1933 בעניין מאגרי מידע לציבור.pdf"],
    images: 4,
    finding: null,
  },
  {
    ministry: "משרד הבינוי והשיכון",
    date: "2024-12-05",
    url: "https://www.odata.org.il/dataset/db15d4be-06f4-466e-a6b1-215524d5013a",
    files: ["מענה לבקשת מידע.pdf"],
    images: 3,
    finding: null,
  },
  {
    ministry: "משרד ירושלים ומסורת ישראל",
    date: "2024-12-10",
    url: "https://www.odata.org.il/dataset/fbab09a7-ed41-432c-a518-25183930eb1b",
    files: ["מענה סופי - אלעד מן - החלטת הממשלה 1933 מאגרי מידע - 9.12.24.pdf"],
    images: 2,
    finding: null,
  },
  {
    ministry: "משרד החינוך",
    date: "2024-12-25",
    url: "https://www.odata.org.il/dataset/1118a606-2cd9-4350-80c6-398d371f56ec",
    files: ["מענה למבקר המדינה -מענה פעם אחת.pdf", "מענה למבקר המדינה -מענה פעם אחת.pdf", "1933 (1)_241115_154137.pdf", "מבקר המידנה מדיניות פעם אחת.pdf", "אלעד מן - החלטת ממשלה 1933- מאגרי מידע.pdf"],
    images: 10,
    finding: null,
  },
  {
    ministry: "המשרד לשיתוף פעולה איזורי",
    date: "2024-12-25",
    url: "https://www.odata.org.il/dataset/8606a2c8-4a5a-45e0-811c-87fb31f1b78e",
    files: ["עמותת הצלחה - בקשת חופש מידע-החלטת הממשלה 1933 בעניין מאגרי מידע (003).pdf"],
    images: 0,
    finding: null,
  },
  {
    ministry: "משרד התיירות",
    date: "2025-01-01",
    url: "https://www.odata.org.il/dataset/1493e5a1-7f17-493b-8cde-b5348600a637",
    files: ["החלטת ממשלה 1933 מאגרי מידע -מענה לבקשתך באמצעות חוק חופש המידע (002).pdf", "אופן הוצאת נתונים ל OPEN DATA.pdf", "חשיבה מחדש על תכני אתר מידע למטייל - סיכום ישיבה - 2 נוסף.pdf", "אופן טיפול בעיות ב OPEN DATA 3.pdf", "דגשים להוצאת מאגרים ל OPEN DATA 4.pdf", "חשיבה מחדש על תכני אתר מידע למטייל - סיכום ישיבה 5.pdf", "נוהל הוועדה להעברת מידע בין גופים ציבוריים.pdf"],
    images: 0,
    finding: null,
  },
  {
    ministry: "משרד הכלכלה",
    date: "2025-01-16",
    url: "https://www.odata.org.il/dataset/8fdc8461-24da-457c-881e-000c98706dc5",
    files: ["מענה.pdf"],
    images: 8,
    finding: null,
  },
  {
    ministry: "משרד החדשנות, המדע והטכנולוגיה",
    date: "2025-01-21",
    url: "https://www.odata.org.il/dataset/b2843348-0df4-42bd-b89d-229ec16134fb",
    also: ["https://www.odata.org.il/dataset/2210694a-9159-4fe7-8707-f8cbf311f3f4"],
    files: ["מענה בעניין החלטת ממשלה 1933.pdf", "הנגשת מאגרים - משרד המדע.xlsx"],
    images: 0,
    finding: null,
  },
  {
    ministry: "משרד הפנים",
    date: "2025-02-26",
    url: "https://www.odata.org.il/dataset/dcec1a5b-a8cb-45fc-9e5a-1c888aba3632",
    files: ["Untitled_20250226_133523.pdf"],
    images: 0,
    finding: null,
  },
  {
    ministry: "המשרד לשיוויון חברתי",
    date: "2025-03-03",
    url: "https://www.odata.org.il/dataset/74da2369-bb6a-4eb0-956a-2651a5682593",
    files: ["תשובה - החלטת ממשלה 1933 בעניין מאגרי מידע 31-24 -.pdf"],
    images: 0,
    finding: null,
  },
  {
    ministry: "משרד הביטחון",
    date: "2025-09-02",
    url: "https://www.odata.org.il/dataset/18c6b812-22f9-42a1-b913-8e4c78ad0055",
    files: ["מענה חח''מ 239134.pdf", "נספח א.pdf", "נספח ב.pdf", "נספח ג.pdf"],
    images: 1,
    finding: null,
  },
  {
    ministry: "משרד ראש הממשלה",
    date: "2026-01-12",
    url: "https://www.odata.org.il/dataset/a9c6abab-d8db-45d9-be0d-96c0b03d5287",
    files: ["ועדת היגוי מאי 2021.pdf", "מענה החלטת ממשלה 1933 בעניין מאגרי מידע.pdf"],
    images: 3,
    finding: null,
  },
];
