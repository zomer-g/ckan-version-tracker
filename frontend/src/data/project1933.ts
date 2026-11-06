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
  /** Attached documents, without the e-mail signature images: the file name
   *  as uploaded, and its download link on מידע לעם. */
  files: { name: string; url: string }[];
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
    files: [
      { name: "מענה הצלחה החמ 1933.pdf", url: "https://www.odata.org.il/dataset/ffbefa57-6763-444c-a5fe-0fe357eb8fcb/resource/85384685-2ef0-4e6e-b90a-1581392b5537/download/-1933.pdf" },
    ],
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
    files: [
      { name: "MH_87_2018.pdf", url: "https://www.odata.org.il/dataset/bff3ebb7-a648-48ed-af5c-477ac049eb8e/resource/10bada73-636e-4e89-a407-358214000418/download/mh_87_2018.pdf" },
      { name: "פורמט לנספח - הנגשת מאגרי מידע והעברות מידע - משרד האנרגיה - 2019.pdf", url: "https://www.odata.org.il/dataset/bff3ebb7-a648-48ed-af5c-477ac049eb8e/resource/c468cd1f-2412-4794-be92-bcba839ead25/download/-2019.pdf" },
      { name: "MH_92_2018.xlsx", url: "https://www.odata.org.il/dataset/bff3ebb7-a648-48ed-af5c-477ac049eb8e/resource/1b6993b6-dbd7-43b3-a6ee-31661a874348/download/mh_92_2018.xlsx" },
      { name: "dov_549_2024.pdf", url: "https://www.odata.org.il/dataset/bff3ebb7-a648-48ed-af5c-477ac049eb8e/resource/1575dac0-0572-4670-a329-b0c5b96c8625/download/dov_549_2024.pdf" },
    ],
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
    files: [
      { name: "מענה לפניה.pdf", url: "https://www.odata.org.il/dataset/c841657b-7b9a-4c4b-b3fc-12a0094b3e5e/resource/03f34c6e-8b41-4ecb-8733-39d42bac4ced/download/-.pdf" },
      { name: "עותק של הנגשת מאגרים - משרד התרבות והספורט.xlsx", url: "https://www.odata.org.il/dataset/c841657b-7b9a-4c4b-b3fc-12a0094b3e5e/resource/2df623cc-2d38-491c-b662-9123cae68c2e/download/-.xlsx" },
    ],
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
    files: [
      { name: "החלטת ממשלה 1933 - משרד החקלאות.docx", url: "https://www.odata.org.il/dataset/d96ed509-0398-48cf-bfc4-df74283a2c9c/resource/6fa30b1c-3c70-4730-8d84-9a43a34a173e/download/-1933-.docx" },
    ],
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
    files: [
      { name: "מענה לבקשת מאגרי מידע.pdf", url: "https://www.odata.org.il/dataset/4518408b-7046-485e-81e1-f76e016b31c2/resource/61584133-8b77-49c9-a647-fd301f7b1886/download/-.pdf" },
    ],
    images: 1,
    finding: null,
    findingNote: "המשרד השיב במשפט אחד: \"אין ברשות משרד התפוצות והמאבק באנטישמיות מאגרי מידע\". התשובה לא מתאימה לאף אחת מהקטגוריות, ולכן לא סווגה.",
  },
  {
    ministry: "המשרד לשירותי דת",
    date: "2024-11-28",
    url: "https://www.odata.org.il/dataset/68c54698-ddbb-40bd-b868-4350e1868838",
    also: ["https://www.odata.org.il/dataset/15968372-cf3d-4725-a4b5-9b5ccaa60fe1"],
    files: [
      { name: "מענה לבקשת חופש מידע בנוגע להחלטת ממשלה 1933 בעניין מאגרי מידע לציבור.pdf", url: "https://www.odata.org.il/dataset/95ec1484-0270-4b9a-a115-df7bcf3d18ac/resource/a72c02cc-be7a-4d0f-a08b-6335c1052d99/download/-1933-.pdf" },
    ],
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
    files: [
      { name: "מענה לבקשת מידע.pdf", url: "https://www.odata.org.il/dataset/cad9e9d5-0167-4386-8eae-7ee949de9a95/resource/8a816fc8-f068-4f7c-8ba2-c594c5e39736/download/-.pdf" },
    ],
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
    files: [
      { name: "מענה סופי - אלעד מן - החלטת הממשלה 1933 מאגרי מידע - 9.12.24.pdf", url: "https://www.odata.org.il/dataset/778535cd-8aad-4f4b-b359-2087b0da0367/resource/917a78d1-4339-45de-a8bd-f513938bd5ef/download/-1933-9.12.24.pdf" },
    ],
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
    files: [
      { name: "מענה למבקר המדינה -מענה פעם אחת.pdf", url: "https://www.odata.org.il/dataset/324ccff7-f0aa-4154-8fef-e53062821e33/resource/1a263815-c328-423b-bc7a-b4367c59e921/download/-.pdf" },
      { name: "מענה למבקר המדינה -מענה פעם אחת.pdf", url: "https://www.odata.org.il/dataset/324ccff7-f0aa-4154-8fef-e53062821e33/resource/86b9734c-211d-4f40-9d2a-a024e2692100/download/-.pdf" },
      { name: "1933 (1)_241115_154137.pdf", url: "https://www.odata.org.il/dataset/324ccff7-f0aa-4154-8fef-e53062821e33/resource/4ff8b799-b9b1-43a1-b96d-1bca109e0a30/download/1933-1_241115_154137.pdf" },
      { name: "מבקר המידנה מדיניות פעם אחת.pdf", url: "https://www.odata.org.il/dataset/324ccff7-f0aa-4154-8fef-e53062821e33/resource/38e04926-aa79-4e3a-b702-ed77f75ccbc7/download/-.pdf" },
      { name: "אלעד מן - החלטת ממשלה 1933- מאגרי מידע.pdf", url: "https://www.odata.org.il/dataset/324ccff7-f0aa-4154-8fef-e53062821e33/resource/a79f9cd3-648f-4044-9204-eec891f993d6/download/-1933-.pdf" },
    ],
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
    files: [
      { name: "עמותת הצלחה - בקשת חופש מידע-החלטת הממשלה 1933 בעניין מאגרי מידע (003).pdf", url: "https://www.odata.org.il/dataset/ee5ccc83-2ed8-495c-a4d5-0f15c843e1b2/resource/5b1c10e8-8a9b-4521-aeac-8be18d4afee8/download/-1933-003.pdf" },
    ],
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
    files: [
      { name: "החלטת ממשלה 1933 מאגרי מידע -מענה לבקשתך באמצעות חוק חופש המידע (002).pdf", url: "https://www.odata.org.il/dataset/17c40028-80f7-401e-94b8-f92aa3880f1a/resource/4a5d255e-e862-4201-9199-5d4e882f7b9f/download/-1933-002.pdf" },
      { name: "אופן הוצאת נתונים ל OPEN DATA.pdf", url: "https://www.odata.org.il/dataset/17c40028-80f7-401e-94b8-f92aa3880f1a/resource/8395cb1c-945b-49fc-a698-463e0c1ef500/download/-open-data.pdf" },
      { name: "חשיבה מחדש על תכני אתר מידע למטייל - סיכום ישיבה - 2 נוסף.pdf", url: "https://www.odata.org.il/dataset/17c40028-80f7-401e-94b8-f92aa3880f1a/resource/d8765078-db55-4170-bfa9-0b8af03ed088/download/-2-.pdf" },
      { name: "אופן טיפול בעיות ב OPEN DATA 3.pdf", url: "https://www.odata.org.il/dataset/17c40028-80f7-401e-94b8-f92aa3880f1a/resource/0fe1306f-15f8-4486-88cc-a33ea614a763/download/-open-data-3.pdf" },
      { name: "דגשים להוצאת מאגרים ל OPEN DATA 4.pdf", url: "https://www.odata.org.il/dataset/17c40028-80f7-401e-94b8-f92aa3880f1a/resource/5550d0a7-59c4-4476-aaf4-84029a9806c3/download/-open-data-4.pdf" },
      { name: "חשיבה מחדש על תכני אתר מידע למטייל - סיכום ישיבה 5.pdf", url: "https://www.odata.org.il/dataset/17c40028-80f7-401e-94b8-f92aa3880f1a/resource/fbee5e76-8d45-4ea3-8162-aadf409a321c/download/-5.pdf" },
      { name: "נוהל הוועדה להעברת מידע בין גופים ציבוריים.pdf", url: "https://www.odata.org.il/dataset/17c40028-80f7-401e-94b8-f92aa3880f1a/resource/d61fba75-9b57-4a18-aa10-4d4fe018d11b/download/-.pdf" },
    ],
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
    files: [
      { name: "מענה.pdf", url: "https://www.odata.org.il/dataset/7bb260eb-64ba-47af-8ce6-4c21e946a3e3/resource/7153d231-79b1-4a43-98df-6fea0765fb59/download/.pdf" },
    ],
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
    files: [
      { name: "מענה בעניין החלטת ממשלה 1933.pdf", url: "https://www.odata.org.il/dataset/6a80ac8e-9682-46cb-8f4c-7a3c0139110b/resource/648decd7-00e0-4c75-94f1-04ea2f1353b6/download/-1933.pdf" },
      { name: "הנגשת מאגרים - משרד המדע.xlsx", url: "https://www.odata.org.il/dataset/6a80ac8e-9682-46cb-8f4c-7a3c0139110b/resource/696b79e6-e5f7-4705-bb30-b731a389a12d/download/-.xlsx" },
    ],
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
    files: [
      { name: "Untitled_20250226_133523.pdf", url: "https://www.odata.org.il/dataset/ec231ffd-15c4-4e73-ab15-bb518498aa0b/resource/4aff3bc3-5407-4917-a396-0bb89a35cc32/download/untitled_20250226_133523.pdf" },
    ],
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
    files: [
      { name: "תשובה - החלטת ממשלה 1933 בעניין מאגרי מידע 31-24 -.pdf", url: "https://www.odata.org.il/dataset/3f8f8df2-3b61-4b34-982e-685e8f0a222e/resource/98f2e01d-c1e4-4d79-a0d2-ba2fd9305a3e/download/-1933-31-24-.pdf" },
    ],
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
    files: [
      { name: "מענה חח''מ 239134.pdf", url: "https://www.odata.org.il/dataset/291d1d52-c6f7-4134-b294-3eccd20abc88/resource/41e0e405-b69a-4c5a-b6fd-a2567a13a8bb/download/-239134.pdf" },
      { name: "נספח א.pdf", url: "https://www.odata.org.il/dataset/291d1d52-c6f7-4134-b294-3eccd20abc88/resource/bc2c4c83-085a-472f-b5f1-58ec852e7918/download/-.pdf" },
      { name: "נספח ב.pdf", url: "https://www.odata.org.il/dataset/291d1d52-c6f7-4134-b294-3eccd20abc88/resource/cca5be6d-e99c-451e-8c20-049ea5f3d978/download/-.pdf" },
      { name: "נספח ג.pdf", url: "https://www.odata.org.il/dataset/291d1d52-c6f7-4134-b294-3eccd20abc88/resource/21960951-2c01-40a6-94ac-17df72f99a3a/download/-.pdf" },
    ],
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
    files: [
      { name: "ועדת היגוי מאי 2021.pdf", url: "https://www.odata.org.il/dataset/0e832707-ea0e-4eb0-b85b-7f7934ba3228/resource/5d16b4f6-d01a-4268-b1e8-a61bc64a588f/download/-2021.pdf" },
      { name: "מענה החלטת ממשלה 1933 בעניין מאגרי מידע.pdf", url: "https://www.odata.org.il/dataset/0e832707-ea0e-4eb0-b85b-7f7934ba3228/resource/b1482f40-4415-4c5a-8b5e-b09323df8822/download/-1933-.pdf" },
    ],
    images: 3,
    finding: {
      status: "partial",
      summary:
        "נציגי המשרד השתתפו בוועדת ההיגוי שריכזה רשות התקשוב, והוועדה לא התכנסה מאז מאי 2021; נספח הנגשת מאגרים פורסם בספרי תוכניות העבודה של 2017–2019 בלבד. לסיכומי הדיונים הופנתה העמותה למערך הדיגיטל.",
    },
  },
];
