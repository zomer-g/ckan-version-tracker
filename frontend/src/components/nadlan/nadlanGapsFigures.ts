/**
 * The 36 screenshots behind the nadlan/mekarkein gap report, in the order the
 * appendix shows them: for each parcel, the tax-authority rows first and the
 * nadlan.gov.il rows after, so the pair can be read side by side.
 *
 * The images themselves are static files under public/nadlan-gaps/ rather than
 * inline data URIs: 2.9 MB of base64 in the bundle would be paid for by every
 * visitor to every page, and these are only ever needed by one tab.
 */
export type GapFigure = {
  /** Display number, 1-36. Also the file name: fig-01.jpg .. fig-36.jpg */
  n: number;
  /** Which site the screenshot is from. */
  src: "tax" | "nadlan";
  host: string;
  place: string;
  cap: string;
};

export const GAP_FIGURES: GapFigure[] = [
  { n: 1, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 1 מתוך 5, תת־חלקה 028 מופיעה פעמיים ב־20/06/2010 בחצי כל אחת, ותת־חלקה 018 אינה מוצגת כלל באתר הנדל״ן" },
  { n: 2, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 2 מתוך 5, תת־חלקה 027 בשלוש מכירות נפרדות: 2010, 2011, 2014" },
  { n: 3, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 3 מתוך 5, תת־חלקה 010 בשתי מחציות באותו יום, ותת־חלקה 030 עם שטח ושנת בנייה 0" },
  { n: 4, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 4 מתוך 5, תת־חלקה 019 ב־0.666 ו־0.333 שסכומן 705,000" },
  { n: 5, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 5 מתוך 5, 49 רשומות בסך הכול" },
  { n: 6, src: "nadlan", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 1 מתוך 2, 20 עסקאות, וכל שורה בתת־חלקה אחרת" },
  { n: 7, src: "nadlan", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 2 מתוך 2, עשר תתי־החלקה הנותרות, אף אחת אינה חוזרת" },
  { n: 8, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "עמוד 1 מתוך 2, תת־חלקה 010 ב־27/09/2006: תמורה מוצהרת 122,700 מול שווי מכירה 279,600, שטח 97 מ״ר" },
  { n: 9, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "עמוד 2 מתוך 2, שלושה מקרי חלקים שסכומם שווה למוצג באתר הנדל״ן" },
  { n: 10, src: "nadlan", host: "nadlan.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "עמוד 1 מתוך 2, אותה תת־חלקה 010: 276,000 ש״ח ושטח 85 מ״ר" },
  { n: 11, src: "nadlan", host: "nadlan.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "עמוד 2 מתוך 2, 04/05/1999 ב־606,669 ש״ח" },
  { n: 12, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 12593 חלקה 23 · נופית", cap: "רשומה אחת, 2,550,000 ש״ח" },
  { n: 13, src: "nadlan", host: "nadlan.gov.il", place: "גוש 12593 חלקה 23 · נופית", cap: "עסקה אחת, אותו סכום בדיוק" },
  { n: 14, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 7787 חלקה 464 · תל מונד", cap: "חמש רשומות, כולן תחת תת־חלקה 000, 869,186 ו־816,872, ואין עסקה ב־18/01/2017" },
  { n: 15, src: "nadlan", host: "nadlan.gov.il", place: "גוש 7787 חלקה 464 · תל מונד", cap: "שש עסקאות, 869,000 ו־816,000 מעוגלים, ועסקה ב־18/01/2017 שאין לה מקבילה" },
  { n: 16, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 1 מתוך 5, 16/11/1999 ב־3,102,841, ותת־חלקה 017 בשטח 1,250 מ״ר" },
  { n: 17, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 2 מתוך 5, תת־חלקה 012 שאינה מוצגת באתר הנדל״ן" },
  { n: 18, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 3 מתוך 5, תת־חלקה 023 בשתי מחציות עם תמורה מוצהרת שונה זו מזו" },
  { n: 19, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 4 מתוך 5, תת־חלקה 001 ב־25/02/2004: 737,288 ש״ח, שטח 100 מ״ר" },
  { n: 20, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 5 מתוך 5, 07/01/2025 ב־1,553,000 בחלק 0.500" },
  { n: 21, src: "nadlan", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 1 מתוך 4, 39 עסקאות; 07/01/2025 ב־1,553,000" },
  { n: 22, src: "nadlan", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 2 מתוך 4, אותה תת־חלקה 001 ב־25/02/2004: 870,000 ש״ח, שטח 137.4 מ״ר" },
  { n: 23, src: "nadlan", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 3 מתוך 4, סכומים מדויקים לשקל: 779,450 ו־637,607" },
  { n: 24, src: "nadlan", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 4 מתוך 4, 16/11/1999 ב־3,102,000" },
  { n: 25, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "עמוד 1 מתוך 3, תת־חלקה 001 בארבע שורות באותו יום, שלוש מהן זהות לחלוטין" },
  { n: 26, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "עמוד 2 מתוך 3, ארבע שורות ב־09/05/2022 שסכומן 1,479,998, ושש שורות ב־01/10/2023" },
  { n: 27, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "עמוד 3 מתוך 3, השורה השישית שמשלימה בדיוק ל־1,500,000" },
  { n: 28, src: "nadlan", host: "nadlan.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "תשע עסקאות, 01/10/2023 ב־1,500,000 ו־09/05/2022 ב־1,541,600" },
  { n: 29, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 17032 חלקה 221 · כפר תבור", cap: "שלוש רשומות, תת־חלקה 001 נמכרה ב־2018 וב־2024" },
  { n: 30, src: "nadlan", host: "nadlan.gov.il", place: "גוש 17032 חלקה 221 · כפר תבור", cap: "שתי עסקאות, רק מכירת 2024 מוצגת; זו של 2018 אינה" },
  { n: 31, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 6982 חלקה 34 · תל אביב־יפו", cap: "שתים־עשרה רשומות, 11/09/2017 מפוצלת ל־2,136,400 ול־113,600, שתיהן בחלק 1.000" },
  { n: 32, src: "nadlan", host: "nadlan.gov.il", place: "גוש 6982 חלקה 34 · תל אביב־יפו", cap: "שלוש עסקאות, אותה עסקה מוצגת כשורה אחת של 2,250,000" },
  { n: 33, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 5134 חלקה 28 · שדמה / כפר מרדכי", cap: "תשע רשומות, כולל שנת בנייה 1800 וחלק נמכר 0.000" },
  { n: 34, src: "nadlan", host: "nadlan.gov.il", place: "גוש 5134 חלקה 28 · שדמה / כפר מרדכי", cap: "״המיקום לא נמצא״, לחלקה אין דף כלל" },
  { n: 35, src: "tax", host: "nadlan.taxes.gov.il", place: "גוש 7369 חלקה 15 · כוכב יאיר / צור יגאל", cap: "רשומה אחת, שם היישוב ״צור יגאל״" },
  { n: 36, src: "nadlan", host: "nadlan.gov.il", place: "גוש 7369 חלקה 15 · כוכב יאיר / צור יגאל", cap: "אותה עסקה, שם היישוב ״כוכב יאיר־צור יגאל״" },
];

/** The full screenshot, shown in the lightbox. ~80 KB each. */
export const figureUrl = (n: number) => `/nadlan-gaps/fig-${String(n).padStart(2, "0")}.jpg`;

/**
 * The 520px crop the appendix grid shows. Native loading="lazy" turned out not
 * to hold the full images back here (the browser fetched all 36 on tab open,
 * 2.9 MB), so the grid gets its own copies: the same 36 pictures for 0.56 MB.
 */
export const thumbUrl = (n: number) => `/nadlan-gaps/thumb-${String(n).padStart(2, "0")}.jpg`;
