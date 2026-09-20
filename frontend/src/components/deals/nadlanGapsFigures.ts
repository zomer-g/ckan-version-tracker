/**
 * The 68 screenshots behind the nadlan/mekarkein gap report, in the order the
 * appendix shows them: for each parcel, the tax-authority pages first, then the
 * nadlan.gov.il table pages, then one shot per table row whose
 * "עסקאות קודמות לנכס" window has history, with that window open.
 *
 * The images themselves are static files under public/nadlan-gaps/ rather than
 * inline data URIs: several MB of base64 in the bundle would be paid for by every
 * visitor to every page, and these are only ever needed by one tab.
 */
export type GapFigure = {
  /** Display number, 1-68. Also the file name: fig-01.jpg .. fig-68.jpg */
  n: number;
  /** Which site the screenshot is from. */
  src: "tax" | "nadlan";
  /**
   * "page" is a page of a results table; "history" is a nadlan.gov.il row with
   * its previous-sales window open. The appendix groups the two apart, because
   * the difference between them is the report's first finding.
   */
  kind: "page" | "history";
  host: string;
  place: string;
  cap: string;
};

export const GAP_FIGURES: GapFigure[] = [
  { n: 1, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 1 מתוך 5, תת־חלקה 028 מופיעה פעמיים ב־20/06/2010 בחצי כל אחת, ותת־חלקה 018 אינה מוצגת כלל באתר הנדל״ן" },
  { n: 2, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 2 מתוך 5, תת־חלקה 027 בשלוש מכירות נפרדות: 2010, 2011, 2014" },
  { n: 3, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 3 מתוך 5, תת־חלקה 010 בשתי מחציות באותו יום, ותת־חלקה 030 עם שטח ושנת בנייה 0" },
  { n: 4, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 4 מתוך 5, תת־חלקה 019 ב־0.666 ו־0.333 שסכומן 705,000" },
  { n: 5, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 5 מתוך 5, 49 רשומות בסך הכול" },
  { n: 6, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 1 מתוך 2, 20 עסקאות, וכל שורה בתת־חלקה אחרת" },
  { n: 7, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "עמוד 2 מתוך 2, עשר תתי־החלקה הנותרות, אף אחת אינה חוזרת" },
  { n: 8, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-5. בשורת הטבלה: 22/05/2017 · 520,000 ₪. בחלון ״עסקאות קודמות לנכס״ 2 מכירות קודמות: 04/04/2014 · 369,000; 29/09/2013 · 330,000" },
  { n: 9, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-10. בשורת הטבלה: 25/06/2021 · 550,000 ₪. בחלון ״עסקאות קודמות לנכס״ 3 מכירות קודמות: 12/12/2016 · 460,000; 16/02/2011 · 302,000; 13/06/2008 · 112,000" },
  { n: 10, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-12. בשורת הטבלה: 28/02/2023 · 660,000 ₪. בחלון ״עסקאות קודמות לנכס״ 4 מכירות קודמות: 08/03/2022 · 560,000; 23/03/2017 · 412,000; 10/04/2013 · 325,000; 28/07/2010 · 260,000" },
  { n: 11, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-13. בשורת הטבלה: 09/03/2023 · 250,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 14/01/2018 · 98,000" },
  { n: 12, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-15. בשורת הטבלה: 18/04/2024 · 720,000 ₪. בחלון ״עסקאות קודמות לנכס״ 3 מכירות קודמות: 18/07/2022 · 515,000; 29/07/2013 · 285,000; 19/08/2010 · 234,000" },
  { n: 13, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-27. בשורת הטבלה: 04/04/2021 · 510,000 ₪. בחלון ״עסקאות קודמות לנכס״ 4 מכירות קודמות: 15/01/2014 · 356,000; 12/12/2011 · 315,000; 07/10/2010 · 200,000; 07/10/2008 · 160,000" },
  { n: 14, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-28. בשורת הטבלה: 20/06/2010 · 210,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 11/12/2003 · 285,000" },
  { n: 15, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-29. בשורת הטבלה: 02/03/2023 · 520,000 ₪. בחלון ״עסקאות קודמות לנכס״ 3 מכירות קודמות: 08/08/2021 · 465,000; 14/12/2017 · 460,000; 09/11/2015 · 325,000" },
  { n: 16, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17457 חלקה 63 · מגדל העמק", cap: "נכס 17457-63-30. בשורת הטבלה: 21/01/2024 · 645,000 ₪. בחלון ״עסקאות קודמות לנכס״ 2 מכירות קודמות: 27/04/2020 · 440,000; 25/10/2011 · 255,000" },
  { n: 17, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "עמוד 1 מתוך 2, תת־חלקה 010 ב־27/09/2006: תמורה מוצהרת 122,700 מול שווי מכירה 279,600, שטח 97 מ״ר" },
  { n: 18, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "עמוד 2 מתוך 2, שלושה מקרי חלקים שסכומם שווה למוצג באתר הנדל״ן" },
  { n: 19, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "עמוד 1 מתוך 2, אותה תת־חלקה 010: 276,000 ש״ח ושטח 85 מ״ר" },
  { n: 20, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "עמוד 2 מתוך 2, 04/05/1999 ב־606,669 ש״ח" },
  { n: 21, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "נכס 7151-316-2. בשורת הטבלה: 16/02/2009 · 295,000 ₪. בחלון ״עסקאות קודמות לנכס״ 2 מכירות קודמות: 28/05/2007 · 192,000; 05/10/2006 · 139,000" },
  { n: 22, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "נכס 7151-316-6. בשורת הטבלה: 12/01/2025 · 1,895,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 06/08/2007 · 630,000" },
  { n: 23, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "נכס 7151-316-10. בשורת הטבלה: 27/09/2006 · 276,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 09/06/2005 · 616,000" },
  { n: 24, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 7151 חלקה 316 · בת ים", cap: "נכס 7151-316-13. בשורת הטבלה: 13/04/2016 · 1,400,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 31/08/1998 · 646,000" },
  { n: 25, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 12593 חלקה 23 · נופית", cap: "רשומה אחת, 2,550,000 ש״ח" },
  { n: 26, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 12593 חלקה 23 · נופית", cap: "עסקה אחת, אותו סכום בדיוק" },
  { n: 27, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 7787 חלקה 464 · תל מונד", cap: "חמש רשומות, כולן תחת תת־חלקה 000, 869,186 ו־816,872, ואין עסקה ב־18/01/2017" },
  { n: 28, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 7787 חלקה 464 · תל מונד", cap: "שש עסקאות, 869,000 ו־816,000 מעוגלים, ועסקה ב־18/01/2017 שאין לה מקבילה" },
  { n: 29, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 1 מתוך 5, 16/11/1999 ב־3,102,841, ותת־חלקה 017 בשטח 1,250 מ״ר" },
  { n: 30, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 2 מתוך 5, תת־חלקה 012 שאינה מוצגת באתר הנדל״ן" },
  { n: 31, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 3 מתוך 5, תת־חלקה 023 בשתי מחציות עם תמורה מוצהרת שונה זו מזו" },
  { n: 32, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 4 מתוך 5, תת־חלקה 001 ב־25/02/2004: 737,288 ש״ח, שטח 100 מ״ר" },
  { n: 33, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 5 מתוך 5, 07/01/2025 ב־1,553,000 בחלק 0.500" },
  { n: 34, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 1 מתוך 4, 39 עסקאות; 07/01/2025 ב־1,553,000" },
  { n: 35, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 2 מתוך 4, אותה תת־חלקה 001 ב־25/02/2004: 870,000 ש״ח, שטח 137.4 מ״ר" },
  { n: 36, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 3 מתוך 4, סכומים מדויקים לשקל: 779,450 ו־637,607" },
  { n: 37, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "עמוד 4 מתוך 4, 16/11/1999 ב־3,102,000" },
  { n: 38, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-2. בשורת הטבלה: 05/01/2016 · 1,490,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 03/09/2002 · 740,000" },
  { n: 39, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-6. בשורת הטבלה: 30/12/2015 · 1,450,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 13/05/2002 · 737,000" },
  { n: 40, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-10. בשורת הטבלה: 07/01/2025 · 1,553,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 13/04/2004 · 1,010,000" },
  { n: 41, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-11. בשורת הטבלה: 27/12/2013 · 1,355,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 18/11/2002 · 750,000" },
  { n: 42, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-15. בשורת הטבלה: 05/02/2014 · 1,510,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 25/04/2002 · 710,000" },
  { n: 43, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-19. בשורת הטבלה: 28/10/2009 · 950,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 28/09/2001 · 992,000" },
  { n: 44, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-23. בשורת הטבלה: 11/04/2019 · 1,595,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 09/01/2003 · 810,000" },
  { n: 45, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-24. בשורת הטבלה: 21/12/2025 · 2,500,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 19/02/2002 · 847,000" },
  { n: 46, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-25. בשורת הטבלה: 16/04/2019 · 1,640,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 04/04/2001 · 760,000" },
  { n: 47, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-26. בשורת הטבלה: 17/03/2010 · 1,150,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 08/01/2002 · 840,000" },
  { n: 48, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-27. בשורת הטבלה: 18/12/2014 · 1,450,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 12/02/2002 · 811,000" },
  { n: 49, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-35. בשורת הטבלה: 01/08/2010 · 1,050,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 22/01/2007 · 886,200" },
  { n: 50, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-37. בשורת הטבלה: 25/04/2021 · 1,425,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 18/11/2001 · 845,000" },
  { n: 51, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 10236 חלקה 99 · קריית ביאליק", cap: "נכס 10236-99-39. בשורת הטבלה: 23/07/2008 · 1,355,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 19/07/2001 · 1,250,000" },
  { n: 52, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "עמוד 1 מתוך 3, תת־חלקה 001 בארבע שורות באותו יום, שלוש מהן זהות לחלוטין" },
  { n: 53, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "עמוד 2 מתוך 3, ארבע שורות ב־09/05/2022 שסכומן 1,479,998, ושש שורות ב־01/10/2023" },
  { n: 54, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "עמוד 3 מתוך 3, השורה השישית שמשלימה בדיוק ל־1,500,000" },
  { n: 55, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "תשע עסקאות, 01/10/2023 ב־1,500,000 ו־09/05/2022 ב־1,541,600" },
  { n: 56, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "נכס 7136-208-1. בשורת הטבלה: 16/05/2004 · 281,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 08/06/2001 · 211,000" },
  { n: 57, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "נכס 7136-208-5. בשורת הטבלה: 07/02/2017 · 1,350,000 ₪. בחלון ״עסקאות קודמות לנכס״ 2 מכירות קודמות: 31/08/2012 · 1,225,000; 05/04/2011 · 830,000" },
  { n: 58, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 7136 חלקה 208 · בת ים", cap: "נכס 7136-208-6. בשורת הטבלה: 09/05/2022 · 1,541,600 ₪. בחלון ״עסקאות קודמות לנכס״ 2 מכירות קודמות: 13/12/2017 · 650,000; 23/07/2003 · 380,712" },
  { n: 59, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 17032 חלקה 221 · כפר תבור", cap: "שלוש רשומות, תת־חלקה 001 נמכרה ב־2018 וב־2024" },
  { n: 60, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 17032 חלקה 221 · כפר תבור", cap: "שתי שורות בטבלה, תת־חלקה 001 מוצגת עם מכירת 2024; מכירת 2018 נמצאת בחלון ההיסטוריה של אותו נכס" },
  { n: 61, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 17032 חלקה 221 · כפר תבור", cap: "נכס 17032-221-1. בשורת הטבלה: 23/05/2024 · 3,380,000 ₪. בחלון ״עסקאות קודמות לנכס״ מכירה קודמת אחת: 08/03/2018 · 1,800,000" },
  { n: 62, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 6982 חלקה 34 · תל אביב־יפו", cap: "שתים־עשרה רשומות, 11/09/2017 מפוצלת ל־2,136,400 ול־113,600, שתיהן בחלק 1.000" },
  { n: 63, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 6982 חלקה 34 · תל אביב־יפו", cap: "שלוש עסקאות, אותה עסקה מוצגת כשורה אחת של 2,250,000" },
  { n: 64, src: "nadlan", kind: "history", host: "nadlan.gov.il", place: "גוש 6982 חלקה 34 · תל אביב־יפו", cap: "נכס 6982-34-5. בשורת הטבלה: 11/09/2017 · 2,250,000 ₪. בחלון ״עסקאות קודמות לנכס״ 4 מכירות קודמות: 26/11/2013 · 1,450,000; 13/05/2008 · 900,000; 17/03/2004 · 448,000; 10/05/2001 · 518,000" },
  { n: 65, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 5134 חלקה 28 · שדמה / כפר מרדכי", cap: "תשע רשומות, כולל שנת בנייה 1800 וחלק נמכר 0.000" },
  { n: 66, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 5134 חלקה 28 · שדמה / כפר מרדכי", cap: "״המיקום לא נמצא״, לחלקה אין דף כלל" },
  { n: 67, src: "tax", kind: "page", host: "nadlan.taxes.gov.il", place: "גוש 7369 חלקה 15 · כוכב יאיר / צור יגאל", cap: "רשומה אחת, שם היישוב ״צור יגאל״" },
  { n: 68, src: "nadlan", kind: "page", host: "nadlan.gov.il", place: "גוש 7369 חלקה 15 · כוכב יאיר / צור יגאל", cap: "אותה עסקה, שם היישוב ״כוכב יאיר־צור יגאל״" },
];

/** The full screenshot, shown in the lightbox. */
export const figureUrl = (n: number) => `/nadlan-gaps/fig-${String(n).padStart(2, "0")}.jpg`;

/**
 * The 520px crop the appendix grid shows. Native loading="lazy" did not hold the
 * full images back here (the browser fetched every one on tab open), so the grid
 * gets its own copies and the full image is fetched only when the lightbox opens.
 */
export const thumbUrl = (n: number) => `/nadlan-gaps/thumb-${String(n).padStart(2, "0")}.jpg`;
