/**
 * The public security log shown on /security.
 *
 * One entry per security event: an outside report, or an internal finding that
 * changed what the site stores or exposes. Each entry links the commits that
 * fixed it, so a reader can check the fix rather than take our word for it.
 * Newest first. Adding an entry is one commit to this file, and that commit
 * is itself the record of when the log changed.
 *
 * Honesty rules for this list:
 *   - `kind: "report"` is reserved for a finding that came from OUTSIDE. The
 *     reporter is credited by name only with their consent.
 *   - `exposure` says what was actually reachable, by whom, for how long, as
 *     far as we know. "Nothing was exposed" is a claim and needs the reason.
 *   - A commit hash is one that exists on master of the PUBLIC repository
 *     (github.com/zomer-g/ckan-version-tracker), which is where the link
 *     goes. A local SHA is not that: this repo ships to GitHub and to the
 *     host by cherry-pick, which gives the same commit a different hash on
 *     each. Check with `git merge-base --is-ancestor <sha> origin/master`
 *     before writing one here.
 */
export type SecurityEntryKind = "report" | "internal";

export interface SecurityEntry {
  /** ISO date the report arrived or the finding was made. */
  date: string;
  kind: SecurityEntryKind;
  /** ISO date the fix was live in production. */
  fixed?: string;
  title: { he: string; en: string };
  /** What was wrong, as a reader would want to know it. */
  finding: { he: string; en: string };
  /** What was reachable, by whom, and what we know about whether it was used. */
  exposure: { he: string; en: string };
  /** What changed. */
  fix: { he: string; en: string };
  /** Who found it. Omitted for internal findings. */
  credit?: { he: string; en: string };
  /** Commit hashes on master that carry the fix. */
  commits: { sha: string; label: { he: string; en: string } }[];
}

export const REPO_URL = "https://github.com/zomer-g/ckan-version-tracker";
export const SECURITY_EMAIL = "guy@z-g.co.il";

export const SECURITY_LOG: SecurityEntry[] = [
  {
    date: "2026-10-04",
    kind: "report",
    fixed: "2026-10-04",
    title: {
      he: "שם וכתובת אימייל של מבקשי מעקב נחשפו בקטלוג הציבורי",
      en: "Requesters' names and email addresses exposed in the public catalog",
    },
    finding: {
      he: "התשובה של GET /api/datasets, שעמוד הבית טוען לכל גולש בלי התחברות, כללה לכל מאגר את השדות requester_name ו-requester_email. ב-75 מאגרים השדות היו מלאים בשם ובכתובת של החשבון שפתח את המעקב.",
      en: "The response of GET /api/datasets, which the home page loads for every visitor without signing in, carried requester_name and requester_email for every dataset. For 75 datasets they held the name and address of the account that had opened the tracking.",
    },
    exposure: {
      he: "כל מי שפתח את כלי המפתחים בדפדפן, או קרא את ה-API ישירות, יכול היה לראות את השדות. בדיקה של כל המידע שהיה גלוי העלתה שהשם היחיד שהופיע בכל 75 המאגרים הוא גיא זומר, מפעיל האתר, עם כתובת האימייל שלו. לא נחשף ולא זלג שם או כתובת של אף אדם אחר: בקשות מעקב מהציבור נשמרות בלי זהות, ורק מאגרים שנפתחו מחשבון המפעיל נשאו מבקש. איננו יודעים כמה זמן המצב נמשך לפני הדיווח. המדווח ציין שלא שמר ולא העביר את הנתונים.",
      en: "Anyone opening the browser's developer tools, or reading the API directly, could see the fields. A review of everything that was visible found that the only name appearing in all 75 datasets was Guy Zomer, the site's operator, with his email address. No other person's name or address was exposed or leaked: tracking requests from the public are stored without an identity, and only datasets opened from the operator's account carried a requester. We do not know how long this had been the case before the report. The reporter stated they kept and passed on nothing.",
    },
    fix: {
      he: "שני השדות חוזרים כ-null בכל תשובה ציבורית וממולאים רק ברשימת הניהול שמאחורי אימות אדמין; נקודת הקצה הציבורית הפסיקה לבצע JOIN לטבלת המשתמשים בכלל. בהמשך לכך צומצם איסוף המידע באתר: טופס הבקשה למעקב אינו מקבל יותר שם או פרטי קשר, ומשתמשי ה-MCP נשמרים לפי כתובת אימייל בלבד, בלי שם ובלי מזהה Google.",
      en: "Both fields are null in every public response and populated only in the admin-gated list; the public endpoint no longer joins the users table at all. As a follow-up the site collects less: the tracking-request form takes no name or contact details, and MCP users are kept as an email address only, with no name and no Google id.",
    },
    credit: {
      he: "דווח באופן פרטי במייל על ידי משתמש של האתר. תודה.",
      en: "Reported privately by email by a user of the site. Thank you.",
    },
    commits: [
      { sha: "468c5c5", label: { he: "הקטלוג הציבורי ללא פרטי המבקש", en: "public catalog without requester details" } },
      { sha: "245fcae", label: { he: "צמצום איסוף: טופס הבקשה ומשתמשי MCP", en: "less collected: request form and MCP users" } },
    ],
  },
  {
    date: "2026-09-10",
    kind: "internal",
    fixed: "2026-09-10",
    title: {
      he: "חשבון מחובר שאינו אדמין ראה את תפריט הניהול",
      en: "A signed-in non-admin account was shown the admin menu",
    },
    finding: {
      he: "ההגנה על /admin בצד הלקוח בדקה \"מחובר\" ולא \"אדמין\". כשקונסולות ה-SQL התחילו לבקש התחברות מכל אחד, חשבון Google חדש שפתח את /admin ראה את כל התפריט, וכל פאנל נכשל בהודעת \"Admin access required\".",
      en: "The client-side guard on /admin checked \"signed in\", never \"admin\". Once the SQL consoles asked everyone to sign in, a fresh Google account opening /admin saw the whole menu, with every panel failing on \"Admin access required\".",
    },
    exposure: {
      he: "לא נחשף מידע: כל 188 נתיבי הניהול בשרת דורשים הרשאת אדמין ודחו כל קריאה. הפער היה בממשק בלבד.",
      en: "No data was exposed: all 188 admin routes on the server require admin and refused every call. The gap was in the interface only.",
    },
    fix: {
      he: "/admin דורש is_admin ומציג הודעת אין-גישה פשוטה.",
      en: "/admin requires is_admin and shows a plain no-access notice.",
    },
    commits: [
      { sha: "6cae6d6", label: { he: "התיקון", en: "the fix" } },
    ],
  },
  {
    date: "2026-09-10",
    kind: "internal",
    fixed: "2026-09-10",
    title: {
      he: "הקשחה לקראת מסד נתונים אחד: אדמין מסוד, טבלאות הרשאות בסכימה נפרדת, הוכחה באתחול",
      en: "Hardening for a single database: admin from a secret, credential tables in their own schema, a proof at boot",
    },
    finding: {
      he: "המעבר לאירוח שבו קונסולת ה-SQL הציבורית וטבלאות האפליקציה חולקות מסד נתונים אחד הפך שתי הנחות לסיכון: הרשאת אדמין שנשמרה כדגל בשורה (מה שיכול לכתוב את השורה הופך לאדמין), ו-fallback שנתן לקונסולה הציבורית תפקיד מלא כשמשתנה סביבה אחד חסר.",
      en: "Moving to hosting where the public SQL console and the application's tables share one database turned two assumptions into risks: admin stored as a row flag (whatever can write the row becomes admin), and a fallback that handed the public console a full-privilege role when one environment variable was missing.",
    },
    exposure: {
      he: "לא ידוע על ניצול. השינויים נעשו לפני המעבר, כתנאי לו.",
      en: "No exploitation is known. The changes were made before the move, as a precondition for it.",
    },
    fix: {
      he: "אדמין נגזר בכל בקשה מסוד פלטפורמה ואין נתיב קוד שמוסיף אדמין; שש הטבלאות שמחזיקות הרשאות או מידע אישי עברו לסכימה auth ללא הרשאת PUBLIC; ה-fallback הוסר ונכשל במקומו; ובכל אתחול השרת שואל את Postgres אילו טבלאות תפקיד הקונסולה יכול לקרוא, ומסרב לעלות אם אחת מהן רגישה.",
      en: "Admin is re-derived on every request from a platform secret and no code path adds an admin; the six tables holding credentials or personal data moved to an auth schema with no PUBLIC grant; the fallback was removed and fails instead; and at every boot the server asks Postgres which tables the console role can read, refusing to start if any is sensitive.",
    },
    commits: [
      { sha: "7136a70", label: { he: "אדמין מסוד", en: "admin from a secret" } },
      { sha: "d0c3bfc", label: { he: "סכימת auth, fail closed, הוכחה באתחול", en: "auth schema, fail closed, boot proof" } },
      { sha: "e5ee6aa", label: { he: "טבלאות האפליקציה מחוץ ל-public", en: "app tables out of public" } },
    ],
  },
  {
    date: "2026-08-13",
    kind: "internal",
    fixed: "2026-08-13",
    title: {
      he: "מפתח ה-API של מחבר Looker Studio היה קריא לכל מי שקיבל את הקישור",
      en: "The Looker Studio connector's API key was readable by anyone with the link",
    },
    finding: {
      he: "הפצת המחבר דרך קישור מחייבת שיתוף של פרויקט ה-Apps Script לצפייה, וצופים רואים את ה-Script Properties, שבהן נשמר המפתח.",
      en: "Distributing the connector by link requires sharing the Apps Script project for viewing, and viewers can see the Script Properties, where the key was stored.",
    },
    exposure: {
      he: "המפתח מעולם לא הגן על נתונים (כל המידע ציבורי ולקריאה בלבד); הוא רק ניתב תעבורה לתקציב משותף. חשיפתו אפשרה לכל היותר לצרוך מהתקציב הזה.",
      en: "The key never guarded data (everything is public and read-only); it only routed traffic to a shared budget. Exposing it allowed, at most, drawing on that budget.",
    },
    fix: {
      he: "אין יותר סוד בצד הלקוח: השרת מזהה תעבורת מחבר לפי טווחי הכתובות שגוגל מפרסמת, והסקריפט לא שולח כותרת כלל.",
      en: "No client-side secret: the server recognises connector traffic by Google's published egress ranges, and the script sends no header at all.",
    },
    commits: [
      { sha: "db03e30", label: { he: "התיקון", en: "the fix" } },
    ],
  },
  {
    date: "2026-07-17",
    kind: "internal",
    fixed: "2026-07-17",
    title: {
      he: "טוקן ההתחברות של האדמין עבר בכתובת ה-URL",
      en: "The admin sign-in token travelled in the URL",
    },
    finding: {
      he: "אחרי התחברות דרך Google, ה-JWT של האדמין הועבר לדפדפן כפרמטר בכתובת (sso_token=...), וכך נחת בהיסטוריית הדפדפן, ביומני השרת וב-Referer של הדף הבא.",
      en: "After signing in through Google, the admin's JWT was handed to the browser as a URL parameter (sso_token=...), which puts it in browser history, server logs and the next page's Referer.",
    },
    exposure: {
      he: "לא ידוע על ניצול. באותה עת האדמין היה המשתמש המחובר היחיד באתר.",
      en: "No exploitation is known. At the time the admin was the site's only signed-in user.",
    },
    fix: {
      he: "ההתחברות וחיבור ה-Drive משתמשים בקוד חד-פעמי קצר-מועד שמוחלף בטוקן בבקשת POST; תוקף הטוקן קוצר לשעתיים.",
      en: "Sign-in and the Drive connection use a short-lived one-time code exchanged for the token in a POST; the token's lifetime was cut to two hours.",
    },
    commits: [
      { sha: "8832f08", label: { he: "התיקון", en: "the fix" } },
    ],
  },
];
