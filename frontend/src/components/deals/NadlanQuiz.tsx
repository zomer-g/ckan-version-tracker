/**
 * "שניים אוחזין בעסקה" — a trivia round over the gap report next door.
 *
 * Two government sites publish the same property transactions, and read the
 * way they present themselves they seem to disagree about almost everything,
 * until every row on nadlan.gov.il is opened. The report at ?tab=gaps says why in prose;
 * this tab asks you to guess first, which turns out to be the only way anyone
 * remembers that "מחיר העסקה" on nadlan.gov.il is the assessed value and not
 * the declared one.
 *
 * Deliberately un-pushy: no timer, no streak to protect, no nagging to come
 * back. Every answer is followed by the explanation whether you got it right or
 * wrong, and a link to the report sits on screen the whole time. Cheating is
 * the intended failure mode, because a player who goes and reads the thing has
 * done exactly what the tab is for.
 *
 * A round is 25 questions drawn from a larger bank and shuffled, so a second
 * round is not the same round.
 */
import { useCallback, useMemo, useState } from "react";
import { QUIZ_QUESTIONS, QuizQuestion, ROUND_SIZE } from "./nadlanQuizQuestions";

const GAME_NAME = "שניים אוחזין בעסקה";
const GAME_PATH = "/projects/deals?tab=quiz";

/** One question as a round actually serves it: options shuffled, answer moved. */
type Served = {
  source: QuizQuestion;
  options: string[];
  correct: number;
};

/** Fisher-Yates. Returns a new array; never mutates the bank. */
function shuffled<T>(items: readonly T[]): T[] {
  const out = items.slice();
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

function dealRound(): Served[] {
  return shuffled(QUIZ_QUESTIONS)
    .slice(0, ROUND_SIZE)
    .map((source) => {
      // Shuffle the answers too, or "the longest option" becomes a strategy.
      const order = shuffled(source.options.map((_, i) => i));
      return {
        source,
        options: order.map((i) => source.options[i]),
        correct: order.indexOf(source.correct),
      };
    });
}

/* ── how the score is reported back ─────────────────────────────────────── */

type Rank = { title: string; note: string };

function rankFor(score: number, total: number): Rank {
  const pct = score / total;
  if (pct >= 0.96) {
    return {
      title: "שניים אוחזין, ואתם מחזיקים בשניהם",
      note: "אתם יודעים על הפערים יותר משני האתרים יודעים זה על זה.",
    };
  }
  if (pct >= 0.8) {
    return {
      title: "שמאי מכריע",
      note: "כשהמאגרים סותרים זה את זה, אתם כבר יודעים למי להאמין ולמה.",
    };
  }
  if (pct >= 0.6) {
    return {
      title: "בודק נאותות",
      note: "תופסים את רוב הפערים. את השאר כדאי לתפוס לפני החתימה.",
    };
  }
  if (pct >= 0.4) {
    return {
      title: "סמכתם על אתר הנדל״ן",
      note: "טעות מובנת. הטבלה שלו באמת נראית כאילו היא מציגה את כל העסקאות.",
    };
  }
  if (pct >= 0.2) {
    return {
      title: "קניתם לפי המספר שהופיע במסך",
      note: "המספר הזה, אגב, הוא שווי המכירה ולא התמורה שהוצהרה.",
    };
  }
  return {
    title: "טרם קראתם את הדוח",
    note: "וזה לגמרי בסדר, הוא נמצא בלשונית ממש כאן ליד.",
  };
}

/** A Wordle-style grid: five per line, right to left like everything else. */
function scoreGrid(results: boolean[]): string {
  const lines: string[] = [];
  for (let i = 0; i < results.length; i += 5) {
    lines.push(results.slice(i, i + 5).map((ok) => (ok ? "🟩" : "🟥")).join(""));
  }
  return lines.join("\n");
}

/* ── the game ───────────────────────────────────────────────────────────── */

type Phase = "intro" | "playing" | "done";

export default function NadlanQuiz({ onReadReport }: { onReadReport: () => void }) {
  const [phase, setPhase] = useState<Phase>("intro");
  const [deck, setDeck] = useState<Served[]>([]);
  const [idx, setIdx] = useState(0);
  const [picked, setPicked] = useState<number | null>(null);
  const [results, setResults] = useState<boolean[]>([]);
  const [shareNote, setShareNote] = useState<string | null>(null);

  const start = useCallback(() => {
    setDeck(dealRound());
    setIdx(0);
    setPicked(null);
    setResults([]);
    setShareNote(null);
    setPhase("playing");
  }, []);

  const pick = useCallback(
    (option: number) => {
      if (picked != null) return; // answered already; the second click is a misclick
      setPicked(option);
      setResults((r) => [...r, option === deck[idx].correct]);
    },
    [picked, deck, idx],
  );

  const next = useCallback(() => {
    if (picked == null) return;
    if (idx + 1 >= deck.length) { setPhase("done"); return; }
    setIdx(idx + 1);
    setPicked(null);
  }, [picked, idx, deck.length]);

  const score = useMemo(() => results.filter(Boolean).length, [results]);

  const shareText = useMemo(() => {
    const rank = rankFor(score, deck.length || ROUND_SIZE);
    const url = typeof window === "undefined"
      ? `https://over.org.il${GAME_PATH}`
      : `${window.location.origin}${GAME_PATH}`;
    return `${GAME_NAME} 🏛\n${score}/${deck.length} · ${rank.title}\n\n${scoreGrid(results)}\n\n${url}`;
  }, [score, deck.length, results]);

  const share = useCallback(async () => {
    // navigator.share is the native sheet on mobile; the clipboard is the
    // fallback everywhere else, and both can be refused, hence the message.
    try {
      if (navigator.share) {
        await navigator.share({ title: GAME_NAME, text: shareText });
        return;
      }
      await navigator.clipboard.writeText(shareText);
      setShareNote("התוצאה הועתקה, אפשר להדביק אותה איפה שתרצו.");
    } catch {
      setShareNote("השיתוף נחסם בדפדפן. אפשר לסמן את הטקסט למטה ולהעתיק ידנית.");
    }
  }, [shareText]);

  /* ── intro ── */
  if (phase === "intro") {
    return (
      <div className="nquiz">
        <div className="nquiz-hero">
          <div className="nquiz-kicker">משחק ניחושים · על סמך הבדיקה בלשונית שלצד</div>
          <h2>{GAME_NAME}</h2>
          <p className="nquiz-lede">
            שני אתרים ממשלתיים מפרסמים את אותן עסקאות נדל״ן, ומי שקורא אותם כפי שהם מוצגים
            יקבל שתי תמונות שונות: מספר עסקאות אחר, סכומים אחרים, לפעמים אפילו שם יישוב אחר.
            עשר חלקות אקראיות נבדקו בשניהם עמוד אחר עמוד, כולל כל חלון היסטוריה, וכל שאלה
            כאן היא ממצא אמיתי מהבדיקה הזו.
          </p>
          <p className="nquiz-lede">
            {ROUND_SIZE} שאלות, בלי טיימר ובלי לחץ. אחרי כל תשובה מופיע ההסבר, גם אם
            צדקתם. ואם בא לכם לרמות ולקרוא קודם את הדוח, זו בדיוק המטרה.
          </p>
          <div className="nquiz-cta">
            <button type="button" className="btn-primary" onClick={start}>
              יאללה, מתחילים
            </button>
            <button type="button" className="nquiz-link" onClick={onReadReport}>
              רגע, קודם אקרא את הדוח ↗
            </button>
          </div>
          <p className="nquiz-fine">
            {ROUND_SIZE} שאלות נשלפות מתוך מאגר של {QUIZ_QUESTIONS.length}, בסדר אקראי,
            כך שסיבוב שני לא יהיה אותו סיבוב.
          </p>
        </div>
      </div>
    );
  }

  /* ── results ── */
  if (phase === "done") {
    const rank = rankFor(score, deck.length);
    return (
      <div className="nquiz">
        <div className="nquiz-hero nquiz-result">
          <div className="nquiz-kicker">{GAME_NAME} · התוצאה שלכם</div>
          <div className="nquiz-score" aria-live="polite">
            <b>{score}</b>
            <span>מתוך {deck.length}</span>
          </div>
          <h2>{rank.title}</h2>
          <p className="nquiz-lede">{rank.note}</p>

          <pre className="nquiz-grid" aria-label={`${score} תשובות נכונות מתוך ${deck.length}`}>
            {scoreGrid(results)}
          </pre>

          <div className="nquiz-cta">
            <button type="button" className="btn-primary" onClick={share}>
              שיתוף התוצאה
            </button>
            <button type="button" className="btn-secondary" onClick={start}>
              סיבוב נוסף
            </button>
            <button type="button" className="nquiz-link" onClick={onReadReport}>
              לקרוא את הדוח המלא ↗
            </button>
          </div>
          {shareNote && <p className="nquiz-sharenote">{shareNote}</p>}

          <details className="nquiz-review">
            <summary>לראות את כל {deck.length} השאלות והתשובות</summary>
            <ol className="nquiz-reviewlist">
              {deck.map((s, i) => (
                <li key={s.source.id} className={results[i] ? "is-right" : "is-wrong"}>
                  <b>{s.source.q}</b>
                  <span className="nquiz-answer">
                    {results[i] ? "✅ " : "❌ "}
                    {s.options[s.correct]}
                  </span>
                  <span className="nquiz-why">{s.source.why}</span>
                </li>
              ))}
            </ol>
          </details>
        </div>
      </div>
    );
  }

  /* ── a question ── */
  const q = deck[idx];
  const answered = picked != null;
  const gotIt = answered && picked === q.correct;

  return (
    <div className="nquiz">
      <div className="nquiz-bar">
        <span className="nquiz-progress-label">
          שאלה {idx + 1} מתוך {deck.length}
        </span>
        <span className="nquiz-track" aria-hidden="true">
          <span
            className="nquiz-fill"
            style={{ width: `${((idx + (answered ? 1 : 0)) / deck.length) * 100}%` }}
          />
        </span>
        <span className="nquiz-tally">
          {score} נכונות
        </span>
      </div>

      <div className="nquiz-card">
        <h3 className="nquiz-q">{q.source.q}</h3>

        <div className="nquiz-options" role="group" aria-label="אפשרויות">
          {q.options.map((text, i) => {
            let state = "";
            if (answered) {
              if (i === q.correct) state = " is-right";
              else if (i === picked) state = " is-wrong";
              else state = " is-dim";
            }
            return (
              <button
                key={i}
                type="button"
                className={`nquiz-option${state}`}
                onClick={() => pick(i)}
                disabled={answered}
              >
                <span className="nquiz-marker" aria-hidden="true">
                  {answered && i === q.correct ? "✓" : answered && i === picked ? "✕" : ""}
                </span>
                {text}
              </button>
            );
          })}
        </div>

        {answered && (
          <div className={`nquiz-feedback${gotIt ? " is-right" : " is-wrong"}`} aria-live="polite">
            <b>{gotIt ? "נכון." : "לא הפעם."}</b> {q.source.why}
            {q.source.finding > 0 && (
              <span className="nquiz-cite">
                {" "}
                (סעיף {q.source.finding} בדוח)
              </span>
            )}
          </div>
        )}

        <div className="nquiz-foot">
          <button
            type="button"
            className="btn-primary"
            onClick={next}
            disabled={!answered}
          >
            {idx + 1 >= deck.length ? "לתוצאה" : "לשאלה הבאה"}
          </button>
          <button type="button" className="nquiz-link" onClick={onReadReport}>
            לפתוח את הדוח ↗
          </button>
        </div>
      </div>
    </div>
  );
}
