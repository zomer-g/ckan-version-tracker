import { useState } from "react";
import { useTranslation } from "react-i18next";
import { cbs, CbsFeedbackBody } from "../api/client";

// 👍 / 👎 on a search. Fire-and-forget POST to /api/cbs/feedback; the vote is
// remembered per (query+mode) in localStorage so a user isn't nagged to vote
// again on the same search and can see what they picked. Feeds the admin
// feedback report that ranks queries by dislikes.

const KEY = (query: string, mode: string) => `cbsvote:${mode}:${query.trim()}`;

function priorVote(query: string, mode: string): 1 | -1 | 0 {
  try {
    const v = localStorage.getItem(KEY(query, mode));
    return v === "1" ? 1 : v === "-1" ? -1 : 0;
  } catch {
    return 0;
  }
}

interface Props {
  query: string;
  mode: "ask" | "advanced";
  answerType?: string | null;
  topUrl?: string | null;
}

export default function CbsFeedbackButtons({ query, mode, answerType, topUrl }: Props) {
  const { t } = useTranslation();
  const [vote, setVote] = useState<1 | -1 | 0>(() => priorVote(query, mode));

  const send = (v: 1 | -1) => {
    if (!query.trim()) return;
    // Toggle off if the same button is pressed again.
    const next = vote === v ? 0 : v;
    setVote(next);
    try {
      if (next === 0) localStorage.removeItem(KEY(query, mode));
      else localStorage.setItem(KEY(query, mode), String(next));
    } catch {
      /* private mode — non-fatal */
    }
    if (next === 0) return; // un-vote is local only; nothing new to record
    const body: CbsFeedbackBody = {
      query: query.trim(),
      vote: next,
      mode,
      answer_type: answerType ?? null,
      top_url: topUrl ?? null,
      source: "web",
    };
    cbs.feedback(body).catch(() => {
      /* best-effort; the vote stays reflected locally */
    });
  };

  const btn = (v: 1 | -1, emoji: string, label: string): React.CSSProperties => ({
    border: "1px solid var(--border, #e2e8f0)",
    background: vote === v ? (v > 0 ? "#ecfdf5" : "#fef2f2") : "transparent",
    color: vote === v ? (v > 0 ? "#065f46" : "#991b1b") : "var(--text-muted, #64748b)",
    borderColor: vote === v ? (v > 0 ? "#a7f3d0" : "#fecaca") : "var(--border, #e2e8f0)",
    borderRadius: "9999px",
    cursor: "pointer",
    fontSize: "0.8rem",
    padding: "0.2rem 0.6rem",
    lineHeight: 1,
  });

  return (
    <div className="flex" style={{ gap: "0.35rem", alignItems: "center" }}>
      <span className="text-sm text-muted" style={{ fontSize: "0.76rem" }}>
        {vote !== 0
          ? t("cbs.fb_thanks", "תודה על המשוב!")
          : t("cbs.fb_prompt", "עזר לך החיפוש?")}
      </span>
      <button
        type="button"
        onClick={() => send(1)}
        aria-pressed={vote === 1}
        title={t("cbs.fb_like", "עזר לי")}
        style={btn(1, "👍", "עזר לי")}
      >
        👍
      </button>
      <button
        type="button"
        onClick={() => send(-1)}
        aria-pressed={vote === -1}
        title={t("cbs.fb_dislike", "לא עזר")}
        style={btn(-1, "👎", "לא עזר")}
      >
        👎
      </button>
    </div>
  );
}
