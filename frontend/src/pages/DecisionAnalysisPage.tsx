import { useEffect, useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import {
  decisionAnalysis,
  DecisionAnalysisView,
  DecisionSection,
  DecisionTask,
  DecisionTaskStatus,
} from "../api/client";
import { useAuth } from "../auth/AuthContext";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
// The government-decision analysis page. Three layers, all open by default:
//
//   1. the decision's full text, clause by clause — always visible;
//   2. the operative tasks extracted out of the clauses that carry an
//      obligation;
//   3. for each task: what it was worth, what actually happened, what the gap
//      cost.
//
// A reader who lands here sees the whole argument without having to discover a
// button first; the two switches only FOLD a layer away, for someone who wants
// to read the decision's own wording undisturbed. They are switches and not
// action buttons on purpose: the caption names the layer and never changes, and
// the on/off state is carried by the track, the "מוצג/מוסתר" word and
// aria-checked — so it is always readable whether a layer is currently showing,
// rather than having to infer it from what a button offers to do next.
//
// Layers 2 and 3 nest (the analysis renders inside the task cards), so the two
// switches move together where they must: folding the tasks folds the analysis,
// and switching the analysis on brings the tasks back. Each switch stays
// enabled and every click therefore changes something visible — no disabled
// control that does nothing when clicked.
//
// Content comes from GET /api/decision-analysis/{key}, which 404s while the
// analysis is unpublished. An admin then falls back to the draft endpoint and
// gets the same page behind a "draft" banner — the preview and the live page
// are the same component, so what is approved is what ships.

const STATUS_CLASS: Record<DecisionTaskStatus, string> = {
  done: "decision-status-done",
  partial: "decision-status-partial",
  not_done: "decision-status-not-done",
  unknown: "decision-status-unknown",
};

const STATUS_LABEL_KEY: Record<DecisionTaskStatus, string> = {
  done: "status_done",
  partial: "status_partial",
  not_done: "status_not_done",
  unknown: "status_unknown",
};

export default function DecisionAnalysisPage() {
  const { key = "1933" } = useParams();
  const { t } = useTranslation();
  const { user } = useAuth();

  const [view, setView] = useState<DecisionAnalysisView | null>(null);
  useDocumentTitle(view?.doc?.title ? `ניתוח החלטה — ${view.doc.title}` : "ניתוח החלטת ממשלה");
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState<string | null>(null);

  // Everything open on arrival — see the header note.
  const [showTasks, setShowTasks] = useState(true);
  const [showAnalysis, setShowAnalysis] = useState(true);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    setErr(null);
    setView(null);
    // A different decision starts fully open again, like a first visit.
    setShowTasks(true);
    setShowAnalysis(true);

    decisionAnalysis
      .get(key)
      .catch((e) => {
        // Unpublished (404) — an admin may still preview the draft.
        if (user?.is_admin) return decisionAnalysis.getDraft(key);
        throw e;
      })
      .then((data) => {
        if (alive) setView(data);
      })
      .catch((e) => {
        if (alive) setErr((e as Error)?.message ?? String(e));
      })
      .finally(() => {
        if (alive) setLoading(false);
      });

    return () => {
      alive = false;
    };
  }, [key, user?.is_admin]);

  const doc = view?.doc;

  // Sections in document order, grouped into the parts they belong to. The part
  // is a plain string on each section, so a rename in the admin editor just
  // re-groups without any id bookkeeping.
  const parts = useMemo(() => {
    const out: { part: string; sections: DecisionSection[] }[] = [];
    for (const section of doc?.sections ?? []) {
      const last = out[out.length - 1];
      if (last && last.part === section.part) last.sections.push(section);
      else out.push({ part: section.part, sections: [section] });
    }
    return out;
  }, [doc]);

  const taskCount = useMemo(
    () => (doc?.sections ?? []).reduce((n, s) => n + s.tasks.length, 0),
    [doc],
  );

  const label = (name: string, fallback: string) => doc?.labels?.[name] || fallback;

  const toggleTasks = () => {
    const next = !showTasks;
    setShowTasks(next);
    // Folding the tasks folds the analysis with them — it renders inside the
    // task cards, so its switch has to follow rather than sit on "מוצג" over an
    // empty page.
    if (!next) setShowAnalysis(false);
  };

  const toggleAnalysis = () => {
    const next = !showAnalysis;
    setShowAnalysis(next);
    // ...and the reverse: switching the analysis on has to bring its host cards
    // back, so the click is never a no-op.
    if (next) setShowTasks(true);
  };

  if (loading) {
    return (
      <div className="container mt-3">
        <div className="loading" role="status">
          {t("common.loading")}
        </div>
      </div>
    );
  }

  if (err || !doc) {
    return (
      <div className="about-section">
        <div className="about-card">
          <h2>{t("decision.not_found_title", "הניתוח אינו זמין")}</h2>
          <p>
            {t(
              "decision.not_found_text",
              "העמוד הזה עדיין לא פורסם. בינתיים אפשר לקרוא את עמוד הרציונל.",
            )}
          </p>
          <p>
            <Link to="/rationale">{t("nav.rationale", "הרציונל")}</Link>
          </p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="about-hero">
        <div className="container">
          <h1>{doc.title}</h1>
          <p className="rationale-hero-sub">{doc.subtitle}</p>
        </div>
      </div>

      <div className="about-section">
        {!view?.published && (
          <div className="decision-draft-banner" role="status">
            <strong>{t("decision.draft_title", "טיוטה — לא מוצג לציבור")}</strong>
            <span>
              {t(
                "decision.draft_text",
                'העמוד גלוי לך כמנהל בלבד. לפרסום, עברו ללשונית "ניתוח החלטה" בפאנל הניהול.',
              )}
            </span>
          </div>
        )}

        <div className="about-card">
          <p className="decision-meta">
            <a href={doc.decision_url} target="_blank" rel="noopener noreferrer">
              {t("decision.meta_decision", "החלטת ממשלה")} {doc.decision_number}
            <span className="sr-only"> (נפתח בחלון חדש)</span></a>
            {doc.decision_date ? ` · ${doc.decision_date}` : ""}
            {` · ${parts.reduce((n, p) => n + p.sections.length, 0)} ${t("decision.meta_sections", "סעיפים")}`}
            {` · ${taskCount} ${t("decision.meta_tasks", "משימות אופרטיביות")}`}
          </p>
          <p>{doc.intro}</p>

          <div
            className="decision-controls"
            role="group"
            aria-label={t("decision.controls_label", "מה מוצג בעמוד")}
          >
            <DecisionToggle
              checked={showTasks}
              onChange={toggleTasks}
              text={label("toggle_tasks", "המשימות האופרטיביות")}
            />
            <DecisionToggle
              checked={showAnalysis}
              onChange={toggleAnalysis}
              text={label("toggle_analysis", "הניתוח — מה זה היה שווה ומה יצא מזה")}
            />
          </div>
          <p className="decision-controls-hint">
            {t(
              "decision.controls_hint",
              "הכול פתוח כברירת מחדל. כיבוי מתג מקפל את השכבה ומשאיר את נוסח ההחלטה עצמו.",
            )}
          </p>
        </div>

        {parts.map((group) => (
          <section key={group.part} className="decision-part">
            <h2 className="decision-part-title">{group.part}</h2>
            {group.sections.map((section) => (
              <article key={section.id} id={`s-${section.id}`} className="about-card decision-section">
                <header className="decision-section-head">
                  <span className="decision-section-label">{section.label}</span>
                  <h3>{section.heading}</h3>
                </header>
                <p className="decision-text">{section.text}</p>

                {showTasks && section.tasks.length > 0 && (
                  <div className="decision-tasks">
                    <h4 className="decision-tasks-title">
                      {label("tasks_heading", "המשימות האופרטיביות שנגזרות מהסעיף")}
                    </h4>
                    {section.tasks.map((task) => (
                      <TaskCard
                        key={task.id}
                        task={task}
                        showAnalysis={showAnalysis}
                        label={label}
                      />
                    ))}
                  </div>
                )}
              </article>
            ))}
          </section>
        ))}

        <div className="about-card">
          <p>
            <Link to="/rationale">
              {t("decision.back_to_rationale", "חזרה לעמוד הרציונל")}
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}

// A switch, not a button: the caption names the layer and stays put, while the
// track, the state word and aria-checked all say whether that layer is showing
// right now. An action-labelled button ("hide the tasks") has to be decoded —
// is that what it does, or what it is? — which is exactly what confused readers
// of the first version.
function DecisionToggle({
  checked,
  onChange,
  text,
}: {
  checked: boolean;
  onChange: () => void;
  text: string;
}) {
  const { t } = useTranslation();
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      onClick={onChange}
      className={`decision-toggle${checked ? " is-on" : ""}`}
    >
      <span className="decision-toggle-track" aria-hidden="true">
        <span className="decision-toggle-thumb" />
      </span>
      <span className="decision-toggle-text">{text}</span>
      <span className="decision-toggle-state">
        {checked
          ? t("decision.state_shown", "מוצג")
          : t("decision.state_hidden", "מוסתר")}
      </span>
    </button>
  );
}

function TaskCard({
  task,
  showAnalysis,
  label,
}: {
  task: DecisionTask;
  showAnalysis: boolean;
  label: (name: string, fallback: string) => string;
}) {
  const status = (task.status || "unknown") as DecisionTaskStatus;
  return (
    <div className="decision-task">
      <div className="decision-task-head">
        <h5>{task.title}</h5>
        <span className={`decision-status ${STATUS_CLASS[status]}`}>
          {label(STATUS_LABEL_KEY[status], status)}
        </span>
      </div>
      {task.obligation && <p className="decision-task-obligation">{task.obligation}</p>}
      {(task.responsible || task.due) && (
        <dl className="decision-task-meta">
          {task.responsible && (
            <div>
              <dt>{label("responsible", "האחריות")}</dt>
              <dd>{task.responsible}</dd>
            </div>
          )}
          {task.due && (
            <div>
              <dt>{label("due", "המועד שנקבע")}</dt>
              <dd>{task.due}</dd>
            </div>
          )}
        </dl>
      )}

      {showAnalysis && (
        <div className="decision-analysis">
          <AnalysisBlock
            kind="potential"
            title={label("potential", "הפוטנציאל שהיה")}
            body={task.potential}
          />
          <AnalysisBlock
            kind="actual"
            title={label("actual", "מה קרה בפועל")}
            body={task.actual}
          />
          <AnalysisBlock
            kind="damage"
            title={label("damage", "מה זה עלה לנו")}
            body={task.damage}
          />
        </div>
      )}
    </div>
  );
}

function AnalysisBlock({
  kind,
  title,
  body,
}: {
  kind: "potential" | "actual" | "damage";
  title: string;
  body: string;
}) {
  const { t } = useTranslation();
  return (
    <div className={`decision-analysis-block decision-analysis-${kind}`}>
      <h6>{title}</h6>
      <p>{body || t("decision.analysis_empty", "טרם נכתב.")}</p>
    </div>
  );
}
