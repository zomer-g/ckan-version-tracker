import { useEffect } from "react";
import { Trans, useTranslation } from "react-i18next";
import { Link, useLocation } from "react-router-dom";
import { usePageContentOverrides } from "../hooks/usePageContentOverrides";

import { useDocumentTitle } from "../hooks/useDocumentTitle";
import { GLOSSARY } from "../components/a11y/Abbr";
// Small helper for external links so the Trans <1> placeholder stays terse.
function ExtLink({ href, children }: { href: string; children?: React.ReactNode }) {
  return (
    <a href={href} target="_blank" rel="noopener noreferrer">
      {children}
    <span className="sr-only"> (נפתח בחלון חדש)</span></a>
  );
}

export default function AboutPage() {
  useDocumentTitle("אודות");
  const { t } = useTranslation();
  usePageContentOverrides("about");

  // Footer links to /about#privacy and /about#accessibility arrive as a
  // client-side navigation, which never scrolls to the fragment on its own
  // (App only skips its scroll-to-top when a hash is present).
  const { hash } = useLocation();
  useEffect(() => {
    if (!hash) return;
    document.getElementById(decodeURIComponent(hash.slice(1)))?.scrollIntoView();
  }, [hash]);

  return (
    <div>
      <div className="about-hero">
        <div className="container">
          <h1>{t("about.title")}</h1>
        </div>
      </div>

      <div className="about-section">
        <div className="about-card">
          <h2>{t("about.what_title")}</h2>
          <p>
            <Trans
              i18nKey="about.what_text"
              components={{ 1: <ExtLink href="https://data.gov.il" /> }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.why_title")}</h2>
          <p>{t("about.why_text")}</p>
          <p>
            <Trans
              i18nKey="about.rationale_teaser"
              components={{ 1: <Link to="/rationale" /> }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.how_title")}</h2>
          <p>
            <Trans
              i18nKey="about.how_text"
              components={{
                1: <ExtLink href="https://data.gov.il" />,
                2: <ExtLink href="https://www.odata.org.il" />,
              }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.for_whom_title")}</h2>
          <ul>
            <li>{t("about.for_whom_item1")}</li>
            <li>{t("about.for_whom_item2")}</li>
            <li>{t("about.for_whom_item3")}</li>
          </ul>
        </div>

        <div className="about-card">
          <h2>{t("about.what_can_do_title")}</h2>
          <ul>
            <li>{t("about.what_can_do_item1")}</li>
            <li>{t("about.what_can_do_item2")}</li>
            <li>{t("about.what_can_do_item3")}</li>
          </ul>
        </div>

        <div className="about-card">
          <h2>{t("about.who_title")}</h2>
          <p>
            <Trans
              i18nKey="about.who_text"
              components={{
                strong: <strong />,
                1: <ExtLink href="https://www.z-g.co.il/projects" />,
              }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.family_title")}</h2>
          <p>{t("about.family_intro")}</p>
          <ul className="family-list">
            <li>
              <strong>
                <ExtLink href="https://www.odata.org.il">
                  {t("about.family_odata_name")}
                </ExtLink>
              </strong>
              {": "}
              {t("about.family_odata_desc")}
            </li>
            <li>
              <strong>
                {/* Hosted here now, unlike its siblings — ocoi.org.il is being
                    retired, so this must not point out at a service that is
                    about to stop answering. */}
                <Link to="/projects/ocoi">{t("about.family_ocoi_name")}</Link>
              </strong>
              {": "}
              {t("about.family_ocoi_desc")}
            </li>
            <li>
              <strong>
                <ExtLink href="https://ocal.org.il">
                  {t("about.family_ocal_name")}
                </ExtLink>
              </strong>
              {": "}
              {t("about.family_ocal_desc")}
            </li>
          </ul>
        </div>

        {/* The two story documents are served by the backend straight from the
            repo root (GET /story/<name>), not bundled into the SPA — so they
            are reached with a plain <a>, never a react-router <Link>, which
            would try to resolve them client-side and land on a 404.

            They are embedded rather than only linked because someone reading
            "what is this project" should be able to see the answer without
            leaving the page; each carries its own full-page link for a reader
            who wants it larger, or wants to present from it. */}
        <div className="about-card" id="story">
          <h2>{t("about.story_title")}</h2>
          <p>{t("about.story_intro")}</p>

          <h3 className="story-embed-head">{t("about.story_deck_title")}</h3>
          <p className="text-muted text-sm">{t("about.story_deck_desc")}</p>
          <iframe
            className="story-embed story-embed--deck"
            src="/story/deck"
            title={t("about.story_deck_title")}
            loading="lazy"
            allow="fullscreen"
          />
          <p className="story-embed-link">
            <ExtLink href="/story/deck">{t("about.story_open_deck")}</ExtLink>
          </p>

          <h3 className="story-embed-head">{t("about.story_timeline_title")}</h3>
          <p className="text-muted text-sm">{t("about.story_timeline_desc")}</p>
          <iframe
            className="story-embed story-embed--timeline"
            src="/story/timeline"
            title={t("about.story_timeline_title")}
            loading="lazy"
          />
          <p className="story-embed-link">
            <ExtLink href="/story/timeline">{t("about.story_open_timeline")}</ExtLink>
            {" · "}
            <a href="/story/timeline-deck.pptx" download>
              {t("about.story_download_timeline_pptx")}
            </a>
          </p>

          {/* A .pptx cannot be embedded, so this one is presented as what it
              is: a file to download and present from. Not an <ExtLink> — the
              route answers with Content-Disposition: attachment, so a new tab
              would open and immediately close itself. */}
          <h3 className="story-embed-head">{t("about.story_pptx_title")}</h3>
          <p className="text-muted text-sm">{t("about.story_pptx_desc")}</p>
          <p className="story-embed-link">
            <a href="/story/spatial-deck.pptx" download>
              {t("about.story_download_pptx")}
            </a>
          </p>
        </div>

        {/* WCAG 3.1.3 (Unusual Words) and 3.1.4 (Abbreviations) ask that a
            reader be able to look up jargon they cannot work out from context.
            The site is full of it — CKAN, ODATA, WFS, גוש/חלקה. One list, read
            from the same table the inline <Abbr> tooltips use, so the two
            cannot drift apart. */}
        <div className="about-card" id="glossary">
          <h2>מילון מונחים</h2>
          <p className="text-muted text-sm">
            מונחים שחוזרים באתר. כל מונח מסומן בקו מקווקו בטקסט — אפשר לעצור
            עליו כדי לראות את ההסבר, וכאן מופיעה הרשימה המלאה.
          </p>
          <dl style={{ display: "grid", gap: "0.7rem", marginTop: "1rem" }}>
            {Object.entries(GLOSSARY).map(([term, meaning]) => (
              <div key={term}>
                <dt style={{ fontWeight: 700 }} dir={/^[A-Za-z]/.test(term) ? "ltr" : "rtl"}>
                  {term}
                </dt>
                <dd style={{ margin: 0, color: "var(--text-muted)" }}>{meaning}</dd>
              </div>
            ))}
          </dl>
        </div>

        <div className="about-card" id="privacy">
          <h2>{t("about.privacy_title")}</h2>
          <p>
            <Trans
              i18nKey="about.privacy_text"
              components={{
                strong: <strong />,
                1: <a href="mailto:guy@z-g.co.il" />,
              }}
            />
          </p>
        </div>

        <div className="about-card" id="accessibility">
          <h2>{t("about.accessibility_title")}</h2>
          <p>
            <Trans
              i18nKey="about.accessibility_text"
              components={{
                1: <a href="mailto:guy@z-g.co.il" />,
              }}
            />
          </p>
        </div>

        <div className="about-card">
          <h2>{t("about.contact_title")}</h2>
          <p>
            <Trans
              i18nKey="about.contact_text"
              components={{
                1: <a href="mailto:guy@z-g.co.il" />,
              }}
            />
          </p>
        </div>
      </div>
    </div>
  );
}
