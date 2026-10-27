import { Routes, Route, useLocation, useNavigationType } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useEffect, useRef, Suspense } from "react";
import { sources } from "./api/client";
import { primeRegistryBadges } from "./utils/sourceBadge";
import { AuthProvider } from "./auth/AuthContext";
import ProtectedRoute from "./auth/ProtectedRoute";
import Navbar from "./components/Navbar";
import { RouteAnnouncer } from "./components/a11y";
import SessionWarning from "./components/a11y/SessionWarning";
import Footer from "./components/Footer";
import HomePage from "./pages/HomePage";
import { lazyPage } from "./utils/lazyPage";
// Every page but the home page is its own chunk, fetched on first visit. The
// single bundle used to carry all ~35 pages (1.5MB, ~420KB gzipped), so the
// landing page downloaded and parsed the SQL console, the Knesset tools and
// the admin screens before showing anything. The home page stays eager: it is
// where most visits start, and a second round-trip there would only add delay.
const LoginPage = lazyPage(() => import("./pages/LoginPage"));
const LookupPage = lazyPage(() => import("./pages/LookupPage"));
const VersionsPage = lazyPage(() => import("./pages/VersionsPage"));
const AppendArchivePage = lazyPage(() => import("./pages/AppendArchivePage"));
const AdminPage = lazyPage(() => import("./pages/AdminPage"));
const AboutPage = lazyPage(() => import("./pages/AboutPage"));
const SecurityPage = lazyPage(() => import("./pages/SecurityPage"));
const RationalePage = lazyPage(() => import("./pages/RationalePage"));
const DecisionAnalysisPage = lazyPage(() => import("./pages/DecisionAnalysisPage"));
const ApiPage = lazyPage(() => import("./pages/ApiPage"));
const ProjectImportPage = lazyPage(() => import("./pages/ProjectImportPage"));
const OcalPage = lazyPage(() => import("./pages/OcalPage"));
const OcoiPage = lazyPage(() => import("./pages/OcoiPage"));
const NadlanPage = lazyPage(() => import("./pages/NadlanPage"));
const DealsPage = lazyPage(() => import("./pages/DealsPage"));
const PricesPage = lazyPage(() => import("./pages/PricesPage"));
const QuestionsPage = lazyPage(() => import("./pages/QuestionsPage"));
const OrganizationsPage = lazyPage(() => import("./pages/OrganizationsPage"));
const OrganizationDetailPage = lazyPage(() => import("./pages/OrganizationDetailPage"));
const TagsPage = lazyPage(() => import("./pages/TagsPage"));
const TagDetailPage = lazyPage(() => import("./pages/TagDetailPage"));
const SourcesPage = lazyPage(() => import("./pages/SourcesPage"));
const SourceDetailPage = lazyPage(() => import("./pages/SourceDetailPage"));
const CbsPage = lazyPage(() => import("./pages/CbsPage"));
const CbsFeedbackPage = lazyPage(() => import("./pages/CbsFeedbackPage"));
const KnessetDbPage = lazyPage(() => import("./pages/KnessetDbPage"));
const DataSqlPage = lazyPage(() => import("./pages/DataSqlPage"));
const DataExplorePage = lazyPage(() => import("./pages/DataExplorePage"));
const DataGuidePage = lazyPage(() => import("./pages/DataGuidePage"));
const SettlementNormalizePage = lazyPage(() => import("./pages/SettlementNormalizePage"));
const GrowthPage = lazyPage(() => import("./pages/GrowthPage"));
const KnessetGuidePage = lazyPage(() => import("./pages/KnessetGuidePage"));

export default function App() {
  const { t, i18n } = useTranslation();
  const location = useLocation();
  const mainRef = useRef<HTMLElement>(null);

  // Language of the page, honestly (WCAG 3.1.1) — and of its parts (3.1.2).
  //
  // The i18n layer covers 465 keys; roughly 2,900 Hebrew strings live directly
  // in the components and do not translate. So switching to EN used to stamp
  // lang="en" on a document that stayed overwhelmingly Hebrew, and a screen
  // reader would try to pronounce Hebrew with an English voice — worse than no
  // toggle at all.
  //
  // What is true in both modes: the document's language is Hebrew. The chrome
  // (navbar, footer) is what actually translates, so it declares its own
  // language and direction where they differ — see Navbar.tsx / Footer.tsx.
  useEffect(() => {
    document.documentElement.lang = "he";
    document.documentElement.dir = "rtl";
  }, [i18n.language]);

  // Chips for sources declared by the scraper worker instead of hardcoded in
  // sourceBadge.ts. Non-blocking and best-effort: until it lands (or if it
  // fails) such a dataset wears the generic scraper chip. Re-runs on a
  // language switch because the source-link label is per-language.
  useEffect(() => {
    let cancelled = false;
    sources
      .registry()
      .then((data) => {
        if (!cancelled) primeRegistryBadges(data.sources || [], i18n.language);
      })
      .catch(() => {
        /* generic chip is a fine fallback — never block the app on this */
      });
    return () => {
      cancelled = true;
    };
  }, [i18n.language]);

  // Focus + scroll management on route change.
  //
  // React Router does not reset scroll on navigation, so following a link from
  // halfway down a long page landed on the NEXT page still scrolled — header
  // above the viewport, content starting mid-way. It only looked right when the
  // destination happened to be short (the browser clamps the offset) or was
  // still loading its data, which is why it struck intermittently.
  //
  // Two exceptions:
  //   · POP (back/forward) — leave it alone; the browser restores the previous
  //     position, which is exactly what going back is for.
  //   · A hash (/x#section) — the caller named a target, don't override it.
  // preventScroll stops the a11y focus from re-introducing the same jump:
  // #main-content sits below the navbar, so focusing it can pull the header
  // back out of view on pages where it doesn't span the viewport.
  //   · A query-string change — ?q=, ?page= and friends are a change WITHIN a
  //     page, not a move to a new one. The component that owns the parameter
  //     knows where focus belongs (the pager sends it to the list heading);
  //     stealing it back to <main> a frame later undid that. navigationType is
  //     read here but must not TRIGGER this effect: pushing a query string
  //     flips it from POP to PUSH, which is what used to re-fire it.
  const navigationType = useNavigationType();
  const lastPath = useRef<string | null>(null);
  useEffect(() => {
    if (lastPath.current === location.pathname) return;
    lastPath.current = location.pathname;
    mainRef.current?.focus({ preventScroll: true });
    if (navigationType !== "POP" && !location.hash) window.scrollTo(0, 0);
  }, [location.pathname, location.hash, navigationType]);

  return (
    <AuthProvider>
      <a href="#main-content" className="skip-link">
        {t("nav.skip_to_content", "Skip to content")}
      </a>
      <Navbar />
      {/* Focus moving to <main> says "something changed" but not what. This
          reads the new page title once the route has set it (WCAG 4.1.3). */}
      <RouteAnnouncer />
      <main id="main-content" ref={mainRef} tabIndex={-1} style={{ outline: "none", flex: 1 }}>
        {/* One boundary for every lazy page. Inside <main>, so the navbar and
            footer stay put while a page's chunk loads. */}
        <Suspense fallback={<div className="loading" role="status">{t("common.loading")}</div>}>
        <Routes>
          <Route path="/" element={<HomePage />} />
          {/* Shareable deep link → the dataset's versions page if tracked, the
              collection request form if not. /direct/<url> is the shape tools
              build by concatenation; in production the server redirects it
              before the SPA loads, so this route is the dev/safety-net twin. */}
          <Route path="/lookup" element={<LookupPage />} />
          <Route path="/direct/*" element={<LookupPage />} />
          <Route
            path="/versions/:datasetId"
            element={<div className="container mt-3"><VersionsPage /></div>}
          />
          <Route path="/archive/:datasetId" element={<AppendArchivePage />} />
          <Route path="/organizations" element={<OrganizationsPage />} />
          <Route path="/organizations/:orgId" element={<OrganizationDetailPage />} />
          <Route path="/tags" element={<TagsPage />} />
          <Route path="/tags/:tagId" element={<TagDetailPage />} />
          <Route path="/sources" element={<SourcesPage />} />
          <Route path="/sources/:sourceId" element={<SourceDetailPage />} />
          <Route path="/cbs" element={<CbsPage />} />
          <Route
            path="/cbs/feedback"
            element={
              <ProtectedRoute>
                <CbsFeedbackPage />
              </ProtectedRoute>
            }
          />
          <Route path="/data" element={<DataSqlPage />} />
          <Route path="/data/explore" element={<DataExplorePage />} />
          <Route path="/data/guide" element={<DataGuidePage />} />
          <Route path="/data/normalize" element={<SettlementNormalizePage />} />
          <Route path="/knesset" element={<KnessetDbPage />} />
          <Route
            path="/knesset/guide"
            element={<KnessetGuidePage />}
          />
          <Route path="/api" element={<ApiPage />} />
          {/* A first-party page, so it gets its own component rather than the
              ProjectImportPage template (that one is for external sibling sites). */}
          <Route path="/projects/questions" element={<QuestionsPage />} />
          <Route path="/projects/odata" element={<ProjectImportPage project="odata" />} />
          <Route path="/projects/ocal" element={<OcalPage />} />
          <Route path="/projects/ocoi" element={<OcoiPage />} />
          <Route path="/projects/nadlan" element={<NadlanPage />} />
          {/* The deal register and the gap report, which used to be two tabs of
              /projects/nadlan and were never property lookups. */}
          <Route path="/projects/deals" element={<DealsPage />} />
          <Route path="/projects/prices" element={<PricesPage />} />
          <Route path="/about" element={<AboutPage />} />
          {/* Beside the privacy policy and the accessibility statement: how to
              report, and the public log of what was reported and fixed. */}
          <Route path="/security" element={<SecurityPage />} />
          <Route path="/rationale" element={<RationalePage />} />
          {/* The page itself gates on `published` server-side, so the route may
              exist while the analysis is still a draft — only an admin sees it. */}
          <Route path="/rationale/:key" element={<DecisionAnalysisPage />} />
          <Route
            path="/growth"
            element={<GrowthPage />}
          />
          <Route
            path="/admin"
            element={
              <ProtectedRoute requireAdmin>
                <div className="container mt-3"><AdminPage /></div>
              </ProtectedRoute>
            }
          />
          <Route path="/admin/login" element={<LoginPage />} />
        </Routes>
        </Suspense>
      </main>
      <Footer />
      <SessionWarning />
    </AuthProvider>
  );
}
