import { Routes, Route, useLocation, useNavigationType } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useEffect, useRef, lazy, Suspense } from "react";
import { sources } from "./api/client";
import { primeRegistryBadges } from "./utils/sourceBadge";
import { AuthProvider } from "./auth/AuthContext";
import ProtectedRoute from "./auth/ProtectedRoute";
import Navbar from "./components/Navbar";
import { RouteAnnouncer } from "./components/a11y";
import SessionWarning from "./components/a11y/SessionWarning";
import Footer from "./components/Footer";
import LoginPage from "./pages/LoginPage";
import HomePage from "./pages/HomePage";
import LookupPage from "./pages/LookupPage";
import VersionsPage from "./pages/VersionsPage";
import AppendArchivePage from "./pages/AppendArchivePage";
import AdminPage from "./pages/AdminPage";
import AboutPage from "./pages/AboutPage";
import RationalePage from "./pages/RationalePage";
import DecisionAnalysisPage from "./pages/DecisionAnalysisPage";
import ApiPage from "./pages/ApiPage";
import ProjectImportPage from "./pages/ProjectImportPage";
import OcalPage from "./pages/OcalPage";
import OcoiPage from "./pages/OcoiPage";
import NadlanPage from "./pages/NadlanPage";
import QuestionsPage from "./pages/QuestionsPage";
import OrganizationsPage from "./pages/OrganizationsPage";
import OrganizationDetailPage from "./pages/OrganizationDetailPage";
import TagsPage from "./pages/TagsPage";
import TagDetailPage from "./pages/TagDetailPage";
import SourcesPage from "./pages/SourcesPage";
import SourceDetailPage from "./pages/SourceDetailPage";
import CbsPage from "./pages/CbsPage";
import CbsFeedbackPage from "./pages/CbsFeedbackPage";
import KnessetDbPage from "./pages/KnessetDbPage";
import DataSqlPage from "./pages/DataSqlPage";
import DataExplorePage from "./pages/DataExplorePage";
import DataGuidePage from "./pages/DataGuidePage";
import SettlementNormalizePage from "./pages/SettlementNormalizePage";
// Lazy: the growth page pulls Leaflet + the streaming JSON parser, ~60 KB
// gzipped. Other pages should not pay that cost.
const GrowthPage = lazy(() => import("./pages/GrowthPage"));
// Lazy: the Knesset SQL guide pulls the marked renderer (~15 KB gzipped).
const KnessetGuidePage = lazy(() => import("./pages/KnessetGuidePage"));

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
  const navigationType = useNavigationType();
  useEffect(() => {
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
            element={
              <Suspense fallback={<div className="loading" role="status">{t("common.loading")}</div>}>
                <KnessetGuidePage />
              </Suspense>
            }
          />
          <Route path="/api" element={<ApiPage />} />
          {/* A first-party page, so it gets its own component rather than the
              ProjectImportPage template (that one is for external sibling sites). */}
          <Route path="/projects/questions" element={<QuestionsPage />} />
          <Route path="/projects/odata" element={<ProjectImportPage project="odata" />} />
          <Route path="/projects/ocal" element={<OcalPage />} />
          <Route path="/projects/ocoi" element={<OcoiPage />} />
          <Route path="/projects/nadlan" element={<NadlanPage />} />
          <Route path="/about" element={<AboutPage />} />
          <Route path="/rationale" element={<RationalePage />} />
          {/* The page itself gates on `published` server-side, so the route may
              exist while the analysis is still a draft — only an admin sees it. */}
          <Route path="/rationale/:key" element={<DecisionAnalysisPage />} />
          <Route
            path="/growth"
            element={
              <Suspense fallback={<div className="loading" role="status">{t("common.loading")}</div>}>
                <GrowthPage />
              </Suspense>
            }
          />
          <Route
            path="/admin"
            element={
              <ProtectedRoute>
                <div className="container mt-3"><AdminPage /></div>
              </ProtectedRoute>
            }
          />
          <Route path="/admin/login" element={<LoginPage />} />
        </Routes>
      </main>
      <Footer />
      <SessionWarning />
    </AuthProvider>
  );
}
