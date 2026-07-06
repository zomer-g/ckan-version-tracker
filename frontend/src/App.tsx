import { Routes, Route, useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useEffect, useRef, lazy, Suspense } from "react";
import { AuthProvider } from "./auth/AuthContext";
import ProtectedRoute from "./auth/ProtectedRoute";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import LoginPage from "./pages/LoginPage";
import HomePage from "./pages/HomePage";
import VersionsPage from "./pages/VersionsPage";
import AppendArchivePage from "./pages/AppendArchivePage";
import AdminPage from "./pages/AdminPage";
import AboutPage from "./pages/AboutPage";
import RationalePage from "./pages/RationalePage";
import ApiPage from "./pages/ApiPage";
import OrganizationsPage from "./pages/OrganizationsPage";
import OrganizationDetailPage from "./pages/OrganizationDetailPage";
import TagsPage from "./pages/TagsPage";
import TagDetailPage from "./pages/TagDetailPage";
import SourcesPage from "./pages/SourcesPage";
import SourceDetailPage from "./pages/SourceDetailPage";
import CbsPage from "./pages/CbsPage";
// Lazy: the growth page pulls Leaflet + the streaming JSON parser, ~60 KB
// gzipped. Other pages should not pay that cost.
const GrowthPage = lazy(() => import("./pages/GrowthPage"));

export default function App() {
  const { t, i18n } = useTranslation();
  const location = useLocation();
  const mainRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    document.documentElement.dir = i18n.language === "he" ? "rtl" : "ltr";
    document.documentElement.lang = i18n.language;
  }, [i18n.language]);

  // Focus management on route change
  useEffect(() => {
    mainRef.current?.focus();
  }, [location.pathname]);

  return (
    <AuthProvider>
      <a href="#main-content" className="skip-link">
        {t("nav.skip_to_content", "Skip to content")}
      </a>
      <Navbar />
      <div id="main-content" ref={mainRef} tabIndex={-1} role="main" style={{ outline: "none", flex: 1 }}>
        <Routes>
          <Route path="/" element={<HomePage />} />
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
          <Route path="/api" element={<ApiPage />} />
          <Route path="/about" element={<AboutPage />} />
          <Route path="/rationale" element={<RationalePage />} />
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
      </div>
      <Footer />
    </AuthProvider>
  );
}
