import { Routes, Route, useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useEffect, useRef } from "react";
import { AuthProvider } from "./auth/AuthContext";
import ProtectedRoute from "./auth/ProtectedRoute";
import Navbar from "./components/Navbar";
import LoginPage from "./pages/LoginPage";
import RegisterPage from "./pages/RegisterPage";
import SearchPage from "./pages/SearchPage";
import TrackedPage from "./pages/TrackedPage";
import VersionsPage from "./pages/VersionsPage";
import DiffPage from "./pages/DiffPage";

export default function App() {
  const { t, i18n } = useTranslation();
  const location = useLocation();
  const mainRef = useRef<HTMLElement>(null);

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
      <main id="main-content" ref={mainRef} tabIndex={-1} className="container mt-3" style={{ outline: "none" }}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="/register" element={<RegisterPage />} />
          <Route
            path="/"
            element={
              <ProtectedRoute>
                <SearchPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/tracked"
            element={
              <ProtectedRoute>
                <TrackedPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/versions/:datasetId"
            element={
              <ProtectedRoute>
                <VersionsPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/diff/:datasetId"
            element={
              <ProtectedRoute>
                <DiffPage />
              </ProtectedRoute>
            }
          />
        </Routes>
      </main>
    </AuthProvider>
  );
}
