import { Routes, Route, useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useEffect, useRef } from "react";
import { AuthProvider } from "./auth/AuthContext";
import ProtectedRoute from "./auth/ProtectedRoute";
import Navbar from "./components/Navbar";
import Footer from "./components/Footer";
import LoginPage from "./pages/LoginPage";
import SearchPage from "./pages/SearchPage";
import TrackedPage from "./pages/TrackedPage";
import VersionsPage from "./pages/VersionsPage";
import DiffPage from "./pages/DiffPage";
import AdminPage from "./pages/AdminPage";
import AboutPage from "./pages/AboutPage";

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
      <div id="main-content" ref={mainRef} tabIndex={-1} style={{ outline: "none", flex: 1 }}>
        <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route path="/about" element={<AboutPage />} />
          <Route
            path="/"
            element={
              <ProtectedRoute>
                <div className="container mt-3"><SearchPage /></div>
              </ProtectedRoute>
            }
          />
          <Route
            path="/tracked"
            element={
              <ProtectedRoute>
                <div className="container mt-3"><TrackedPage /></div>
              </ProtectedRoute>
            }
          />
          <Route
            path="/versions/:datasetId"
            element={
              <ProtectedRoute>
                <div className="container mt-3"><VersionsPage /></div>
              </ProtectedRoute>
            }
          />
          <Route
            path="/diff/:datasetId"
            element={
              <ProtectedRoute>
                <div className="container mt-3"><DiffPage /></div>
              </ProtectedRoute>
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
        </Routes>
      </div>
      <Footer />
    </AuthProvider>
  );
}
