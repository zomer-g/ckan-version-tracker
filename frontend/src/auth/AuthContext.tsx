import {
  createContext,
  useContext,
  useState,
  useEffect,
  ReactNode,
} from "react";
import { auth as authApi, setToken, clearToken } from "../api/client";

interface User {
  id: string;
  email: string;
  display_name: string;
  is_admin: boolean;
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  loginWithToken: (token: string) => Promise<void>;
  logout: () => void;
  /** Set when the silent refresh has failed and the session is about to end. */
  sessionWarning: boolean;
  /** Retry the refresh from the warning banner. */
  renewSession: () => Promise<void>;
}

const AuthContext = createContext<AuthContextType>(null!);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);
  // A failed silent refresh used to be swallowed: the next real call returned
  // 401 and the user was thrown out mid-task, losing whatever they had typed.
  // WCAG 2.2.1 wants warning before a time limit expires, and 2.2.5 wants the
  // work to survive re-authentication — so the failure is surfaced here and
  // the banner offers to renew in place.
  const [sessionWarning, setSessionWarning] = useState(false);

  useEffect(() => {
    let cancelled = false;

    // The SSO callback redirects here with a ONE-TIME code (?code=), not a JWT.
    // Swap it for a token via POST, then scrub the query string. Anything with
    // a token in it never reaches the URL/Referer/history/logs.
    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");

    const bootstrap = async () => {
      if (code) {
        window.history.replaceState({}, "", window.location.pathname);
        try {
          const { token } = await authApi.exchange(code);
          setToken(token);
        } catch {
          /* invalid/expired code — fall through to any stored token */
        }
      }

      const token = localStorage.getItem("token");
      if (!token) {
        if (!cancelled) setLoading(false);
        return;
      }
      try {
        const me = await authApi.me();
        if (cancelled) return;
        setUser(me);
        // Slide the short-lived session forward the moment we confirm it's good.
        try {
          const { token: fresh } = await authApi.refresh();
          setToken(fresh);
        } catch {
          /* refresh is best-effort; the current token is still valid */
        }
      } catch {
        clearToken();
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    bootstrap();
    return () => {
      cancelled = true;
    };
  }, []);

  // Keep a logged-in session alive: refresh well inside the ~2h token TTL so an
  // active admin is never bounced. Only runs while a user is present.
  useEffect(() => {
    if (!user) return;
    const REFRESH_INTERVAL_MS = 45 * 60 * 1000; // 45 min < 120 min TTL
    const id = window.setInterval(() => {
      authApi
        .refresh()
        .then(({ token }) => {
          setToken(token);
          setSessionWarning(false);
        })
        .catch(() => setSessionWarning(true));
    }, REFRESH_INTERVAL_MS);
    return () => window.clearInterval(id);
  }, [user]);

  const loginWithToken = async (token: string) => {
    setToken(token);
    const me = await authApi.me();
    setUser(me);
  };

  const logout = () => {
    clearToken();
    setUser(null);
    setSessionWarning(false);
  };

  const renewSession = async () => {
    const { token } = await authApi.refresh();
    setToken(token);
    setSessionWarning(false);
  };

  return (
    <AuthContext.Provider value={{ user, loading, loginWithToken, logout, sessionWarning, renewSession }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
