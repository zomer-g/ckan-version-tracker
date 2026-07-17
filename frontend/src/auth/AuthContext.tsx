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
}

const AuthContext = createContext<AuthContextType>(null!);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

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
        .then(({ token }) => setToken(token))
        .catch(() => {
          /* transient; the next tick (or a 401 on a real call) handles it */
        });
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
  };

  return (
    <AuthContext.Provider value={{ user, loading, loginWithToken, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
