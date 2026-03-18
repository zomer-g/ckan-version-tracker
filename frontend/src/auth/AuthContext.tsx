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
}

interface AuthContextType {
  user: User | null;
  loading: boolean;
  login: (email: string, password: string) => Promise<void>;
  register: (email: string, password: string, displayName: string) => Promise<void>;
  loginWithToken: (token: string) => Promise<void>;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType>(null!);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Check for SSO token in URL (from OAuth callback redirect)
    const params = new URLSearchParams(window.location.search);
    const ssoToken = params.get("sso_token");
    if (ssoToken) {
      // Clean the URL
      window.history.replaceState({}, "", window.location.pathname);
      setToken(ssoToken);
    }

    const token = localStorage.getItem("token");
    if (token) {
      authApi
        .me()
        .then(setUser)
        .catch(() => clearToken())
        .finally(() => setLoading(false));
    } else {
      setLoading(false);
    }
  }, []);

  const login = async (email: string, password: string) => {
    const { access_token } = await authApi.login(email, password);
    setToken(access_token);
    const me = await authApi.me();
    setUser(me);
  };

  const register = async (email: string, password: string, displayName: string) => {
    const { access_token } = await authApi.register(email, password, displayName);
    setToken(access_token);
    const me = await authApi.me();
    setUser(me);
  };

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
    <AuthContext.Provider value={{ user, loading, login, register, loginWithToken, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
