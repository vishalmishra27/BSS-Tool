import { createContext, useContext, useState, useEffect } from 'react';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [auth, setAuth] = useState(null);      // { token, user, permissions }
  const [loading, setLoading] = useState(true);

  // Restore session from localStorage on mount
  useEffect(() => {
    const stored = localStorage.getItem('bss_auth');
    if (stored) {
      try {
        const parsed = JSON.parse(stored);
        // Verify token is still valid by checking /api/auth/me
        fetch('/api/auth/me', {
          headers: { Authorization: `Bearer ${parsed.token}` },
        })
          .then(r => r.json())
          .then(data => {
            if (data.user) {
              setAuth({ ...parsed, permissions: data.permissions });
            } else {
              localStorage.removeItem('bss_auth');
            }
          })
          .catch(() => localStorage.removeItem('bss_auth'))
          .finally(() => setLoading(false));
      } catch {
        localStorage.removeItem('bss_auth');
        setLoading(false);
      }
    } else {
      setLoading(false);
    }
  }, []);

  const login = (data) => {
    const session = { token: data.token, user: data.user, permissions: data.permissions };
    localStorage.setItem('bss_auth', JSON.stringify(session));
    setAuth(session);
  };

  const logout = () => {
    localStorage.removeItem('bss_auth');
    setAuth(null);
  };

  // Convenience helpers
  const can = (permission) => auth?.permissions?.[permission] === true;
  const hasModule = (module) => auth?.permissions?.modules?.includes(module) ?? false;
  const isReadOnly = () => auth?.permissions?.read_only === true;
  const role = auth?.user?.role;

  return (
    <AuthContext.Provider value={{ auth, login, logout, can, hasModule, isReadOnly, role, loading }}>
      {children}
    </AuthContext.Provider>
  );
}

export const useAuth = () => useContext(AuthContext);
