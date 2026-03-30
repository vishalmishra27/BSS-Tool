import { useState } from 'react';

const ROLE_LABELS = {
  programme_director: 'Programme Director',
  engagement_manager: 'Engagement Manager',
  bss_consultant:     'BSS Consultant',
  qa_manager:         'QA / Test Manager',
  data_analyst:       'Data Analyst',
  client_sponsor:     'Client Programme Sponsor',
  client_it_lead:     'Client IT Lead',
  client_operations:  'Client Operations Lead',
};

const ROLE_COLOURS = {
  programme_director: '#001F5B',
  engagement_manager: '#003087',
  bss_consultant:     '#0070c0',
  qa_manager:         '#006600',
  data_analyst:       '#555',
  client_sponsor:     '#7a2800',
  client_it_lead:     '#5a0070',
  client_operations:  '#6b4c00',
};

export default function LoginPage({ onLogin }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || 'Login failed');
      } else {
        onLogin(data);
      }
    } catch {
      setError('Could not connect to server. Please ensure the backend is running.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      minHeight: '100vh',
      background: 'linear-gradient(135deg, #001024 0%, #001F5B 50%, #003087 100%)',
      display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
      fontFamily: 'system-ui, sans-serif', padding: 20,
    }}>
      {/* KPMG Header */}
      <div style={{ marginBottom: 32, textAlign: 'center' }}>
        <div style={{
          display: 'inline-block',
          background: '#003087', color: '#00B0F0',
          fontWeight: 900, fontSize: 32, letterSpacing: 4,
          padding: '6px 18px', border: '3px solid #00B0F0',
          marginBottom: 12,
        }}>KPMG</div>
        <div style={{ color: 'rgba(255,255,255,0.9)', fontSize: 18, fontWeight: 600 }}>BSS Migration Assurance Tool</div>
        <div style={{ color: 'rgba(255,255,255,0.5)', fontSize: 13, marginTop: 4 }}>Agentic AI-Powered Project Intelligence Platform</div>
      </div>

      {/* Login card */}
      <div style={{
        background: '#fff', borderRadius: 12, width: 420, maxWidth: '100%',
        boxShadow: '0 12px 50px rgba(0,0,0,0.4)',
        overflow: 'hidden',
      }}>
        {/* Card header */}
        <div style={{ background: '#f5f7fa', padding: '20px 28px', borderBottom: '1px solid #e8e8e8' }}>
          <h2 style={{ margin: 0, fontSize: 17, color: '#001F5B', fontWeight: 700 }}>Sign in to your account</h2>
          <p style={{ margin: '4px 0 0', fontSize: 12, color: '#888' }}>KPMG Advisory — Confidential</p>
        </div>

        <form onSubmit={handleSubmit} style={{ padding: '24px 28px' }}>
          {/* Error */}
          {error && (
            <div style={{
              background: '#fff5f5', border: '1px solid #fcc', borderRadius: 6,
              padding: '10px 14px', marginBottom: 16, fontSize: 13, color: '#c00000',
            }}>
              {error}
            </div>
          )}

          <div style={{ marginBottom: 16 }}>
            <label style={labelStyle}>Username</label>
            <input
              value={username}
              onChange={e => setUsername(e.target.value)}
              required autoFocus
              placeholder="e.g. eng_manager"
              style={inputStyle}
            />
          </div>

          <div style={{ marginBottom: 20 }}>
            <label style={labelStyle}>Password</label>
            <input
              type="password"
              value={password}
              onChange={e => setPassword(e.target.value)}
              required
              placeholder="••••••••"
              style={inputStyle}
            />
          </div>

          <button
            type="submit"
            disabled={loading}
            style={{
              width: '100%', padding: '11px', borderRadius: 6, border: 'none',
              background: loading ? '#aaa' : '#001F5B', color: '#fff',
              fontSize: 14, fontWeight: 600, cursor: loading ? 'not-allowed' : 'pointer',
              letterSpacing: 0.3,
            }}
          >
            {loading ? 'Signing in…' : 'Sign In'}
          </button>

          <p style={{ marginTop: 14, fontSize: 11, color: '#aaa', textAlign: 'center' }}>
            Default password for all demo accounts: <code style={{ background: '#f0f0f0', padding: '1px 5px', borderRadius: 3 }}>kpmg1234</code>
          </p>
        </form>

        {/* Demo accounts reference */}
        <div style={{ padding: '0 28px 24px' }}>
          <div style={{ fontSize: 12, fontWeight: 600, color: '#555', marginBottom: 8 }}>Demo accounts</div>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
            {Object.entries(ROLE_LABELS).map(([role, label]) => (
              <button
                key={role}
                type="button"
                onClick={() => {
                  const map = {
                    programme_director: 'prog_director',
                    engagement_manager: 'eng_manager',
                    bss_consultant: 'bss_consultant',
                    qa_manager: 'qa_manager',
                    data_analyst: 'data_analyst',
                    client_sponsor: 'client_sponsor',
                    client_it_lead: 'client_it',
                    client_operations: 'client_ops',
                  };
                  setUsername(map[role] || role);
                  setPassword('kpmg1234');
                }}
                style={{
                  padding: '3px 9px', borderRadius: 12, border: 'none',
                  background: ROLE_COLOURS[role] || '#555',
                  color: '#fff', fontSize: 11, cursor: 'pointer', opacity: 0.85,
                }}
              >
                {label}
              </button>
            ))}
          </div>
        </div>
      </div>

      <div style={{ marginTop: 24, color: 'rgba(255,255,255,0.3)', fontSize: 11 }}>
        KPMG CONFIDENTIAL — NOT FOR DISTRIBUTION
      </div>
    </div>
  );
}

const labelStyle = {
  display: 'block', fontSize: 13, fontWeight: 600, color: '#444', marginBottom: 6,
};

const inputStyle = {
  width: '100%', padding: '9px 12px', borderRadius: 6,
  border: '1px solid #d0d8f0', fontSize: 14, outline: 'none',
  boxSizing: 'border-box', color: '#222',
};
