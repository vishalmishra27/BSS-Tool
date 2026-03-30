import { useState } from 'react';
import { BrowserRouter, Routes, Route, NavLink, useLocation } from 'react-router-dom';
import { LoadingProvider } from './context/LoadingContext';
import { SidebarProvider, useSidebar } from './context/SidebarContext';
import { AuthProvider, useAuth } from './context/AuthContext';
import AgentChatWidget from './components/AgentChatWidget';

// Pages
import LoginPage from './pages/LoginPage';
import AuditLogPage from './pages/AuditLogPage';
import TransformationDashboardStatic from './pages/TransformationDashboardStatic';
import TransformationDashboardPage from './pages/TransformationDashboardPage';
import ProjectMilestones from './pages/ProjectMilestones';
import StatusTrackerPage from './pages/StatusTrackerPage';
import ReconciliationDashboardPage from './pages/ReconciliationDashboardPage';
import ProductDashboardPage from './pages/ProductDashboardPage';
import WorkflowTrackerPage from './pages/WorkflowTrackerPage';
import Bpm from './pages/Bpm';
import DashboardPage from './pages/DashboardPage';
import UserAnalyticsPage from './pages/UserAnalyticsPage';
import ConversionPage from './pages/ConversionPage';
import OrdersPage from './pages/OrdersPage';
import AnalyticsPage from './pages/AnalyticsPage';
import AllProjects from './pages/AllProjects';
import AllTasks from './pages/AllTasks';
import Task from './pages/Task';
import Flowchart from './pages/Flowchart';
import Summary from './pages/Summary';
import TransformationLOBPage from './pages/TransformationLOBPage';
import TestcaseDetailPage from './pages/TestcaseDetailPage';
import CrudAgentPage from './pages/CrudAgentPage';
import ReconciliationAgentPage from './pages/ReconciliationAgentPage';
import OcrAgentPage from './pages/OcrAgentPage';
import DataManagementAgentPage from './pages/DataManagementAgentPage';

// ─── Icons ────────────────────────────────────────────────────────────────────
const IconDashboard = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/>
    <rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/>
  </svg>
);
const IconMilestone = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M18 20V10"/><path d="M12 20V4"/><path d="M6 20v-6"/>
  </svg>
);
const IconRecon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="10"/><path d="M12 8v4l3 3"/>
  </svg>
);
const IconProduct = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M21 16V8a2 2 0 00-1-1.73l-7-4a2 2 0 00-2 0l-7 4A2 2 0 003 8v8a2 2 0 001 1.73l7 4a2 2 0 002 0l7-4A2 2 0 0021 16z"/>
  </svg>
);
const IconUAT = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <polyline points="9 11 12 14 22 4"/><path d="M21 12v7a2 2 0 01-2 2H5a2 2 0 01-2-2V5a2 2 0 012-2h11"/>
  </svg>
);
const IconWorkflow = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="3" y="3" width="5" height="5"/><rect x="16" y="3" width="5" height="5"/>
    <rect x="3" y="16" width="5" height="5"/><path d="M8 5.5h8M5.5 8v8M21 8.5v7"/>
  </svg>
);
const IconBPM = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>
  </svg>
);
const IconMigration = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M5 12h14"/><path d="M12 5l7 7-7 7"/>
  </svg>
);
const IconAudit = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/>
    <polyline points="14 2 14 8 20 8"/>
    <line x1="16" y1="13" x2="8" y2="13"/>
    <line x1="16" y1="17" x2="8" y2="17"/>
  </svg>
);
const IconAgent = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <circle cx="12" cy="12" r="3"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/>
  </svg>
);
const IconChevron = ({ collapsed }) => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"
    style={{ transform: collapsed ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}>
    <polyline points="15 18 9 12 15 6"/>
  </svg>
);

// ─── Nav items with module permission keys ────────────────────────────────────
const ALL_NAV_ITEMS = [
  { label: 'Transformation Dashboard', path: '/dashboard',          icon: <IconDashboard />, module: 'dashboard' },
  { label: 'Milestones',               path: '/milestones',          icon: <IconMilestone />, module: 'milestones' },
  { label: 'Workflow',                  path: '/workflow',            icon: <IconWorkflow />, module: 'workflow' },
  { label: 'Reconciliation Dashboard', path: '/reconciliation',      icon: <IconRecon />,     module: 'reconciliation', dot: true },
  {
    label: 'Product', path: '/product-dashboard', icon: <IconProduct />, module: 'product', dot: true,
    children: [{ label: 'Product Journey', path: '/product-dashboard' }],
  },
  {
    label: 'UAT', path: '/uat', icon: <IconUAT />, module: 'uat', dot: true,
    children: [{ label: 'UAT Dashboard', path: '/uat' }],
  },
  { label: 'BPM',       path: '/bpm',       icon: <IconBPM />,      module: 'dashboard', dot: true },
  { label: 'Migration', path: '/migration', icon: <IconMigration />,module: 'dashboard', dot: true },
  {
    label: 'AI Agents', path: '/agent/crud', icon: <IconAgent />, module: 'dashboard',
    children: [
      { label: 'Data Management', path: '/agent/data' },
      { label: 'CRUD Operations', path: '/agent/crud' },
      { label: 'Reconciliation', path: '/agent/reconciliation' },
      { label: 'Document Analysis', path: '/agent/ocr' },
    ],
  },
  { label: 'Audit Log', path: '/audit-log', icon: <IconAudit />,    module: 'audit_log' },
];

// ─── Sidebar ──────────────────────────────────────────────────────────────────
function Sidebar({ onLogout }) {
  const { isCollapsed, toggleSidebar } = useSidebar();
  const [expanded, setExpanded] = useState({});
  const location = useLocation();
  const { auth, hasModule } = useAuth();

  const navItems = ALL_NAV_ITEMS.filter(item => !item.module || hasModule(item.module));

  return (
    <div style={{
      width: isCollapsed ? '52px' : '210px', minHeight: '100vh',
      background: 'linear-gradient(180deg, #001F5B 0%, #003087 100%)',
      display: 'flex', flexDirection: 'column',
      transition: 'width 0.25s ease', overflow: 'hidden',
      flexShrink: 0, zIndex: 100, position: 'relative',
    }}>
      {/* KPMG Logo */}
      <div style={{ padding: isCollapsed ? '16px 8px' : '16px 16px', borderBottom: '1px solid rgba(255,255,255,0.1)', display: 'flex', alignItems: 'center', justifyContent: isCollapsed ? 'center' : 'space-between', gap: 8 }}>
        {!isCollapsed && (
          <div style={{ background: '#003087', color: '#00B0F0', fontWeight: 900, fontSize: 22, letterSpacing: 2, padding: '2px 8px', border: '2px solid #00B0F0', userSelect: 'none' }}>KPMG</div>
        )}
        <button onClick={toggleSidebar} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'rgba(255,255,255,0.7)', padding: 4, borderRadius: 4, display: 'flex', alignItems: 'center' }}>
          <IconChevron collapsed={isCollapsed} />
        </button>
      </div>

      {/* Nav items */}
      <nav style={{ flex: 1, paddingTop: 8 }}>
        {navItems.map(item => {
          const isActive = location.pathname === item.path || item.children?.some(c => c.path === location.pathname);
          const isExpanded = expanded[item.label];
          return (
            <div key={item.label}>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                <NavLink to={item.path} style={({ isActive: na }) => ({
                  display: 'flex', alignItems: 'center', gap: 10,
                  padding: isCollapsed ? '10px 0' : '10px 14px',
                  justifyContent: isCollapsed ? 'center' : 'flex-start',
                  color: isActive || na ? '#ffffff' : 'rgba(255,255,255,0.65)',
                  textDecoration: 'none', fontSize: 13, fontWeight: isActive ? 600 : 400,
                  background: isActive ? 'rgba(255,255,255,0.12)' : 'transparent',
                  borderLeft: isActive ? '3px solid #00B0F0' : '3px solid transparent',
                  flex: 1, minWidth: 0, transition: 'all 0.15s', whiteSpace: 'nowrap', overflow: 'hidden',
                })}>
                  <span style={{ flexShrink: 0 }}>{item.icon}</span>
                  {!isCollapsed && (
                    <>
                      <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' }}>{item.label}</span>
                      {item.dot && <span style={{ width: 7, height: 7, borderRadius: '50%', background: '#00B0F0', flexShrink: 0, marginRight: 2 }} />}
                    </>
                  )}
                </NavLink>
                {!isCollapsed && item.children && (
                  <button onClick={() => setExpanded(p => ({ ...p, [item.label]: !p[item.label] }))} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'rgba(255,255,255,0.5)', padding: '10px 8px', fontSize: 10 }}>
                    {isExpanded ? '▲' : '▼'}
                  </button>
                )}
              </div>
              {!isCollapsed && item.children && isExpanded && (
                <div style={{ paddingLeft: 32 }}>
                  {item.children.map(child => (
                    <NavLink key={child.path} to={child.path} style={({ isActive }) => ({ display: 'block', padding: '7px 12px', color: isActive ? '#fff' : 'rgba(255,255,255,0.55)', textDecoration: 'none', fontSize: 12, background: isActive ? 'rgba(255,255,255,0.08)' : 'transparent', borderRadius: 4, marginBottom: 2 })}>
                      {child.label}
                    </NavLink>
                  ))}
                </div>
              )}
            </div>
          );
        })}
      </nav>

      {/* User info + sign out */}
      {!isCollapsed && auth && (
        <div style={{ padding: '12px 16px', borderTop: '1px solid rgba(255,255,255,0.1)' }}>
          <div style={{ color: 'rgba(255,255,255,0.9)', fontSize: 12, fontWeight: 600, marginBottom: 2, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{auth.user.full_name}</div>
          <div style={{ color: '#00B0F0', fontSize: 10, marginBottom: 8, textTransform: 'capitalize' }}>{auth.user.role.replace(/_/g, ' ')}</div>
          <button onClick={onLogout} style={{ background: 'none', border: '1px solid rgba(255,255,255,0.25)', borderRadius: 4, cursor: 'pointer', color: 'rgba(255,255,255,0.6)', fontSize: 11, padding: '4px 10px', width: '100%' }}>
            Sign Out
          </button>
        </div>
      )}
      {isCollapsed && (
        <div style={{ padding: '12px 8px', borderTop: '1px solid rgba(255,255,255,0.1)', display: 'flex', justifyContent: 'center' }}>
          <button onClick={onLogout} title="Sign Out" style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'rgba(255,255,255,0.5)', fontSize: 16 }}>⏻</button>
        </div>
      )}
    </div>
  );
}

// ─── Authenticated App Layout ──────────────────────────────────────────────────
function AppLayout({ onLogout }) {
  const { auth, can, isReadOnly } = useAuth();

  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      <Sidebar onLogout={onLogout} />
      <main style={{ flex: 1, overflow: 'auto', minWidth: 0 }}>
        <Routes>
          <Route path="/"                  element={<TransformationDashboardPage />} />
          <Route path="/dashboard"         element={<TransformationDashboardPage />} />
          <Route path="/milestones"        element={<ProjectMilestones />} />
          <Route path="/status"            element={<StatusTrackerPage />} />
          <Route path="/reconciliation"    element={<DashboardPage />} />
          <Route path="/product-dashboard" element={<ProductDashboardPage />} />
          <Route path="/uat"               element={<ReconciliationDashboardPage />} />
          <Route path="/workflow"          element={<WorkflowTrackerPage readOnly={isReadOnly()} canAssign={can('workflow_assign')} canComment={can('workflow_comment')} canUpload={can('workflow_upload')} />} />
          <Route path="/bpm"               element={<Bpm />} />
          <Route path="/migration"         element={<Summary />} />
          <Route path="/pdf-analysis"      element={<AnalyticsPage />} />
          <Route path="/analytics"         element={<UserAnalyticsPage />} />
          <Route path="/conversion"        element={<ConversionPage />} />
          <Route path="/orders"            element={<OrdersPage />} />
          <Route path="/all-projects"      element={<AllProjects />} />
          <Route path="/all-tasks"         element={<AllTasks />} />
          <Route path="/task/:id"          element={<Task />} />
          <Route path="/flowchart"         element={<Flowchart />} />
          <Route path="/lob/:lob"          element={<TransformationLOBPage />} />
          <Route path="/testcase/:id"      element={<TestcaseDetailPage />} />
          <Route path="/agent/data"           element={<DataManagementAgentPage />} />
          <Route path="/agent/crud"          element={<CrudAgentPage />} />
          <Route path="/agent/reconciliation" element={<ReconciliationAgentPage />} />
          <Route path="/agent/ocr"            element={<OcrAgentPage />} />
          <Route path="/audit-log"         element={<AuditLogPage />} />
        </Routes>
      </main>
      <AgentChatWidget
        username={auth?.user?.full_name || 'Programme User'}
        canUseAgent={can('can_use_agent')}
        canBulk={can('agent_bulk')}
      />
    </div>
  );
}

// ─── Auth Gate — shows login or app ──────────────────────────────────────────
function AuthGate() {
  const { auth, login, logout, loading } = useAuth();

  if (loading) {
    return (
      <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#001F5B' }}>
        <div style={{ color: '#00B0F0', fontSize: 16 }}>Loading…</div>
      </div>
    );
  }

  if (!auth) return <LoginPage onLogin={login} />;

  return (
    <SidebarProvider>
      <LoadingProvider>
        <AppLayout onLogout={logout} />
      </LoadingProvider>
    </SidebarProvider>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <AuthProvider>
        <AuthGate />
      </AuthProvider>
    </BrowserRouter>
  );
}
