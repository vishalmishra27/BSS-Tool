import { useState } from 'react';
import { BrowserRouter, Routes, Route, NavLink, useLocation } from 'react-router-dom';
import { LoadingProvider } from './context/LoadingContext';
import { SidebarProvider, useSidebar } from './context/SidebarContext';
import { AuthProvider, useAuth } from './context/AuthContext';
import logo from "./public/logo.png";

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
import NetworkPage from './pages/NetworkPage';
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
import UATDashboardPage from './pages/UATDashboardPage';
import UATAutomationPage from './pages/UATAutomationPage';
import UATAutomationNewPage from './pages/UATAutomationNewPage';
import AgentChatPage from './pages/AgentChatPage';
import AgentDescriptionsPage from './pages/AgentDescriptionsPage';
import LandingPage from './pages/LandingPage';
import CommandAgentPage from './pages/CommandAgentPage';

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
const IconEnterprise = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M3 21h18"/><path d="M5 21V7l8-4v18"/><path d="M19 21V11l-6-4"/><path d="M9 9h1"/><path d="M9 13h1"/><path d="M9 17h1"/>
  </svg>
);
const IconBPM = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 2L2 7l10 5 10-5-10-5z"/><path d="M2 17l10 5 10-5"/><path d="M2 12l10 5 10-5"/>
  </svg>
);
const IconNetwork = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="9" y="2" width="6" height="6" rx="1"/><rect x="16" y="16" width="6" height="6" rx="1"/><rect x="2" y="16" width="6" height="6" rx="1"/>
    <path d="M12 8v4"/><path d="M5 16v-2a2 2 0 012-2h10a2 2 0 012 2v2"/>
  </svg>
);
const IconUATAuto = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <rect x="2" y="3" width="20" height="14" rx="2"/><path d="M8 21h8"/><path d="M12 17v4"/><polygon points="10 8 10 14 15 11 10 8"/>
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
  { label: 'Transformation', path: '/dashboard', icon: <IconDashboard />, module: 'dashboard' },
  { label: 'Milestone Tracker', path: '/milestones', icon: <IconMilestone />, module: 'milestones' },
  { label: 'Workflow', path: '/workflow', icon: <IconWorkflow />, module: 'workflow' },
  { label: 'Reconciliation', path: '/reconciliation', icon: <IconRecon />, module: 'reconciliation', dot: true },
  {
    label: 'Product', path: '/product-dashboard', icon: <IconProduct />, module: 'product', dot: true,
    children: [{ label: 'Product Lifecycle', path: '/product-dashboard' }],
  },
  { label: 'UAT Dashboard', path: '/uat', icon: <IconUAT />, module: 'uat', dot: true },
  {
    label: 'Enterprise', path: '/enterprise/bpm', icon: <IconEnterprise />, module: 'dashboard', dot: true,
    children: [
      { label: 'Business Process', path: '/enterprise/bpm' },
      { label: 'Migration Summary', path: '/enterprise/bpm-summary' },
      { label: 'Network I/O', path: '/enterprise/network' },
      { label: 'Document Intelligence', path: '/agent/ocr' },
    ],
  },
  { label: 'Test Automation', path: '/uat-automation', icon: <IconUATAuto />, module: 'dashboard', dot: true },
  {
    label: 'AI Agents', path: '/agent/about', icon: <IconAgent />, module: 'dashboard',
    children: [
      { label: 'Agent Overview', path: '/agent/about' },
      { label: 'Command Agent', path: '/agent/command' },
    ],
  },
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
      width: isCollapsed ? '56px' : '230px', minHeight: '100vh',
      background: 'linear-gradient(180deg, #0a1628 0%, #0f2847 60%, #132e4a 100%)',
      display: 'flex', flexDirection: 'column',
      transition: 'width 0.25s cubic-bezier(0.4,0,0.2,1)', overflow: 'hidden',
      flexShrink: 0, zIndex: 100, position: 'relative',
      boxShadow: '4px 0 24px rgba(0,0,0,0.25)',
    }}>
      {/* Logo */}
      <div style={{
        padding: isCollapsed ? '20px 8px' : '20px 18px',
        borderBottom: '1px solid rgba(255,255,255,0.06)',
        display: 'flex', alignItems: 'center',
        justifyContent: isCollapsed ? 'center' : 'space-between',
        gap: 8,
      }}>
        {!isCollapsed && (
          <NavLink to="/" style={{
            fontWeight: 800, fontSize: 16, color: '#fff', letterSpacing: 1.5,
            textDecoration: 'none', cursor: 'pointer',
            display: 'flex', alignItems: 'center', gap: 10,
          }}>
            <span style={{
              background: 'linear-gradient(135deg, #00B0F0, #0066CC)',
              borderRadius: 8, width: 28, height: 28,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              fontSize: 13, fontWeight: 900, color: '#fff',
            }}>B</span>
            BSS Tool
          </NavLink>
        )}
        <button onClick={toggleSidebar} style={{
          background: 'rgba(255,255,255,0.06)', border: 'none', cursor: 'pointer',
          color: 'rgba(255,255,255,0.5)', padding: 6, borderRadius: 6,
          display: 'flex', alignItems: 'center',
          transition: 'all 0.15s',
        }}
          onMouseEnter={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.12)'; e.currentTarget.style.color = '#fff'; }}
          onMouseLeave={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.06)'; e.currentTarget.style.color = 'rgba(255,255,255,0.5)'; }}
        >
          <IconChevron collapsed={isCollapsed} />
        </button>
      </div>

      {!isCollapsed && (
        <div style={{ padding: '10px 18px 6px', fontSize: 9, fontWeight: 700, color: 'rgba(255,255,255,0.25)', textTransform: 'uppercase', letterSpacing: 2 }}>
          Navigation
        </div>
      )}

      {/* Nav items */}
      <nav style={{ flex: 1, padding: isCollapsed ? '4px 0' : '4px 8px', overflowY: 'auto' }}>
        {navItems.map(item => {
          const isActive = location.pathname === item.path || item.children?.some(c => c.path === location.pathname);
          const isExpanded = expanded[item.label];
          const activeStyle = {
            background: 'linear-gradient(90deg, rgba(0,176,240,0.15), rgba(0,176,240,0.05))',
            color: '#fff',
            borderLeft: '3px solid #00B0F0',
          };
          const inactiveStyle = {
            background: 'transparent',
            color: 'rgba(255,255,255,0.55)',
            borderLeft: '3px solid transparent',
          };
          const baseStyle = {
            display: 'flex', alignItems: 'center', gap: 10,
            padding: isCollapsed ? '10px 0' : '9px 12px',
            justifyContent: isCollapsed ? 'center' : 'flex-start',
            fontSize: 13, fontWeight: isActive ? 600 : 400,
            borderRadius: isCollapsed ? 0 : 8,
            flex: 1, minWidth: 0, transition: 'all 0.2s',
            whiteSpace: 'nowrap', overflow: 'hidden',
            cursor: 'pointer', textDecoration: 'none',
            marginBottom: 2,
          };

          return (
            <div key={item.label}>
              <div style={{ display: 'flex', alignItems: 'center' }}>
                {item.children ? (
                  <div
                    onClick={() => setExpanded(p => ({ ...p, [item.label]: !p[item.label] }))}
                    style={{ ...baseStyle, ...(isActive ? activeStyle : inactiveStyle) }}
                    onMouseEnter={e => { if (!isActive) { e.currentTarget.style.background = 'rgba(255,255,255,0.06)'; e.currentTarget.style.color = '#fff'; } }}
                    onMouseLeave={e => { if (!isActive) { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'rgba(255,255,255,0.55)'; } }}
                  >
                    <span style={{ flexShrink: 0, opacity: isActive ? 1 : 0.7 }}>{item.icon}</span>
                    {!isCollapsed && (
                      <>
                        <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' }}>{item.label}</span>
                        {item.dot && <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#00B0F0', flexShrink: 0 }} />}
                        <span style={{
                          fontSize: 8, color: 'rgba(255,255,255,0.35)',
                          transition: 'transform 0.2s',
                          transform: isExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
                        }}>▼</span>
                      </>
                    )}
                  </div>
                ) : (
                  <NavLink to={item.path} style={({ isActive: na }) => ({
                    ...baseStyle,
                    ...((isActive || na) ? activeStyle : inactiveStyle),
                  })}
                    onMouseEnter={e => { if (!isActive) { e.currentTarget.style.background = 'rgba(255,255,255,0.06)'; e.currentTarget.style.color = '#fff'; } }}
                    onMouseLeave={e => { if (!isActive) { e.currentTarget.style.background = 'transparent'; e.currentTarget.style.color = 'rgba(255,255,255,0.55)'; } }}
                  >
                    <span style={{ flexShrink: 0, opacity: isActive ? 1 : 0.7 }}>{item.icon}</span>
                    {!isCollapsed && (
                      <>
                        <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis' }}>{item.label}</span>
                        {item.dot && <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#00B0F0', flexShrink: 0 }} />}
                      </>
                    )}
                  </NavLink>
                )}
              </div>
              {!isCollapsed && item.children && isExpanded && (
                <div style={{ marginLeft: 20, paddingLeft: 12, borderLeft: '1px solid rgba(255,255,255,0.08)', marginBottom: 4 }}>
                  {item.children.map(child => {
                    const childActive = location.pathname === child.path;
                    return (
                      <NavLink key={child.path} to={child.path} style={{
                        display: 'flex', alignItems: 'center', gap: 8,
                        padding: '7px 12px', borderRadius: 6, marginBottom: 1,
                        color: childActive ? '#00B0F0' : 'rgba(255,255,255,0.45)',
                        textDecoration: 'none', fontSize: 12, fontWeight: childActive ? 600 : 400,
                        background: childActive ? 'rgba(0,176,240,0.08)' : 'transparent',
                        transition: 'all 0.15s',
                      }}
                        onMouseEnter={e => { if (!childActive) { e.currentTarget.style.color = 'rgba(255,255,255,0.8)'; e.currentTarget.style.background = 'rgba(255,255,255,0.04)'; } }}
                        onMouseLeave={e => { if (!childActive) { e.currentTarget.style.color = 'rgba(255,255,255,0.45)'; e.currentTarget.style.background = 'transparent'; } }}
                      >
                        <span style={{ width: 4, height: 4, borderRadius: '50%', background: childActive ? '#00B0F0' : 'rgba(255,255,255,0.2)', flexShrink: 0 }} />
                        {child.label}
                      </NavLink>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </nav>

      {/* User info + sign out */}
      {!isCollapsed && auth && (
        <div style={{ padding: '14px 18px', borderTop: '1px solid rgba(255,255,255,0.06)', background: 'rgba(0,0,0,0.15)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 10 }}>
            <div style={{
              width: 32, height: 32, borderRadius: 8,
              background: 'linear-gradient(135deg, #00B0F0, #0066CC)',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              color: '#fff', fontSize: 13, fontWeight: 700, flexShrink: 0,
            }}>
              {auth.user.full_name?.charAt(0)?.toUpperCase() || 'U'}
            </div>
            <div style={{ minWidth: 0 }}>
              <div style={{ color: '#fff', fontSize: 12, fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{auth.user.full_name}</div>
              <div style={{ color: '#00B0F0', fontSize: 10, textTransform: 'capitalize' }}>{auth.user.role.replace(/_/g, ' ')}</div>
            </div>
          </div>
          <button onClick={onLogout} style={{
            background: 'rgba(255,255,255,0.06)', border: '1px solid rgba(255,255,255,0.1)',
            borderRadius: 6, cursor: 'pointer', color: 'rgba(255,255,255,0.5)',
            fontSize: 11, fontWeight: 500, padding: '6px 0', width: '100%',
            transition: 'all 0.15s',
          }}
            onMouseEnter={e => { e.currentTarget.style.background = 'rgba(220,38,38,0.15)'; e.currentTarget.style.borderColor = 'rgba(220,38,38,0.3)'; e.currentTarget.style.color = '#f87171'; }}
            onMouseLeave={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.06)'; e.currentTarget.style.borderColor = 'rgba(255,255,255,0.1)'; e.currentTarget.style.color = 'rgba(255,255,255,0.5)'; }}
          >
            Sign Out
          </button>
        </div>
      )}
      {isCollapsed && (
        <div style={{ padding: '14px 8px', borderTop: '1px solid rgba(255,255,255,0.06)', display: 'flex', justifyContent: 'center' }}>
          <button onClick={onLogout} title="Sign Out" style={{
            background: 'rgba(255,255,255,0.06)', border: 'none', cursor: 'pointer',
            color: 'rgba(255,255,255,0.4)', fontSize: 14, borderRadius: 6,
            width: 32, height: 32, display: 'flex', alignItems: 'center', justifyContent: 'center',
            transition: 'all 0.15s',
          }}
            onMouseEnter={e => { e.currentTarget.style.background = 'rgba(220,38,38,0.15)'; e.currentTarget.style.color = '#f87171'; }}
            onMouseLeave={e => { e.currentTarget.style.background = 'rgba(255,255,255,0.06)'; e.currentTarget.style.color = 'rgba(255,255,255,0.4)'; }}
          >⏻</button>
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
      <main style={{ flex: 1, overflow: 'auto', minWidth: 0, position: 'relative' }}>
        <div style={{ position: 'fixed', top: 12, right: 20, zIndex: 50 }}>
          <img src={logo} alt="KPMG Logo" style={{ height: 60, width: 'auto', objectFit: 'contain' }} />
        </div>
        <Routes>
          <Route path="/"                  element={<LandingPage />} />
          <Route path="/dashboard"         element={<TransformationDashboardPage />} />
          <Route path="/milestones"        element={<ProjectMilestones />} />
          <Route path="/status"            element={<StatusTrackerPage />} />
          <Route path="/reconciliation"    element={<DashboardPage />} />
          <Route path="/product-dashboard" element={<ProductDashboardPage />} />
          <Route path="/uat"               element={<UATDashboardPage />} />
          <Route path="/uat/automation"    element={<UATAutomationPage />} />
          <Route path="/workflow"          element={<WorkflowTrackerPage readOnly={isReadOnly()} canAssign={can('workflow_assign')} canComment={can('workflow_comment')} canUpload={can('workflow_upload')} />} />
          <Route path="/bpm"               element={<Bpm />} />
          <Route path="/enterprise/bpm"    element={<Bpm />} />
          <Route path="/enterprise/network" element={<NetworkPage />} />
          <Route path="/uat-automation"     element={<UATAutomationNewPage />} />
          <Route path="/enterprise/bpm-summary" element={<Summary />} />
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
          <Route path="/agent"                 element={<OcrAgentPage />} />
          <Route path="/agent/about"            element={<AgentDescriptionsPage />} />
          <Route path="/agent/data"           element={<DataManagementAgentPage />} />
          <Route path="/agent/crud"          element={<CrudAgentPage />} />
          <Route path="/agent/reconciliation" element={<ReconciliationAgentPage />} />
          <Route path="/agent/ocr"            element={<OcrAgentPage />} />
          <Route path="/agent/command"      element={<CommandAgentPage />} />
          <Route path="/audit-log"         element={<AuditLogPage />} />
        </Routes>
      </main>
    </div>
  );
}

// ─── Auth Gate — shows login or app ──────────────────────────────────────────
function AuthGate() {
  const { auth, login, logout, loading } = useAuth();

  if (loading) {
    return (
      <div style={{ minHeight: '100vh', display: 'flex', alignItems: 'center', justifyContent: 'center', background: '#F4F6FA' }}>
        <div style={{ color: '#003087', fontSize: 16, fontWeight: 600 }}>Loading…</div>
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
