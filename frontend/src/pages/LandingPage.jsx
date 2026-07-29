import { useNavigate } from 'react-router-dom';
import {
  LayoutDashboard,
  Flag,
  GitBranch,
  BarChart3,
  Package,
  CheckSquare,
  Building2,
  Layers,
  FileSpreadsheet,
  Network,
  MonitorPlay,
  Bot,
  ScanText,
} from 'lucide-react';

const sections = [
  {
    title: 'Programme Management',
    desc: 'End-to-end transformation programme oversight, milestone tracking, and workflow management.',
    icon: LayoutDashboard,
    accent: '#00338D',
    items: [
      {
        title: 'Transformation Overview',
        desc: 'Real-time KPIs, programme health indicators, and transformation metrics across all workstreams.',
        icon: LayoutDashboard,
        path: '/dashboard',
        gradient: 'from-[#00338D] to-[#002266]',
      },
      {
        title: 'Milestone Tracker',
        desc: 'Monitor programme milestones, track deliverable timelines, and manage dependencies across phases.',
        icon: Flag,
        path: '/milestones',
        gradient: 'from-[#0091DA] to-[#00338D]',
      },
      {
        title: 'Workflow Management',
        desc: 'Manage task assignments, track workflow stages, upload documents, and collaborate across teams.',
        icon: GitBranch,
        path: '/workflow',
        gradient: 'from-[#483698] to-[#2D1F6B]',
      },
    ],
  },
  {
    title: 'Enterprise Assurance',
    desc: 'Business process management, network configuration, and document intelligence.',
    icon: Building2,
    accent: '#483698',
    items: [
      {
        title: 'Business Process Modelling',
        desc: 'Visual flow diagrams, process documentation, compliance tracking, and BPM lifecycle management.',
        icon: Layers,
        path: '/enterprise/bpm',
        gradient: 'from-[#483698] to-[#2D1F6B]',
      },
      {
        title: 'Migration Summary',
        desc: 'Consolidated migration status breakdowns, progress charts, and executive-level reporting.',
        icon: FileSpreadsheet,
        path: '/enterprise/bpm-summary',
        gradient: 'from-[#00338D] to-[#002266]',
      },
      {
        title: 'Network I/O Configuration',
        desc: 'Upload network config files, parse device parameters, and export structured data as spreadsheets.',
        icon: Network,
        path: '/enterprise/network',
        gradient: 'from-[#0091DA] to-[#006699]',
      },
      {
        title: 'Document Intelligence',
        desc: 'AI-powered OCR for text extraction from PDFs, scanned documents, and structured data export.',
        icon: ScanText,
        path: '/agent/ocr',
        gradient: 'from-[#470A68] to-[#2D0A42]',
      },
    ],
  },
  {
    title: 'Testing & Quality Assurance',
    desc: 'Comprehensive test management, automated execution, and quality reporting.',
    icon: CheckSquare,
    accent: '#0091DA',
    items: [
      {
        title: 'UAT Dashboard',
        desc: 'Test case analytics, priority distributions, LOB-wise tracking, and quality assurance metrics.',
        icon: CheckSquare,
        path: '/uat',
        gradient: 'from-[#0091DA] to-[#006699]',
      },
      {
        title: 'Test Automation',
        desc: 'Upload Excel test suites, execute automated browser tests via Playwright, and review step-by-step results.',
        icon: MonitorPlay,
        path: '/uat-automation',
        gradient: 'from-[#00338D] to-[#002266]',
      },
    ],
  },
  {
    title: 'Data & Intelligence',
    desc: 'Data reconciliation, product lifecycle tracking, and AI-powered analytics.',
    icon: BarChart3,
    accent: '#00338D',
    items: [
      {
        title: 'Reconciliation Engine',
        desc: 'CBS vs CLM service data reconciliation, dataset uploads, and automated KPI discrepancy analysis.',
        icon: BarChart3,
        path: '/reconciliation',
        gradient: 'from-[#0091DA] to-[#006699]',
      },
      {
        title: 'Product Lifecycle',
        desc: 'End-to-end product lifecycle tracking from configuration through deployment and validation.',
        icon: Package,
        path: '/product-dashboard',
        gradient: 'from-[#483698] to-[#2D1F6B]',
      },
      {
        title: 'AI Agents',
        desc: 'CRUD Operations Agent and Document Analysis Agent for intelligent data management and insights.',
        icon: Bot,
        path: '/agent/about',
        gradient: 'from-[#00338D] to-[#002266]',
      },
      {
        title: 'Command Agent',
        desc: 'Manage projects, tasks, workflows & dashboards via natural language prompts with smart confirmations.',
        icon: Bot,
        path: '/agent/command',
        gradient: 'from-[#483698] to-[#00338D]',
      },
    ],
  },
];

function FeatureCard({ item, navigate }) {
  const Icon = item.icon;
  return (
    <div
      onClick={() => navigate(item.path)}
      className="bg-white rounded-2xl border border-gray-100 shadow-sm hover:shadow-xl hover:border-gray-200 hover:-translate-y-1 transition-all duration-300 cursor-pointer group overflow-hidden"
    >
      <div className={`bg-gradient-to-br ${item.gradient} px-5 py-5 flex items-center gap-4`}>
        <div className="bg-white bg-opacity-20 backdrop-blur rounded-xl p-2.5 group-hover:scale-110 transition-transform duration-300">
          <Icon size={22} className="text-white" />
        </div>
        <div>
          <h3 className="text-white font-bold text-sm">{item.title}</h3>
        </div>
      </div>
      <div className="px-5 py-4 flex flex-col justify-between">
        <p className="text-gray-500 text-sm leading-relaxed">{item.desc}</p>
        <div className="flex items-center gap-1.5 mt-3 text-xs font-semibold text-gray-400 group-hover:text-blue-600 transition-colors">
          <span>Open module</span>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="group-hover:translate-x-1 transition-transform">
            <path d="M5 12h14"/><path d="M12 5l7 7-7 7"/>
          </svg>
        </div>
      </div>
    </div>
  );
}

export default function LandingPage() {
  const navigate = useNavigate();
  const totalModules = sections.reduce((sum, s) => sum + s.items.length, 0);

  return (
    <div className="min-h-screen bg-slate-50">

      {/* Hero Section */}
      <div className="relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-[#0a1628] via-[#0f2847] to-[#132e4a]" />
        <div className="absolute inset-0 opacity-10" style={{ backgroundImage: 'radial-gradient(circle at 25% 50%, #00B0F0 0%, transparent 50%), radial-gradient(circle at 75% 50%, #0066CC 0%, transparent 50%)' }} />
        <div className="relative px-8 py-16">
          <div className="max-w-6xl mx-auto">
            <div className="flex items-center gap-3 mb-4">
              <div className="px-3 py-1 bg-white bg-opacity-10 backdrop-blur border border-white border-opacity-10 rounded-full text-xs font-semibold text-blue-300 tracking-wide">
                KPMG Advisory
              </div>
            </div>
            <h1 className="text-4xl md:text-5xl font-extrabold text-white mb-4 tracking-tight">
              BSS Transformation<br />
              <span className="bg-gradient-to-r from-[#00B0F0] to-[#00d4ff] bg-clip-text text-transparent">Tool</span>
            </h1>
            <p className="text-lg text-blue-200 max-w-2xl leading-relaxed mb-8">
              A unified platform for managing end-to-end BSS transformation — from reconciliation
              and workflow tracking to automated testing and AI-powered analytics.
            </p>
            <div className="flex items-center gap-4 flex-wrap">
              <button
                onClick={() => navigate('/dashboard')}
                className="px-7 py-3 bg-gradient-to-r from-[#00B0F0] to-[#0088cc] text-white font-bold rounded-xl hover:shadow-lg hover:shadow-blue-500/30 hover:-translate-y-0.5 transition-all duration-300 text-sm flex items-center gap-2"
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                  <rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/>
                </svg>
                Go to Dashboard
              </button>
              <button
                onClick={() => navigate('/agent/about')}
                className="px-7 py-3 bg-white bg-opacity-10 backdrop-blur border border-white border-opacity-20 text-white font-semibold rounded-xl hover:bg-opacity-20 hover:-translate-y-0.5 transition-all duration-300 text-sm flex items-center gap-2"
              >
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <circle cx="12" cy="12" r="3"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/>
                </svg>
                AI Agents
              </button>
            </div>
            <div className="flex items-center gap-6 mt-8 text-sm">
              <span className="flex items-center gap-2 text-blue-300">
                <span className="w-2 h-2 bg-green-400 rounded-full inline-block animate-pulse"></span>
                {totalModules} Modules
              </span>
              <span className="flex items-center gap-2 text-blue-300">
                <span className="w-2 h-2 bg-blue-400 rounded-full inline-block"></span>
                {sections.length} Categories
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Sections */}
      <div className="max-w-6xl mx-auto px-8 py-10 space-y-10">
        {sections.map((section) => {
          const SectionIcon = section.icon;
          return (
            <div key={section.title}>
              {/* Section header */}
              <div className="flex items-center gap-3 mb-2">
                <div className="rounded-lg p-2" style={{ backgroundColor: section.accent + '15' }}>
                  <SectionIcon size={20} style={{ color: section.accent }} />
                </div>
                <div>
                  <h2 className="text-lg font-bold text-gray-800">{section.title}</h2>
                  <p className="text-xs text-gray-500">{section.desc}</p>
                </div>
              </div>
              <div className="border-b border-gray-200 mb-5"></div>

              {/* Cards grid */}
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-5">
                {section.items.map((item) => (
                  <FeatureCard key={item.title} item={item} navigate={navigate} />
                ))}
              </div>
            </div>
          );
        })}
      </div>

      {/* Footer */}
      <div className="border-t border-gray-200 bg-white px-8 py-5">
        <div className="max-w-6xl mx-auto flex items-center justify-between text-xs text-gray-400">
          <span>BSS Transformation Tool — KPMG Advisory</span>
          <span className="text-gray-300">Confidential</span>
        </div>
      </div>
    </div>
  );
}
