import { Database, PenLine, FileSearch, FileText, Sparkles, ArrowRight } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

const AGENTS = [
  {
    id: 'data', name: 'Data Management Agent', command: '/data',
    icon: Database, color: 'blue',
    colorClass: 'bg-blue-500 shadow-blue-500/20',
    tagline: 'Your database operations co-pilot',
    desc: 'Upload CSV/PSV files, sanitize and clean data, run SQL queries, manage database tables, and bulk-load data into PostgreSQL — all through natural language conversation.',
    capabilities: [
      'Upload & parse CSV, PSV, and TXT files with automatic delimiter detection',
      'Sanitize data: clean headers, trim whitespace, remove duplicates',
      'Execute read-only SQL queries and display results',
      'Create, alter, and drop database tables with confirmation',
      'Bulk-load cleaned data into PostgreSQL tables',
      'View table schemas, row counts, and column types',
    ],
  },
  {
    id: 'crud', name: 'CRUD Operations Agent', command: '/crud',
    icon: PenLine, color: 'violet',
    colorClass: 'bg-violet-500 shadow-violet-500/20',
    tagline: 'Safe database writes with approval workflows',
    desc: 'Perform Create, Read, Update, and Delete operations on your database using natural language. Every write operation shows a diff preview and requires your explicit approval before execution.',
    capabilities: [
      'Natural language to SQL generation for CRUD operations',
      'Diff preview: see before/after values before any write executes',
      'Approve or reject each operation individually',
      'Bulk update support with row count confirmation',
      'Full audit trail of all confirmed and rejected operations',
      'Read operations execute immediately; writes always require approval',
    ],
  },
  {
    id: 'reconciliation', name: 'Reconciliation Agent', command: '/recon',
    icon: FileSearch, color: 'cyan',
    colorClass: 'bg-cyan-500 shadow-cyan-500/20',
    tagline: 'CBS vs CLM data comparison engine',
    desc: 'Compare and reconcile data between CBS and CLM systems. Identify status mismatches, generate aggregated reports, and track reconciliation health — all read-only queries, no writes.',
    capabilities: [
      'Compare CBS and CLM status fields to find mismatches',
      'Aggregated reconciliation breakdowns by status, account code, service code',
      'Identify records that are active in one system but inactive in another',
      'Calculate overall reconciliation health percentage',
      'Top-N analysis: most frequent discrepancies, service codes, link codes',
      'All queries are read-only — no data modifications',
    ],
  },
  {
    id: 'ocr', name: 'Document Analysis Agent', command: '/doc',
    icon: FileText, color: 'emerald',
    colorClass: 'bg-emerald-500 shadow-emerald-500/20',
    tagline: 'PDF text extraction with configurable export',
    desc: 'Upload PDF documents to extract text content. Configure your export with file/page selection, column toggles, output format (CSV, XLSX, TXT), and preview before downloading.',
    capabilities: [
      'Extract text from readable PDF documents using PyMuPDF',
      'Select specific files and pages for export',
      'Choose output format: CSV, XLSX, or TXT',
      'Toggle output columns: File Name, Page Number, Extracted Text',
      'Structure options: one row per page or one row per line',
      'Live preview of export before download',
    ],
  },
];

export default function AgentDescriptionsPage() {
  const navigate = useNavigate();

  return (
    <div className="min-h-screen bg-[#f5f7fb] font-sans">
      <div className="max-w-5xl mx-auto px-6 py-12">
        {/* Header */}
        <div className="text-center mb-12">
          <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-blue-500 to-violet-500 flex items-center justify-center mx-auto mb-5 shadow-lg shadow-blue-500/20">
            <Sparkles size={32} className="text-white" />
          </div>
          <h1 className="text-3xl font-bold text-gray-900 mb-3">AI Agents</h1>
          <p className="text-base text-gray-500 max-w-2xl mx-auto leading-relaxed">
            Four specialized agents to help you manage data, perform CRUD operations, reconcile systems, and analyze documents. Access them all from the unified chat using <kbd className="px-1.5 py-0.5 bg-gray-100 rounded text-xs font-mono text-gray-600">/command</kbd> syntax.
          </p>
          <button onClick={() => navigate('/agent')}
            className="mt-6 inline-flex items-center gap-2 px-5 py-2.5 bg-gray-900 text-white rounded-full text-sm font-semibold hover:bg-gray-800 transition-colors shadow-sm">
            Open Agent Chat <ArrowRight size={16} />
          </button>
        </div>

        {/* Agent cards */}
        <div className="space-y-8">
          {AGENTS.map(a => {
            const Icon = a.icon;
            return (
              <div key={a.id} className="bg-white rounded-2xl border border-gray-100 shadow-sm overflow-hidden">
                <div className="p-8">
                  <div className="flex items-start gap-5 mb-6">
                    <div className={`w-12 h-12 rounded-xl ${a.colorClass} flex items-center justify-center flex-shrink-0`}>
                      <Icon size={24} className="text-white" />
                    </div>
                    <div>
                      <div className="flex items-center gap-3 mb-1">
                        <h2 className="text-xl font-bold text-gray-900">{a.name}</h2>
                        <span className="px-2 py-0.5 bg-gray-100 rounded-full text-xs font-mono text-gray-500">{a.command}</span>
                      </div>
                      <p className="text-sm text-gray-400 font-medium">{a.tagline}</p>
                    </div>
                  </div>

                  <p className="text-sm text-gray-600 leading-relaxed mb-6">{a.desc}</p>

                  <div>
                    <h3 className="text-xs font-bold text-gray-400 uppercase tracking-wider mb-3">Capabilities</h3>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-2">
                      {a.capabilities.map((cap, i) => (
                        <div key={i} className="flex items-start gap-2.5 text-sm text-gray-600">
                          <div className={`w-1.5 h-1.5 rounded-full mt-2 flex-shrink-0 ${a.colorClass.split(' ')[0]}`} />
                          {cap}
                        </div>
                      ))}
                    </div>
                  </div>
                </div>

                <div className="px-8 py-4 bg-gray-50 border-t border-gray-100 flex items-center justify-between">
                  <span className="text-xs text-gray-400">Type <kbd className="px-1 py-0.5 bg-white rounded text-[10px] font-mono border border-gray-200">{a.command}</kbd> in the chat to activate</span>
                  <button onClick={() => navigate('/agent')}
                    className={`px-4 py-1.5 rounded-lg text-xs font-bold text-white ${a.colorClass.split(' ')[0]} hover:opacity-90 transition-opacity`}>
                    Try it
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
