import { useState } from 'react';
import { PenLine, FileText, Sparkles, ArrowRight, X, MessageSquare } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

const AGENTS = [
  {
    id: 'crud', name: 'CRUD Operations Agent', command: '/crud',
    icon: PenLine, color: 'violet',
    colorClass: 'bg-violet-500 shadow-violet-500/20',
    tagline: 'Control dashboards, workflows & lifecycles via prompts',
    desc: 'Manage all core BSS modules through natural language prompts. Control the Transformation Dashboard, Workflow Tracker, Product Lifecycle, and UAT Lifecycle — create, update, and query records with AI-generated SQL. Every write operation shows a diff preview and requires your explicit approval before execution.',
    capabilities: [
      'Control Transformation Dashboard — update KPIs, project status, and metrics',
      'Manage Workflow Tracker — assign tasks, update stages, track progress',
      'Product Lifecycle management — create, update, and track products end-to-end',
      'UAT Lifecycle control — manage test cases, update statuses, track priorities',
      'Natural language to SQL generation with diff preview before writes',
      'Approve or reject each operation individually with full audit trail',
    ],
    chatPath: '/agent/crud',
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
    chatPath: '/agent/ocr',
  },
];

// The two primary agents shown in the "Open Agent Chat" selector
const PRIMARY_AGENTS = AGENTS.filter(a => a.id === 'crud' || a.id === 'ocr');

function AgentSelectorModal({ onClose, navigate }) {
  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50" onClick={onClose}>
      <div className="bg-white rounded-2xl shadow-2xl w-[480px] max-w-[92vw] overflow-hidden" onClick={e => e.stopPropagation()}>
        <div className="flex items-center justify-between px-6 py-5 border-b border-gray-100">
          <div>
            <h2 className="text-lg font-bold text-gray-900">Select an Agent</h2>
            <p className="text-sm text-gray-500 mt-0.5">Choose which agent to chat with</p>
          </div>
          <button onClick={onClose} className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors">
            <X size={18} className="text-gray-400" />
          </button>
        </div>
        <div className="p-5 space-y-3">
          {PRIMARY_AGENTS.map(a => {
            const Icon = a.icon;
            return (
              <button
                key={a.id}
                onClick={() => { onClose(); navigate(a.chatPath); }}
                className="w-full flex items-center gap-4 p-4 rounded-xl border border-gray-200 hover:border-gray-300 hover:shadow-md hover:-translate-y-0.5 transition-all text-left group"
              >
                <div className={`w-12 h-12 rounded-xl ${a.colorClass} flex items-center justify-center flex-shrink-0`}>
                  <Icon size={24} className="text-white" />
                </div>
                <div className="flex-1 min-w-0">
                  <h3 className="font-bold text-gray-900 text-sm">{a.name}</h3>
                  <p className="text-xs text-gray-500 mt-0.5 line-clamp-2">{a.tagline}</p>
                </div>
                <ArrowRight size={18} className="text-gray-300 group-hover:text-gray-500 transition-colors flex-shrink-0" />
              </button>
            );
          })}
        </div>
        <div className="px-6 py-3 bg-gray-50 border-t border-gray-100">
          <p className="text-xs text-gray-400 text-center">All agents are also accessible from the unified chat via <kbd className="px-1 py-0.5 bg-white rounded text-[10px] font-mono border border-gray-200">/command</kbd></p>
        </div>
      </div>
    </div>
  );
}

export default function AgentDescriptionsPage() {
  const navigate = useNavigate();
  const [showSelector, setShowSelector] = useState(false);

  return (
    <div className="min-h-screen bg-[#f5f7fb] font-sans">
      {showSelector && <AgentSelectorModal onClose={() => setShowSelector(false)} navigate={navigate} />}

      <div className="max-w-5xl mx-auto px-6 py-12">
        {/* Header */}
        <div className="text-center mb-12">
          <div className="w-16 h-16 rounded-2xl bg-gradient-to-br from-blue-500 to-violet-500 flex items-center justify-center mx-auto mb-5 shadow-lg shadow-blue-500/20">
            <Sparkles size={32} className="text-white" />
          </div>
          <h1 className="text-3xl font-bold text-gray-900 mb-3">AI Agents</h1>
          <p className="text-base text-gray-500 max-w-2xl mx-auto leading-relaxed">
            Two specialized agents to help you control dashboards, manage workflows, and analyze documents — all through natural language.
          </p>
          <button
            onClick={() => setShowSelector(true)}
            className="mt-6 inline-flex items-center gap-2 px-6 py-2.5 bg-gray-900 text-white rounded-full text-sm font-semibold hover:bg-gray-800 transition-colors shadow-sm"
          >
            <MessageSquare size={16} />
            Open Agent Chat
            <ArrowRight size={16} />
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
                  <button onClick={() => navigate(a.chatPath)}
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
