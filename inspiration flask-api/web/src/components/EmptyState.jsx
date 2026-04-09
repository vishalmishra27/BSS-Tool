import React from 'react'

export default function EmptyState({ onNewChat }) {
  return (
    <div className="empty-state">
      <div className="empty-state-logo">Rishit</div>
      <h2>Control Testing Agent</h2>
      <p>
        AI-powered compliance analysis for SOX, IFC, ICOFR, and Internal Audit. Load RCMs, identify gaps,
        test controls, and generate audit workpapers.
      </p>
      <div className="empty-state-capabilities">
        <div className="capability-card">
          <span className="capability-icon">📊</span>
          <span>Load & inspect RCM data</span>
        </div>
        <div className="capability-card">
          <span className="capability-icon">🔍</span>
          <span>AI gap analysis & suggestions</span>
        </div>
        <div className="capability-card">
          <span className="capability-icon">✓</span>
          <span>Test of Design & Effectiveness</span>
        </div>
        <div className="capability-card">
          <span className="capability-icon">📋</span>
          <span>Control assessment & sampling</span>
        </div>
      </div>
      <button className="new-chat-btn" onClick={onNewChat}>
        + Start a new conversation
      </button>
    </div>
  )
}
