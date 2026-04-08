import React from 'react'

export default function NextStepsPanel({ steps, onAction }) {
  if (!steps || steps.length === 0) return null

  return (
    <div className="next-steps-panel">
      <div className="next-steps-header">
        <span className="next-steps-icon">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="9 18 15 12 9 6" />
          </svg>
        </span>
        <span className="next-steps-title">What you can do next</span>
      </div>
      <div className="next-steps-list">
        {steps.map((step, idx) => (
          <button
            key={idx}
            className="next-step-chip"
            onClick={() => onAction(step.prompt || step.label)}
            type="button"
          >
            <div className="next-step-content">
              <span className="next-step-label">{step.label}</span>
              {step.description && step.description !== step.label && (
                <span className="next-step-desc">{step.description}</span>
              )}
            </div>
            <span className="next-step-arrow">&rarr;</span>
          </button>
        ))}
      </div>
    </div>
  )
}
