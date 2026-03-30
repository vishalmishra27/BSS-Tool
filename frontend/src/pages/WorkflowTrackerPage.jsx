import { useState, useEffect } from 'react';
import { useSearchParams } from 'react-router-dom';

const PHASE_NAMES = {
  'phase1': 'Initiation & Planning',
  'phase2': 'SRS Finalization',
  'phase3': 'Product Rationalization',
  'phase4': 'Configuration Validation',
  'phase5': 'Data Cleanup & Preparation',
  'phase6': 'UAT Execution',
  'phase7': 'Trial / Dry Run Migration',
  'phase8': 'Final Migration & Cutover',
  'phase9': 'Post-Migration Stabilization',
};

const phaseNum = (phase_id) => phase_id?.replace('phase', '') || '';

const STATUS_COLOUR = {
  complete: '#16a34a',
  current:  '#d97706',
  pending:  '#9ca3af',
};

// ─── Expandable Checklist Item ───────────────────────────────────────────────
function ChecklistItemRow({ item, readOnly, onToggle }) {
  const [expanded, setExpanded] = useState(false);
  const [comments, setComments] = useState([]);
  const [attachments, setAttachments] = useState([]);
  const [newComment, setNewComment] = useState('');
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [uploading, setUploading] = useState(false);

  const fetchDetails = async () => {
    setLoadingDetails(true);
    try {
      const [commRes, attRes] = await Promise.all([
        fetch(`/api/checklist/${item.ch_id}/comments`).then(r => r.json()),
        fetch(`/api/checklist/${item.ch_id}/attachments`).then(r => r.json()),
      ]);
      setComments(Array.isArray(commRes) ? commRes : []);
      setAttachments(Array.isArray(attRes) ? attRes : []);
    } catch (e) {
      console.error('Error fetching item details:', e);
    }
    setLoadingDetails(false);
  };

  const handleExpand = () => {
    if (!expanded) fetchDetails();
    setExpanded(!expanded);
  };

  const submitComment = async () => {
    if (!newComment.trim()) return;
    setSubmitting(true);
    try {
      await fetch(`/api/checklist/${item.ch_id}/comments`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ comment: newComment, username: 'Programme User' }),
      });
      setNewComment('');
      fetchDetails();
    } catch (e) {
      console.error('Error adding comment:', e);
    }
    setSubmitting(false);
  };

  const handleFileUpload = async (e) => {
    const file = e.target.files[0];
    if (!file) return;
    setUploading(true);
    try {
      const formData = new FormData();
      formData.append('file', file);
      formData.append('username', 'Programme User');
      await fetch(`/api/checklist/${item.ch_id}/attachments`, {
        method: 'POST',
        body: formData,
      });
      fetchDetails();
    } catch (e) {
      console.error('Error uploading file:', e);
    }
    setUploading(false);
    e.target.value = '';
  };

  const isComplete = item.status === 'complete';

  return (
    <div style={{ borderBottom: '1px solid #f0f0f0' }}>
      {/* Main row */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '9px 0' }}>
        <button
          onClick={() => onToggle(item)}
          disabled={readOnly}
          style={{
            width: 20, height: 20, borderRadius: 4, flexShrink: 0, cursor: readOnly ? 'default' : 'pointer',
            background: isComplete ? '#16a34a' : '#fff',
            border: `2px solid ${isComplete ? '#16a34a' : '#d1d5db'}`,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
          }}
        >
          {isComplete && <span style={{ color: '#fff', fontSize: 12, lineHeight: 1 }}>✓</span>}
        </button>
        <span style={{
          fontSize: 13, color: isComplete ? '#6b7280' : '#222',
          textDecoration: isComplete ? 'line-through' : 'none',
          flex: 1,
        }}>
          {item.item_text}
        </span>
        <span style={{
          fontSize: 11, padding: '2px 8px', borderRadius: 10,
          background: isComplete ? '#dcfce7' : '#f1f5f9',
          color: isComplete ? '#16a34a' : '#6b7280',
        }}>
          {item.status}
        </span>
        <button
          onClick={handleExpand}
          style={{
            background: expanded ? '#e0e8f0' : '#f8faff',
            border: '1px solid #d0d8e8',
            borderRadius: 4, cursor: 'pointer', padding: '4px 10px',
            fontSize: 11, fontWeight: 600, color: '#001F5B',
          }}
        >
          {expanded ? '▲ Close' : '▼ Details'}
        </button>
      </div>

      {/* Expandable panel */}
      {expanded && (
        <div style={{
          margin: '0 0 12px 32px', padding: 16,
          background: '#f8faff', borderRadius: 8, border: '1px solid #e0e8f0',
        }}>
          {loadingDetails ? (
            <div style={{ color: '#aaa', fontSize: 13 }}>Loading...</div>
          ) : (
            <>
              {/* Comments section */}
              <div style={{ marginBottom: 16 }}>
                <div style={{ fontSize: 13, fontWeight: 700, color: '#001F5B', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}>
                  💬 Comments ({comments.length})
                </div>
                {comments.length === 0 ? (
                  <div style={{ fontSize: 12, color: '#aaa', marginBottom: 8 }}>No comments yet.</div>
                ) : (
                  <div style={{ maxHeight: 200, overflowY: 'auto', marginBottom: 8 }}>
                    {comments.map(c => (
                      <div key={c.id} style={{
                        padding: '8px 12px', marginBottom: 6, background: '#fff',
                        borderRadius: 6, border: '1px solid #e8eef8',
                      }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 3 }}>
                          <span style={{ fontSize: 12, fontWeight: 600, color: '#222' }}>{c.username}</span>
                          <span style={{ fontSize: 10, color: '#999' }}>
                            {new Date(c.created_at).toLocaleString('en-GB', { dateStyle: 'short', timeStyle: 'short' })}
                          </span>
                        </div>
                        <div style={{ fontSize: 13, color: '#333' }}>{c.comment}</div>
                      </div>
                    ))}
                  </div>
                )}
                {!readOnly && (
                  <div style={{ display: 'flex', gap: 8 }}>
                    <input
                      type="text"
                      value={newComment}
                      onChange={e => setNewComment(e.target.value)}
                      onKeyDown={e => e.key === 'Enter' && submitComment()}
                      placeholder="Add a comment..."
                      style={{
                        flex: 1, padding: '7px 10px', border: '1px solid #d0d8f0',
                        borderRadius: 5, fontSize: 12, outline: 'none',
                      }}
                    />
                    <button
                      onClick={submitComment}
                      disabled={submitting || !newComment.trim()}
                      style={{
                        ...btnBlue,
                        opacity: submitting || !newComment.trim() ? 0.5 : 1,
                      }}
                    >
                      {submitting ? '...' : 'Post'}
                    </button>
                  </div>
                )}
              </div>

              {/* Attachments section */}
              <div>
                <div style={{ fontSize: 13, fontWeight: 700, color: '#001F5B', marginBottom: 8, display: 'flex', alignItems: 'center', gap: 6 }}>
                  📎 Attachments ({attachments.length})
                </div>
                {attachments.length === 0 ? (
                  <div style={{ fontSize: 12, color: '#aaa', marginBottom: 8 }}>No attachments yet.</div>
                ) : (
                  <div style={{ marginBottom: 8 }}>
                    {attachments.map(a => (
                      <div key={a.id} style={{
                        display: 'flex', alignItems: 'center', gap: 8, padding: '6px 10px',
                        marginBottom: 4, background: '#fff', borderRadius: 5,
                        border: '1px solid #e8eef8', fontSize: 12,
                      }}>
                        <span style={{ flex: 1, color: '#222' }}>📄 {a.file_name}</span>
                        <span style={{ fontSize: 10, color: '#999' }}>
                          {a.uploaded_by} · {new Date(a.uploaded_at).toLocaleString('en-GB', { dateStyle: 'short', timeStyle: 'short' })}
                        </span>
                        <a
                          href={`/api/checklist/attachments/download/${a.id}`}
                          style={{ fontSize: 11, color: '#0070c0', textDecoration: 'none', fontWeight: 600 }}
                        >
                          Download
                        </a>
                      </div>
                    ))}
                  </div>
                )}
                {!readOnly && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <label style={{
                      ...btnGreen,
                      display: 'inline-flex', alignItems: 'center', gap: 4,
                      opacity: uploading ? 0.5 : 1, cursor: uploading ? 'wait' : 'pointer',
                    }}>
                      {uploading ? 'Uploading...' : '+ Upload File'}
                      <input type="file" onChange={handleFileUpload} style={{ display: 'none' }} disabled={uploading} />
                    </label>
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}

// ─── Main Page ───────────────────────────────────────────────────────────────
export default function WorkflowTrackerPage({ canComment = true, canUpload = true, readOnly = false }) {
  const [searchParams] = useSearchParams();
  const initialPhaseId = searchParams.get('phase');
  const [phases, setPhases]         = useState([]);
  const [selectedPhase, setSelectedPhase] = useState(null);
  const [checklist, setChecklist]   = useState([]);
  const [comments, setComments]     = useState([]);
  const [loadingPhases, setLoadingPhases] = useState(true);
  const [loadingChecklist, setLoadingChecklist] = useState(false);

  // Phase-level comment modal state
  const [showCommentModal, setShowCommentModal]     = useState(false);
  const [showAttachmentModal, setShowAttachmentModal] = useState(false);
  const [newComment, setNewComment]       = useState('');
  const [attachmentName, setAttachmentName] = useState('');
  const [saving, setSaving] = useState(false);
  const [submittingPhase, setSubmittingPhase] = useState(false);

  useEffect(() => {
    fetch('/api/phases')
      .then(r => r.json())
      .then(data => {
        const sorted = [...data].sort((a, b) => parseInt(a.phase_id) - parseInt(b.phase_id));
        setPhases(sorted);
        const fromUrl = initialPhaseId ? sorted.find(p => p.phase_id === initialPhaseId) : null;
        const current = fromUrl || sorted.find(p => p.curr_status === 'current') || sorted[0];
        if (current) setSelectedPhase(current);
        setLoadingPhases(false);
      })
      .catch(() => setLoadingPhases(false));
  }, []);

  useEffect(() => {
    if (!selectedPhase) return;
    setLoadingChecklist(true);
    Promise.all([
      fetch(`/api/checklist/${selectedPhase.phase_id}`).then(r => r.json()),
      fetch(`/api/workflow/comments/${selectedPhase.phase_id}`).then(r => r.json()),
    ]).then(([cl, co]) => {
      setChecklist(Array.isArray(cl) ? cl : []);
      setComments(Array.isArray(co) ? co : []);
      setLoadingChecklist(false);
    }).catch(() => setLoadingChecklist(false));
  }, [selectedPhase]);

  const completedCount = checklist.filter(c => c.status === 'complete').length;
  const totalCount = checklist.length;
  const pct = totalCount > 0 ? Math.round((completedCount / totalCount) * 100) : 0;
  const allComplete = totalCount > 0 && completedCount === totalCount;

  const handleSubmitAndProceed = async () => {
    if (!allComplete || !selectedPhase) return;
    setSubmittingPhase(true);
    try {
      await fetch(`/api/phases/${selectedPhase.phase_id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ curr_status: 'complete' }),
      });
      const currentIdx = phases.findIndex(p => p.phase_id === selectedPhase.phase_id);
      const nextPhase = phases[currentIdx + 1];
      if (nextPhase && nextPhase.curr_status === 'pending') {
        await fetch(`/api/phases/${nextPhase.phase_id}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ curr_status: 'current' }),
        });
      }
      const refreshed = await fetch('/api/phases').then(r => r.json());
      const sorted = [...refreshed].sort((a, b) => parseInt(a.phase_id) - parseInt(b.phase_id));
      setPhases(sorted);
      const updated = sorted.find(p => p.phase_id === selectedPhase.phase_id);
      if (updated) setSelectedPhase(updated);
    } catch (e) {
      console.error('Error submitting phase:', e);
    }
    setSubmittingPhase(false);
  };

  const toggleChecklistItem = async (item) => {
    if (readOnly) return;
    const newStatus = item.status === 'complete' ? 'pending' : 'complete';
    await fetch(`/api/checklist/${item.ch_id}/status`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: newStatus }),
    });
    const updatedChecklist = checklist.map(c => c.ch_id === item.ch_id ? { ...c, status: newStatus } : c);
    setChecklist(updatedChecklist);

    // If unchecking an item on a completed phase, revert phase to 'current'
    if (newStatus === 'pending' && selectedPhase?.curr_status === 'complete') {
      await fetch(`/api/phases/${selectedPhase.phase_id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ curr_status: 'current' }),
      });
      const refreshed = await fetch('/api/phases').then(r => r.json());
      const sorted = [...refreshed].sort((a, b) => parseInt(a.phase_id) - parseInt(b.phase_id));
      setPhases(sorted);
      const updated = sorted.find(p => p.phase_id === selectedPhase.phase_id);
      if (updated) setSelectedPhase(updated);
    }
  };

  const submitComment = async (action = 'Commented', attName = null) => {
    if (!newComment.trim() && !attName) return;
    setSaving(true);
    await fetch('/api/workflow/comments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        phase_id: selectedPhase.phase_id,
        username: 'Programme User',
        action,
        comment: newComment || `Attachment uploaded: ${attName}`,
        attachment_name: attName,
      }),
    });
    const co = await fetch(`/api/workflow/comments/${selectedPhase.phase_id}`).then(r => r.json());
    setComments(Array.isArray(co) ? co : []);
    setNewComment('');
    setAttachmentName('');
    setShowCommentModal(false);
    setShowAttachmentModal(false);
    setSaving(false);
  };

  return (
    <div style={{ padding: 24, fontFamily: 'system-ui, sans-serif', background: '#f8faff', minHeight: '100vh' }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ margin: 0, fontSize: 22, fontWeight: 700, color: '#001F5B' }}>Workflow & Milestones</h1>
        <p style={{ margin: '4px 0 0', color: '#666', fontSize: 13 }}>BSS Migration Programme — Phase progression and task management</p>
      </div>

      <div style={{ display: 'flex', gap: 20, alignItems: 'flex-start', flexWrap: 'wrap' }}>
        {/* Left: Phase list */}
        <div style={{ width: 220, flexShrink: 0 }}>
          <div style={{ background: '#fff', borderRadius: 8, border: '1px solid #e0e8f0', overflow: 'hidden' }}>
            <div style={{ padding: '10px 14px', background: '#001F5B', color: '#fff', fontSize: 12, fontWeight: 600 }}>PROGRAMME PHASES</div>
            {loadingPhases ? (
              <div style={{ padding: 20, color: '#aaa', fontSize: 13 }}>Loading…</div>
            ) : phases.map(phase => (
              <button
                key={phase.id}
                onClick={() => setSelectedPhase(phase)}
                style={{
                  display: 'flex', alignItems: 'center', gap: 10,
                  width: '100%', padding: '10px 14px', textAlign: 'left',
                  background: selectedPhase?.id === phase.id ? '#f0f4ff' : '#fff',
                  borderLeft: selectedPhase?.id === phase.id ? '3px solid #0070c0' : '3px solid transparent',
                  border: 'none', borderBottom: '1px solid #f0f0f0', cursor: 'pointer',
                }}
              >
                <span style={{
                  width: 8, height: 8, borderRadius: '50%', flexShrink: 0,
                  background: STATUS_COLOUR[phase.curr_status] || '#9ca3af',
                }} />
                <div>
                  <div style={{ fontSize: 12, fontWeight: 600, color: '#222' }}>Phase {phaseNum(phase.phase_id)}</div>
                  <div style={{ fontSize: 11, color: '#666', marginTop: 1 }}>{PHASE_NAMES[phase.phase_id] || phase.phase_id}</div>
                </div>
              </button>
            ))}
          </div>
        </div>

        {/* Right: Phase detail */}
        <div style={{ flex: 1, minWidth: 300 }}>
          {!selectedPhase ? (
            <div style={{ padding: 40, textAlign: 'center', color: '#aaa' }}>Select a phase</div>
          ) : (
            <>
              {/* Phase header card */}
              <div style={{ background: '#fff', borderRadius: 8, border: '1px solid #e0e8f0', padding: 20, marginBottom: 16 }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 10 }}>
                  <div>
                    <div style={{ fontSize: 17, fontWeight: 700, color: '#001F5B' }}>
                      Phase {phaseNum(selectedPhase.phase_id)} — {PHASE_NAMES[selectedPhase.phase_id]}
                    </div>
                    <div style={{ display: 'flex', gap: 16, marginTop: 6, fontSize: 12, color: '#666', flexWrap: 'wrap' }}>
                      {selectedPhase.start_dt && <span>Start: <b>{selectedPhase.start_dt}</b></span>}
                      {selectedPhase.end_dt   && <span>End: <b>{selectedPhase.end_dt}</b></span>}
                      {selectedPhase.lob      && <span>LOB: <b>{selectedPhase.lob}</b></span>}
                    </div>
                  </div>
                  <span style={{
                    padding: '4px 12px', borderRadius: 12, fontSize: 12, fontWeight: 600,
                    background: selectedPhase.curr_status === 'complete' ? '#dcfce7' : selectedPhase.curr_status === 'current' ? '#fef3c7' : '#f1f5f9',
                    color: STATUS_COLOUR[selectedPhase.curr_status] || '#555',
                  }}>
                    {selectedPhase.curr_status}
                  </span>
                </div>

                {totalCount > 0 && (
                  <div style={{ marginTop: 14 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, color: '#555', marginBottom: 5 }}>
                      <span>Checklist progress</span>
                      <span><b>{completedCount}/{totalCount}</b> items complete</span>
                    </div>
                    <div style={{ height: 8, background: '#e5e7eb', borderRadius: 4, overflow: 'hidden' }}>
                      <div style={{ height: '100%', width: `${pct}%`, background: pct === 100 ? '#16a34a' : '#0070c0', borderRadius: 4, transition: 'width 0.3s' }} />
                    </div>
                  </div>
                )}
              </div>

              {/* Checklist with expandable items */}
              <div style={{ background: '#fff', borderRadius: 8, border: '1px solid #e0e8f0', padding: 20, marginBottom: 16 }}>
                <div style={{ fontWeight: 600, fontSize: 14, color: '#001F5B', marginBottom: 14, borderBottom: '1px solid #e8e8e8', paddingBottom: 10 }}>
                  CHECKLIST ITEMS
                </div>
                {loadingChecklist ? (
                  <div style={{ color: '#aaa', fontSize: 13 }}>Loading…</div>
                ) : checklist.length === 0 ? (
                  <div style={{ color: '#aaa', fontSize: 13 }}>No checklist items for this phase.</div>
                ) : checklist.map(item => (
                  <ChecklistItemRow
                    key={item.ch_id}
                    item={item}
                    readOnly={readOnly}
                    onToggle={toggleChecklistItem}
                  />
                ))}

                {/* Submit & Proceed button */}
                {!readOnly && selectedPhase.curr_status !== 'complete' && (
                  <div style={{ marginTop: 16, display: 'flex', justifyContent: 'flex-end' }}>
                    <button
                      onClick={handleSubmitAndProceed}
                      disabled={!allComplete || submittingPhase}
                      style={{
                        padding: '10px 24px', borderRadius: 6, border: 'none', fontSize: 13, fontWeight: 700,
                        cursor: allComplete && !submittingPhase ? 'pointer' : 'not-allowed',
                        background: allComplete ? '#16a34a' : '#d1d5db',
                        color: allComplete ? '#fff' : '#888',
                        opacity: submittingPhase ? 0.6 : 1,
                      }}
                    >
                      {submittingPhase ? 'Submitting…' : allComplete ? '✓ Submit & Proceed' : 'Complete all items to submit'}
                    </button>
                  </div>
                )}
                {selectedPhase.curr_status === 'complete' && (
                  <div style={{ marginTop: 16, padding: '10px 16px', background: '#dcfce7', borderRadius: 6, color: '#16a34a', fontSize: 13, fontWeight: 600, textAlign: 'center' }}>
                    ✓ Phase completed and submitted
                  </div>
                )}
              </div>

              {/* Action history + buttons */}
              <div style={{ background: '#fff', borderRadius: 8, border: '1px solid #e0e8f0', padding: 20 }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 14, borderBottom: '1px solid #e8e8e8', paddingBottom: 10 }}>
                  <div style={{ fontWeight: 600, fontSize: 14, color: '#001F5B' }}>ACTION HISTORY</div>
                  <div style={{ display: 'flex', gap: 8 }}>
                    {canComment && !readOnly && (
                      <button onClick={() => setShowCommentModal(true)} style={btnBlue}>Add Comment</button>
                    )}
                    {canUpload && !readOnly && (
                      <button onClick={() => setShowAttachmentModal(true)} style={btnGreen}>Add Attachment</button>
                    )}
                  </div>
                </div>

                {comments.length === 0 ? (
                  <div style={{ color: '#aaa', fontSize: 13 }}>No activity recorded yet for this phase.</div>
                ) : comments.map(c => (
                  <div key={c.id} style={{ padding: '12px 0', borderBottom: '1px solid #f5f5f5' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                      <span style={{ fontSize: 13, fontWeight: 600, color: '#222' }}>{c.username}</span>
                      <span style={{ fontSize: 11, color: '#999' }}>{new Date(c.created_at).toLocaleString('en-GB', { dateStyle: 'short', timeStyle: 'short' })}</span>
                    </div>
                    <div style={{ display: 'flex', gap: 16, fontSize: 11, color: '#666', marginBottom: 6 }}>
                      <span>Action: <b>{c.action}</b></span>
                      <span>Phase: <b>{c.phase_id}</b></span>
                    </div>
                    <div style={{ fontSize: 13, color: '#333', background: '#f8faff', padding: '8px 12px', borderRadius: 6, border: '1px solid #e8eef8' }}>
                      {c.comment}
                    </div>
                    {c.attachment_name && (
                      <div style={{ marginTop: 6, fontSize: 12, color: '#0070c0' }}>📎 {c.attachment_name}</div>
                    )}
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      </div>

      {/* Comment Modal */}
      {showCommentModal && (
        <div style={overlay}>
          <div style={modal}>
            <h3 style={{ margin: '0 0 14px', fontSize: 15, color: '#001F5B' }}>Add Comment</h3>
            <textarea
              value={newComment}
              onChange={e => setNewComment(e.target.value)}
              rows={4}
              placeholder="Enter your comment…"
              style={{ width: '100%', padding: '8px 10px', border: '1px solid #d0d8f0', borderRadius: 6, fontSize: 13, resize: 'vertical', boxSizing: 'border-box' }}
            />
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 14 }}>
              <button onClick={() => setShowCommentModal(false)} style={btnGray}>Cancel</button>
              <button onClick={() => submitComment('Commented')} disabled={saving || !newComment.trim()} style={btnBlue}>{saving ? 'Saving…' : 'Submit'}</button>
            </div>
          </div>
        </div>
      )}

      {/* Attachment Modal */}
      {showAttachmentModal && (
        <div style={overlay}>
          <div style={modal}>
            <h3 style={{ margin: '0 0 14px', fontSize: 15, color: '#001F5B' }}>Add Attachment</h3>
            <input type="file" onChange={e => setAttachmentName(e.target.files[0]?.name || '')} style={{ marginBottom: 10, fontSize: 13 }} />
            <input
              value={attachmentName}
              onChange={e => setAttachmentName(e.target.value)}
              placeholder="Attachment name…"
              style={{ width: '100%', padding: '8px 10px', border: '1px solid #d0d8f0', borderRadius: 6, fontSize: 13, boxSizing: 'border-box' }}
            />
            <textarea
              value={newComment}
              onChange={e => setNewComment(e.target.value)}
              rows={2}
              placeholder="Optional note…"
              style={{ width: '100%', marginTop: 8, padding: '8px 10px', border: '1px solid #d0d8f0', borderRadius: 6, fontSize: 13, resize: 'vertical', boxSizing: 'border-box' }}
            />
            <div style={{ display: 'flex', justifyContent: 'flex-end', gap: 8, marginTop: 14 }}>
              <button onClick={() => setShowAttachmentModal(false)} style={btnGray}>Cancel</button>
              <button onClick={() => submitComment('Attachment Added', attachmentName)} disabled={saving || !attachmentName.trim()} style={btnGreen}>{saving ? 'Saving…' : 'Upload'}</button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

const btnBlue = { padding: '7px 16px', background: '#0070c0', color: '#fff', border: 'none', borderRadius: 5, fontSize: 12, fontWeight: 600, cursor: 'pointer' };
const btnGreen = { padding: '7px 16px', background: '#16a34a', color: '#fff', border: 'none', borderRadius: 5, fontSize: 12, fontWeight: 600, cursor: 'pointer' };
const btnGray = { padding: '7px 16px', background: '#f1f5f9', color: '#444', border: '1px solid #d1d5db', borderRadius: 5, fontSize: 12, cursor: 'pointer' };
const overlay = { position: 'fixed', inset: 0, background: 'rgba(0,0,0,0.45)', zIndex: 9000, display: 'flex', alignItems: 'center', justifyContent: 'center' };
const modal = { background: '#fff', borderRadius: 10, padding: 24, width: 420, maxWidth: '95vw', boxShadow: '0 8px 40px rgba(0,0,0,0.2)' };
