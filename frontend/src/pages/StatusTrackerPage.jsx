import React, { useState, useEffect, useRef } from 'react';
import ChecklistItem from '../components/ChecklistItem';
import { useEditor, EditorContent } from '@tiptap/react';
import StarterKit from '@tiptap/starter-kit';
import Underline from '@tiptap/extension-underline';
import { Bold, Italic, Underline as UnderlineIcon } from 'lucide-react';
import { useNavigate, useLocation } from 'react-router-dom';

  // Helper function to format date to YYYY-MM-DD
  const formatDateToYYYYMMDD = (dateString) => {
    const parts = dateString.split(/[\/\-]/);
    if (parts.length === 3) {
      let [part1, part2, part3] = parts;
      if (part3.length === 4) {
        let year = part3;
        let month, day;
        // Determine if part1 or part2 is month or day based on value
        if (parseInt(part1) > 12) {
          // part1 is day, part2 is month (DD/MM/YYYY)
          day = part1;
          month = part2;
        } else if (parseInt(part2) > 12) {
          // part2 is day, part1 is month (MM/DD/YYYY)
          month = part1;
          day = part2;
        } else {
          // Ambiguous, assume MM/DD/YYYY
          month = part1;
          day = part2;
        }
        return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
      }
    }
    return dateString;
  };

  // Helper function to calculate phase start and end dates from checklist items
  const calculatePhaseDates = (checklistItems) => {
    if (!checklistItems || checklistItems.length === 0) {
      return { startDate: null, endDate: null };
    }

    let minStartDate = null;
    let maxEndDate = null;

    checklistItems.forEach(item => {
      if (item.start_date) {
        const startDate = new Date(item.start_date);
        if (!isNaN(startDate.getTime()) && (!minStartDate || startDate < minStartDate)) {
          minStartDate = startDate;
        }
      }
      if (item.end_date) {
        const endDate = new Date(item.end_date);
        if (!isNaN(endDate.getTime()) && (!maxEndDate || endDate > maxEndDate)) {
          maxEndDate = endDate;
        }
      }
    });

    return {
      startDate: minStartDate ? minStartDate.toISOString().split('T')[0] : null,
      endDate: maxEndDate ? maxEndDate.toISOString().split('T')[0] : null
    };
  };



const MenuBar = ({ editor }) => {
  if (!editor) {
    return null;
  }

  return (
    <div className="flex items-center gap-2 border-b border-t border-l border-r border-gray-300 rounded-t-md p-2 bg-gray-50">
      <button
        onClick={() => editor.chain().focus().toggleBold().run()}
        disabled={!editor.can().chain().focus().toggleBold().run()}
        className={editor.isActive('bold') ? 'p-1 bg-gray-300 rounded' : 'p-1'}
      >
        <Bold size={16} />
      </button>
      <button
        onClick={() => editor.chain().focus().toggleItalic().run()}
        disabled={!editor.can().chain().focus().toggleItalic().run()}
        className={editor.isActive('italic') ? 'p-1 bg-gray-300 rounded' : 'p-1'}
      >
        <Italic size={16} />
      </button>
      <button
        onClick={() => editor.chain().focus().toggleUnderline().run()}
        className={editor.isActive('underline') ? 'p-1 bg-gray-300 rounded' : 'p-1'}
      >
        <UnderlineIcon size={16} />
      </button>
    </div>
  );
};

const StatusStepper = ({ levels, currentLevelIndex, onLevelClick }) => {
  const getNodeClass = (level) => {
    const status = level.status?.toLowerCase() || 'pending';
    switch (status) {
      case 'completed':
        return 'bg-green-500';
      case 'pending':
        return 'bg-red-500';
      case 'rfi':
        return 'bg-red-500';
      case 'in-progress':
        return 'bg-orange-500';
      default:
        return 'bg-gray-400';
    }
  };


 
  return (
    <div className="flex items-center w-full">
      {levels.map((level, index) => (
        <React.Fragment key={level.ch_id || level.id || index}>
          <div
            className={`flex flex-col items-center text-center cursor-pointer`}
            onClick={() => onLevelClick(index)}
          >
            <div className={`w-16 h-16 rounded-full flex flex-col items-center justify-center text-white font-bold text-lg shadow-inner transition-all duration-200 ${getNodeClass(level)} ${index === currentLevelIndex ? 'ring-4 ring-blue-500 ring-offset-2 scale-110 shadow-lg' : 'hover:scale-105'}`}>
              <span className="text-xs font-normal">Activity</span>
              <span>{index + 1}</span>
             
            </div>
          </div>
          {index < levels.length - 1 && <div className="flex-1 h-1.5 bg-gray-300"></div>}
        </React.Fragment>
      ))}
    </div>
  );
};

const DetailsGrid = ({ details, onDetailsChange, currentChecklistItem, isEditingEndDate, onEditEndDate, onSaveEndDate, onCancelEdit, checklistItems, currentLevelIndex }) => {
  const [editingValue, setEditingValue] = useState('');

  // Check if editing is allowed: only when current activity is pending
  const canEdit = currentChecklistItem.status === 'pending';

  const handleInputChange = (e) => {
    setEditingValue(e.target.value);
  };

  const handleInputBlur = () => {
    const originalValue = currentChecklistItem.end_date ? new Date(currentChecklistItem.end_date).toISOString().split('T')[0] : '';
    if (editingValue && editingValue !== originalValue) {
      onDetailsChange('End Date', editingValue);
    } else {
      setEditingValue('');
      onCancelEdit(); // This will set isEditingEndDate to false
    }
  };

  const handleSaveDate = () => {
    const originalValue = currentChecklistItem.end_date ? new Date(currentChecklistItem.end_date).toISOString().split('T')[0] : '';
    if (editingValue && editingValue !== originalValue) {
      onDetailsChange('End Date', editingValue);
    }
    setEditingValue('');
    onCancelEdit(); // This will set isEditingEndDate to false
  };

  const handleCancelEdit = () => {
    setEditingValue('');
    onCancelEdit(); // This will set isEditingEndDate to false
  };

  const startEditing = () => {
    const currentValue = currentChecklistItem.end_date ? new Date(currentChecklistItem.end_date).toISOString().split('T')[0] : '';
    setEditingValue(currentValue);
    onEditEndDate();
  };

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-x-6 gap-y-4 text-sm">
      {Object.keys(details).map(key => (
        <div key={key}>
          <p className="text-gray-500">{key}</p>
          {key === 'End Date' ? (
            <div className="flex items-center gap-2">
              {isEditingEndDate ? (
                <input
                  type="date"
                  value={editingValue}
                  onChange={handleInputChange}
                  onBlur={handleInputBlur}
                  className="px-2 py-1 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm font-semibold text-gray-800"
                />
              ) : (
                <>
                  <p className="font-semibold text-gray-800">{details[key] || '—'}</p>
                  {canEdit ? (
                    <button
                      onClick={startEditing}
                      className="px-2 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700"
                    >
                      Edit
                    </button>
                  ) : (
                    <button
                      disabled
                      className="px-2 py-1 bg-gray-400 text-gray-600 text-xs rounded cursor-not-allowed"
                      title="Date editing is only allowed for pending activities"
                    >
                      Edit
                    </button>
                  )}
                </>
              )}
            </div>
          ) : (
            <p className="font-semibold text-gray-800">{details[key] || '—'}</p>
          )}
        </div>
      ))}
    </div>
  );
};

// const InitiatorInfo = ({ info }) => (
//   <div>
//     <h2 className="text-base font-semibold text-gray-700 border-b pb-2 mb-4">INITIATOR INFO</h2>
//     <div className="space-y-3">
//       {Object.entries(info).map(([key, value]) => (
//         <div key={key}>
//           <p className="text-xs text-gray-500">{key}</p>
//           <p className="text-sm font-semibold text-gray-800">{value}</p>
//         </div>
//       ))}
//     </div>
//   </div>
// );

const formatComments = (comments) => {
  if (!comments) return null;

  // Split comments by newlines and format each one
  const commentLines = comments.split('\n').filter(line => line.trim());

  return commentLines.map((line, index) => {
    // Check if it's a log entry first
    if (line.startsWith('[LOG]')) {
      const logMatch = line.match(/^\[LOG\]\s*\[(.*?)\]\s*(.*?):\s*(.*)$/);
      if (logMatch) {
        const [, timestamp, user, commentText] = logMatch;
        // Format timestamp for better readability
        const formattedTimestamp = new Date(timestamp).toLocaleString();
        return (
          <div key={index} className="mb-2 p-2 bg-red-50 rounded border-l-4 border-red-500">
            <div className="text-xs text-red-600 font-medium">
              <span className="inline-block w-8 text-center bg-red-100 rounded px-1 mr-2">LOG</span>
              <span>{formattedTimestamp}</span> - <span className="font-medium">{user}</span>
            </div>
            <div className="text-sm mt-1 text-red-800 font-medium">{commentText}</div>
          </div>
        );
      }
    }

    // Check if it's a user comment with [COMMENT] prefix
    if (line.startsWith('[COMMENT]')) {
      const commentMatch = line.match(/^\[COMMENT\]\s*\[(.*?)\]\s*(.*?):\s*(.*)$/);
      if (commentMatch) {
        const [, timestamp, user, commentText] = commentMatch;
        // Format timestamp for better readability
        const formattedTimestamp = new Date(timestamp).toLocaleString();
        return (
          <div key={index} className="mb-2 p-2 bg-blue-50 rounded border-l-4 border-blue-500">
            <div className="text-xs text-blue-600 font-medium">
              <span className="inline-block w-12 text-center bg-blue-100 rounded px-1 mr-2">COMMENT</span>
              <span>{formattedTimestamp}</span> - <span className="font-medium">{user}</span>
            </div>
            <div className="text-sm mt-1 text-blue-800 font-medium">{commentText}</div>
          </div>
        );
      }
    }

    // Check if it's a user comment (legacy format without prefix)
    const userMatch = line.match(/^\[(.*?)\]\s*(.*?):\s*(.*)$/);
    if (userMatch) {
      const [, timestamp, user, commentText] = userMatch;
      return (
        <div key={index} className="mb-2 p-2 bg-gray-50 rounded">
          <div className="text-xs text-gray-500">
            <span>{timestamp}</span> - <span className="font-medium">{user}</span>
          </div>
          <div className="text-sm mt-1 text-gray-700">{commentText}</div>
        </div>
      );
    }

    // Fallback for lines that don't match expected format
    return (
      <div key={index} className="mb-2 p-2 bg-gray-50 rounded">
        <div className="text-sm text-gray-700">{line}</div>
      </div>
    );
  });
};

const ActionAttachments = ({ attachments }) => {
  const parseAttachments = (attachmentsString) => {
    if (!attachmentsString) return [];
   
    // Handle both formats: simple file paths and formatted entries
    const entries = attachmentsString.split('\n').filter(entry => entry.trim());
   
    return entries.map(entry => {
      // Try to match the formatted entry pattern
      const match = entry.match(/^\[(.*?)\]\s*(.*?):\s*(.*?)\s*\|\s*(.*?)$/);
      if (match) {
        return {
          timestamp: match[1],
          uploadedBy: match[2],
          filename: match[3],
          filePath: match[4],
          originalEntry: entry,
          isFormatted: true
        };
      } else {
        // Handle simple file path format
        const fileName = entry.split('/').pop() || entry;
        return {
          timestamp: new Date().toLocaleString(),
          uploadedBy: 'System',
          filename: fileName,
          filePath: entry,
          originalEntry: entry,
          isFormatted: false
        };
      }
    }).filter(Boolean);
  };

  const attachmentList = parseAttachments(attachments);

  return (
    <div>
      {attachmentList.length === 0 ? (
        <div className="text-center py-4 text-gray-500">
          <p>No attachments uploaded yet</p>
        </div>
      ) : (
        attachmentList.map((attachment, index) => (
          <div key={index} className={`py-4 ${index < attachmentList.length - 1 ? 'border-b border-gray-200' : ''}`}>
            <div className="flex justify-between items-center mb-2">
              <p className="font-semibold text-gray-800 text-sm">Uploaded By: {attachment.uploadedBy}</p>
              <p className="text-xs text-gray-500">{attachment.timestamp}</p>
            </div>
            <div className="text-sm text-gray-600 bg-gray-50 p-3 rounded-md border border-gray-200">
              <div className="flex items-center gap-2">
                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="text-blue-600">
                  <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"></path>
                  <polyline points="14 2 14 8 20 8"></polyline>
                  <line x1="16" y1="13" x2="8" y2="13"></line>
                  <line x1="16" y1="17" x2="8" y2="17"></line>
                  <polyline points="10 9 9 9 8 9"></polyline>
                </svg>
                <a
                  href={attachment.filePath}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-blue-600 hover:underline"
                >
                  {attachment.filename}
                </a>
              </div>
            </div>
          </div>
        ))
      )}
    </div>
  );
};

const scrollbarStyles = `
  .custom-scrollbar::-webkit-scrollbar { width: 12px; }
  .custom-scrollbar::-webkit-scrollbar-track { background: #f1f1f1; }
  .custom-scrollbar::-webkit-scrollbar-thumb { background-color: #a8a8a8; border-radius: 10px; border: 3px solid #f1f1f1; }
  .custom-scrollbar::-webkit-scrollbar-thumb:hover { background: #555; }
`;

// Custom Modal Component
const ConfirmationModal = ({ isOpen, onClose, onConfirm, title, message }) => {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-6 w-96 max-w-md">
        <h3 className="text-lg font-semibold mb-4">{title}</h3>
        <p className="text-gray-700 mb-6">{message}</p>
        <div className="flex justify-end gap-3">
          <button
            onClick={onClose}
            className="px-4 py-2 text-gray-600 font-medium rounded hover:bg-gray-100"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            className="px-4 py-2 bg-blue-600 text-white font-medium rounded hover:bg-blue-700"
          >
            Confirm
          </button>
        </div>
      </div>
    </div>
  );
};

const StatusTrackerPage = () => {
  const location = useLocation();
  const navigate = useNavigate();
  const queryParams = new URLSearchParams(location.search);
  const initialPhaseId = queryParams.get('phaseId');
  const initialChecklistId = queryParams.get('checklistId');

  const [phases, setPhases] = useState([]);
  const [phaseId, setPhaseId] = useState(initialPhaseId);
  const [nodes, setNodes] = useState([]);
  const [checklistItems, setChecklistItems] = useState([]);
  const [currentLevelIndex, setCurrentLevelIndex] = useState(0);
  const [comment, setComment] = useState('');
  // const [initiatorInfo, setInitiatorInfo] = useState({});
  const [phaseDetails, setPhaseDetails] = useState(null);
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  // State for confirmation modals
  const [showMoveNextModal, setShowMoveNextModal] = useState(false);
  const [showRejectModal, setShowRejectModal] = useState(false);
  // State for end date editing
  const [isEditingEndDate, setIsEditingEndDate] = useState(false);
  const [showSaveEndDateModal, setShowSaveEndDateModal] = useState(false);
  const [pendingEndDate, setPendingEndDate] = useState('');
  const [showConfirmDateChangeModal, setShowConfirmDateChangeModal] = useState(false);
  const [dateChangeData, setDateChangeData] = useState(null);

  useEffect(() => {
    setLoading(true);
    setError(null);
   
    // Fetch phases list to allow phase selection
    fetch('/api/phases')
      .then(res => {
        if (!res.ok) throw new Error('Failed to fetch phases');
        return res.json();
      })
      .then(data => {
        // Sort phases by ID with improved sorting logic
        const sortedData = data ? [...data].sort((a, b) => {
          const idA = a['Phase id'] || a.id || '';
          const idB = b['Phase id'] || b.id || '';

          // Try numeric sorting first
          const numA = parseInt(idA, 10);
          const numB = parseInt(idB, 10);

          if (!isNaN(numA) && !isNaN(numB)) {
            return numA - numB;
          }

          // If not numeric, try alphanumeric sorting
          return idA.localeCompare(idB, undefined, { numeric: true, sensitivity: 'base' });
        }) : [];

        setPhases(sortedData);
        if (!initialPhaseId && sortedData && sortedData.length > 0) {
          setPhaseId(sortedData[0]['Phase id'] || sortedData[0].id);
        }
      })
      .catch(err => {
        console.error('Failed to fetch phases:', err);
        setError('Failed to load phases. Please check your connection.');
      })
      .finally(() => setLoading(false));
  }, [initialPhaseId]);

  useEffect(() => {
    if (!phaseId) return;
   
    setLoading(true);
   
    Promise.all([
      fetch(`/api/workflow_nodes`).then(res => {
        if (!res.ok) {
          throw new Error(`Failed to fetch workflow nodes: ${res.status} ${res.statusText}`);
        }
        return res.json();
      }),
      fetch(`/api/checklist/${phaseId}`).then(res => {
        if (!res.ok) {
          throw new Error(`Failed to fetch checklist items: ${res.status} ${res.statusText}`);
        }
        return res.json();
      }),
      fetch(`/api/phase/${phaseId}`).then(res => {
        if (!res.ok) {
          throw new Error(`Failed to fetch phase details: ${res.status} ${res.statusText}`);
        }
        return res.json();
      })
    ])
    .then(([nodesData, checklistData, phaseData]) => {
      console.log('Workflow nodes data:', nodesData);
      console.log('Checklist data:', checklistData);
      console.log('Phase data:', phaseData);
     
      setNodes(nodesData || []);
      setChecklistItems(checklistData || []);
      setPhaseDetails(phaseData);

      // Check if a specific checklist item was requested
      if (checklistData && checklistData.length > 0) {
        if (initialChecklistId) {
          // Find the checklist item by ID or index
          const targetIndex = checklistData.findIndex(item =>
            item.ch_id && item.ch_id.toString() === initialChecklistId.toString()
          );

          if (targetIndex !== -1) {
            setCurrentLevelIndex(targetIndex);
            // Filter out log lines when setting comment for editing
            const fullComment = checklistData[targetIndex].comments || '';
            const lines = fullComment.split('\n');
            const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
            const userCommentText = userComments.join('\n');
            setComment(userCommentText);
          } else {
            // If not found by ID, try using as index
            const index = parseInt(initialChecklistId);
            if (!isNaN(index) && index >= 0 && index < checklistData.length) {
              setCurrentLevelIndex(index);
              // Filter out log lines when setting comment for editing
              const fullComment = checklistData[index].comments || '';
              const lines = fullComment.split('\n');
              const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
              const userCommentText = userComments.join('\n');
              setComment(userCommentText);
            } else {
              // Fallback to first item if not found
              setCurrentLevelIndex(0);
              // Filter out log lines when setting comment for editing
              const fullComment = checklistData[0].comments || '';
              const lines = fullComment.split('\n');
              const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
              const userCommentText = userComments.join('\n');
              setComment(userCommentText);
            }
          }
        } else {
          // Auto-navigate to first pending task if no specific item requested
          const pendingIndex = checklistData.findIndex(item =>
            !item.status || item.status === 'pending' || item.status === 'in-progress'
          );

          if (pendingIndex !== -1) {
            setCurrentLevelIndex(pendingIndex);
            // Filter out log lines when setting comment for editing
            const fullComment = checklistData[pendingIndex].comments || '';
            const lines = fullComment.split('\n');
            const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
            const userCommentText = userComments.join('\n');
            setComment(userCommentText);
          } else {
            // If all completed, go to last item
            setCurrentLevelIndex(checklistData.length - 1);
            // Filter out log lines when setting comment for editing
            const fullComment = checklistData[checklistData.length - 1].comments || '';
            const lines = fullComment.split('\n');
            const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
            const userCommentText = userComments.join('\n');
            setComment(userCommentText);
          }
        }
      } else {
        setCurrentLevelIndex(0);
        setComment('');
      }
     
      // Update history from comments
      // updateHistoryFromComments(checklistData || []);
    })
    .catch(err => {
      console.error('Error fetching data:', err);
      setError(err.message || 'Failed to load workflow data');
    })
    .finally(() => setLoading(false));
  }, [phaseId]);

  const handlePhaseChange = (event) => {
    setPhaseId(event.target.value);
    setCurrentLevelIndex(0);
  };

  const handleLevelClick = async (index) => {
    // Refetch checklist data to ensure we have the latest dates and information
    try {
      const updatedChecklist = await fetch(`/api/checklist/${phaseId}`).then(res => {
        if (!res.ok) throw new Error('Failed to fetch updated checklist data');
        return res.json();
      });

      setChecklistItems(updatedChecklist);

      // Update the current level index
      setCurrentLevelIndex(index);

      // Filter out log lines when setting comment for editing
      const fullComment = updatedChecklist[index]?.comments || '';
      const lines = fullComment.split('\n');
      const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
      const userCommentText = userComments.join('\n');
      setComment(userCommentText);
    } catch (error) {
      console.error('Error fetching updated checklist data:', error);
      // Fallback to local data if API fails
      setCurrentLevelIndex(index);
      const fullComment = checklistItems[index]?.comments || '';
      const lines = fullComment.split('\n');
      const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
      const userCommentText = userComments.join('\n');
      setComment(userCommentText);
    }
  };

  const handleMoveToNextLevel = async () => {
    setShowMoveNextModal(true);
  };

  const confirmMoveToNextLevel = async () => {
    setShowMoveNextModal(false);
   
    try {
      const currentItem = checklistItems[currentLevelIndex];
     
      // Update the current checklist item status to completed
      const response = await fetch('/api/checklist/update-status', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          ch_id: currentItem.ch_id,
          status: 'completed',
          user: 'current_user'
        }),
      });

      if (!response.ok) {
        console.error('Failed to update status:', response.statusText);
        return;
      }

      // Refresh checklist data first
      const updatedChecklist = await fetch(`/api/checklist/${phaseId}`).then(res => res.json());
      setChecklistItems(updatedChecklist);
     
      // Check if this is the final level
      const isFinalLevel = currentLevelIndex === checklistItems.length - 1;
     
      if (isFinalLevel) {
        // If this is the final level, update the phase status to completed
        try {
          const phaseResponse = await fetch('/api/phases/update-status', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              phase_id: phaseId,
              status: 'completed'
            }),
          });
         
          if (!phaseResponse.ok) {
            console.error('Failed to update phase status:', phaseResponse.statusText);
          }
        } catch (phaseError) {
          console.error('Error updating phase status:', phaseError);
        }
       
        // Navigate back to project milestones
        navigate('/milestones');
      } else {
        // Move to next level if not the final level
        setCurrentLevelIndex(currentLevelIndex + 1);
        // Filter out log lines when setting comment for editing
        const fullComment = updatedChecklist[currentLevelIndex + 1]?.comments || '';
        const lines = fullComment.split('\n');
        const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
        const userCommentText = userComments.join('\n');
        setComment(userCommentText);
      }
    } catch (error) {
      console.error('Error updating status:', error);
    }
  };

  const handleRFI = async () => {
    setShowRejectModal(true);
  };

  const confirmRFI = async () => {
    setShowRejectModal(false);
   
    try {
      const currentItem = checklistItems[currentLevelIndex];
      const response = await fetch('/api/checklist/update-status', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          ch_id: currentItem.ch_id,
          status: 'pending',
          user: 'current_user'
        }),
      });

      if (response.ok) {
        // Only move to previous level if not on the first level
        if (currentLevelIndex > 0) {
          setCurrentLevelIndex(currentLevelIndex - 1);
          // Filter out log lines when setting comment for editing
          const fullComment = checklistItems[currentLevelIndex - 1]?.comments || '';
          const lines = fullComment.split('\n');
          const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
          const userCommentText = userComments.join('\n');
          setComment(userCommentText);
        }
       
        // Refresh checklist data
        const updatedChecklist = await fetch(`/api/checklist/${phaseId}`).then(res => res.json());
        setChecklistItems(updatedChecklist);
      }
    } catch (error) {
      console.error('Error updating status:', error);
    }
  };

  const handleCommentChange = (newComment) => {
    // Separate user comments from logs when editing
    const lines = newComment.split('\n');
    const userComments = lines.filter(line => !line.trim().startsWith('[LOG]'));
    const userCommentText = userComments.join('\n');

    setComment(userCommentText);
    setChecklistItems(items => {
      const updated = [...items];
      if (updated[currentLevelIndex]) {
        updated[currentLevelIndex] = { ...updated[currentLevelIndex], comments: userCommentText };
      }
      return updated;
    });
  };

  const handleSaveComment = async (newComment) => {
    if (!newComment.trim() || !currentChecklistItem.ch_id) return;
   
    try {
      const payload = {
        ch_id: currentChecklistItem.ch_id,
        comments: newComment,
        attachments_path: currentChecklistItem.attachments || ''
      };
     
      const response = await fetch('/api/checklist/update', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });
     
      if (!response.ok) throw new Error('Failed to save comment');
     
      // Refresh checklist data
      const updatedChecklist = await fetch(`/api/checklist/${phaseId}`).then(res => res.json());
      setChecklistItems(updatedChecklist);
     
      // Update history with new comments
      // updateHistoryFromComments(updatedChecklist);
     
    } catch (error) {
      console.error('Error saving comment:', error);
      alert('Error saving comment. Please try again.');
    }
  };

  const handleAttachmentUpload = async (event) => {
    const file = event.target.files[0];
    if (!file || !currentChecklistItem.ch_id) return;

    const formData = new FormData();
    formData.append('file', file);

    try {
      const uploadResponse = await fetch('/api/checklist/upload-attachment', {
        method: 'POST',
        body: formData
      });

      if (!uploadResponse.ok) throw new Error('Failed to upload attachment');

      const uploadData = await uploadResponse.json();
     
      // Format attachment entry with timestamp and username
      const timestamp = new Date().toLocaleString('en-US', {
        day: '2-digit',
        month: 'short',
        year: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: true
      });
     
      const newAttachment = `[${timestamp}] User: ${file.name} | ${uploadData.file_path}`;
     
      // Get existing attachments
      const existingAttachments = currentChecklistItem.attachments || '';
      const updatedAttachments = existingAttachments
        ? `${existingAttachments}\n${newAttachment}`
        : newAttachment;

      // Update checklist with new attachment
      const payload = {
        ch_id: currentChecklistItem.ch_id,
        comments: currentChecklistItem.comments || '',
        attachments_path: updatedAttachments
      };

      const response = await fetch('/api/checklist/update', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) throw new Error('Failed to save attachment');

      // Refresh checklist data
      const updatedChecklist = await fetch(`/api/checklist/${phaseId}`).then(res => res.json());
      setChecklistItems(updatedChecklist);
     
    } catch (error) {
      console.error('Error uploading attachment:', error);
      alert('Error uploading attachment. Please try again.');
    }
  };

   const handleDetailsChange = async (key, value) => {
    if (key === 'End Date' && currentChecklistItem.ch_id) {
      // Show confirmation modal before updating
      setDateChangeData({ key, value });
      setShowConfirmDateChangeModal(true);
    } else {
      setPhaseDetails(prevDetails => ({
        ...prevDetails,
        [key.toLowerCase().replace(' ', '')]: value
      }));
    }
  };

  const confirmDateChange = async () => {
    if (!dateChangeData) return;

    const { key, value } = dateChangeData;
    setShowConfirmDateChangeModal(false);
    setDateChangeData(null);

    try {
      // Update the checklist item's end_date in the database
      const payload = {
        ch_id: currentChecklistItem.ch_id,
        end_date: value,
        attachments_path: currentChecklistItem.attachments || ''
      };

      // Only include start_date if it exists and is different from the new end_date
      if (currentChecklistItem.start_date && currentChecklistItem.start_date !== value) {
        payload.start_date = currentChecklistItem.start_date;
      }

      const response = await fetch('/api/checklist/update', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
      });

      if (!response.ok) throw new Error('Failed to update end date');

      // Update local state
      setChecklistItems(items => {
        const updated = [...items];
        if (updated[currentLevelIndex]) {
          updated[currentLevelIndex] = { ...updated[currentLevelIndex], end_date: value };
        }
        return updated;
      });

      // Refresh the page after successful update
      window.location.reload();
    } catch (error) {
      console.error('Error updating end date:', error);
      alert('Error updating end date. Please try again.');
    }
  };

  const cancelDateChange = () => {
    setShowConfirmDateChangeModal(false);
    setDateChangeData(null);
  };

  const currentChecklistItem = checklistItems[currentLevelIndex] || {};
  const currentNode = nodes[currentLevelIndex] || {};

  // Calculate phase dates from all checklist items using checklist API data
  const phaseDates = calculatePhaseDates(checklistItems);

  const details = phaseDetails ? {
    'Start Date': currentChecklistItem.start_date ? new Date(currentChecklistItem.start_date).toISOString().split('T')[0] : '—',
    'End Date': currentChecklistItem.end_date ? new Date(currentChecklistItem.end_date).toISOString().split('T')[0] : '—',
    'Phase ID': phaseDetails['Phase id'] || '—',
  } : {};
  if (loading) {
    return (
      <div className="bg-gray-50 font-sans text-gray-800 h-screen flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
          <p className="text-lg font-medium text-gray-600">Loading workflow status...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-gray-50 font-sans text-gray-800 h-screen flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
            <strong>Error:</strong> {error}
          </div>
          <button
            onClick={() => window.location.reload()}
            className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!phaseId) {
    return (
      <div className="bg-gray-50 font-sans text-gray-800 h-screen flex items-center justify-center">
        <div className="text-center">
          <p className="text-lg font-medium text-gray-600">No phase selected</p>
          <p className="text-sm text-gray-500 mt-2">Please select a phase to view status</p>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-gray-50 font-sans text-gray-800 h-screen overflow-y-auto custom-scrollbar">
      {/* Confirmation Modals */}
      <ConfirmationModal
        isOpen={showMoveNextModal}
        onClose={() => setShowMoveNextModal(false)}
        onConfirm={confirmMoveToNextLevel}
        title="Confirm Action"
        message="Are you sure you want to move to the next level?"
      />
     
      <ConfirmationModal
        isOpen={showRejectModal}
        onClose={() => setShowRejectModal(false)}
        onConfirm={confirmRFI}
        title="Confirm Action"
        message="Are you sure you want to reject and move to the previous level?"
      />

      <ConfirmationModal
        isOpen={showConfirmDateChangeModal}
        onClose={cancelDateChange}
        onConfirm={confirmDateChange}
        title="Confirm Date Change"
        message={`Are you sure you want to change the end date to ${dateChangeData ? new Date(dateChangeData.value).toLocaleDateString() : ''}?`}
      />
     
      <main className="max-w-7xl mx-auto p-4 md:p-6 pt-20">
        {/* Enhanced Phase Selection */}
        <div className="mb-8 bg-gradient-to-r from-blue-50 to-indigo-50 p-6 rounded-xl border border-blue-200 shadow-lg">
          <div className="flex items-center gap-3 mb-4">
            <div className="w-10 h-10 bg-blue-600 rounded-full flex items-center justify-center">
              <svg className="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v10a2 2 0 002 2h8a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
              </svg>
            </div>
            <div>
              <h1 className="text-2xl font-bold text-gray-900">Status Tracker</h1>
              <p className="text-gray-600">Monitor and manage your project activities</p>
            </div>
          </div>

          <div className="bg-white p-4 rounded-lg border border-gray-200">
            <label htmlFor="phase-select" className="block text-lg font-semibold text-gray-800 mb-3 flex items-center gap-2">
              <svg className="w-5 h-5 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
              </svg>
              Select Phase
            </label>
            <select
              id="phase-select"
              value={phaseId || ''}
              onChange={handlePhaseChange}
              className="block w-full rounded-lg border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 text-lg py-3 px-4 bg-gray-50 hover:bg-white transition-colors"
            >
              {phases.map(phase => (
                <option key={phase['Phase id'] || phase.id} value={phase['Phase id'] || phase.id}>
                  {phase.name || `Phase ${phase['Phase id'] || phase.id}`}
                </option>
              ))}
            </select>
          </div>
        </div>

        {/* Enhanced Current Activity Display */}
        <div className="bg-gradient-to-r from-green-50 to-emerald-50 p-6 rounded-xl border border-green-200 shadow-lg mb-8">
          <div className="flex items-center gap-4 mb-4">
            <div className="w-12 h-12 bg-green-600 rounded-full flex items-center justify-center">
              <svg className="w-7 h-7 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
              </svg>
            </div>
            <div>
              <h2 className="text-xl font-bold text-gray-900">Current Activity</h2>
              <p className="text-lg text-green-700 font-semibold">{currentChecklistItem.item_text || currentChecklistItem.id || currentNode.label || 'No activity selected'}</p>
            </div>
          </div>
          <div className="bg-white p-4 rounded-lg border border-gray-200">
            <StatusStepper levels={checklistItems} currentLevelIndex={currentLevelIndex} onLevelClick={handleLevelClick} />
          </div>
        </div>

        {/* Enhanced Details Section */}
        <div className="bg-white p-8 rounded-xl shadow-lg border border-gray-200 mb-8">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 bg-purple-600 rounded-full flex items-center justify-center">
              <svg className="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-gray-900">Activity Details</h2>
          </div>
          <div className="border-t border-gray-200 pt-6">
            <DetailsGrid
              details={details}
              onDetailsChange={handleDetailsChange}
              currentChecklistItem={currentChecklistItem}
              isEditingEndDate={isEditingEndDate}
              onEditEndDate={() => setIsEditingEndDate(true)}
              onSaveEndDate={() => setIsEditingEndDate(false)}
              onCancelEdit={() => setIsEditingEndDate(false)}
              checklistItems={checklistItems}
              currentLevelIndex={currentLevelIndex}
            />
          </div>
        </div>
       
        {/* Enhanced Activity Section */}
        <div className="bg-white p-8 rounded-xl shadow-lg border border-gray-200 mb-8">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 bg-orange-600 rounded-full flex items-center justify-center">
              <svg className="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-gray-900">Activity Details</h2>
          </div>
          <div className="border-t border-gray-200 pt-6">
            {checklistItems.length > 0 ? (
              <ChecklistItem
                item={currentChecklistItem}
                onUpdate={(formData) => {
                  fetch('/api/checklist/update', {
                    method: 'POST',
                    body: formData,
                  })
                    .then((res) => {
                      if (!res.ok) throw new Error('Failed to update checklist item');
                      return res.json();
                    })
                    .then(() => {
                      fetch(`/api/checklist/${phaseId}`)
                        .then(res => res.json())
                        .then(data => setChecklistItems(data))
                        .catch(err => console.error('Failed to refresh checklist data:', err));
                    })
                    .catch((err) => {
                      console.error(err);
                      alert('Error updating checklist item');
                    });
                }}
              />
            ) : (
              <div className="text-center py-12 text-gray-500">
                <svg className="w-16 h-16 mx-auto mb-4 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                </svg>
                <p className="text-lg font-medium">No checklist items found for this phase</p>
                <p className="text-sm mt-2">Please select a different phase or contact your administrator</p>
              </div>
            )}
          </div>
        </div>

        {/* Enhanced Attachments & Actions Section */}
        <div className="bg-white p-8 rounded-xl shadow-lg border border-gray-200">
          <div className="flex items-center gap-3 mb-6">
            <div className="w-10 h-10 bg-indigo-600 rounded-full flex items-center justify-center">
              <svg className="w-6 h-6 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-gray-900">Attachments & Actions</h2>
          </div>
          <div className="border-t border-gray-200 pt-6">
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
              <div className="space-y-4">
                <div className="flex items-center gap-2">
                  <svg className="w-5 h-5 text-indigo-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15.172 7l-6.586 6.586a2 2 0 102.828 2.828l6.414-6.586a4 4 0 00-5.656-5.656l-6.415 6.585a6 6 0 108.486 8.486L20.5 13" />
                  </svg>
                  <h3 className="text-lg font-semibold text-gray-800">Attachments</h3>
                </div>
                <ActionAttachments attachments={currentChecklistItem.attachments} />
                <div className="mt-6">
                  <label className="block text-sm font-medium text-gray-700 mb-3 flex items-center gap-2">
                    <svg className="w-4 h-4 text-gray-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                    </svg>
                    Upload New Attachment
                  </label>
                  <input
                    type="file"
                    onChange={handleAttachmentUpload}
                    className="block w-full text-sm text-gray-500 file:mr-4 file:py-3 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-semibold file:bg-indigo-50 file:text-indigo-700 hover:file:bg-indigo-100 file:cursor-pointer transition-colors"
                  />
                </div>
              </div>
              <div className="space-y-4">
                <div className="flex items-center gap-2">
                  <svg className="w-5 h-5 text-indigo-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 10V3L4 14h7v7l9-11h-7z" />
                  </svg>
                  <h3 className="text-lg font-semibold text-gray-800">Actions</h3>
                </div>
                <div className="flex flex-col gap-4">
                  <button
                    onClick={handleRFI}
                    className="flex items-center gap-3 px-6 py-4 bg-gradient-to-r from-red-500 to-red-600 text-white rounded-lg hover:from-red-600 hover:to-red-700 transition-all duration-200 shadow-md hover:shadow-lg transform hover:-translate-y-0.5"
                  >
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-2.5L13.732 4c-.77-.833-1.964-.833-2.732 0L3.732 16.5c-.77.833.192 2.5 1.732 2.5z" />
                    </svg>
                    <span className="font-medium">Request for Information (RFI)</span>
                  </button>
                  <button
                    onClick={handleMoveToNextLevel}
                    className="flex items-center gap-3 px-6 py-4 bg-gradient-to-r from-green-500 to-green-600 text-white rounded-lg hover:from-green-600 hover:to-green-700 transition-all duration-200 shadow-md hover:shadow-lg transform hover:-translate-y-0.5"
                  >
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7l5 5m0 0l-5 5m5-5H6" />
                    </svg>
                    <span className="font-medium">Move to Next Level</span>
                  </button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </main>
      <style>{scrollbarStyles}</style>
    </div>
  );
};

export default StatusTrackerPage;