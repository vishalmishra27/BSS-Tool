import { useState } from 'react';

const ChecklistItem = ({ item, onUpdate }) => {
  const [comment, setComment] = useState('');
  const [attachments, setAttachments] = useState([]);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [userName, setUserName] = useState('Current User');

  const handleCommentChange = (e) => {
    setComment(e.target.value);
  };

  const handleFileSelect = (e) => {
    setAttachments(e.target.files ? Array.from(e.target.files) : []);
  };

  const handleUserNameChange = (e) => {
    setUserName(e.target.value);
  };

  const handleSubmit = async () => {
    if (!comment.trim() && attachments.length === 0) return;
    if (!item.ch_id) return;

    setIsSubmitting(true);
    try {
      let attachmentPath = '';
      let fileName = '';

      if (attachments.length > 0) {
        const formData = new FormData();
        formData.append('file', attachments[0]);
        formData.append('ch_id', item.ch_id);

        const uploadResponse = await fetch('/api/checklist/upload-attachment', {
          method: 'POST',
          body: formData,
        });

        if (uploadResponse.ok) {
          const uploadResult = await uploadResponse.json();
          attachmentPath = uploadResult.file_path;
          fileName = uploadResult.filename;
        } else {
          const errorData = await uploadResponse.json();
          throw new Error(errorData.error || 'File upload failed');
        }
      }

      let finalComment = comment;
      if (attachmentPath && fileName) {
        if (finalComment.trim()) {
          finalComment += ` [File: ${fileName} (${attachmentPath})]`;
        } else {
          finalComment = `Uploaded file: ${fileName} (${attachmentPath})`;
        }
      }

      const payload = {
        ch_id: item.ch_id,
        comments: finalComment,
        attachments_path: attachmentPath,
        user: userName || 'System User'
      };

      const response = await fetch('/api/checklist/update', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to update checklist');
      }

      const result = await response.json();
      setComment('');
      setAttachments([]);
      if (onUpdate) onUpdate(result);
      window.location.reload();
    } catch (error) {
      console.error('Error adding comment:', error);
      alert('Error: ' + error.message);
    } finally {
      setIsSubmitting(false);
    }
  };

  const formatComments = (comments) => {
    if (!comments) return null;
    const isCompleted = item.status && item.status.toLowerCase() === 'completed';
    const greyClass = isCompleted ? 'opacity-50' : '';

    const commentLines = comments.split('\n').filter(line => line.trim());
    return commentLines.map((line, index) => {
      if (line.startsWith('[LOG]')) {
        const logMatch = line.match(/^\[LOG\]\s*\[(.*?)\]\s*(.*?):\s*(.*)$/);
        if (logMatch) {
          const [, timestamp, user, commentText] = logMatch;
          const formattedTimestamp = new Date(timestamp).toLocaleString();
          return (
            <div key={index} className={`mb-2 p-2 bg-red-50 rounded border-l-4 border-red-500 ${greyClass}`}>
              <div className="text-xs text-red-600 font-medium">
                <span className="inline-block w-8 text-center bg-red-100 rounded px-1 mr-2">LOG</span>
                <span>{formattedTimestamp}</span> - <span className="font-medium">{user}</span>
              </div>
              <div className="text-sm mt-1 text-red-800 font-medium">{commentText}</div>
            </div>
          );
        }
      }

      if (line.startsWith('[COMMENT]')) {
        const commentMatch = line.match(/^\[COMMENT\]\s*\[(.*?)\]\s*(.*?):\s*(.*)$/);
        if (commentMatch) {
          const [, timestamp, user, commentText] = commentMatch;
          const formattedTimestamp = new Date(timestamp).toLocaleString();
          return (
            <div key={index} className={`mb-2 p-2 bg-blue-50 rounded border-l-4 border-blue-500 ${greyClass}`}>
              <div className="text-xs text-blue-600 font-medium">
                <span className="inline-block w-12 text-center bg-blue-100 rounded px-1 mr-2">COMMENT</span>
                <span>{formattedTimestamp}</span> - <span className="font-medium">{user}</span>
              </div>
              <div className="text-sm mt-1 text-blue-800 font-medium">{commentText}</div>
            </div>
          );
        }
      }

      const userMatch = line.match(/^\[(.*?)\]\s*(.*?):\s*(.*)$/);
      if (userMatch) {
        const [, timestamp, user, commentText] = userMatch;
        return (
          <div key={index} className={`mb-2 p-2 bg-gray-50 rounded ${greyClass}`}>
            <div className="text-xs text-gray-500">
              <span>{timestamp}</span> - <span className="font-medium">{user}</span>
            </div>
            <div className="text-sm mt-1 text-gray-700">{commentText}</div>
          </div>
        );
      }

      return (
        <div key={index} className={`mb-2 p-2 bg-gray-50 rounded ${greyClass}`}>
          <div className="text-sm text-gray-700">{line}</div>
        </div>
      );
    });
  };

  const isCompleted = item.status && item.status.toLowerCase() === 'completed';

  return (
    <div className="border p-3 rounded-md mb-3 bg-white shadow-sm">
      <p className="font-medium mb-1">{item.item_text}</p>

      {item.comments && (
        <div className="text-sm text-gray-600 mb-2">
          <strong>Comments:</strong>
          <div className="mt-1">
            {formatComments(item.comments)}
          </div>
        </div>
      )}

      {item.username && <p className="text-xs text-gray-500">By: {item.username}</p>}
      {item.timestamp && <p className="text-xs text-gray-500">At: {new Date(item.timestamp).toLocaleString()}</p>}

      {!isCompleted && (
        <div className="mt-3">
          <label className="block text-sm font-medium text-gray-700 mb-1">Your Name:</label>
          <input
            type="text"
            value={userName}
            onChange={handleUserNameChange}
            placeholder="Enter your name"
            className="w-full border border-gray-300 rounded p-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 mb-2"
          />

          <label className="block text-sm font-medium text-gray-700 mb-1">Add Comment:</label>
          <textarea
            value={comment}
            onChange={handleCommentChange}
            placeholder="Enter your comment here..."
            className="w-full border border-gray-300 rounded p-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
            rows={3}
            disabled={isSubmitting}
          />

          <div className="flex gap-2 mt-2">
            <input
              type="file"
              onChange={handleFileSelect}
              className="text-sm"
              disabled={isSubmitting}
            />
            <button
              onClick={handleSubmit}
              disabled={isSubmitting}
              className={`px-4 py-2 font-semibold rounded text-sm ${
                isSubmitting
                  ? 'bg-gray-300 text-gray-500 cursor-not-allowed'
                  : 'bg-blue-600 text-white hover:bg-blue-700'
              }`}
            >
              {isSubmitting ? 'Adding...' : 'Add Comment'}
            </button>
          </div>
        </div>
      )}

      {isCompleted && (
        <div className="mt-3 p-3 bg-gray-50 rounded border border-gray-200">
          <p className="text-sm text-gray-500 italic">
            Comments cannot be added to completed activities
          </p>
        </div>
      )}
    </div>
  );
};

export default ChecklistItem;
