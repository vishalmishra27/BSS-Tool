/**
 * API client for the SOX Audit Agent server.
 *
 * REST endpoints for chat CRUD + SSE streaming for agent responses.
 */

const API_BASE = '/api';

// -- REST --

export async function fetchChats() {
  const res = await fetch(`${API_BASE}/chats`);
  if (!res.ok) throw new Error('Failed to fetch chats');
  const data = await res.json();
  return data.chats;
}

export async function createChat() {
  const res = await fetch(`${API_BASE}/chats`, { method: 'POST' });
  if (!res.ok) throw new Error('Failed to create chat');
  return res.json();
}

export async function renameChat(chatId, title) {
  const res = await fetch(`${API_BASE}/chats/${chatId}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ title }),
  });
  if (!res.ok) throw new Error('Failed to rename chat');
  return res.json();
}

export async function deleteChat(chatId) {
  const res = await fetch(`${API_BASE}/chats/${chatId}`, { method: 'DELETE' });
  if (!res.ok) throw new Error('Failed to delete chat');
  return res.json();
}

export async function fetchMessages(chatId) {
  const res = await fetch(`${API_BASE}/chats/${chatId}/messages`);
  if (!res.ok) throw new Error('Failed to fetch messages');
  return res.json();
}

export async function fetchStarterActions(chatId) {
  const res = await fetch(`${API_BASE}/chats/${chatId}/starter-actions`);
  if (!res.ok) throw new Error('Failed to fetch starter actions');
  const data = await res.json();
  return Array.isArray(data.actions) ? data.actions : [];
}

// -- File Upload --

export async function uploadFile(file) {
  const formData = new FormData();
  formData.append('file', file);
  const res = await fetch(`${API_BASE}/upload`, { method: 'POST', body: formData });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: 'Upload failed' }));
    throw new Error(err.detail || `Upload failed (${res.status})`);
  }
  return res.json();
}

export function getDownloadUrl(path) {
  return `${API_BASE}/download?path=${encodeURIComponent(path)}`;
}

// -- TOE Approval --

export async function submitToeApproval(chatId, schemas) {
  const res = await fetch(`${API_BASE}/chats/${chatId}/toe-approval`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ schemas }),
  });
  if (!res.ok) throw new Error('Failed to submit TOE approval');
  return res.json();
}

// -- SSE Streaming --

/**
 * Send a message and stream the response via SSE.
 *
 * @param {string} chatId
 * @param {string} content
 * @param {object} callbacks
 * @param {function} callbacks.onToken - Called for each streamed token
 * @param {function} callbacks.onToolStart - Called when a tool starts
 * @param {function} callbacks.onToolEnd - Called when a tool finishes
 * @param {function} callbacks.onError - Called on error
 * @param {function} callbacks.onDone - Called when streaming is complete
 * @returns {AbortController} - Call .abort() to cancel
 */
export function streamMessage(chatId, content, callbacks) {
  const controller = new AbortController();

  (async () => {
    let doneFired = false;

    const fireDone = () => {
      if (doneFired) return;
      doneFired = true;
      callbacks.onDone?.();
    };

    try {
      const res = await fetch(`${API_BASE}/chats/${chatId}/messages`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ content }),
        signal: controller.signal,
      });

      if (!res.ok) {
        callbacks.onError?.(`HTTP ${res.status}: ${res.statusText}`);
        fireDone();
        return;
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        let eventType = null;
        for (const line of lines) {
          if (line.startsWith('event: ')) {
            eventType = line.slice(7).trim();
          } else if (line.startsWith('data: ') && eventType) {
            const dataStr = line.slice(6);
            try {
              const data = JSON.parse(dataStr);
              switch (eventType) {
                case 'token':
                  callbacks.onToken?.(data.token);
                  break;
                case 'tool_start':
                  callbacks.onToolStart?.(data.tool, data.args);
                  break;
                case 'tool_end':
                  callbacks.onToolEnd?.(data.tool, data.success, data.result, data.notes);
                  break;
                case 'error':
                  callbacks.onError?.(data.error);
                  break;
                case 'done':
                  fireDone();
                  return; // Stop processing — stream is done
              }
            } catch (e) {
              // Skip malformed JSON
            }
            eventType = null;
          } else if (line === '') {
            eventType = null;
          }
        }
      }

      // Process any remaining data in buffer
      if (buffer.trim()) {
        const lines = buffer.split('\n');
        let eventType = null;
        for (const line of lines) {
          if (line.startsWith('event: ')) {
            eventType = line.slice(7).trim();
          } else if (line.startsWith('data: ') && eventType) {
            try {
              const data = JSON.parse(line.slice(6));
              if (eventType === 'token') callbacks.onToken?.(data.token);
              else if (eventType === 'tool_end') callbacks.onToolEnd?.(data.tool, data.success, data.result, data.notes);
              else if (eventType === 'done') { fireDone(); return; }
            } catch (e) { /* skip */ }
            eventType = null;
          }
        }
      }

      // Stream ended — ensure done fires
      fireDone();
    } catch (err) {
      if (err.name !== 'AbortError') {
        callbacks.onError?.(err.message);
      }
      fireDone();
    }
  })();

  return controller;
}
