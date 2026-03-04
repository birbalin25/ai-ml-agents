const API_BASE = '/api';

async function request(path, options = {}) {
  const url = `${API_BASE}${path}`;
  const config = {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  };

  const response = await fetch(url, config);

  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.detail || `Request failed: ${response.status}`);
  }

  return response.json();
}

export const api = {
  // Health
  getHealth: () => request('/health'),

  // Stats
  getStats: () => request('/stats'),

  // Tickets
  listTickets: (params = {}) => {
    const qs = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== null && v !== '') qs.set(k, v);
    });
    return request(`/tickets?${qs}`);
  },

  getTicket: (id) => request(`/tickets/${id}`),

  createTicket: (data) =>
    request('/tickets', { method: 'POST', body: JSON.stringify(data) }),

  updateTicket: (id, data) =>
    request(`/tickets/${id}`, { method: 'PUT', body: JSON.stringify(data) }),

  // Comments
  listComments: (ticketId) => request(`/tickets/${ticketId}/comments`),

  createComment: (ticketId, data) =>
    request(`/tickets/${ticketId}/comments`, {
      method: 'POST',
      body: JSON.stringify(data),
    }),

  // History
  getHistory: (ticketId) => request(`/tickets/${ticketId}/history`),
};
