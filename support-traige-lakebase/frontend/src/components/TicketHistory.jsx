import { useEffect, useState } from 'react';
import { History } from 'lucide-react';
import { api } from '../api';

function timeAgo(dateStr) {
  const d = new Date(dateStr);
  const now = new Date();
  const diff = Math.floor((now - d) / 1000);
  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return d.toLocaleString();
}

export default function TicketHistory({ ticketId }) {
  const [entries, setEntries] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.getHistory(ticketId)
      .then(setEntries)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [ticketId]);

  if (loading) return <div className="loading"><div className="spinner" /> Loading history...</div>;

  if (entries.length === 0) {
    return <p style={{ color: '#94a3b8', fontSize: '0.875rem' }}>No changes recorded yet.</p>;
  }

  return (
    <div>
      <h3 style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16 }}>
        <History size={20} /> Change History
      </h3>
      <ul className="history-list">
        {entries.map((e) => (
          <li className="history-item" key={e.id}>
            <div className="history-dot" />
            <div>
              <div className="history-text">
                <strong>{e.changed_by_name || e.changed_by_email}</strong> changed{' '}
                <strong>{e.field_name}</strong> from{' '}
                <em>{e.old_value || '(empty)'}</em> to <em>{e.new_value || '(empty)'}</em>
              </div>
              <div className="history-time">{timeAgo(e.created_at)}</div>
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}
