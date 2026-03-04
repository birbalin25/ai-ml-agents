import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Ticket, AlertCircle, Clock, CheckCircle2,
  AlertTriangle, TrendingUp,
} from 'lucide-react';
import { api } from '../api';
import { StatusBadge } from './common/StatusBadge';
import { PriorityBadge } from './common/PriorityBadge';

export default function Dashboard() {
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const navigate = useNavigate();

  useEffect(() => {
    api.getStats().then(setStats).catch(console.error).finally(() => setLoading(false));
  }, []);

  if (loading) {
    return <div className="loading"><div className="spinner" /> Loading dashboard...</div>;
  }
  if (!stats) {
    return <div className="empty-state"><h3>Failed to load stats</h3></div>;
  }

  const statCards = [
    { label: 'Total Tickets', value: stats.total_tickets, icon: Ticket, color: 'blue' },
    { label: 'Open', value: stats.open_tickets, icon: AlertCircle, color: 'cyan' },
    { label: 'In Progress', value: stats.in_progress_tickets, icon: Clock, color: 'yellow' },
    { label: 'Resolved', value: stats.resolved_tickets, icon: CheckCircle2, color: 'green' },
    { label: 'Critical', value: stats.critical_tickets, icon: AlertTriangle, color: 'red' },
    { label: 'High Priority', value: stats.high_priority_tickets, icon: TrendingUp, color: 'purple' },
  ];

  const maxStatus = Math.max(1, ...Object.values(stats.by_status));
  const maxPriority = Math.max(1, ...Object.values(stats.by_priority));

  return (
    <>
      <div className="stats-grid">
        {statCards.map((s) => (
          <div className="stat-card" key={s.label}>
            <div className={`stat-icon ${s.color}`}><s.icon /></div>
            <div className="stat-label">{s.label}</div>
            <div className="stat-value">{s.value}</div>
          </div>
        ))}
      </div>

      <div className="breakdown-grid">
        <div className="card">
          <div className="card-header">By Status</div>
          <div className="card-body">
            {Object.entries(stats.by_status).map(([k, v]) => (
              <div key={k}>
                <div className="breakdown-item">
                  <span className="breakdown-label">{k.replace('_', ' ')}</span>
                  <span className="breakdown-count">{v}</span>
                </div>
                <div className="breakdown-bar">
                  <div
                    className="breakdown-bar-fill"
                    style={{ width: `${(v / maxStatus) * 100}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
        <div className="card">
          <div className="card-header">By Priority</div>
          <div className="card-body">
            {Object.entries(stats.by_priority).map(([k, v]) => (
              <div key={k}>
                <div className="breakdown-item">
                  <span className="breakdown-label">{k}</span>
                  <span className="breakdown-count">{v}</span>
                </div>
                <div className="breakdown-bar">
                  <div
                    className="breakdown-bar-fill"
                    style={{ width: `${(v / maxPriority) * 100}%` }}
                  />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="card">
        <div className="card-header">Recent Tickets</div>
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>Ticket</th>
                <th>Title</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {stats.recent_tickets.map((t) => (
                <tr key={t.id} onClick={() => navigate(`/tickets/${t.id}`)}>
                  <td><span className="ticket-number">{t.ticket_number}</span></td>
                  <td><span className="ticket-title-cell">{t.title}</span></td>
                  <td><StatusBadge status={t.status} /></td>
                  <td><PriorityBadge priority={t.priority} /></td>
                  <td>{new Date(t.created_at).toLocaleDateString()}</td>
                </tr>
              ))}
              {stats.recent_tickets.length === 0 && (
                <tr><td colSpan={5} style={{ textAlign: 'center', color: '#94a3b8' }}>No tickets yet</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </>
  );
}
