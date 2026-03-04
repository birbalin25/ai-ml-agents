import { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { ArrowLeft, Edit3, Save, X } from 'lucide-react';
import { api } from '../api';
import { StatusBadge } from './common/StatusBadge';
import { PriorityBadge } from './common/PriorityBadge';
import { Badge } from './common/Badge';
import CommentSection from './CommentSection';
import TicketHistory from './TicketHistory';

const STATUS_TRANSITIONS = {
  open: ['in_progress', 'pending', 'closed'],
  in_progress: ['pending', 'resolved', 'open'],
  pending: ['in_progress', 'open', 'closed'],
  resolved: ['closed', 'open'],
  closed: ['open'],
};

export default function TicketDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [ticket, setTicket] = useState(null);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('comments');
  const [editing, setEditing] = useState(null);
  const [editValue, setEditValue] = useState('');
  const [saving, setSaving] = useState(false);

  const loadTicket = () => {
    api.getTicket(id).then(setTicket).catch(console.error).finally(() => setLoading(false));
  };

  useEffect(() => { loadTicket(); }, [id]);

  const updateField = async (field, value) => {
    setSaving(true);
    try {
      const updated = await api.updateTicket(id, { [field]: value });
      setTicket(updated);
      setEditing(null);
    } catch (err) {
      console.error(err);
    } finally {
      setSaving(false);
    }
  };

  const handleStatusChange = (newStatus) => updateField('status', newStatus);

  if (loading) return <div className="loading"><div className="spinner" /> Loading ticket...</div>;
  if (!ticket) return <div className="empty-state"><h3>Ticket not found</h3></div>;

  const transitions = STATUS_TRANSITIONS[ticket.status] || [];

  const renderEditableField = (field, label, type = 'text', options = null) => {
    const isEditing = editing === field;
    const currentValue = ticket[field] || '';

    if (isEditing) {
      return (
        <div className="inline-edit">
          {options ? (
            <select
              className="form-select"
              value={editValue}
              onChange={(e) => setEditValue(e.target.value)}
            >
              {options.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
          ) : (
            <input
              className="form-input"
              type={type}
              value={editValue}
              onChange={(e) => setEditValue(e.target.value)}
            />
          )}
          <button className="btn btn-sm btn-primary" disabled={saving} onClick={() => updateField(field, editValue)}>
            <Save size={12} />
          </button>
          <button className="btn btn-sm btn-ghost" onClick={() => setEditing(null)}>
            <X size={12} />
          </button>
        </div>
      );
    }

    return (
      <span
        className="meta-value"
        style={{ cursor: 'pointer' }}
        onClick={() => { setEditing(field); setEditValue(currentValue); }}
        title="Click to edit"
      >
        {currentValue || <span style={{ color: '#94a3b8' }}>-</span>}
        <Edit3 size={12} style={{ marginLeft: 4, opacity: 0.4 }} />
      </span>
    );
  };

  return (
    <>
      <button className="btn btn-ghost" onClick={() => navigate('/tickets')} style={{ marginBottom: 16 }}>
        <ArrowLeft size={16} /> Back to Tickets
      </button>

      <div className="ticket-detail-layout">
        <div className="ticket-main">
          <div className="ticket-header">
            <div>
              <div className="ticket-header-meta">
                <span className="ticket-number">{ticket.ticket_number}</span>
                <StatusBadge status={ticket.status} />
                <PriorityBadge priority={ticket.priority} />
                <Badge className={`badge-${ticket.severity}`}>{ticket.severity.toUpperCase()}</Badge>
              </div>
              <h1 style={{ marginTop: 8 }}>{ticket.title}</h1>
            </div>
          </div>

          <div className="status-actions">
            {transitions.map((s) => (
              <button
                key={s}
                className={`btn btn-sm ${s === 'resolved' ? 'btn-primary' : s === 'closed' ? 'btn-danger' : 'btn-secondary'}`}
                onClick={() => handleStatusChange(s)}
                disabled={saving}
              >
                Move to {s.replace('_', ' ')}
              </button>
            ))}
          </div>

          <div className="card" style={{ marginTop: 24 }}>
            <div className="card-header">Description</div>
            <div className="card-body">
              <div className="ticket-description">
                {ticket.description || 'No description provided.'}
              </div>
            </div>
          </div>

          <div className="tabs" style={{ marginTop: 32 }}>
            <button className={`tab ${activeTab === 'comments' ? 'active' : ''}`} onClick={() => setActiveTab('comments')}>
              Comments {ticket.comment_count > 0 && `(${ticket.comment_count})`}
            </button>
            <button className={`tab ${activeTab === 'history' ? 'active' : ''}`} onClick={() => setActiveTab('history')}>
              History
            </button>
          </div>

          {activeTab === 'comments' && <CommentSection ticketId={id} />}
          {activeTab === 'history' && <TicketHistory ticketId={id} />}
        </div>

        <div className="ticket-sidebar">
          <div className="card">
            <div className="card-header">Details</div>
            <div className="card-body">
              <div className="meta-grid">
                <span className="meta-label">Priority</span>
                {renderEditableField('priority', 'Priority', 'text', [
                  { value: 'critical', label: 'Critical' },
                  { value: 'high', label: 'High' },
                  { value: 'medium', label: 'Medium' },
                  { value: 'low', label: 'Low' },
                ])}

                <span className="meta-label">Category</span>
                {renderEditableField('category', 'Category', 'text', [
                  { value: 'bug', label: 'Bug' },
                  { value: 'feature_request', label: 'Feature Request' },
                  { value: 'question', label: 'Question' },
                  { value: 'incident', label: 'Incident' },
                  { value: 'task', label: 'Task' },
                ])}

                <span className="meta-label">Severity</span>
                {renderEditableField('severity', 'Severity', 'text', [
                  { value: 'sev1', label: 'SEV-1' },
                  { value: 'sev2', label: 'SEV-2' },
                  { value: 'sev3', label: 'SEV-3' },
                  { value: 'sev4', label: 'SEV-4' },
                ])}

                <span className="meta-label">Environment</span>
                {renderEditableField('environment', 'Environment')}

                <span className="meta-label">Component</span>
                {renderEditableField('component', 'Component')}
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header">People</div>
            <div className="card-body">
              <div className="meta-grid">
                <span className="meta-label">Reporter</span>
                <span className="meta-value">{ticket.reporter_name || ticket.reporter_email}</span>

                <span className="meta-label">Assignee</span>
                {renderEditableField('assignee_email', 'Assignee')}
              </div>
            </div>
          </div>

          <div className="card">
            <div className="card-header">Dates</div>
            <div className="card-body">
              <div className="meta-grid">
                <span className="meta-label">Created</span>
                <span className="meta-value">{new Date(ticket.created_at).toLocaleString()}</span>

                <span className="meta-label">Updated</span>
                <span className="meta-value">{new Date(ticket.updated_at).toLocaleString()}</span>

                {ticket.resolved_at && (
                  <>
                    <span className="meta-label">Resolved</span>
                    <span className="meta-value">{new Date(ticket.resolved_at).toLocaleString()}</span>
                  </>
                )}

                {ticket.closed_at && (
                  <>
                    <span className="meta-label">Closed</span>
                    <span className="meta-value">{new Date(ticket.closed_at).toLocaleString()}</span>
                  </>
                )}

                <span className="meta-label">Due Date</span>
                <span className="meta-value">
                  {ticket.due_date ? new Date(ticket.due_date).toLocaleString() : '-'}
                </span>
              </div>
            </div>
          </div>

          {ticket.tags && ticket.tags.length > 0 && (
            <div className="card">
              <div className="card-header">Tags</div>
              <div className="card-body" style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {ticket.tags.map((tag) => (
                  <Badge key={tag} className="badge-task">{tag}</Badge>
                ))}
              </div>
            </div>
          )}
        </div>
      </div>
    </>
  );
}
