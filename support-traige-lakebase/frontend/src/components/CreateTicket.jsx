import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Save, X } from 'lucide-react';
import { api } from '../api';

export default function CreateTicket() {
  const navigate = useNavigate();
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState('');
  const [form, setForm] = useState({
    title: '',
    description: '',
    priority: 'medium',
    category: 'question',
    severity: 'sev3',
    assignee_email: '',
    assignee_name: '',
    environment: '',
    component: '',
    tags: '',
    due_date: '',
  });

  const set = (field) => (e) => setForm({ ...form, [field]: e.target.value });

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!form.title.trim()) {
      setError('Title is required');
      return;
    }
    setSaving(true);
    setError('');
    try {
      const payload = {
        ...form,
        tags: form.tags ? form.tags.split(',').map((t) => t.trim()).filter(Boolean) : [],
        due_date: form.due_date || null,
        assignee_email: form.assignee_email || null,
        assignee_name: form.assignee_name || null,
      };
      const ticket = await api.createTicket(payload);
      navigate(`/tickets/${ticket.id}`);
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div style={{ maxWidth: 800, margin: '0 auto' }}>
      <div className="card">
        <div className="card-header">Create New Ticket</div>
        <div className="card-body">
          {error && (
            <div style={{ background: '#fee2e2', color: '#dc2626', padding: '8px 12px', borderRadius: 6, marginBottom: 16, fontSize: '0.875rem' }}>
              {error}
            </div>
          )}
          <form onSubmit={handleSubmit}>
            <div className="form-group">
              <label className="form-label">Title *</label>
              <input className="form-input" value={form.title} onChange={set('title')} placeholder="Brief summary of the issue" />
            </div>

            <div className="form-group">
              <label className="form-label">Description</label>
              <textarea className="form-textarea" rows={5} value={form.description} onChange={set('description')} placeholder="Detailed description of the issue, steps to reproduce, expected behavior..." />
            </div>

            <div className="form-row">
              <div className="form-group">
                <label className="form-label">Priority</label>
                <select className="form-select" value={form.priority} onChange={set('priority')}>
                  <option value="critical">Critical</option>
                  <option value="high">High</option>
                  <option value="medium">Medium</option>
                  <option value="low">Low</option>
                </select>
              </div>
              <div className="form-group">
                <label className="form-label">Category</label>
                <select className="form-select" value={form.category} onChange={set('category')}>
                  <option value="bug">Bug</option>
                  <option value="feature_request">Feature Request</option>
                  <option value="question">Question</option>
                  <option value="incident">Incident</option>
                  <option value="task">Task</option>
                </select>
              </div>
            </div>

            <div className="form-row">
              <div className="form-group">
                <label className="form-label">Severity</label>
                <select className="form-select" value={form.severity} onChange={set('severity')}>
                  <option value="sev1">SEV-1 (Critical Impact)</option>
                  <option value="sev2">SEV-2 (Major Impact)</option>
                  <option value="sev3">SEV-3 (Minor Impact)</option>
                  <option value="sev4">SEV-4 (Minimal Impact)</option>
                </select>
              </div>
              <div className="form-group">
                <label className="form-label">Environment</label>
                <input className="form-input" value={form.environment} onChange={set('environment')} placeholder="e.g., production, staging" />
              </div>
            </div>

            <div className="form-row">
              <div className="form-group">
                <label className="form-label">Assignee Email</label>
                <input className="form-input" value={form.assignee_email} onChange={set('assignee_email')} placeholder="assignee@company.com" />
              </div>
              <div className="form-group">
                <label className="form-label">Assignee Name</label>
                <input className="form-input" value={form.assignee_name} onChange={set('assignee_name')} placeholder="Assignee name" />
              </div>
            </div>

            <div className="form-row">
              <div className="form-group">
                <label className="form-label">Component</label>
                <input className="form-input" value={form.component} onChange={set('component')} placeholder="e.g., API, Frontend, Database" />
              </div>
              <div className="form-group">
                <label className="form-label">Due Date</label>
                <input className="form-input" type="datetime-local" value={form.due_date} onChange={set('due_date')} />
              </div>
            </div>

            <div className="form-group">
              <label className="form-label">Tags</label>
              <input className="form-input" value={form.tags} onChange={set('tags')} placeholder="Comma-separated tags, e.g., urgent, backend, auth" />
            </div>

            <div style={{ display: 'flex', gap: 12, justifyContent: 'flex-end', marginTop: 8 }}>
              <button type="button" className="btn btn-secondary" onClick={() => navigate('/tickets')}>
                <X size={16} /> Cancel
              </button>
              <button type="submit" className="btn btn-primary" disabled={saving}>
                <Save size={16} /> {saving ? 'Creating...' : 'Create Ticket'}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>
  );
}
