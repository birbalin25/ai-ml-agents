import { useEffect, useState } from 'react';
import { MessageSquare, Send } from 'lucide-react';
import { api } from '../api';

function timeAgo(dateStr) {
  const d = new Date(dateStr);
  const now = new Date();
  const diff = Math.floor((now - d) / 1000);
  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return d.toLocaleDateString();
}

function getInitials(name, email) {
  if (name) return name.split(' ').map((w) => w[0]).join('').slice(0, 2).toUpperCase();
  return (email || '?')[0].toUpperCase();
}

export default function CommentSection({ ticketId }) {
  const [comments, setComments] = useState([]);
  const [loading, setLoading] = useState(true);
  const [content, setContent] = useState('');
  const [isInternal, setIsInternal] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  const loadComments = () => {
    api.listComments(ticketId)
      .then(setComments)
      .catch(console.error)
      .finally(() => setLoading(false));
  };

  useEffect(() => { loadComments(); }, [ticketId]);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!content.trim()) return;
    setSubmitting(true);
    try {
      await api.createComment(ticketId, { content, is_internal: isInternal });
      setContent('');
      setIsInternal(false);
      loadComments();
    } catch (err) {
      console.error(err);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="comments-section">
      <h3 style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16 }}>
        <MessageSquare size={20} /> Comments ({comments.length})
      </h3>

      {loading ? (
        <div className="loading"><div className="spinner" /> Loading comments...</div>
      ) : comments.length === 0 ? (
        <p style={{ color: '#94a3b8', fontSize: '0.875rem', marginBottom: 16 }}>No comments yet.</p>
      ) : (
        comments.map((c) => (
          <div className="comment-item" key={c.id}>
            <div className={`comment-avatar ${c.is_internal ? 'internal' : ''}`}>
              {getInitials(c.author_name, c.author_email)}
            </div>
            <div className="comment-body">
              <div className="comment-header">
                <span className="comment-author">{c.author_name || c.author_email}</span>
                <span className="comment-time">{timeAgo(c.created_at)}</span>
                {c.is_internal && <span className="comment-internal-badge">Internal</span>}
              </div>
              <div className="comment-content">{c.content}</div>
            </div>
          </div>
        ))
      )}

      <form className="comment-form" onSubmit={handleSubmit}>
        <textarea
          className="form-textarea"
          rows={3}
          value={content}
          onChange={(e) => setContent(e.target.value)}
          placeholder="Add a comment..."
        />
        <div className="comment-form-actions">
          <label>
            <input type="checkbox" checked={isInternal} onChange={(e) => setIsInternal(e.target.checked)} />
            Internal note
          </label>
          <div style={{ marginLeft: 'auto' }}>
            <button type="submit" className="btn btn-primary btn-sm" disabled={submitting || !content.trim()}>
              <Send size={14} /> {submitting ? 'Posting...' : 'Post Comment'}
            </button>
          </div>
        </div>
      </form>
    </div>
  );
}
