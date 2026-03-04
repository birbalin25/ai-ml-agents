import { useEffect, useState, useCallback } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { Search, ArrowUpDown, PlusCircle } from 'lucide-react';
import { api } from '../api';
import { StatusBadge } from './common/StatusBadge';
import { PriorityBadge } from './common/PriorityBadge';
import { Pagination } from './common/Pagination';

export default function TicketList() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  const page = parseInt(searchParams.get('page') || '1');
  const status = searchParams.get('status') || '';
  const priority = searchParams.get('priority') || '';
  const category = searchParams.get('category') || '';
  const search = searchParams.get('search') || '';
  const sortBy = searchParams.get('sort_by') || 'created_at';
  const sortOrder = searchParams.get('sort_order') || 'desc';

  const fetchTickets = useCallback(() => {
    setLoading(true);
    api.listTickets({ page, per_page: 20, status, priority, category, search, sort_by: sortBy, sort_order: sortOrder })
      .then(setData)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, [page, status, priority, category, search, sortBy, sortOrder]);

  useEffect(() => { fetchTickets(); }, [fetchTickets]);

  const setFilter = (key, value) => {
    const params = new URLSearchParams(searchParams);
    if (value) params.set(key, value);
    else params.delete(key);
    params.set('page', '1');
    setSearchParams(params);
  };

  const toggleSort = (field) => {
    const params = new URLSearchParams(searchParams);
    if (sortBy === field) {
      params.set('sort_order', sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      params.set('sort_by', field);
      params.set('sort_order', 'desc');
    }
    setSearchParams(params);
  };

  const SortIcon = ({ field }) =>
    sortBy === field ? (
      <span className="sort-icon">{sortOrder === 'asc' ? '\u25B2' : '\u25BC'}</span>
    ) : null;

  return (
    <>
      <div className="filters-bar">
        <div style={{ position: 'relative' }}>
          <Search size={16} style={{ position: 'absolute', left: 10, top: 9, color: '#94a3b8' }} />
          <input
            className="form-input search-input"
            style={{ paddingLeft: 32 }}
            placeholder="Search tickets..."
            value={search}
            onChange={(e) => setFilter('search', e.target.value)}
          />
        </div>
        <select className="form-select" value={status} onChange={(e) => setFilter('status', e.target.value)}>
          <option value="">All Status</option>
          <option value="open">Open</option>
          <option value="in_progress">In Progress</option>
          <option value="pending">Pending</option>
          <option value="resolved">Resolved</option>
          <option value="closed">Closed</option>
        </select>
        <select className="form-select" value={priority} onChange={(e) => setFilter('priority', e.target.value)}>
          <option value="">All Priority</option>
          <option value="critical">Critical</option>
          <option value="high">High</option>
          <option value="medium">Medium</option>
          <option value="low">Low</option>
        </select>
        <select className="form-select" value={category} onChange={(e) => setFilter('category', e.target.value)}>
          <option value="">All Category</option>
          <option value="bug">Bug</option>
          <option value="feature_request">Feature Request</option>
          <option value="question">Question</option>
          <option value="incident">Incident</option>
          <option value="task">Task</option>
        </select>
        <div style={{ marginLeft: 'auto' }}>
          <button className="btn btn-primary" onClick={() => navigate('/tickets/new')}>
            <PlusCircle size={16} /> New Ticket
          </button>
        </div>
      </div>

      <div className="card">
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th onClick={() => toggleSort('ticket_number')} className={sortBy === 'ticket_number' ? 'sorted' : ''}>
                  # <SortIcon field="ticket_number" />
                </th>
                <th onClick={() => toggleSort('title')} className={sortBy === 'title' ? 'sorted' : ''}>
                  Title <SortIcon field="title" />
                </th>
                <th onClick={() => toggleSort('status')} className={sortBy === 'status' ? 'sorted' : ''}>
                  Status <SortIcon field="status" />
                </th>
                <th onClick={() => toggleSort('priority')} className={sortBy === 'priority' ? 'sorted' : ''}>
                  Priority <SortIcon field="priority" />
                </th>
                <th>Category</th>
                <th>Assignee</th>
                <th onClick={() => toggleSort('created_at')} className={sortBy === 'created_at' ? 'sorted' : ''}>
                  Created <SortIcon field="created_at" />
                </th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={7}><div className="loading"><div className="spinner" /> Loading...</div></td></tr>
              ) : data?.tickets.length === 0 ? (
                <tr><td colSpan={7} className="empty-state">
                  <h3>No tickets found</h3>
                  <p>Try adjusting your filters or create a new ticket.</p>
                  <button className="btn btn-primary" onClick={() => navigate('/tickets/new')}>
                    <PlusCircle size={16} /> Create Ticket
                  </button>
                </td></tr>
              ) : (
                data?.tickets.map((t) => (
                  <tr key={t.id} onClick={() => navigate(`/tickets/${t.id}`)}>
                    <td><span className="ticket-number">{t.ticket_number}</span></td>
                    <td><span className="ticket-title-cell">{t.title}</span></td>
                    <td><StatusBadge status={t.status} /></td>
                    <td><PriorityBadge priority={t.priority} /></td>
                    <td><span className={`badge badge-${t.category}`}>{t.category.replace('_', ' ')}</span></td>
                    <td>{t.assignee_name || t.assignee_email || '-'}</td>
                    <td>{new Date(t.created_at).toLocaleDateString()}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        {data && data.total > 0 && (
          <Pagination
            page={data.page}
            totalPages={data.total_pages}
            total={data.total}
            perPage={data.per_page}
            onPageChange={(p) => setFilter('page', String(p))}
          />
        )}
      </div>
    </>
  );
}
