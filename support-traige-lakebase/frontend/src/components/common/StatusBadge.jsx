import { Badge } from './Badge';

const LABELS = {
  open: 'Open',
  in_progress: 'In Progress',
  pending: 'Pending',
  resolved: 'Resolved',
  closed: 'Closed',
};

export function StatusBadge({ status }) {
  return (
    <Badge className={`badge-${status}`}>
      {LABELS[status] || status}
    </Badge>
  );
}
