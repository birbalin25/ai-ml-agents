import { Badge } from './Badge';

const LABELS = {
  critical: 'Critical',
  high: 'High',
  medium: 'Medium',
  low: 'Low',
};

export function PriorityBadge({ priority }) {
  return (
    <Badge className={`badge-${priority}`}>
      {LABELS[priority] || priority}
    </Badge>
  );
}
