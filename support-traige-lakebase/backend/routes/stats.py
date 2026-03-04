import json

from fastapi import APIRouter

from backend import database as db
from backend.models import StatsResponse, TicketResponse

router = APIRouter()


def _row_to_ticket(row) -> TicketResponse:
    return TicketResponse(
        id=row["id"],
        ticket_number=row["ticket_number"],
        title=row["title"],
        description=row["description"] or "",
        status=row["status"],
        priority=row["priority"],
        category=row["category"],
        severity=row["severity"],
        reporter_email=row["reporter_email"],
        reporter_name=row["reporter_name"] or "",
        assignee_email=row["assignee_email"],
        assignee_name=row["assignee_name"],
        environment=row["environment"] or "",
        component=row["component"] or "",
        tags=json.loads(row["tags"]) if isinstance(row["tags"], str) else (row["tags"] or []),
        due_date=row["due_date"],
        resolved_at=row["resolved_at"],
        closed_at=row["closed_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.get("/stats", response_model=StatsResponse)
async def get_stats():
    # Counts by status
    status_rows = await db.fetch(
        "SELECT status, COUNT(*) AS cnt FROM tickets GROUP BY status"
    )
    by_status = {r["status"]: r["cnt"] for r in status_rows}

    # Counts by priority
    priority_rows = await db.fetch(
        "SELECT priority, COUNT(*) AS cnt FROM tickets GROUP BY priority"
    )
    by_priority = {r["priority"]: r["cnt"] for r in priority_rows}

    # Counts by category
    category_rows = await db.fetch(
        "SELECT category, COUNT(*) AS cnt FROM tickets GROUP BY category"
    )
    by_category = {r["category"]: r["cnt"] for r in category_rows}

    total = sum(by_status.values())

    # Recent tickets
    recent_rows = await db.fetch(
        "SELECT * FROM tickets ORDER BY created_at DESC LIMIT 5"
    )
    recent = [_row_to_ticket(r) for r in recent_rows]

    return StatsResponse(
        total_tickets=total,
        open_tickets=by_status.get("open", 0),
        in_progress_tickets=by_status.get("in_progress", 0),
        pending_tickets=by_status.get("pending", 0),
        resolved_tickets=by_status.get("resolved", 0),
        closed_tickets=by_status.get("closed", 0),
        critical_tickets=by_priority.get("critical", 0),
        high_priority_tickets=by_priority.get("high", 0),
        by_status=by_status,
        by_priority=by_priority,
        by_category=by_category,
        recent_tickets=recent,
    )
