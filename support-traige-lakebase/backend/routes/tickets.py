import json
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from backend import database as db
from backend.models import (
    TicketCreate,
    TicketListResponse,
    TicketResponse,
    TicketUpdate,
)

router = APIRouter()

VALID_STATUSES = {"open", "in_progress", "pending", "resolved", "closed"}
VALID_PRIORITIES = {"critical", "high", "medium", "low"}
VALID_CATEGORIES = {"bug", "feature_request", "question", "incident", "task"}
VALID_SEVERITIES = {"sev1", "sev2", "sev3", "sev4"}
VALID_SORT_FIELDS = {"created_at", "updated_at", "priority", "status", "ticket_number", "title"}


def _get_user(request: Request) -> tuple[str, str]:
    email = request.headers.get("X-Forwarded-Email", "anonymous@example.com")
    name = request.headers.get("X-Forwarded-Preferred-Username", email.split("@")[0])
    return email, name


def _row_to_ticket(row, comment_count: int = 0) -> TicketResponse:
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
        comment_count=comment_count,
    )


@router.get("/tickets", response_model=TicketListResponse)
async def list_tickets(
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    status: Optional[str] = None,
    priority: Optional[str] = None,
    category: Optional[str] = None,
    severity: Optional[str] = None,
    assignee_email: Optional[str] = None,
    reporter_email: Optional[str] = None,
    search: Optional[str] = None,
    sort_by: str = "created_at",
    sort_order: str = "desc",
):
    conditions = []
    params = []
    idx = 1

    if status:
        conditions.append(f"t.status = ${idx}")
        params.append(status)
        idx += 1
    if priority:
        conditions.append(f"t.priority = ${idx}")
        params.append(priority)
        idx += 1
    if category:
        conditions.append(f"t.category = ${idx}")
        params.append(category)
        idx += 1
    if severity:
        conditions.append(f"t.severity = ${idx}")
        params.append(severity)
        idx += 1
    if assignee_email:
        conditions.append(f"t.assignee_email = ${idx}")
        params.append(assignee_email)
        idx += 1
    if reporter_email:
        conditions.append(f"t.reporter_email = ${idx}")
        params.append(reporter_email)
        idx += 1
    if search:
        conditions.append(f"(t.title ILIKE ${idx} OR t.description ILIKE ${idx} OR t.ticket_number ILIKE ${idx})")
        params.append(f"%{search}%")
        idx += 1

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # Validate sort
    if sort_by not in VALID_SORT_FIELDS:
        sort_by = "created_at"
    if sort_order.lower() not in ("asc", "desc"):
        sort_order = "desc"

    # Count total
    count_query = f"SELECT COUNT(*) FROM tickets t {where}"
    total = await db.fetchval(count_query, *params)

    # Fetch page
    offset = (page - 1) * per_page
    query = f"""
        SELECT t.*, COALESCE(c.cnt, 0) AS comment_count
        FROM tickets t
        LEFT JOIN (SELECT ticket_id, COUNT(*) AS cnt FROM comments GROUP BY ticket_id) c
            ON c.ticket_id = t.id
        {where}
        ORDER BY t.{sort_by} {sort_order}
        LIMIT ${idx} OFFSET ${idx + 1}
    """
    params.extend([per_page, offset])

    rows = await db.fetch(query, *params)
    tickets = [_row_to_ticket(r, comment_count=r["comment_count"]) for r in rows]

    total_pages = max(1, (total + per_page - 1) // per_page)
    return TicketListResponse(
        tickets=tickets, total=total, page=page,
        per_page=per_page, total_pages=total_pages,
    )


@router.post("/tickets", response_model=TicketResponse, status_code=201)
async def create_ticket(body: TicketCreate, request: Request):
    email, name = _get_user(request)
    ticket_id = uuid.uuid4()

    # Validate enums
    if body.priority not in VALID_PRIORITIES:
        raise HTTPException(400, f"Invalid priority: {body.priority}")
    if body.category not in VALID_CATEGORIES:
        raise HTTPException(400, f"Invalid category: {body.category}")
    if body.severity not in VALID_SEVERITIES:
        raise HTTPException(400, f"Invalid severity: {body.severity}")

    # Generate ticket number
    seq = await db.fetchval("SELECT nextval('ticket_number_seq')")
    ticket_number = f"TKT-{seq:05d}"

    tags_json = json.dumps(body.tags)

    await db.execute(
        """
        INSERT INTO tickets (
            id, ticket_number, title, description, status, priority, category, severity,
            reporter_email, reporter_name, assignee_email, assignee_name,
            environment, component, tags, due_date
        ) VALUES (
            $1, $2, $3, $4, 'open', $5, $6, $7,
            $8, $9, $10, $11,
            $12, $13, $14::jsonb, $15
        )
        """,
        ticket_id, ticket_number, body.title, body.description,
        body.priority, body.category, body.severity,
        email, name, body.assignee_email, body.assignee_name,
        body.environment, body.component, tags_json, body.due_date,
    )

    row = await db.fetchrow("SELECT * FROM tickets WHERE id = $1", ticket_id)
    return _row_to_ticket(row)


@router.get("/tickets/{ticket_id}", response_model=TicketResponse)
async def get_ticket(ticket_id: uuid.UUID):
    row = await db.fetchrow(
        """
        SELECT t.*, COALESCE(c.cnt, 0) AS comment_count
        FROM tickets t
        LEFT JOIN (SELECT ticket_id, COUNT(*) AS cnt FROM comments GROUP BY ticket_id) c
            ON c.ticket_id = t.id
        WHERE t.id = $1
        """,
        ticket_id,
    )
    if not row:
        raise HTTPException(404, "Ticket not found")
    return _row_to_ticket(row, comment_count=row["comment_count"])


@router.put("/tickets/{ticket_id}", response_model=TicketResponse)
async def update_ticket(ticket_id: uuid.UUID, body: TicketUpdate, request: Request):
    email, name = _get_user(request)

    existing = await db.fetchrow("SELECT * FROM tickets WHERE id = $1", ticket_id)
    if not existing:
        raise HTTPException(404, "Ticket not found")

    updates = {}
    history_entries = []

    field_map = {
        "title": body.title,
        "description": body.description,
        "status": body.status,
        "priority": body.priority,
        "category": body.category,
        "severity": body.severity,
        "assignee_email": body.assignee_email,
        "assignee_name": body.assignee_name,
        "environment": body.environment,
        "component": body.component,
        "tags": json.dumps(body.tags) if body.tags is not None else None,
        "due_date": body.due_date,
    }

    for field, new_val in field_map.items():
        if new_val is None:
            continue
        old_val = existing[field]
        if field == "tags":
            old_str = json.dumps(old_val) if not isinstance(old_val, str) else old_val
            if old_str == new_val:
                continue
            updates[field] = new_val
            history_entries.append((field, old_str, new_val))
        else:
            old_str = str(old_val) if old_val is not None else None
            new_str = str(new_val) if new_val is not None else None
            if old_str == new_str:
                continue
            updates[field] = new_val
            history_entries.append((field, old_str, new_str))

    if not updates:
        row = await db.fetchrow("SELECT * FROM tickets WHERE id = $1", ticket_id)
        return _row_to_ticket(row)

    # Validate enums if changed
    if "status" in updates and updates["status"] not in VALID_STATUSES:
        raise HTTPException(400, f"Invalid status: {updates['status']}")
    if "priority" in updates and updates["priority"] not in VALID_PRIORITIES:
        raise HTTPException(400, f"Invalid priority: {updates['priority']}")
    if "category" in updates and updates["category"] not in VALID_CATEGORIES:
        raise HTTPException(400, f"Invalid category: {updates['category']}")
    if "severity" in updates and updates["severity"] not in VALID_SEVERITIES:
        raise HTTPException(400, f"Invalid severity: {updates['severity']}")

    # Handle status transitions
    now = datetime.now(timezone.utc)
    if "status" in updates:
        if updates["status"] == "resolved":
            updates["resolved_at"] = now
        elif updates["status"] == "closed":
            updates["closed_at"] = now

    updates["updated_at"] = now

    # Build UPDATE query
    set_clauses = []
    params = []
    idx = 1
    for col, val in updates.items():
        if col == "tags":
            set_clauses.append(f"{col} = ${idx}::jsonb")
        else:
            set_clauses.append(f"{col} = ${idx}")
        params.append(val)
        idx += 1

    params.append(ticket_id)
    query = f"UPDATE tickets SET {', '.join(set_clauses)} WHERE id = ${idx}"
    await db.execute(query, *params)

    # Record history
    for field_name, old_value, new_value in history_entries:
        await db.execute(
            """
            INSERT INTO ticket_history (id, ticket_id, changed_by_email, changed_by_name, field_name, old_value, new_value)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            uuid.uuid4(), ticket_id, email, name, field_name, old_value, new_value,
        )

    row = await db.fetchrow(
        """
        SELECT t.*, COALESCE(c.cnt, 0) AS comment_count
        FROM tickets t
        LEFT JOIN (SELECT ticket_id, COUNT(*) AS cnt FROM comments GROUP BY ticket_id) c
            ON c.ticket_id = t.id
        WHERE t.id = $1
        """,
        ticket_id,
    )
    return _row_to_ticket(row, comment_count=row["comment_count"])
