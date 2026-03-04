import uuid

from fastapi import APIRouter, HTTPException, Request

from backend import database as db
from backend.models import CommentCreate, CommentResponse

router = APIRouter()


def _get_user(request: Request) -> tuple[str, str]:
    email = request.headers.get("X-Forwarded-Email", "anonymous@example.com")
    name = request.headers.get("X-Forwarded-Preferred-Username", email.split("@")[0])
    return email, name


def _row_to_comment(row) -> CommentResponse:
    return CommentResponse(
        id=row["id"],
        ticket_id=row["ticket_id"],
        author_email=row["author_email"],
        author_name=row["author_name"] or "",
        content=row["content"],
        is_internal=row["is_internal"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


@router.get("/tickets/{ticket_id}/comments", response_model=list[CommentResponse])
async def list_comments(ticket_id: uuid.UUID):
    # Verify ticket exists
    exists = await db.fetchval("SELECT 1 FROM tickets WHERE id = $1", ticket_id)
    if not exists:
        raise HTTPException(404, "Ticket not found")

    rows = await db.fetch(
        "SELECT * FROM comments WHERE ticket_id = $1 ORDER BY created_at ASC",
        ticket_id,
    )
    return [_row_to_comment(r) for r in rows]


@router.post("/tickets/{ticket_id}/comments", response_model=CommentResponse, status_code=201)
async def create_comment(ticket_id: uuid.UUID, body: CommentCreate, request: Request):
    email, name = _get_user(request)

    # Verify ticket exists
    exists = await db.fetchval("SELECT 1 FROM tickets WHERE id = $1", ticket_id)
    if not exists:
        raise HTTPException(404, "Ticket not found")

    comment_id = uuid.uuid4()
    await db.execute(
        """
        INSERT INTO comments (id, ticket_id, author_email, author_name, content, is_internal)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        comment_id, ticket_id, email, name, body.content, body.is_internal,
    )

    row = await db.fetchrow("SELECT * FROM comments WHERE id = $1", comment_id)
    return _row_to_comment(row)
