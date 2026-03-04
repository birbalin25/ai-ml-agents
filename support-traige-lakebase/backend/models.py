from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


# --- Tickets ---

class TicketCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=500)
    description: str = ""
    priority: str = "medium"
    category: str = "question"
    severity: str = "sev3"
    assignee_email: Optional[str] = None
    assignee_name: Optional[str] = None
    environment: str = ""
    component: str = ""
    tags: list[str] = []
    due_date: Optional[datetime] = None


class TicketUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[str] = None
    category: Optional[str] = None
    severity: Optional[str] = None
    assignee_email: Optional[str] = None
    assignee_name: Optional[str] = None
    environment: Optional[str] = None
    component: Optional[str] = None
    tags: Optional[list[str]] = None
    due_date: Optional[datetime] = None


class TicketResponse(BaseModel):
    id: UUID
    ticket_number: str
    title: str
    description: str
    status: str
    priority: str
    category: str
    severity: str
    reporter_email: str
    reporter_name: str
    assignee_email: Optional[str]
    assignee_name: Optional[str]
    environment: str
    component: str
    tags: list
    due_date: Optional[datetime]
    resolved_at: Optional[datetime]
    closed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    comment_count: int = 0


class TicketListResponse(BaseModel):
    tickets: list[TicketResponse]
    total: int
    page: int
    per_page: int
    total_pages: int


# --- Comments ---

class CommentCreate(BaseModel):
    content: str = Field(..., min_length=1)
    is_internal: bool = False


class CommentResponse(BaseModel):
    id: UUID
    ticket_id: UUID
    author_email: str
    author_name: str
    content: str
    is_internal: bool
    created_at: datetime
    updated_at: datetime


# --- History ---

class HistoryEntry(BaseModel):
    id: UUID
    ticket_id: UUID
    changed_by_email: str
    changed_by_name: str
    field_name: str
    old_value: Optional[str]
    new_value: Optional[str]
    created_at: datetime


# --- Stats ---

class StatsResponse(BaseModel):
    total_tickets: int
    open_tickets: int
    in_progress_tickets: int
    pending_tickets: int
    resolved_tickets: int
    closed_tickets: int
    critical_tickets: int
    high_priority_tickets: int
    by_status: dict[str, int]
    by_priority: dict[str, int]
    by_category: dict[str, int]
    recent_tickets: list[TicketResponse]
