-- Support Triage Portal Schema

-- Create app schema
CREATE SCHEMA IF NOT EXISTS support_app;
SET search_path TO support_app;

-- Sequence for ticket numbers
CREATE SEQUENCE IF NOT EXISTS ticket_number_seq START 1;

-- Tickets table
CREATE TABLE IF NOT EXISTS tickets (
    id UUID PRIMARY KEY,
    ticket_number TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    description TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'in_progress', 'pending', 'resolved', 'closed')),
    priority TEXT NOT NULL DEFAULT 'medium'
        CHECK (priority IN ('critical', 'high', 'medium', 'low')),
    category TEXT NOT NULL DEFAULT 'question'
        CHECK (category IN ('bug', 'feature_request', 'question', 'incident', 'task')),
    severity TEXT NOT NULL DEFAULT 'sev3'
        CHECK (severity IN ('sev1', 'sev2', 'sev3', 'sev4')),
    reporter_email TEXT NOT NULL,
    reporter_name TEXT NOT NULL DEFAULT '',
    assignee_email TEXT,
    assignee_name TEXT,
    environment TEXT DEFAULT '',
    component TEXT DEFAULT '',
    tags JSONB DEFAULT '[]'::jsonb,
    due_date TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    closed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Comments table
CREATE TABLE IF NOT EXISTS comments (
    id UUID PRIMARY KEY,
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    author_email TEXT NOT NULL,
    author_name TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL,
    is_internal BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ticket history (audit trail)
CREATE TABLE IF NOT EXISTS ticket_history (
    id UUID PRIMARY KEY,
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    changed_by_email TEXT NOT NULL,
    changed_by_name TEXT NOT NULL DEFAULT '',
    field_name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Attachments (metadata only)
CREATE TABLE IF NOT EXISTS attachments (
    id UUID PRIMARY KEY,
    ticket_id UUID NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
    filename TEXT NOT NULL,
    file_url TEXT NOT NULL,
    mime_type TEXT DEFAULT '',
    file_size INTEGER DEFAULT 0,
    uploaded_by_email TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for tickets
CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status);
CREATE INDEX IF NOT EXISTS idx_tickets_priority ON tickets(priority);
CREATE INDEX IF NOT EXISTS idx_tickets_category ON tickets(category);
CREATE INDEX IF NOT EXISTS idx_tickets_reporter ON tickets(reporter_email);
CREATE INDEX IF NOT EXISTS idx_tickets_assignee ON tickets(assignee_email);
CREATE INDEX IF NOT EXISTS idx_tickets_created ON tickets(created_at DESC);

-- Indexes for comments
CREATE INDEX IF NOT EXISTS idx_comments_ticket ON comments(ticket_id);
CREATE INDEX IF NOT EXISTS idx_comments_created ON comments(created_at);

-- Indexes for ticket_history
CREATE INDEX IF NOT EXISTS idx_history_ticket ON ticket_history(ticket_id);
CREATE INDEX IF NOT EXISTS idx_history_created ON ticket_history(created_at);

-- Indexes for attachments
CREATE INDEX IF NOT EXISTS idx_attachments_ticket ON attachments(ticket_id);
