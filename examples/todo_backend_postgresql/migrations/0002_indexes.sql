CREATE INDEX IF NOT EXISTS idx_todos_status_active
    ON todos (status)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_todos_priority_active
    ON todos (priority)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_todos_due_at_active
    ON todos (due_at)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_todos_search_active
    ON todos USING GIN (to_tsvector('simple', coalesce(title, '') || ' ' || coalesce(description, '')))
    WHERE deleted_at IS NULL;
