-- Create tasks table
CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    schedule_time BIGINT NOT NULL,
    command_type TEXT NOT NULL,
    command_payload TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('Pending', 'Completed', 'Failed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for efficient scheduler polling
CREATE INDEX IF NOT EXISTS idx_tasks_poll ON tasks (status, schedule_time);
