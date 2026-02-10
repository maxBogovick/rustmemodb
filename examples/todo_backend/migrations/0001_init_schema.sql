CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'todo_status') THEN
        CREATE TYPE todo_status AS ENUM ('pending', 'in_progress', 'completed', 'archived');
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS todos (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title VARCHAR(200) NOT NULL,
    description TEXT,
    status todo_status NOT NULL DEFAULT 'pending',
    priority SMALLINT NOT NULL DEFAULT 3 CHECK (priority BETWEEN 1 AND 5),
    due_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ,
    CONSTRAINT todos_title_not_blank CHECK (btrim(title) <> '')
);

CREATE OR REPLACE FUNCTION set_todos_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_todos_set_updated_at ON todos;

CREATE TRIGGER trg_todos_set_updated_at
BEFORE UPDATE ON todos
FOR EACH ROW
EXECUTE FUNCTION set_todos_updated_at();
