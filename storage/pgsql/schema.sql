--
-- PostgreSQL schema for taskflow.
--

--
-- Tasks.
--
-- A task represents the execution state of a workflow instance.
--

CREATE TABLE tasks (
    -- Unique task identifier.
    id UUID PRIMARY KEY,

    -- Workflow name.
    workflow TEXT NOT NULL,

    -- Execution queue.
    -- Workers acquire tasks from one or more queues.
    queue TEXT NOT NULL DEFAULT 'default',

    -- Current workflow state.
    state TEXT NOT NULL,

    -- Runtime status.
    status TEXT NOT NULL,

    -- User-defined workflow data.
    data JSONB NOT NULL,

    -- Optimistic concurrency control.
    version BIGINT NOT NULL DEFAULT 0,

    -- Creation timestamp.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Last update timestamp.
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

--
-- Acquire pending tasks efficiently.
--
-- Only pending tasks are candidates for acquisition.
-- Tasks are processed in FIFO order within a queue.
--

CREATE INDEX idx_tasks_pending
ON tasks (queue, created_at)
WHERE status = 'pending';
