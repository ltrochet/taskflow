package pgsql

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/ltrochet/taskflow/runtime"
)

const sqlAcquireTask = `
WITH candidate AS (
	SELECT id
	FROM tasks
	WHERE
		queue = ANY($1)
		AND status = 'pending'
	ORDER BY created_at
	FOR UPDATE SKIP LOCKED
	LIMIT 1
)
UPDATE tasks
SET
	status = 'running',
	version = version + 1,
	updated_at = NOW()
FROM candidate
WHERE tasks.id = candidate.id
RETURNING
	tasks.id,
	tasks.workflow,
	tasks.queue,
	tasks.state,
	tasks.status,
	tasks.data,
	tasks.version,
	tasks.created_at,
	tasks.updated_at
`

// Acquire réserve la prochaine tâche disponible
// dans l'une des queues demandées.
//
// Si aucune queue n'est fournie, la queue par défaut est utilisée.
func (r *Repository[T]) Acquire(
	ctx context.Context,
	queues ...runtime.Queue,
) (*runtime.Task[T], error) {
	if len(queues) == 0 {
		queues = []runtime.Queue{
			runtime.DefaultQueue,
		}
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var record taskRecord

	err = tx.QueryRow(
		ctx,
		sqlAcquireTask,
		queues,
	).Scan(
		&record.ID,
		&record.Workflow,
		&record.Queue,
		&record.State,
		&record.Status,
		&record.Data,
		&record.Version,
		&record.CreatedAt,
		&record.UpdatedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoTaskAvailable
	}

	if err != nil {
		return nil, translatePgError(err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	return unmarshalTask[T](&record)
}
