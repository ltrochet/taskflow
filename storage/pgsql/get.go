package pgsql

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/ltrochet/taskflow/runtime"
	"github.com/ltrochet/taskflow/storage"
)

const sqlGetTask = `
SELECT
	id,
	workflow,
	queue,
	state,
	status,
	data,
	version,
	created_at,
	updated_at
FROM tasks
WHERE id = $1
`

// Get récupère une tâche par son identifiant.
func (r *Repository[T]) Get(
	ctx context.Context,
	id uuid.UUID,
) (*runtime.Task[T], error) {
	var record taskRecord

	err := r.db.QueryRow(
		ctx,
		sqlGetTask,
		id,
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
		return nil, storage.ErrTaskNotFound
	}

	if err != nil {
		return nil, translatePgError(err)
	}

	return unmarshalTask[T](&record)
}
