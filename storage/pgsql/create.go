package pgsql

import (
	"context"

	"github.com/ltrochet/taskflow/runtime"
)

const sqlCreateTask = `
INSERT INTO tasks (
	id,
	workflow,
	queue,
	state,
	status,
	data,
	version
)
VALUES (
	$1,
	$2,
	$3,
	$4,
	$5,
	$6,
	$7
)
`

// Create crée une nouvelle tâche.
func (r *Repository[T]) Create(
	ctx context.Context,
	task *runtime.Task[T],
) error {
	record, err := marshalTask(task)
	if err != nil {
		return err
	}

	_, err = r.db.Exec(
		ctx,
		sqlCreateTask,
		record.ID,
		record.Workflow,
		record.Queue,
		record.State,
		record.Status,
		record.Data,
		record.Version,
	)

	return translatePgError(err)
}
