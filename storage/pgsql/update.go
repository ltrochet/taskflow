package pgsql

import (
	"context"

	"git.infra.sas.ina/an/gamma/taskflow.git/runtime"
)

const sqlUpdateTask = `
UPDATE tasks
SET
	state = $2,
	status = $3,
	data = $4,
	version = version + 1,
	updated_at = NOW()
WHERE
	id = $1
AND
	version = $5
`

// Update met à jour une tâche.
//
// La mise à jour utilise un contrôle de concurrence optimiste basé
// sur le numéro de version.
//
// La queue d'exécution est immuable après création.
func (r *Repository[T]) Update(
	ctx context.Context,
	task *runtime.Task[T],
) error {
	record, err := marshalTask(task)
	if err != nil {
		return err
	}

	tag, err := r.db.Exec(
		ctx,
		sqlUpdateTask,
		record.ID,
		record.State,
		record.Status,
		record.Data,
		record.Version,
	)
	if err != nil {
		return translatePgError(err)
	}

	if tag.RowsAffected() == 1 {
		task.Version++
		return nil
	}

	// Aucune ligne mise à jour.
	// On distingue "tâche inexistante" de "conflit de version".
	_, err = r.Get(ctx, task.ID)
	if err == ErrTaskNotFound {
		return ErrTaskNotFound
	}
	if err != nil {
		return err
	}

	return ErrConcurrentUpdate
}
