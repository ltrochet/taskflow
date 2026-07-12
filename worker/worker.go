package worker

import (
	"context"
	"errors"

	"github.com/ltrochet/taskflow/runtime"
)

// TaskUpdater persiste l'état courant d'une tâche.
type TaskUpdater[T any] interface {
	Update(
		ctx context.Context,
		task *runtime.Task[T],
	) error
}

// Worker exécute une tâche jusqu'à son terme.
type Worker[T any] struct {
	runner  *runtime.Runner[T]
	updater TaskUpdater[T]
}

// New crée un nouveau Worker.
func New[T any](
	runner *runtime.Runner[T],
	updater TaskUpdater[T],
) *Worker[T] {
	return &Worker[T]{
		runner:  runner,
		updater: updater,
	}
}

func (w *Worker[T]) update(
	ctx context.Context,
	task *runtime.Task[T],
	status runtime.Status,
) error {
	task.Status = status

	return w.updater.Update(
		ctx,
		task,
	)
}

// Run exécute une tâche jusqu'à son terme.
func (w *Worker[T]) Run(
	ctx context.Context,
	task *runtime.Task[T],
) error {
	for {

		result, err := w.runner.Step(ctx, task)
		if err != nil {
			return errors.Join(
				err,
				w.update(ctx, task, runtime.StatusFailed),
			)
		}

		task.Version++

		status := runtime.StatusRunning
		if result.Completed {
			status = runtime.StatusCompleted
		}

		if err := w.update(ctx, task, status); err != nil {
			return err
		}

		if result.Completed {
			return nil
		}
	}
}
