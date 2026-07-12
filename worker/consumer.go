package worker

import (
	"context"

	"github.com/ltrochet/taskflow/runtime"
)

// TaskAcquirer récupère une tâche prête à être exécutée.
type TaskAcquirer[T any] interface {
	Acquire(
		ctx context.Context,
		queues ...runtime.Queue,
	) (*runtime.Task[T], error)
}

// TaskRunner exécute une tâche.
type TaskRunner[T any] interface {
	Run(
		ctx context.Context,
		task *runtime.Task[T],
	) error
}

// Consumer acquiert puis exécute des tâches.
type Consumer[T any] struct {
	acquirer TaskAcquirer[T]
	runner   TaskRunner[T]

	queues []runtime.Queue
}

// NewConsumer crée un nouveau Consumer.
//
// Si aucune queue n'est fournie, la queue par défaut
// sera utilisée par le repository.
func NewConsumer[T any](
	acquirer TaskAcquirer[T],
	runner TaskRunner[T],
	queues ...runtime.Queue,
) *Consumer[T] {
	return &Consumer[T]{
		acquirer: acquirer,
		runner:   runner,
		queues:   queues,
	}
}

// Consume acquiert puis exécute une tâche.
func (c *Consumer[T]) Consume(
	ctx context.Context,
) error {
	task, err := c.acquirer.Acquire(
		ctx,
		c.queues...,
	)
	if err != nil {
		return err
	}

	return c.runner.Run(
		ctx,
		task,
	)
}
