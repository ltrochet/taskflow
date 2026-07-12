package executor

import (
	"context"

	"github.com/ltrochet/taskflow/runtime"
)

// TaskAcquirer acquiert une tâche prête à être exécutée.
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

	queues  []runtime.Queue
	backoff Backoff
}

// NewConsumer crée un Consumer.
func NewConsumer[T any](
	acquirer TaskAcquirer[T],
	runner TaskRunner[T],
	options ...Option[T],
) (*Consumer[T], error) {
	backoff, err := NewDefaultBackoff()
	if err != nil {
		return nil, err
	}

	c := &Consumer[T]{
		acquirer: acquirer,
		runner:   runner,
		queues: []runtime.Queue{
			runtime.DefaultQueue,
		},
		backoff: backoff,
	}

	for _, option := range options {
		if option == nil {
			continue
		}

		option(c)
	}

	return c, nil
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
