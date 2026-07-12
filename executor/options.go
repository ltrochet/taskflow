package executor

import "github.com/ltrochet/taskflow/runtime"

// Option configure un Consumer.
type Option[T any] func(*Consumer[T])

// WithQueues configure les queues consommées.
//
// L'appel remplace la configuration par défaut des queues.
func WithQueues[T any](
	queues ...runtime.Queue,
) Option[T] {
	return func(
		c *Consumer[T],
	) {
		c.queues = append(
			[]runtime.Queue(nil),
			queues...,
		)
	}
}

// WithBackoff configure le backoff utilisé par Serve()
// lorsqu'aucune tâche n'est disponible.
func WithBackoff[T any](
	backoff Backoff,
) Option[T] {
	return func(
		c *Consumer[T],
	) {
		if backoff != nil {
			c.backoff = backoff
		}
	}
}
