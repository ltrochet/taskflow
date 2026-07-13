package executor

import (
	"time"

	"github.com/ltrochet/taskflow/runtime"
)

// Option configure un Consumer.
type Option[T any] func(*Consumer[T])

// ErrorHandler est appelé lorsqu'une erreur est rencontrée
// pendant l'exécution de Serve().
type ErrorHandler func(error)

// ErrorPolicy définit le comportement du Consumer lorsqu'une
// erreur survient.
type ErrorPolicy int

const (
	// ErrorPolicyStop arrête le Consumer à la première erreur.
	ErrorPolicyStop ErrorPolicy = iota

	// ErrorPolicyContinue journalise l'erreur puis reprend
	// la consommation après un délai.
	ErrorPolicyContinue
)

// WithQueues configure les queues consommées.
//
// Si aucune queue n'est configurée, le Consumer utilisera
// runtime.DefaultQueue.
func WithQueues[T any](
	queues ...runtime.Queue,
) Option[T] {
	return func(
		c *Consumer[T],
	) {
		if len(queues) == 0 {
			return
		}

		c.queues = append(
			[]runtime.Queue(nil),
			queues...,
		)
	}
}

// WithBackoffFactory configure la fabrique de backoffs
// utilisée par Serve().
func WithBackoffFactory[T any](
	factory BackoffFactory,
) Option[T] {
	return func(
		c *Consumer[T],
	) {
		if factory != nil {
			c.backoffFactory = factory
		}
	}
}

// WithErrorHandler configure le gestionnaire appelé
// lorsqu'une erreur survient pendant Serve().
func WithErrorHandler[T any](
	handler ErrorHandler,
) Option[T] {
	return func(
		c *Consumer[T],
	) {
		c.errorHandler = handler
	}
}

// WithErrorPolicy configure le comportement du Consumer
// lorsqu'une erreur survient.
func WithErrorPolicy[T any](
	policy ErrorPolicy,
) Option[T] {
	return func(
		c *Consumer[T],
	) {
		c.errorPolicy = policy
	}
}

// WithRetryDelay configure le délai d'attente avant une
// nouvelle tentative lorsque ErrorPolicyContinue est utilisé.
func WithRetryDelay[T any](
	delay time.Duration,
) Option[T] {
	return func(
		c *Consumer[T],
	) {
		if delay > 0 {
			c.retryDelay = delay
		}
	}
}

// WithConcurrency configure le nombre de goroutines
// d'exécution utilisées par Serve().
//
// La valeur doit être strictement positive.
// Les valeurs inférieures ou égales à zéro sont ignorées.
func WithConcurrency[T any](
	concurrency int,
) Option[T] {
	return func(
		c *Consumer[T],
	) {
		if concurrency > 0 {
			c.concurrency = concurrency
		}
	}
}
