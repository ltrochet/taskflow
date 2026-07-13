package executor

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ltrochet/taskflow/storage"
)

func wait(
	ctx context.Context,
	delay time.Duration,
) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()

	case <-timer.C:
		return nil
	}
}

func (c *Consumer[T]) handleError(err error) {
	if c.errorHandler == nil {
		return
	}

	defer func() {
		_ = recover()
	}()

	c.errorHandler(err)
}

// serve exécute la boucle de consommation.
//
// Elle ne retourne une erreur métier que lorsque
// ErrorPolicyStop est configurée.
func (c *Consumer[T]) serve(
	ctx context.Context,
	backoff Backoff,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := c.Consume(ctx)

		switch {
		case err == nil:
			backoff.Reset()

		case errors.Is(err, storage.ErrNoTaskAvailable):
			if err := wait(ctx, backoff.Next()); err != nil {
				return err
			}

		default:
			c.handleError(err)

			if c.errorPolicy == ErrorPolicyStop {
				return err
			}

			if err := wait(
				ctx,
				c.retryDelay,
			); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer[T]) goServe(
	ctx context.Context,
	backoff Backoff,
	reportError func(error),
) {
	err := c.serve(
		ctx,
		backoff,
	)

	if err == nil {
		return
	}

	// En mode Continue, les seules erreurs
	// attendues sont liées au contexte.
	if errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) {
		return
	}

	reportError(err)
}

// Serve exécute continuellement les tâches disponibles.
//
// Les tâches sont traitées jusqu'à l'annulation du contexte
// ou jusqu'à la survenue d'une erreur lorsque
// ErrorPolicyStop est configurée.
//
// Lorsque plusieurs workers sont configurés, les tâches
// sont consommées et exécutées en parallèle.
func (c *Consumer[T]) Serve(
	ctx context.Context,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup

	errCh := make(chan error, 1)
	reportError := func(err error) {
		select {
		case errCh <- err:
			cancel()
		default:
		}
	}

	for i := 0; i < c.concurrency; i++ {
		backoff := c.backoffFactory()

		wg.Go(func() {
			c.goServe(
				ctx,
				backoff,
				reportError,
			)
		})
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}
