package executor

import (
	"context"
	"errors"
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

// Serve exécute continuellement les tâches disponibles.
//
// La boucle s'arrête lorsque le contexte est annulé
// ou lorsqu'une erreur non récupérable survient.
func (c *Consumer[T]) Serve(
	ctx context.Context,
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := c.Consume(ctx)

		switch {
		case err == nil:
			c.backoff.Reset()

		case errors.Is(
			err,
			storage.ErrNoTaskAvailable,
		):
			if err := wait(
				ctx,
				c.backoff.Next(),
			); err != nil {
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
