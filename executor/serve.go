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

	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()

	case <-timer.C:
		return nil
	}
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
			if err := wait(ctx, c.backoff.Next()); err != nil {
				return err
			}

		default:
			return err
		}
	}
}
