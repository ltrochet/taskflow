package executor

import (
	"context"
	"errors"
	"time"

	"github.com/ltrochet/taskflow/storage"
)

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
			timer := time.NewTimer(
				c.backoff.Next(),
			)

			select {
			case <-ctx.Done():
				timer.Stop()

				return ctx.Err()

			case <-timer.C:
			}

		default:
			return err
		}
	}
}
