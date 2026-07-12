package repository

import (
	"context"

	"git.infra.sas.ina/an/gamma/taskflow.git/runtime"
	"github.com/google/uuid"
)

type Reader[T any] interface {
	// Get charge une tâche par son identifiant.
	Get(
		ctx context.Context,
		id uuid.UUID,
	) (*runtime.Task[T], error)

	// Acquire récupère une tâche prête à être exécutée
	// dans l'une des queues demandées et en prend la possession
	// de manière atomique.
	Acquire(
		ctx context.Context,
		queues ...runtime.Queue,
	) (*runtime.Task[T], error)
}
type Writer[T any] interface {
	// Create persiste une nouvelle tâche.
	Create(
		ctx context.Context,
		task *runtime.Task[T],
	) error

	// Update persiste l'état courant d'une tâche.
	Update(
		ctx context.Context,
		task *runtime.Task[T],
	) error
}

type Repository[T any] interface {
	Reader[T]
	Writer[T]
}
