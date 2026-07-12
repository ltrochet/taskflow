package pgsql

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository[T any] struct {
	db *pgxpool.Pool
}

func New[T any](db *pgxpool.Pool) *Repository[T] {
	return &Repository[T]{
		db: db,
	}
}

// taskRecord représente une ligne de la table tasks.
type taskRecord struct {
	ID        uuid.UUID
	Workflow  string
	Queue     string
	State     string
	Status    string
	Data      []byte
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Close libère les ressources utilisées par le repository.
func (r *Repository[T]) Close() {
	r.db.Close()
}
