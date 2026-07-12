package pgsql

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	uniqueViolation = "23505"
)

var (
	// ErrTaskAlreadyExists indique qu'une tâche portant le même identifiant existe déjà.
	ErrTaskAlreadyExists = errors.New("task already exists")

	// ErrTaskNotFound indique qu'une tâche n'existe pas.
	ErrTaskNotFound = errors.New("task not found")

	// ErrConcurrentUpdate indique qu'une mise à jour a échoué en raison
	// d'une modification concurrente.
	ErrConcurrentUpdate = errors.New("concurrent update")

	// ErrNoTaskAvailable indique qu'aucune tâche n'est disponible.
	ErrNoTaskAvailable = errors.New("no task available")
)

func translatePgError(err error) error {
	if err == nil {
		return nil
	}

	var pgErr *pgconn.PgError

	if !errors.As(err, &pgErr) {
		return err
	}

	switch pgErr.Code {

	case uniqueViolation:
		return ErrTaskAlreadyExists

	default:
		return err
	}
}
