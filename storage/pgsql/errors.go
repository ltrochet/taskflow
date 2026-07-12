package pgsql

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ltrochet/taskflow/storage"
)

const (
	uniqueViolation = "23505"
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
		return storage.ErrTaskAlreadyExists

	default:
		return err
	}
}
