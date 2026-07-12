package storage

import "errors"

// ErrTaskNotFound indique qu'une tâche demandée
// n'existe pas dans le stockage.
var ErrTaskNotFound = errors.New(
	"task not found",
)

// ErrTaskAlreadyExists indique qu'une tâche avec le
// même identifiant existe déjà.
var ErrTaskAlreadyExists = errors.New(
	"task already exists",
)

// ErrNoTaskAvailable indique qu'aucune tâche n'est
// disponible pour acquisition.
var ErrNoTaskAvailable = errors.New(
	"no task available",
)

// ErrConcurrentUpdate indique qu'une tâche a été
// modifiée depuis sa dernière lecture.
var ErrConcurrentUpdate = errors.New(
	"concurrent update",
)
