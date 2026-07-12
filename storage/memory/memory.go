package memory

import (
	"context"
	"errors"
	"slices"
	"sync"

	"github.com/google/uuid"
	"github.com/ltrochet/taskflow/runtime"
)

var (
	ErrTaskNotFound = errors.New(
		"task not found",
	)

	ErrTaskAlreadyExists = errors.New(
		"task already exists",
	)

	ErrNoTaskAvailable = errors.New(
		"no task available",
	)
)

// Repository implémente un stockage de tâches en mémoire.
type Repository[T any] struct {
	mu    sync.Mutex
	tasks map[uuid.UUID]runtime.Task[T]
}

// New crée un repository mémoire vide.
func New[T any]() *Repository[T] {
	return &Repository[T]{
		tasks: make(map[uuid.UUID]runtime.Task[T]),
	}
}

// Create ajoute une nouvelle tâche.
func (r *Repository[T]) Create(
	ctx context.Context,
	task *runtime.Task[T],
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[task.ID]; exists {
		return ErrTaskAlreadyExists
	}

	r.tasks[task.ID] = cloneTask(task)

	return nil
}

// Update met à jour une tâche existante.
func (r *Repository[T]) Update(
	ctx context.Context,
	task *runtime.Task[T],
) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[task.ID]; !exists {
		return ErrTaskNotFound
	}

	r.tasks[task.ID] = cloneTask(task)

	return nil
}

// Get récupère une copie d'une tâche.
func (r *Repository[T]) Get(
	ctx context.Context,
	id uuid.UUID,
) (*runtime.Task[T], error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	task, ok := r.tasks[id]

	if !ok {
		return nil, ErrTaskNotFound
	}

	copy := cloneTask(&task)

	return &copy, nil
}

// Acquire récupère la première tâche disponible
// dans l'une des queues demandées.
//
// Si aucune queue n'est fournie, la queue par défaut
// est utilisée.
//
// Une tâche acquise passe de Pending à Running.
func (r *Repository[T]) Acquire(
	ctx context.Context,
	queues ...runtime.Queue,
) (*runtime.Task[T], error) {
	if len(queues) == 0 {
		queues = []runtime.Queue{
			runtime.DefaultQueue,
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for id, task := range r.tasks {
		if !slices.Contains(
			queues,
			task.Queue,
		) {
			continue
		}

		if task.Status != runtime.StatusPending {
			continue
		}

		task.Status = runtime.StatusRunning

		r.tasks[id] = task

		copy := cloneTask(&task)

		return &copy, nil
	}

	return nil, ErrNoTaskAvailable
}

func cloneTask[T any](
	task *runtime.Task[T],
) runtime.Task[T] {
	return runtime.Task[T]{
		ID:       task.ID,
		Workflow: task.Workflow,
		Queue:    task.Queue,
		State:    task.State,
		Status:   task.Status,
		Data:     task.Data,
		Version:  task.Version,
	}
}
