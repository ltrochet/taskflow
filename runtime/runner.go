package runtime

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/ltrochet/taskflow/workflow"
)

// Runner exécute les étapes d'un workflow.
type Runner[T any] struct {
	workflow *workflow.Workflow[T]
}

// NewRunner crée un Runner associé à un workflow.
func NewRunner[T any](
	wf *workflow.Workflow[T],
) *Runner[T] {
	return &Runner[T]{
		workflow: wf,
	}
}

// NewTask crée une nouvelle tâche positionnée
// sur l'état initial du workflow.
func (r *Runner[T]) NewTask(
	data T,
	queue Queue,
) *Task[T] {
	return &Task[T]{
		ID:       uuid.New(),
		Workflow: r.workflow.Name(),
		Queue:    queue,
		State:    r.workflow.Initial(),
		Status:   StatusPending,
		Data:     data,
		Version:  0,
	}
}

func (r *Runner[T]) runHandler(
	ctx context.Context,
	handler workflow.HandlerFunc[T],
	data *T,
) (
	event workflow.Event,
	err error,
) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf(
				"%w: %v",
				ErrHandlerPanic,
				recovered,
			)
		}
	}()

	return handler(ctx, data)
}

// Step exécute l'étape courante de la tâche.
//
// Une étape correspond à l'exécution du handler associé
// à l'état courant puis à l'application de la transition
// produite par l'événement retourné.
func (r *Runner[T]) Step(
	ctx context.Context,
	task *Task[T],
) (*StepResult, error) {
	// Une tâche déjà arrivée dans un état terminal
	// ne doit plus être exécutée.
	if workflow.IsTerminalState(task.State) {
		return nil, ErrTaskCompleted
	}

	// Recherche du handler de l'état métier.
	handler, ok := r.workflow.Handler(task.State)
	if !ok {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrUnknownState,
			task.State,
		)
	}

	previous := task.State

	// Exécution de l'étape métier.
	event, err := r.runHandler(
		ctx,
		handler,
		&task.Data,
	)
	if err != nil {
		return nil, err
	}

	// Recherche de la transition associée
	// à l'événement produit.
	next, ok := r.workflow.Next(
		task.State,
		event,
	)

	if !ok {
		return nil, fmt.Errorf(
			"%w: state=%q event=%q",
			ErrInvalidTransition,
			task.State,
			event,
		)
	}

	// La transition est appliquée.
	task.State = next

	return &StepResult{
		PreviousState: previous,
		Event:         event,
		NextState:     next,
		Completed:     workflow.IsTerminalState(next),
	}, nil
}
