package runtime

import "errors"

var (

	// ErrUnknownState indique que la tâche référence un état inexistant.
	ErrUnknownState = errors.New("unknown state")

	// ErrInvalidTransition indique qu'aucune transition n'existe
	// pour l'événement retourné par le handler.
	ErrInvalidTransition = errors.New("invalid transition")

	// ErrHandlerPanic indique qu'un handler de workflow
	// a provoqué un panic.
	ErrHandlerPanic = errors.New("handler panic")

	ErrTaskCompleted = errors.New("task already completed")
)
