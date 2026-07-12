package workflow

// Workflow représente un workflow compilé.
//
// Un Workflow est immutable après compilation et peut être partagé
// entre plusieurs exécutions de tâches.
type Workflow[T any] struct {
	// name identifie le workflow.
	name string

	// initial est l'état de départ.
	initial string

	// handlers contient les handlers associés aux états métier.
	handlers map[string]HandlerFunc[T]

	// graph contient les transitions :
	//
	// état courant -> événement -> état suivant
	graph map[string]map[Event]string
}

// Name retourne le nom du workflow.
func (w *Workflow[T]) Name() string {
	return w.name
}

// Initial retourne l'état initial du workflow.
func (w *Workflow[T]) Initial() string {
	return w.initial
}

// Handler retourne le handler associé à un état.
//
// Les états système ne possèdent pas de handler.
func (w *Workflow[T]) Handler(
	state string,
) (HandlerFunc[T], bool) {
	if IsSystemState(state) {
		return nil, false
	}

	handler, ok := w.handlers[state]

	return handler, ok
}

// Next retourne l'état suivant associé à un événement.
//
// Si aucune transition n'existe, ok vaut false.
func (w *Workflow[T]) Next(
	state string,
	event Event,
) (string, bool) {
	events, ok := w.graph[state]

	if !ok {
		return "", false
	}

	next, ok := events[event]

	return next, ok
}

// CanTransition indique si une transition existe
// pour un état et un événement donnés.
func (w *Workflow[T]) CanTransition(
	state string,
	event Event,
) bool {
	_, ok := w.Next(state, event)

	return ok
}

// IsTerminal indique si un état termine l'exécution.
//
// Les états terminaux sont des états système.
func (w *Workflow[T]) IsTerminal(
	state string,
) bool {
	return IsTerminalState(state)
}
