package workflow

// Builder construit un Workflow.
type Builder[T any] struct {
	name string

	initial string

	states map[string]HandlerFunc[T]

	transitions []transitionDef
}

// transitionDef représente une transition avant compilation.
type transitionDef struct {
	from  string
	event Event
	to    string
}

// StateBuilder permet de définir les transitions
// depuis un état donné.
type StateBuilder[T any] struct {
	builder *Builder[T]

	state string
}

// New crée un nouveau builder.
func New[T any](
	name string,
) *Builder[T] {
	return &Builder[T]{
		name:        name,
		states:      make(map[string]HandlerFunc[T]),
		transitions: make([]transitionDef, 0),
	}
}

// Initial définit l'état initial du workflow.
func (b *Builder[T]) Initial(
	state string,
) *Builder[T] {
	b.initial = state

	return b
}

// State ajoute un état métier.
//
// Les états système sont réservés au framework.
func (b *Builder[T]) State(
	name string,
	handler HandlerFunc[T],
) *StateBuilder[T] {
	b.states[name] = handler

	return &StateBuilder[T]{
		builder: b,
		state:   name,
	}
}

// Success ajoute une transition vers un autre état.
func (s *StateBuilder[T]) Success(
	next string,
) *StateBuilder[T] {
	return s.transition(
		Success,
		next,
	)
}

// Failure ajoute une transition vers un état d'échec.
func (s *StateBuilder[T]) Failure(
	next string,
) *StateBuilder[T] {
	return s.transition(
		Failure,
		next,
	)
}

// Complete ajoute une transition vers la fin normale
// du workflow.
func (s *StateBuilder[T]) Complete() *StateBuilder[T] {
	return s.transition(
		Success,
		StateCompleted,
	)
}

// Fail ajoute une transition vers la fin en erreur.
func (s *StateBuilder[T]) Fail() *StateBuilder[T] {
	return s.transition(
		Failure,
		StateFailed,
	)
}

// transition ajoute une transition.
//
// Les états système ne peuvent pas avoir de sorties.
func (s *StateBuilder[T]) transition(
	event Event,
	next string,
) *StateBuilder[T] {
	s.builder.transitions = append(
		s.builder.transitions,
		transitionDef{
			from:  s.state,
			event: event,
			to:    next,
		},
	)

	return s
}
