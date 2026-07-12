package workflow

import "fmt"

// Build compile le Builder en un Workflow immutable.
func (b *Builder[T]) Build() (*Workflow[T], error) {
	if b.initial == "" {
		return nil, ErrNoInitialState
	}

	handlers, graph, err := b.compile()
	if err != nil {
		return nil, err
	}

	if _, ok := handlers[b.initial]; !ok {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrUnknownState,
			b.initial,
		)
	}

	if err := validateReachability(
		b.initial,
		handlers,
		graph,
	); err != nil {
		return nil, err
	}

	return &Workflow[T]{
		name:     b.name,
		initial:  b.initial,
		handlers: handlers,
		graph:    graph,
	}, nil
}

// compile construit les handlers et le graphe des transitions.
func (b *Builder[T]) compile() (
	map[string]HandlerFunc[T],
	map[string]map[Event]string,
	error,
) {
	handlers := make(
		map[string]HandlerFunc[T],
		len(b.states),
	)

	for state, handler := range b.states {

		// IsSystemState indique si un nom d'état est réservé
		// au framework.
		if IsSystemState(state) {
			return nil, nil, fmt.Errorf(
				"%w: %s",
				ErrReservedState,
				state,
			)
		}

		handlers[state] = handler
	}

	graph := make(
		map[string]map[Event]string,
	)

	for _, transition := range b.transitions {
		if _, ok := handlers[transition.from]; !ok {
			return nil, nil, fmt.Errorf(
				"%w: %s",
				ErrUnknownState,
				transition.from,
			)
		}

		if !IsSystemState(transition.to) {
			if _, ok := handlers[transition.to]; !ok {
				return nil, nil, fmt.Errorf(
					"%w: %s",
					ErrUnknownState,
					transition.to,
				)
			}
		}

		events, ok := graph[transition.from]
		if !ok {
			events = make(map[Event]string)
			graph[transition.from] = events
		}

		if _, exists := events[transition.event]; exists {
			return nil, nil, fmt.Errorf(
				"%w: state=%s event=%s",
				ErrDuplicateTransition,
				transition.from,
				transition.event,
			)
		}

		events[transition.event] = transition.to
	}

	// Les états système existent toujours dans le graphe.
	graph[StateCompleted] = map[Event]string{}
	graph[StateFailed] = map[Event]string{}

	return handlers, graph, nil
}
