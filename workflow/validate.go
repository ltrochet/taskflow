package workflow

import "fmt"

// validateReachability vérifie que tous les états métier
// sont atteignables depuis l'état initial.
func validateReachability[T any](
	initial string,
	handlers map[string]HandlerFunc[T],
	graph map[string]map[Event]string,
) error {
	visited := make(map[string]struct{})

	var visit func(string)

	visit = func(state string) {
		if _, ok := visited[state]; ok {
			return
		}

		visited[state] = struct{}{}

		transitions, ok := graph[state]
		if !ok {
			return
		}

		for _, next := range transitions {
			visit(next)
		}
	}

	visit(initial)

	for state := range handlers {
		if _, ok := visited[state]; !ok {
			return fmt.Errorf(
				"%w: %s",
				ErrUnreachableState,
				state,
			)
		}
	}

	return nil
}
