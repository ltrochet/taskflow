package workflow

import "errors"

var (

	// ErrNoInitialState indique qu'aucun état initial
	// n'a été défini pour le workflow.
	ErrNoInitialState = errors.New(
		"no initial state",
	)

	// ErrDuplicateState indique qu'un état est déclaré
	// plusieurs fois.
	ErrDuplicateState = errors.New(
		"duplicate state",
	)

	// ErrUnknownState indique qu'une transition référence
	// un état inexistant.
	ErrUnknownState = errors.New(
		"unknown state",
	)

	// ErrDuplicateTransition indique que deux transitions
	// identiques ont été déclarées.
	ErrDuplicateTransition = errors.New(
		"duplicate transition",
	)

	// ErrUnreachableState indique qu'un état n'est jamais
	// atteignable depuis l'état initial.
	ErrUnreachableState = errors.New(
		"unreachable state",
	)

	// ErrSystemState indique qu'une opération interdite
	// tente d'utiliser un état réservé du framework.
	ErrSystemState = errors.New(
		"system state",
	)

	// ErrReservedState indique qu'un état utilise
	// un nom réservé au framework.
	ErrReservedState = errors.New(
		"reserved state",
	)
)
