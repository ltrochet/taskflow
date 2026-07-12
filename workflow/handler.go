package workflow

import "context"

// HandlerFunc représente la logique métier associée à un état.
//
// Le handler reçoit :
//
//   - un context.Context pour les annulations, deadlines, traces…
//   - un pointeur vers les données métier de la tâche.
//
// Il retourne :
//
//   - un Event décrivant le résultat métier
//   - une erreur technique éventuelle
//
// Les modifications apportées à data seront persistées par le runtime
// après chaque transition.
type HandlerFunc[T any] func(
	ctx context.Context,
	data *T,
) (Event, error)
