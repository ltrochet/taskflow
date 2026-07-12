package workflow

// Etats système réservés au framework.
//
// Ces états ne représentent pas des étapes métier.
// Ils indiquent uniquement le résultat final d'une exécution.
const (
	// StateCompleted indique qu'un workflow s'est terminé
	// correctement.
	StateCompleted = "__completed__"

	// StateFailed indique qu'un workflow s'est terminé
	// en erreur.
	StateFailed = "__failed__"
)

// IsSystemState indique si un état appartient au framework
// et non au domaine métier.
func IsSystemState(state string) bool {
	switch state {
	case StateCompleted, StateFailed:
		return true
	default:
		return false
	}
}

// IsTerminalState indique si un état termine définitivement
// l'exécution d'un workflow.
func IsTerminalState(state string) bool {
	switch state {
	case StateCompleted, StateFailed:
		return true
	default:
		return false
	}
}
