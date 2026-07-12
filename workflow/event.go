package workflow

// Event représente le résultat de l'exécution d'un état.
//
// Un Event est produit par un Handler et permet de sélectionner
// la transition suivante dans le workflow.
type Event string

const (
	// Success indique que l'état s'est exécuté avec succès.
	Success Event = "success"

	// Failure indique un échec métier.
	Failure Event = "failure"

	// Retry indique que l'état doit être réessayé.
	Retry Event = "retry"

	// Cancel indique que l'exécution est annulée.
	Cancel Event = "cancel"

	// Timeout indique que l'état a dépassé son délai d'exécution.
	Timeout Event = "timeout"
)
