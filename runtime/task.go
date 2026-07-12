package runtime

import "github.com/google/uuid"

// Status représente l'état d'exécution d'une tâche.
type Status string

const (

	// StatusPending indique que la tâche est en attente
	// d'exécution.
	StatusPending Status = "pending"

	// StatusRunning indique que la tâche est en cours
	// d'exécution.
	StatusRunning Status = "running"

	// StatusCompleted indique que le workflow est terminé.
	StatusCompleted Status = "completed"

	// StatusFailed indique que la tâche est en échec
	// définitif.
	StatusFailed Status = "failed"
)

// Task représente une instance d'exécution d'un workflow.
//
// Une Task contient uniquement les informations nécessaires
// à la reprise de son exécution et est conçue pour être persistée.
type Task[T any] struct {
	// ID identifie de manière unique la tâche.
	ID uuid.UUID

	// Workflow identifie le workflow chargé d'exécuter
	// cette tâche.
	Workflow string

	// Queue identifie la file d'exécution dans laquelle
	// la tâche est disponible pour les workers.
	Queue Queue

	// State est le dernier état validé et persisté.
	State string

	// Status représente l'état courant de la tâche.
	Status Status

	// Data contient les données métier manipulées
	// par les handlers.
	Data T

	// Version est utilisé pour le verrouillage optimiste
	// lors de la persistance.
	Version int64
}
