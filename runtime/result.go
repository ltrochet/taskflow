package runtime

import "git.infra.sas.ina/an/gamma/taskflow.git/workflow"

// StepResult décrit le résultat de l'exécution d'une étape.
//
// Il contient les informations nécessaires pour persister
// l'avancement de la tâche et historiser les transitions.
type StepResult struct {
	// PreviousState est l'état avant l'exécution.
	PreviousState string

	// Event est l'événement produit par le handler.
	Event workflow.Event

	// NextState est le prochain état.
	//
	// Il est vide lorsque le workflow est terminé.
	NextState string

	// Completed indique que le workflow est terminé.
	Completed bool
}
