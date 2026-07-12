package workflow

// Transition représente une transition entre deux états.
//
// Les transitions sont déclarées pendant la construction du workflow
// puis compilées dans une représentation optimisée.
type Transition struct {
	From  string
	Event Event
	To    string
}
