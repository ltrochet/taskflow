package executor

import (
	"errors"
	"fmt"
	"time"
)

const (
	defaultMinBackoff = 100 * time.Millisecond
	defaultMaxBackoff = 5 * time.Second
)

// ErrInvalidBackoff indique qu'une configuration de backoff
// est invalide.
var ErrInvalidBackoff = errors.New(
	"invalid backoff",
)

// Backoff détermine le délai d'attente entre deux
// tentatives d'acquisition lorsqu'aucune tâche n'est
// disponible.
type Backoff interface {
	// Reset réinitialise le backoff après une acquisition
	// réussie.
	Reset()

	// Next retourne le délai à attendre avant la prochaine
	// tentative.
	Next() time.Duration
}

// BackoffFactory crée une nouvelle instance de Backoff.
type BackoffFactory func() Backoff

// ExponentialBackoff implémente un backoff exponentiel
// borné.
type ExponentialBackoff struct {
	min time.Duration
	max time.Duration

	current time.Duration
}

// NewExponentialBackoff crée un backoff exponentiel.
//
// Le premier appel à Next() retourne min.
// Les appels suivants doublent progressivement le délai,
// sans jamais dépasser max.
func NewExponentialBackoff(
	min,
	max time.Duration,
) (*ExponentialBackoff, error) {
	switch {
	case min <= 0:
		return nil, fmt.Errorf(
			"%w: min must be greater than zero",
			ErrInvalidBackoff,
		)

	case max < min:
		return nil, fmt.Errorf(
			"%w: max must be greater than or equal to min",
			ErrInvalidBackoff,
		)
	}

	return &ExponentialBackoff{
		min: min,
		max: max,
	}, nil
}

// NewExponentialBackoffFactory crée une fabrique de
// backoffs exponentiels.
func NewExponentialBackoffFactory(
	min,
	max time.Duration,
) (BackoffFactory, error) {
	// Validation de la configuration.
	if _, err := NewExponentialBackoff(
		min,
		max,
	); err != nil {
		return nil, err
	}

	return func() Backoff {
		// Les paramètres étant déjà validés,
		// cette construction ne peut plus échouer.
		return &ExponentialBackoff{
			min: min,
			max: max,
		}
	}, nil
}

// Reset réinitialise le backoff.
//
// Le prochain appel à Next() retournera le délai minimal.
func (b *ExponentialBackoff) Reset() {
	b.current = 0
}

// Next retourne le prochain délai.
//
// Les délais augmentent exponentiellement jusqu'à atteindre
// le délai maximal.
func (b *ExponentialBackoff) Next() time.Duration {
	if b.current == 0 {
		b.current = b.min

		return b.current
	}

	next := b.current * 2
	if next > b.max {
		next = b.max
	}

	b.current = next

	return b.current
}
