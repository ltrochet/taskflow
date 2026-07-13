package executor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ltrochet/taskflow/storage"
)

type mockBackoff struct {
	resetCalls int
	nextCalls  int
	delay      time.Duration
}

func (m *mockBackoff) Reset() {
	m.resetCalls++
}

func (m *mockBackoff) Next() time.Duration {
	m.nextCalls++
	return m.delay
}

func TestConsumer_Serve_ContextCanceled(t *testing.T) {
	acquirer := &mockTaskAcquirer{}
	runner := &mockTaskRunner{}

	consumer := newTestConsumer(
		t,
		acquirer,
		runner,
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	err := consumer.Serve(ctx)

	if !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf(
			"expected %v, got %v",
			context.Canceled,
			err,
		)
	}

	if acquirer.acquireCalls != 0 {
		t.Fatalf(
			"expected 0 acquire, got %d",
			acquirer.acquireCalls,
		)
	}
}

func TestConsumer_Serve_NoTaskAvailable(t *testing.T) {
	acquirer := &mockTaskAcquirer{
		err: storage.ErrNoTaskAvailable,
	}

	runner := &mockTaskRunner{}

	backoff := &mockBackoff{
		delay: 0,
	}

	factoryCalls := 0

	consumer := newTestConsumer(
		t,
		acquirer,
		runner,
		WithBackoffFactory[testData](
			func() Backoff {
				factoryCalls++

				return backoff
			},
		),
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	go func() {
		for acquirer.acquireCalls == 0 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	err := consumer.Serve(ctx)

	if !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf(
			"expected %v, got %v",
			context.Canceled,
			err,
		)
	}

	if backoff.nextCalls == 0 {
		t.Fatal(
			"expected Next() to be called",
		)
	}

	if backoff.resetCalls != 0 {
		t.Fatal(
			"Reset() should not be called",
		)
	}

	if factoryCalls != 1 {
		t.Fatalf(
			"expected 1 backoff, got %d",
			factoryCalls,
		)
	}
}

func TestConsumer_Serve_StopOnError(t *testing.T) {
	expected := errors.New(
		"boom",
	)

	acquirer := &mockTaskAcquirer{
		err: expected,
	}

	runner := &mockTaskRunner{}

	called := false

	consumer := newTestConsumer(
		t,
		acquirer,
		runner,
		WithErrorHandler[testData](
			func(err error) {
				called = true
			},
		),
	)

	err := consumer.Serve(
		context.Background(),
	)

	if !errors.Is(
		err,
		expected,
	) {
		t.Fatalf(
			"expected %v, got %v",
			expected,
			err,
		)
	}

	if !called {
		t.Fatal(
			"expected ErrorHandler to be called",
		)
	}
}

func TestConsumer_Serve_ContinueOnError(t *testing.T) {
	expected := errors.New(
		"boom",
	)

	acquirer := &mockTaskAcquirer{
		err: expected,
	}

	runner := &mockTaskRunner{}

	calls := 0

	consumer := newTestConsumer(
		t,
		acquirer,
		runner,
		WithErrorPolicy[testData](
			ErrorPolicyContinue,
		),
		WithRetryDelay[testData](time.Millisecond),
		WithErrorHandler[testData](
			func(err error) {
				calls++
			},
		),
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	go func() {
		for acquirer.acquireCalls < 2 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	err := consumer.Serve(ctx)

	if !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf(
			"expected %v, got %v",
			context.Canceled,
			err,
		)
	}

	if acquirer.acquireCalls < 2 {
		t.Fatalf(
			"expected at least 2 acquire calls, got %d",
			acquirer.acquireCalls,
		)
	}

	if calls == 0 {
		t.Fatal(
			"expected ErrorHandler to be called",
		)
	}
}

func TestConsumer_Serve_ErrorHandlerPanic(t *testing.T) {
	expected := errors.New(
		"boom",
	)

	acquirer := &mockTaskAcquirer{
		err: expected,
	}

	runner := &mockTaskRunner{}

	consumer := newTestConsumer(
		t,
		acquirer,
		runner,
		WithErrorPolicy[testData](
			ErrorPolicyContinue,
		),
		WithRetryDelay[testData](time.Millisecond),
		WithErrorHandler[testData](
			func(err error) {
				panic("logger failed")
			},
		),
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	go func() {
		for acquirer.acquireCalls < 2 {
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	err := consumer.Serve(ctx)

	if !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf(
			"expected %v, got %v",
			context.Canceled,
			err,
		)
	}
}

func TestConsumer_Serve_Concurrency(t *testing.T) {
	acquirer := &mockTaskAcquirer{
		err: storage.ErrNoTaskAvailable,
	}

	runner := &mockTaskRunner{}

	var factoryCalls atomic.Int32

	consumer := newTestConsumer(
		t,
		acquirer,
		runner,
		WithConcurrency[testData](4),
		WithBackoffFactory[testData](
			func() Backoff {
				factoryCalls.Add(1)

				return &mockBackoff{
					delay: time.Millisecond,
				}
			},
		),
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	go func() {
		for acquirer.acquireCalls < 4 {
			time.Sleep(time.Millisecond)
		}

		cancel()
	}()

	err := consumer.Serve(ctx)

	if !errors.Is(
		err,
		context.Canceled,
	) {
		t.Fatalf(
			"expected %v, got %v",
			context.Canceled,
			err,
		)
	}

	if factoryCalls.Load() != 4 {
		t.Fatalf(
			"expected 4 backoffs, got %d",
			factoryCalls.Load(),
		)
	}
	if acquirer.acquireCalls < 4 {
		t.Fatalf(
			"expected at least 4 acquire calls, got %d",
			acquirer.acquireCalls,
		)
	}
}

func TestConsumer_Serve_CancelStopsWorkers(t *testing.T) {
	acquirer := &mockTaskAcquirer{
		err: storage.ErrNoTaskAvailable,
	}

	runner := &mockTaskRunner{}

	consumer := newTestConsumer(
		t,
		acquirer,
		runner,
		WithConcurrency[testData](4),
		WithBackoffFactory[testData](
			func() Backoff {
				return &mockBackoff{
					delay: time.Millisecond,
				}
			},
		),
	)

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	done := make(chan error, 1)

	go func() {
		done <- consumer.Serve(ctx)
	}()

	// Laisse le temps aux workers de démarrer.
	time.Sleep(10 * time.Millisecond)

	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf(
				"expected %v, got %v",
				context.Canceled,
				err,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("Serve() did not stop after context cancellation")
	}
}
