package workflow

import (
	"context"
	"errors"
	"testing"
)

type testData struct {
	Value int
}

func TestWorkflow_CompileWithCompletedState(t *testing.T) {
	builder := New[testData]("test")

	builder.
		State(
			"start",
			func(
				ctx context.Context,
				data *testData,
			) (Event, error) {
				return Success, nil
			},
		).
		Complete()

	builder.Initial("start")

	wf, err := builder.Build()
	if err != nil {
		t.Fatalf(
			"Build() failed: %v",
			err,
		)
	}

	next, ok := wf.Next(
		"start",
		Success,
	)

	if !ok {
		t.Fatal(
			"expected transition",
		)
	}

	if next != StateCompleted {
		t.Fatalf(
			"unexpected next state: %s",
			next,
		)
	}

	if !wf.IsTerminal(next) {
		t.Fatal(
			"expected terminal state",
		)
	}
}

func TestWorkflow_SystemStatesHaveNoHandler(t *testing.T) {
	builder := New[testData]("test")

	builder.
		State(
			"start",
			func(
				ctx context.Context,
				data *testData,
			) (Event, error) {
				return Success, nil
			},
		).
		Complete()

	builder.Initial("start")

	wf, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	_, ok := wf.Handler(
		StateCompleted,
	)

	if ok {
		t.Fatal(
			"system state should not have handler",
		)
	}
}

func TestWorkflow_SystemStatesAreTerminal(t *testing.T) {
	if !IsTerminalState(StateCompleted) {
		t.Fatal(
			"completed should be terminal",
		)
	}

	if !IsTerminalState(StateFailed) {
		t.Fatal(
			"failed should be terminal",
		)
	}

	if IsTerminalState("start") {
		t.Fatal(
			"business state should not be terminal",
		)
	}
}

func TestWorkflow_InvalidDestinationState(t *testing.T) {
	builder := New[testData]("test")

	builder.
		State(
			"start",
			func(
				ctx context.Context,
				data *testData,
			) (Event, error) {
				return Success, nil
			},
		).
		Success("unknown")

	builder.Initial("start")

	_, err := builder.Build()

	if !errors.Is(
		err,
		ErrUnknownState,
	) {
		t.Fatalf(
			"expected ErrUnknownState, got %v",
			err,
		)
	}
}

func TestWorkflow_CanTransition(t *testing.T) {
	builder := New[testData]("test")

	builder.
		State(
			"start",
			func(
				ctx context.Context,
				data *testData,
			) (Event, error) {
				return Success, nil
			},
		).
		Complete()

	builder.Initial("start")

	wf, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	if !wf.CanTransition(
		"start",
		Success,
	) {
		t.Fatal(
			"expected transition",
		)
	}

	if wf.CanTransition(
		"start",
		Failure,
	) {
		t.Fatal(
			"unexpected transition",
		)
	}
}

func TestWorkflow_CannotDeclareReservedState(t *testing.T) {
	builder := New[testData]("test")

	builder.State(
		StateCompleted,
		func(
			ctx context.Context,
			data *testData,
		) (Event, error) {
			return Success, nil
		},
	)

	builder.Initial(StateCompleted)

	_, err := builder.Build()

	if !errors.Is(err, ErrReservedState) {
		t.Fatalf(
			"expected ErrReservedState, got %v",
			err,
		)
	}
}
