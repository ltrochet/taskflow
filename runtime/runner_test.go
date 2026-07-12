package runtime

import (
	"context"
	"errors"
	"testing"

	"git.infra.sas.ina/an/gamma/taskflow.git/workflow"
	"github.com/google/uuid"
)

type testData struct {
	Counter int
}

func newTestRunner(t *testing.T) *Runner[testData] {
	t.Helper()

	builder := workflow.New[testData]("test")

	builder.
		State(
			"start",
			func(
				ctx context.Context,
				data *testData,
			) (workflow.Event, error) {
				data.Counter++

				return workflow.Success, nil
			},
		).
		Complete()

	builder.Initial("start")

	wf, err := builder.Build()
	if err != nil {
		t.Fatalf("Build() failed: %v", err)
	}

	return NewRunner(wf)
}

func TestRunner_NewTask(t *testing.T) {
	runner := newTestRunner(t)

	task := runner.NewTask(
		testData{},
		DefaultQueue,
	)

	if task.ID == uuid.Nil {
		t.Fatalf("unexpected ID: %s", task.ID)
	}

	if task.State != "start" {
		t.Fatalf("unexpected state: %s", task.State)
	}

	if task.Status != StatusPending {
		t.Fatalf("unexpected status: %s", task.Status)
	}

	if task.Version != 0 {
		t.Fatalf("unexpected version: %d", task.Version)
	}
}

func TestRunner_Step(t *testing.T) {
	runner := newTestRunner(t)

	task := runner.NewTask(
		testData{},
		DefaultQueue,
	)

	result, err := runner.Step(
		context.Background(),
		task,
	)
	if err != nil {
		t.Fatalf("Step() failed: %v", err)
	}

	if task.Data.Counter != 1 {
		t.Fatalf("handler was not executed")
	}

	if task.State != workflow.StateCompleted {
		t.Fatalf("unexpected state: %s", task.State)
	}

	if result.PreviousState != "start" {
		t.Fatalf("unexpected previous state: %s", result.PreviousState)
	}

	if result.NextState != workflow.StateCompleted {
		t.Fatalf("unexpected next state: %s", result.NextState)
	}

	if result.Event != workflow.Success {
		t.Fatalf("unexpected event: %s", result.Event)
	}

	if !result.Completed {
		t.Fatal("workflow should be completed")
	}
}

func TestRunner_UnknownState(t *testing.T) {
	runner := newTestRunner(t)

	task := runner.NewTask(
		testData{},
		DefaultQueue,
	)

	task.State = "unknown"

	_, err := runner.Step(
		context.Background(),
		task,
	)

	if !errors.Is(err, ErrUnknownState) {
		t.Fatalf("expected ErrUnknownState, got %v", err)
	}
}

func TestRunner_InvalidTransition(t *testing.T) {
	builder := workflow.New[testData]("test")

	builder.
		State(
			"start",
			func(
				ctx context.Context,
				data *testData,
			) (workflow.Event, error) {
				return workflow.Failure, nil
			},
		).
		Complete()

	builder.Initial("start")

	wf, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(wf)

	task := runner.NewTask(
		testData{},
		DefaultQueue,
	)

	_, err = runner.Step(
		context.Background(),
		task,
	)

	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected ErrInvalidTransition, got %v", err)
	}
}

func TestRunner_HandlerError(t *testing.T) {
	expected := errors.New("boom")

	builder := workflow.New[testData]("test")

	builder.State(
		"start",
		func(
			ctx context.Context,
			data *testData,
		) (workflow.Event, error) {
			return "", expected
		},
	)

	builder.Initial("start")

	wf, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}

	runner := NewRunner(wf)

	task := runner.NewTask(
		testData{},
		DefaultQueue,
	)

	_, err = runner.Step(
		context.Background(),
		task,
	)

	if !errors.Is(err, expected) {
		t.Fatalf("expected handler error, got %v", err)
	}

	if task.State != "start" {
		t.Fatal("state should not have changed")
	}
}

func TestRunner_TaskCompleted(t *testing.T) {
	runner := newTestRunner(t)

	task := runner.NewTask(
		testData{},
		DefaultQueue,
	)

	task.State = workflow.StateCompleted

	_, err := runner.Step(
		context.Background(),
		task,
	)

	if !errors.Is(err, ErrTaskCompleted) {
		t.Fatalf("expected ErrTaskCompleted, got %v", err)
	}
}
