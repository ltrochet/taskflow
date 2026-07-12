package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/ltrochet/taskflow/runtime"
	"github.com/ltrochet/taskflow/workflow"
)

type testData struct {
	Counter int
}

type mockTaskUpdater struct {
	updateCalls int
	updateErr   error
	lastTask    *runtime.Task[testData]
}

func (m *mockTaskUpdater) Update(
	ctx context.Context,
	task *runtime.Task[testData],
) error {
	m.updateCalls++

	if m.updateErr != nil {
		return m.updateErr
	}

	task.Version++

	m.lastTask = task

	return nil
}

func newTestComponents(
	t *testing.T,
) (
	*runtime.Runner[testData],
	*Worker[testData],
	*mockTaskUpdater,
) {
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
		t.Fatal(err)
	}

	runner := runtime.NewRunner(wf)
	updater := &mockTaskUpdater{}

	worker := New(
		runner,
		updater,
	)

	return runner, worker, updater
}

func TestWorker_Run(t *testing.T) {
	runner, worker, updater := newTestComponents(t)

	task := runner.NewTask(
		testData{},
		runtime.DefaultQueue,
	)

	err := worker.Run(
		context.Background(),
		task,
	)
	if err != nil {
		t.Fatalf(
			"Run() failed: %v",
			err,
		)
	}

	if task.State != workflow.StateCompleted {
		t.Fatalf(
			"unexpected state: %s",
			task.State,
		)
	}

	if task.Status != runtime.StatusCompleted {
		t.Fatalf(
			"unexpected status: %s",
			task.Status,
		)
	}

	if task.Data.Counter != 1 {
		t.Fatal(
			"handler was not executed",
		)
	}

	if task.Version != 1 {
		t.Fatalf(
			"unexpected version: %d",
			task.Version,
		)
	}

	if updater.updateCalls != 1 {
		t.Fatalf(
			"expected 1 update, got %d",
			updater.updateCalls,
		)
	}

	if updater.lastTask != task {
		t.Fatal(
			"unexpected task updated",
		)
	}
}

func TestWorker_UpdateError(t *testing.T) {
	runner, worker, updater := newTestComponents(t)

	updater.updateErr = errors.New(
		"database error",
	)

	task := runner.NewTask(
		testData{},
		runtime.DefaultQueue,
	)

	err := worker.Run(
		context.Background(),
		task,
	)

	if err == nil {
		t.Fatal(
			"expected error",
		)
	}

	if updater.updateCalls != 1 {
		t.Fatalf(
			"expected 1 update, got %d",
			updater.updateCalls,
		)
	}
}

func TestWorker_HandlerError(t *testing.T) {
	expected := errors.New(
		"boom",
	)

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

	runner := runtime.NewRunner(wf)
	updater := &mockTaskUpdater{}

	worker := New(
		runner,
		updater,
	)

	task := runner.NewTask(
		testData{},
		runtime.DefaultQueue,
	)

	err = worker.Run(
		context.Background(),
		task,
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

	if task.Status != runtime.StatusFailed {
		t.Fatalf(
			"unexpected status: %s",
			task.Status,
		)
	}

	if updater.updateCalls != 1 {
		t.Fatalf(
			"expected 1 update, got %d",
			updater.updateCalls,
		)
	}

	if updater.lastTask != task {
		t.Fatal(
			"unexpected task updated",
		)
	}
}

func TestWorker_RunMultipleSteps(t *testing.T) {
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
		Success("process")

	builder.
		State(
			"process",
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
		t.Fatal(err)
	}

	runner := runtime.NewRunner(wf)
	updater := &mockTaskUpdater{}

	worker := New(
		runner,
		updater,
	)

	task := runner.NewTask(
		testData{},
		runtime.DefaultQueue,
	)

	err = worker.Run(
		context.Background(),
		task,
	)
	if err != nil {
		t.Fatalf(
			"Run() failed: %v",
			err,
		)
	}

	if task.State != workflow.StateCompleted {
		t.Fatalf(
			"unexpected state: %s",
			task.State,
		)
	}

	if task.Status != runtime.StatusCompleted {
		t.Fatalf(
			"unexpected status: %s",
			task.Status,
		)
	}

	if task.Data.Counter != 2 {
		t.Fatalf(
			"expected 2 executions, got %d",
			task.Data.Counter,
		)
	}

	if task.Version != 2 {
		t.Fatalf(
			"expected version 2, got %d",
			task.Version,
		)
	}

	if updater.updateCalls != 2 {
		t.Fatalf(
			"expected 2 updates, got %d",
			updater.updateCalls,
		)
	}
}

func TestWorker_VersionManagedByUpdater(t *testing.T) {
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
		Success("process")

	builder.
		State(
			"process",
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
		t.Fatal(err)
	}

	runner := runtime.NewRunner(wf)
	updater := &mockTaskUpdater{}

	worker := New(
		runner,
		updater,
	)

	task := runner.NewTask(
		testData{},
		runtime.DefaultQueue,
	)

	err = worker.Run(
		context.Background(),
		task,
	)
	if err != nil {
		t.Fatalf(
			"Run() failed: %v",
			err,
		)
	}

	if updater.updateCalls != 2 {
		t.Fatalf(
			"expected 2 updates, got %d",
			updater.updateCalls,
		)
	}

	if task.Version != 2 {
		t.Fatalf(
			"expected version 2, got %d",
			task.Version,
		)
	}

	if task.Data.Counter != 2 {
		t.Fatalf(
			"expected 2 executions, got %d",
			task.Data.Counter,
		)
	}
}
