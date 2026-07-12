package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/ltrochet/taskflow/runtime"
)

type mockTaskAcquirer struct {
	acquireCalls int
	lastQueues   []runtime.Queue

	task *runtime.Task[testData]
	err  error
}

func (m *mockTaskAcquirer) Acquire(
	ctx context.Context,
	queues ...runtime.Queue,
) (*runtime.Task[testData], error) {
	m.acquireCalls++

	m.lastQueues = append(
		[]runtime.Queue(nil),
		queues...,
	)

	if m.err != nil {
		return nil, m.err
	}

	return m.task, nil
}

type mockTaskRunner struct {
	runCalls int
	lastTask *runtime.Task[testData]

	err error
}

func (m *mockTaskRunner) Run(
	ctx context.Context,
	task *runtime.Task[testData],
) error {
	m.runCalls++
	m.lastTask = task

	return m.err
}

func TestConsumer_Consume(t *testing.T) {
	task := &runtime.Task[testData]{}

	acquirer := &mockTaskAcquirer{
		task: task,
	}

	runner := &mockTaskRunner{}

	consumer := NewConsumer(
		acquirer,
		runner,
		runtime.Queue("priority"),
	)

	err := consumer.Consume(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"Consume() failed: %v",
			err,
		)
	}

	if acquirer.acquireCalls != 1 {
		t.Fatalf(
			"expected 1 acquire, got %d",
			acquirer.acquireCalls,
		)
	}

	if len(acquirer.lastQueues) != 1 {
		t.Fatalf(
			"unexpected queue count: %d",
			len(acquirer.lastQueues),
		)
	}

	if acquirer.lastQueues[0] != runtime.Queue("priority") {
		t.Fatalf(
			"unexpected queue: %s",
			acquirer.lastQueues[0],
		)
	}

	if runner.runCalls != 1 {
		t.Fatalf(
			"expected 1 run, got %d",
			runner.runCalls,
		)
	}

	if runner.lastTask != task {
		t.Fatal(
			"unexpected task executed",
		)
	}
}

func TestConsumer_AcquireError(t *testing.T) {
	expected := errors.New(
		"boom",
	)

	acquirer := &mockTaskAcquirer{
		err: expected,
	}

	runner := &mockTaskRunner{}

	consumer := NewConsumer(
		acquirer,
		runner,
	)

	err := consumer.Consume(
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

	if runner.runCalls != 0 {
		t.Fatalf(
			"expected 0 run, got %d",
			runner.runCalls,
		)
	}
}

func TestConsumer_RunError(t *testing.T) {
	expected := errors.New(
		"boom",
	)

	task := &runtime.Task[testData]{}

	acquirer := &mockTaskAcquirer{
		task: task,
	}

	runner := &mockTaskRunner{
		err: expected,
	}

	consumer := NewConsumer(
		acquirer,
		runner,
	)

	err := consumer.Consume(
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

	if acquirer.acquireCalls != 1 {
		t.Fatalf(
			"expected 1 acquire, got %d",
			acquirer.acquireCalls,
		)
	}

	if runner.runCalls != 1 {
		t.Fatalf(
			"expected 1 run, got %d",
			runner.runCalls,
		)
	}
}
