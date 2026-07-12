package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/ltrochet/taskflow/runtime"
	"github.com/ltrochet/taskflow/workflow"
)

type testData struct {
	Value string
}

func newTask(status runtime.Status) *runtime.Task[testData] {
	return &runtime.Task[testData]{
		ID:       uuid.New(),
		Workflow: "test",
		Queue:    runtime.DefaultQueue,

		State:   "start",
		Status:  status,
		Data:    testData{Value: "initial"},
		Version: 0,
	}
}

func TestRepository_CreateAndGet(t *testing.T) {
	repo := New[testData]()

	task := newTask(
		runtime.StatusPending,
	)

	err := repo.Create(
		context.Background(),
		task,
	)
	if err != nil {
		t.Fatalf(
			"Create() failed: %v",
			err,
		)
	}

	got, err := repo.Get(
		context.Background(),
		task.ID,
	)
	if err != nil {
		t.Fatalf(
			"Get() failed: %v",
			err,
		)
	}

	if got.ID != task.ID {
		t.Fatalf(
			"unexpected id: %s",
			got.ID,
		)
	}

	if got.Data.Value != "initial" {
		t.Fatalf(
			"unexpected data: %s",
			got.Data.Value,
		)
	}

	if got.Queue != runtime.DefaultQueue {
		t.Fatalf(
			"unexpected queue: %s",
			got.Queue,
		)
	}
}

func TestRepository_CreateDuplicate(t *testing.T) {
	repo := New[testData]()

	task := newTask(
		runtime.StatusPending,
	)

	_ = repo.Create(
		context.Background(),
		task,
	)

	err := repo.Create(
		context.Background(),
		task,
	)

	if !errors.Is(
		err,
		ErrTaskAlreadyExists,
	) {
		t.Fatalf(
			"expected ErrTaskAlreadyExists, got %v",
			err,
		)
	}
}

func TestRepository_GetReturnsCopy(t *testing.T) {
	repo := New[testData]()

	task := newTask(
		runtime.StatusPending,
	)

	_ = repo.Create(
		context.Background(),
		task,
	)

	got, err := repo.Get(
		context.Background(),
		task.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	if got.ID != task.ID {
		t.Fatalf(
			"unexpected id: %s",
			got.ID,
		)
	}

	got.State = "modified"
	got.Queue = "modified"
	got.Data.Value = "changed"

	stored, err := repo.Get(
		context.Background(),
		task.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	if stored.State != "start" {
		t.Fatal(
			"repository was modified without Update()",
		)
	}

	if stored.Queue != runtime.DefaultQueue {
		t.Fatal(
			"repository queue was modified without Update()",
		)
	}

	if stored.Data.Value != "initial" {
		t.Fatal(
			"repository data was modified without Update()",
		)
	}
}

func TestRepository_Update(t *testing.T) {
	repo := New[testData]()

	task := newTask(
		runtime.StatusPending,
	)

	_ = repo.Create(
		context.Background(),
		task,
	)

	task.State = workflow.StateCompleted
	task.Status = runtime.StatusCompleted
	task.Version = 1

	err := repo.Update(
		context.Background(),
		task,
	)
	if err != nil {
		t.Fatal(err)
	}

	got, err := repo.Get(
		context.Background(),
		task.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	if got.Status != runtime.StatusCompleted {
		t.Fatalf(
			"unexpected status: %s",
			got.Status,
		)
	}

	if got.Version != 1 {
		t.Fatalf(
			"unexpected version: %d",
			got.Version,
		)
	}
}

func TestRepository_Acquire(t *testing.T) {
	repo := New[testData]()

	task := newTask(
		runtime.StatusPending,
	)

	_ = repo.Create(
		context.Background(),
		task,
	)

	acquired, err := repo.Acquire(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}

	if acquired.ID != task.ID {
		t.Fatalf(
			"unexpected task: %s",
			acquired.ID,
		)
	}

	if acquired.Status != runtime.StatusRunning {
		t.Fatalf(
			"unexpected status: %s",
			acquired.Status,
		)
	}

	stored, err := repo.Get(
		context.Background(),
		task.ID,
	)
	if err != nil {
		t.Fatal(err)
	}

	if stored.Status != runtime.StatusRunning {
		t.Fatalf(
			"task was not updated: %s",
			stored.Status,
		)
	}
}

func TestRepository_AcquireSpecificQueue(t *testing.T) {
	repo := New[testData]()

	task := newTask(
		runtime.StatusPending,
	)

	task.Queue = runtime.Queue("priority")

	_ = repo.Create(
		context.Background(),
		task,
	)

	_, err := repo.Acquire(
		context.Background(),
		runtime.DefaultQueue,
	)

	if !errors.Is(
		err,
		ErrNoTaskAvailable,
	) {
		t.Fatalf(
			"expected ErrNoTaskAvailable, got %v",
			err,
		)
	}

	acquired, err := repo.Acquire(
		context.Background(),
		runtime.Queue("priority"),
	)
	if err != nil {
		t.Fatal(err)
	}

	if acquired.ID != task.ID {
		t.Fatalf(
			"unexpected task: %s",
			acquired.ID,
		)
	}

	if acquired.Status != runtime.StatusRunning {
		t.Fatalf(
			"unexpected status: %s",
			acquired.Status,
		)
	}
}

func TestRepository_AcquireNoTask(t *testing.T) {
	repo := New[testData]()

	_, err := repo.Acquire(
		context.Background(),
	)

	if !errors.Is(
		err,
		ErrNoTaskAvailable,
	) {
		t.Fatalf(
			"expected ErrNoTaskAvailable, got %v",
			err,
		)
	}
}

func TestRepository_AcquireMultipleQueues(t *testing.T) {
	repo := New[testData]()

	task := newTask(
		runtime.StatusPending,
	)

	task.Queue = runtime.Queue("priority")

	err := repo.Create(
		context.Background(),
		task,
	)
	if err != nil {
		t.Fatal(err)
	}

	acquired, err := repo.Acquire(
		context.Background(),
		runtime.Queue("critical"),
		runtime.Queue("priority"),
	)
	if err != nil {
		t.Fatal(err)
	}

	if acquired.ID != task.ID {
		t.Fatalf(
			"unexpected task: %s",
			acquired.ID,
		)
	}

	if acquired.Queue != runtime.Queue("priority") {
		t.Fatalf(
			"unexpected queue: %s",
			acquired.Queue,
		)
	}

	if acquired.Status != runtime.StatusRunning {
		t.Fatalf(
			"unexpected status: %s",
			acquired.Status,
		)
	}
}
