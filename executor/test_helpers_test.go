package executor

import (
	"github.com/google/uuid"
	"github.com/ltrochet/taskflow/runtime"
)

type testData struct {
	Counter int
}

func newTestTask() *runtime.Task[testData] {
	return &runtime.Task[testData]{
		ID:       uuid.New(),
		Workflow: "test",
		Queue:    runtime.DefaultQueue,
		State:    "start",
		Status:   runtime.StatusPending,
		Version:  0,
	}
}
