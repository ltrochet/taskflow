package pgsql

import (
	"encoding/json"

	"github.com/ltrochet/taskflow/runtime"
)

func marshalTask[T any](task *runtime.Task[T]) (*taskRecord, error) {
	data, err := json.Marshal(task.Data)
	if err != nil {
		return nil, err
	}

	return &taskRecord{
		ID:       task.ID,
		Workflow: task.Workflow,
		Queue:    string(task.Queue),
		State:    task.State,
		Status:   string(task.Status),
		Data:     data,
		Version:  task.Version,
	}, nil
}

func unmarshalTask[T any](record *taskRecord) (*runtime.Task[T], error) {
	var data T

	if err := json.Unmarshal(record.Data, &data); err != nil {
		return nil, err
	}

	return &runtime.Task[T]{
		ID:       record.ID,
		Workflow: record.Workflow,
		Queue:    runtime.Queue(record.Queue),
		State:    record.State,
		Status:   runtime.Status(record.Status),
		Data:     data,
		Version:  record.Version,
	}, nil
}
