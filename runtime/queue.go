package runtime

// Queue identifies the execution queue of a task.
//
// Queues are used by workers to select which tasks they process.
// They are independent from workflows: multiple workflows may share
// the same queue, and a workflow may choose the queue in which its
// tasks are enqueued.
type Queue string

const (
	// DefaultQueue is the default execution queue.
	DefaultQueue Queue = "default"
)
