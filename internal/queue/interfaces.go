package queue

import (
	"context"

	"github.com/google/uuid"
)

// Enqueuer enqueues payment processing tasks.
// Implementations: *Client (asynq). Test doubles can mock this.
type Enqueuer interface {
	EnqueueProcess(ctx context.Context, id uuid.UUID, amount int64) error
}

// TaskReplayer replays archived (failed) tasks.
// Implementations: *Inspector (asynq). Test doubles can mock this.
type TaskReplayer interface {
	ReplayArchivedTask(taskID string) error
}

// Processor processes a single payment by ID.
// This interface breaks the import cycle: queue/handler depends on this
// interface instead of importing the service package directly.
// Implementations: *service.Service.
type Processor interface {
	ProcessPayment(ctx context.Context, id uuid.UUID) error
}

// TerminalError wraps a provider error that should not be retried by Asynq.
// Defined here so queue/handler can check for it without importing service.
type TerminalError struct {
	Cause error
}

func (e *TerminalError) Error() string {
	return e.Cause.Error()
}

func (e *TerminalError) Unwrap() error {
	return e.Cause
}
