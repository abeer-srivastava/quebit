package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

const (
	TypeProcessPayment = "payment:process"

	QueueCritical = "critical"
	QueueDefault  = "default"
	QueueLow      = "low"

	MaxRetry = 8
)

// Amount thresholds for queue routing.
const (
	CriticalAmountThreshold int64 = 10_000
	LowAmountThreshold      int64 = 1_000
)

// QueueFor routes payments by amount:
//
//	>= CriticalAmountThreshold → critical (high concurrency share)
//	<  LowAmountThreshold       → low
//	otherwise                  → default
func QueueFor(amount int64) string {
	switch {
	case amount >= CriticalAmountThreshold:
		return QueueCritical
	case amount < LowAmountThreshold:
		return QueueLow
	default:
		return QueueDefault
	}
}

type ProcessPayload struct {
	PaymentID string `json:"payment_id"`
}

type Client struct {
	c *asynq.Client
}

func NewClient(opt asynq.RedisClientOpt) *Client {
	return &Client{c: asynq.NewClient(opt)}
}

func (c *Client) Close() error {
	return c.c.Close()
}

func (c *Client) EnqueueProcess(ctx context.Context, id uuid.UUID, amount int64) error {
	payload, _ := json.Marshal(ProcessPayload{PaymentID: id.String()})

	_, err := c.c.EnqueueContext(ctx,
		asynq.NewTask(TypeProcessPayment, payload),
		asynq.Queue(QueueFor(amount)),
		asynq.MaxRetry(MaxRetry),
		asynq.TaskID(id.String()), // dedupe at queue level
		asynq.Timeout(30*time.Second),
		asynq.Retention(24*time.Hour),
	)
	if errors.Is(err, asynq.ErrTaskIDConflict) {
		return nil // already queued, safe to ignore
	}
	return err
}
