package payment

import (
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending     Status = "pending"
	StatusProcessing  Status = "processing"
	StatusRetrying    Status = "retrying"
	StatusSucceeded   Status = "succeeded"
	StatusFailedFinal Status = "failed_final"
)

type Payment struct {
	ID             uuid.UUID `json:"id"`
	IdempotencyKey string    `json:"idempotency_key"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	Status         Status    `json:"status"`
	Attempts       int       `json:"attempts"`
	LastError      *string   `json:"last_error,omitempty"`
	ProviderRef    *string   `json:"provider_ref,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}
