package payment

import (
	"context"

	"github.com/google/uuid"
)

// Repository defines all payment persistence operations.
// Implementations: *Store (Postgres). Test doubles can mock this interface.
type Repository interface {
	Create(ctx context.Context, p *Payment) (*Payment, bool, error)
	Get(ctx context.Context, id uuid.UUID) (*Payment, error)
	GetByKey(ctx context.Context, key string) (*Payment, error)
	Claim(ctx context.Context, id uuid.UUID) (*Payment, error)
	MarkSucceeded(ctx context.Context, id uuid.UUID, providerRef string) error
	MarkRetrying(ctx context.Context, id uuid.UUID, msg string) error
	MarkFailedFinal(ctx context.Context, id uuid.UUID, msg string) error
	ResetForRetry(ctx context.Context, id uuid.UUID) (*Payment, error)
	Delete(ctx context.Context, id uuid.UUID) error
}

// OutboxRelay drains the transactional outbox: it hands payments that have been
// persisted but not yet queued to a publish callback. Separated from Repository
// because only the relay needs it.
type OutboxRelay interface {
	PublishPending(ctx context.Context, limit int, publish func(id uuid.UUID, amount int64) error) (int, error)
}

// OutboxResetter requests a re-publish of payments whose queued task may have
// been lost. Separated from Repository because only the sweeper needs it.
type OutboxResetter interface {
	ResetStalePending(ctx context.Context, olderThanSeconds int) (int64, error)
}

// LeaseReclaimer returns payments whose processing lease has expired to the
// pending state. Separated from Repository because only the reaper needs it.
type LeaseReclaimer interface {
	ReclaimExpiredLeases(ctx context.Context) ([]PendingPayment, error)
}
