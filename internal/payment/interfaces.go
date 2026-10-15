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
	ResetForRetry(ctx context.Context, id uuid.UUID) error
	Delete(ctx context.Context, id uuid.UUID) error
}

// StaleFinder finds payments stuck in pending state. Separated from Repository
// because only the sweeper needs this — keeps the core interface focused.
type StaleFinder interface {
	FindStalePending(ctx context.Context, olderThanSeconds int) ([]StalePending, error)
}
