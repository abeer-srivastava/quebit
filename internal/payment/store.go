package payment

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound    = errors.New("payment not found")
	ErrAlreadyDone = errors.New("payment already in terminal state")
	// ErrNotRetryable is returned when a manual retry targets a payment that is
	// not in the failed_final state.
	ErrNotRetryable = errors.New("only failed_final payments can be retried")
)

// DefaultProcessingLease is how long a worker may hold a payment in the
// "processing" state before the reaper assumes the worker died and returns the
// payment to "pending". It must comfortably exceed the provider timeout (10s)
// plus the Asynq task timeout (30s) so a slow-but-alive worker is never
// reclaimed underneath.
const DefaultProcessingLease = 90 * time.Second

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const cols = `id, idempotency_key, amount, currency, status, attempts, last_error, provider_ref, COALESCE(request_hash, '') AS request_hash, locked_until, created_at, updated_at`

func scan(row pgx.Row) (*Payment, error) {
	var p Payment
	err := row.Scan(
		&p.ID, &p.IdempotencyKey, &p.Amount, &p.Currency,
		&p.Status, &p.Attempts, &p.LastError, &p.ProviderRef,
		&p.RequestHash, &p.LockedUntil, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// Create inserts a new payment. If the idempotency key already exists, it
// returns the existing row with created=false — unless the request parameters
// differ from the original, in which case ErrIdempotencyMismatch is returned.
func (s *Store) Create(ctx context.Context, p *Payment) (*Payment, bool, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO payments (id, idempotency_key, amount, currency, status, request_hash)
		VALUES ($1, $2, $3, $4, 'pending', $5)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+cols,
		p.ID, p.IdempotencyKey, p.Amount, p.Currency, p.RequestHash,
	)

	created, err := scan(row)
	if err == nil {
		return created, true, nil
	}
	// ON CONFLICT DO NOTHING returns no rows — fetch the existing one
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}

	existing, err := s.GetByKey(ctx, p.IdempotencyKey)
	if err != nil {
		return nil, false, err
	}
	// Legacy rows written before request_hash existed have an empty hash; treat
	// them as compatible rather than rejecting every replay.
	if existing.RequestHash != "" && p.RequestHash != "" && existing.RequestHash != p.RequestHash {
		return existing, false, ErrIdempotencyMismatch
	}
	return existing, false, nil
}

func (s *Store) Get(ctx context.Context, id uuid.UUID) (*Payment, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+cols+` FROM payments WHERE id = $1`, id)
	p, err := scan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

func (s *Store) GetByKey(ctx context.Context, key string) (*Payment, error) {
	row := s.pool.QueryRow(ctx, `SELECT `+cols+` FROM payments WHERE idempotency_key = $1`, key)
	p, err := scan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, err
}

// Claim atomically moves a payment from a claimable state ("pending" or
// "retrying") to "processing", increments attempts, and takes a processing
// lease. It returns ErrAlreadyDone for any other state.
//
// Restricting the transition to non-processing states is what makes the
// exactly-one-charge invariant hold: if two workers receive the same task
// concurrently, only the first can move the row out of pending/retrying; the
// second sees no matching row and does nothing. A payment stuck in
// "processing" because its worker died is recovered by the reaper, not by a
// concurrent claim.
func (s *Store) Claim(ctx context.Context, id uuid.UUID) (*Payment, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE payments
		SET status = 'processing',
		    attempts = attempts + 1,
		    locked_until = now() + make_interval(secs => $2::double precision),
		    updated_at = now()
		WHERE id = $1 AND status IN ('pending', 'retrying')
		RETURNING `+cols,
		id, DefaultProcessingLease.Seconds(),
	)
	p, err := scan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAlreadyDone
	}
	return p, err
}

func (s *Store) setStatus(ctx context.Context, id uuid.UUID, st Status, errMsg *string, providerRef *string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE payments SET status = $2, last_error = $3, provider_ref = COALESCE($4, provider_ref), locked_until = NULL, updated_at = now() WHERE id = $1`,
		id, st, errMsg, providerRef,
	)
	return err
}

func (s *Store) MarkSucceeded(ctx context.Context, id uuid.UUID, providerRef string) error {
	return s.setStatus(ctx, id, StatusSucceeded, nil, &providerRef)
}

func (s *Store) MarkRetrying(ctx context.Context, id uuid.UUID, msg string) error {
	return s.setStatus(ctx, id, StatusRetrying, &msg, nil)
}

func (s *Store) MarkFailedFinal(ctx context.Context, id uuid.UUID, msg string) error {
	return s.setStatus(ctx, id, StatusFailedFinal, &msg, nil)
}

// ResetForRetry atomically moves a failed_final payment back to pending and
// returns the updated row, so it can be re-enqueued.
//
// The state check is part of the UPDATE, not a separate read: two concurrent
// retry requests cannot both succeed, because only one can observe
// status='failed_final'. Returns ErrNotFound if the payment does not exist and
// ErrNotRetryable if it is not in failed_final.
func (s *Store) ResetForRetry(ctx context.Context, id uuid.UUID) (*Payment, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE payments
		SET status = 'pending', last_error = NULL, locked_until = NULL, published_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'failed_final'
		RETURNING `+cols,
		id,
	)
	p, err := scan(row)
	if err == nil {
		return p, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	// No row transitioned: distinguish "missing" from "wrong state".
	if _, gerr := s.Get(ctx, id); gerr != nil {
		return nil, gerr
	}
	return nil, ErrNotRetryable
}

// Delete removes a payment by ID. Used for test cleanup.
func (s *Store) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM payments WHERE id = $1`, id)
	return err
}
