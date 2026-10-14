package payment

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound    = errors.New("payment not found")
	ErrAlreadyDone = errors.New("payment already in terminal state")
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

const cols = `id, idempotency_key, amount, currency, status, attempts, last_error, created_at, updated_at`

func scan(row pgx.Row) (*Payment, error) {
	var p Payment
	err := row.Scan(
		&p.ID, &p.IdempotencyKey, &p.Amount, &p.Currency,
		&p.Status, &p.Attempts, &p.LastError, &p.CreatedAt, &p.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// Create inserts a new payment. If the idempotency key already exists,
// it returns the existing row with created=false instead of an error.
func (s *Store) Create(ctx context.Context, p *Payment) (*Payment, bool, error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO payments (id, idempotency_key, amount, currency, status)
		VALUES ($1, $2, $3, $4, 'pending')
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+cols,
		p.ID, p.IdempotencyKey, p.Amount, p.Currency,
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
	return existing, false, err
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

// Claim atomically moves a payment to "processing" and increments attempts.
// Returns ErrAlreadyDone if the payment is already succeeded or failed_final,
// preventing a duplicate task delivery from charging twice.
func (s *Store) Claim(ctx context.Context, id uuid.UUID) (*Payment, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE payments
		SET status = 'processing', attempts = attempts + 1, updated_at = now()
		WHERE id = $1 AND status NOT IN ('succeeded', 'failed_final')
		RETURNING `+cols,
		id,
	)
	p, err := scan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrAlreadyDone
	}
	return p, err
}

func (s *Store) setStatus(ctx context.Context, id uuid.UUID, st Status, errMsg *string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE payments SET status = $2, last_error = $3, updated_at = now() WHERE id = $1`,
		id, st, errMsg,
	)
	return err
}

func (s *Store) MarkSucceeded(ctx context.Context, id uuid.UUID) error {
	return s.setStatus(ctx, id, StatusSucceeded, nil)
}

func (s *Store) MarkRetrying(ctx context.Context, id uuid.UUID, msg string) error {
	return s.setStatus(ctx, id, StatusRetrying, &msg)
}

func (s *Store) MarkFailedFinal(ctx context.Context, id uuid.UUID, msg string) error {
	return s.setStatus(ctx, id, StatusFailedFinal, &msg)
}

// ResetForRetry is used by the manual replay endpoint to put a
// failed_final payment back into pending so it can be re-enqueued.
func (s *Store) ResetForRetry(ctx context.Context, id uuid.UUID) error {
	return s.setStatus(ctx, id, StatusPending, nil)
}

// Delete removes a payment by ID. Used for test cleanup.
func (s *Store) Delete(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM payments WHERE id = $1`, id)
	return err
}
