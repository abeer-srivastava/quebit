package payment

import (
	"context"

	"github.com/google/uuid"
)

type StalePending struct {
	ID     uuid.UUID
	Amount int64
}

// FindStalePending returns payments stuck in "pending" for longer than the given interval.
// These are payments where the DB insert succeeded but the queue enqueue failed.
func (s *Store) FindStalePending(ctx context.Context, olderThanSeconds int) ([]StalePending, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, amount FROM payments
		WHERE status = 'pending'
		AND created_at < now() - make_interval(secs => $1)
		ORDER BY created_at
		LIMIT 100`,
		olderThanSeconds,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []StalePending
	for rows.Next() {
		var sp StalePending
		if err := rows.Scan(&sp.ID, &sp.Amount); err != nil {
			return nil, err
		}
		result = append(result, sp)
	}
	return result, rows.Err()
}

// ReclaimExpiredLeases returns payments whose "processing" lease has expired
// back to "pending", and reports them so they can be re-enqueued. This is the
// safety net for the crash window where a worker charged the provider but died
// before persisting the result: without it the payment would stay in
// "processing" forever.
//
// The UPDATE is safe to run concurrently because it only matches rows whose
// lease is still expired; a worker that re-claims the row first clears
// locked_until and is no longer matched.
func (s *Store) ReclaimExpiredLeases(ctx context.Context) ([]StalePending, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE payments
		SET status = 'pending',
		    last_error = 'processing lease expired; re-queued',
		    locked_until = NULL,
		    published_at = NULL,
		    updated_at = now()
		WHERE status = 'processing'
		  AND locked_until IS NOT NULL
		  AND locked_until < now()
		RETURNING id, amount`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []StalePending
	for rows.Next() {
		var sp StalePending
		if err := rows.Scan(&sp.ID, &sp.Amount); err != nil {
			return nil, err
		}
		result = append(result, sp)
	}
	return result, rows.Err()
}
