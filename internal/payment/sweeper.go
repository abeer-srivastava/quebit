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
