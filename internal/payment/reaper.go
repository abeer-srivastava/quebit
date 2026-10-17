package payment

import (
	"context"

	"github.com/google/uuid"
)

// PendingPayment names a payment that needs to be (re)published or
// reprocessed — just the id and amount, which is all the queue needs to route
// the task.
type PendingPayment struct {
	ID     uuid.UUID
	Amount int64
}

// ReclaimExpiredLeases returns payments whose "processing" lease has expired
// back to "pending", and reports them. This is the safety net for the crash
// window where a worker charged the provider but died before persisting the
// result: without it the payment would stay in "processing" forever.
//
// Clearing published_at hands the payment back to the outbox relay, so the
// caller does not need to enqueue it directly.
//
// The UPDATE is safe to run concurrently because it only matches rows whose
// lease is still expired; a worker that re-claims the row first clears
// locked_until and is no longer matched.
func (s *Store) ReclaimExpiredLeases(ctx context.Context) ([]PendingPayment, error) {
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

	var result []PendingPayment
	for rows.Next() {
		var sp PendingPayment
		if err := rows.Scan(&sp.ID, &sp.Amount); err != nil {
			return nil, err
		}
		result = append(result, sp)
	}
	return result, rows.Err()
}
