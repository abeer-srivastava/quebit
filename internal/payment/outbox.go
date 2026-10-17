package payment

import (
	"context"

	"github.com/google/uuid"
)

// PublishPending drains the transactional outbox: it claims up to limit
// unpublished payments in a single transaction and calls publish for each,
// recording the ones that succeed.
//
// Publishing happens inside the transaction, so a payment is only marked
// published once its task is actually on the queue. A publish error stops the
// batch: the rows already handed over are committed and the rest are retried on
// the next drain. If the process dies after publishing but before the commit,
// the payment is simply published again next time — that is safe because tasks
// carry a TaskID and the worker's Claim guard makes processing idempotent.
//
// FOR UPDATE SKIP LOCKED lets more than one relay run at once without
// contending: a row locked by a peer is skipped rather than waited on.
//
// It returns the number of payments published and, if a publish callback
// failed, that error (the already-published rows are still committed).
func (s *Store) PublishPending(ctx context.Context, limit int, publish func(id uuid.UUID, amount int64) error) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op once committed

	rows, err := tx.Query(ctx, `
		SELECT id, amount FROM payments
		WHERE published_at IS NULL
		  AND status IN ('pending', 'retrying')
		ORDER BY created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		limit,
	)
	if err != nil {
		return 0, err
	}

	items := make([]StalePending, 0, limit)
	for rows.Next() {
		var it StalePending
		if err := rows.Scan(&it.ID, &it.Amount); err != nil {
			rows.Close()
			return 0, err
		}
		items = append(items, it)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	published := 0
	var publishErr error
	for _, it := range items {
		if err := publish(it.ID, it.Amount); err != nil {
			publishErr = err
			break
		}
		published++
	}

	if published > 0 {
		ids := make([]uuid.UUID, published)
		for i, it := range items[:published] {
			ids[i] = it.ID
		}
		if _, err := tx.Exec(ctx, `UPDATE payments SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return published, publishErr
}

// ResetStalePending clears the outbox marker on payments that have been pending
// for longer than olderThanSeconds, so the relay hands them to the queue again.
//
// This is the safety net for a task that was published and then lost — for
// example because Redis was flushed — leaving the payment pending with no
// worker working on it. It only touches rows whose marker is still set, so a
// payment that already awaits re-publication is not reset twice.
func (s *Store) ResetStalePending(ctx context.Context, olderThanSeconds int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE payments
		SET published_at = NULL
		WHERE published_at IS NOT NULL
		  AND status = 'pending'
		  AND updated_at < now() - make_interval(secs => $1)`,
		olderThanSeconds,
	)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
