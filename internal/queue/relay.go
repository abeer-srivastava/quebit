package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

// Relay drains the transactional outbox: it polls for payments that have been
// persisted but not yet handed to the queue and publishes them.
//
// This replaces the old "insert, then enqueue, and hope" flow. Because the
// payment row and its unpublished marker are written by the same INSERT, a
// payment can never exist without a relay eventually finding it — the enqueue
// is no longer a step that can be skipped by a crash.
//
// Publishing happens inside the store's transaction; if the relay dies after a
// task is queued but before the marker is committed, the payment is published
// again, which is safe because tasks carry a TaskID and the worker's Claim
// guard makes processing idempotent.
type Relay struct {
	store    payment.OutboxRelay
	client   Enqueuer
	interval time.Duration
	batch    int
	log      *slog.Logger
}

// NewRelay polls quickly: the API answers 202 as soon as the row is committed,
// so any delay here is added request latency.
func NewRelay(store payment.OutboxRelay, client Enqueuer, log *slog.Logger) *Relay {
	return &Relay{
		store:    store,
		client:   client,
		interval: 250 * time.Millisecond,
		batch:    100,
		log:      log,
	}
}

// Run starts the relay loop. It blocks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.log.Info("outbox relay started", "interval", r.interval, "batch", r.batch)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("outbox relay stopped")
			return nil
		case <-ticker.C:
			r.drain(ctx)
		}
	}
}

// drain publishes one batch at a time until the outbox is empty. A short batch
// means the backlog is smaller than the limit, so there is nothing left to do.
func (r *Relay) drain(ctx context.Context) {
	for {
		published, err := r.store.PublishPending(ctx, r.batch, func(id uuid.UUID, amount int64) error {
			return r.client.EnqueueProcess(ctx, id, amount)
		})
		if err != nil {
			r.log.Error("outbox relay: publish failed", "err", err)
			return
		}
		if published > 0 {
			r.log.Debug("outbox relay: published", "count", published)
		}
		if published < r.batch {
			return
		}
	}
}
