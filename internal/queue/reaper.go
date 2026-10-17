package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

// Reaper periodically returns payments whose "processing" lease expired back to
// the pending state and re-enqueues them. It is the safety net for a worker
// that crashes between charging the provider and persisting the result —
// without it, such a payment would remain in "processing" forever.
//
// Re-charging is safe: the provider call carries a per-payment idempotency key,
// so a replayed charge for the same payment is de-duplicated by the provider.
type Reaper struct {
	store    payment.LeaseReclaimer
	client   Enqueuer
	interval time.Duration
	log      *slog.Logger
}

func NewReaper(store payment.LeaseReclaimer, client Enqueuer, log *slog.Logger) *Reaper {
	return &Reaper{
		store:    store,
		client:   client,
		interval: 30 * time.Second,
		log:      log,
	}
}

// Run starts the reap loop. It blocks until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.log.Info("reaper started", "interval", r.interval, "lease", payment.DefaultProcessingLease)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reaper stopped")
			return nil
		case <-ticker.C:
			r.reap(ctx)
		}
	}
}

func (r *Reaper) reap(ctx context.Context) {
	reclaimed, err := r.store.ReclaimExpiredLeases(ctx)
	if err != nil {
		r.log.Error("reaper: failed to reclaim expired leases", "err", err)
		return
	}
	if len(reclaimed) == 0 {
		return
	}

	r.log.Warn("reaper: reclaimed expired processing leases", "count", len(reclaimed))

	for _, sp := range reclaimed {
		if err := r.client.EnqueueProcess(ctx, sp.ID, sp.Amount); err != nil {
			r.log.Error("reaper: failed to re-enqueue", "id", sp.ID, "err", err)
			continue
		}
		r.log.Info("reaper: re-enqueued", "id", sp.ID)
	}
}
