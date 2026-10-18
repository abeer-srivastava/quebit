package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

// Reaper periodically returns payments whose "processing" lease expired back to
// the pending state. It is the safety net for a worker that crashes between
// charging the provider and persisting the result — without it, such a payment
// would remain in "processing" forever.
//
// The store clears the payment's outbox marker as part of the same UPDATE, so
// the relay re-publishes it; the reaper does not enqueue anything itself.
//
// Re-charging is safe: the provider call carries a per-payment idempotency key,
// so a replayed charge for the same payment is de-duplicated by the provider.
type Reaper struct {
	store    payment.LeaseReclaimer
	interval time.Duration
	log      *slog.Logger
}

// ReaperOption customizes a Reaper; unset fields keep the defaults above.
type ReaperOption func(*Reaper)

// WithReapInterval overrides how often the reaper reclaims expired leases.
func WithReapInterval(d time.Duration) ReaperOption {
	return func(r *Reaper) { r.interval = d }
}

func NewReaper(store payment.LeaseReclaimer, log *slog.Logger, opts ...ReaperOption) *Reaper {
	r := &Reaper{
		store:    store,
		interval: 30 * time.Second,
		log:      log,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run starts the reap loop. It blocks until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.log.Info("reaper started", "interval", r.interval, "lease_secs", int(payment.DefaultProcessingLease.Seconds()))

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
	if len(reclaimed) > 0 {
		r.log.Warn("reaper: reclaimed expired processing leases", "count", len(reclaimed))
	}
}
