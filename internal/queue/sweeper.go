package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

// Sweeper periodically re-queues payments that have been stuck in "pending".
//
// It is the safety net for a task that was published and then lost — most
// likely because Redis was flushed or failed over. It clears the outbox marker
// on stale payments in a single statement and lets the relay republish them,
// so recovery throughput is bounded by the relay rather than by this tick.
type Sweeper struct {
	store    payment.OutboxResetter
	interval time.Duration
	staleAge int // seconds a payment must be pending before it's considered stale
	log      *slog.Logger
}

// SweeperOption customizes a Sweeper; unset fields keep the defaults above.
type SweeperOption func(*Sweeper)

// WithSweepInterval overrides how often the sweeper looks for stale pendings.
func WithSweepInterval(d time.Duration) SweeperOption {
	return func(s *Sweeper) { s.interval = d }
}

// WithSweepStaleAge overrides how long (in seconds) a payment may stay
// pending before the sweeper treats it as lost.
func WithSweepStaleAge(d time.Duration) SweeperOption {
	return func(s *Sweeper) { s.staleAge = int(d.Seconds()) }
}

func NewSweeper(store payment.OutboxResetter, log *slog.Logger, opts ...SweeperOption) *Sweeper {
	s := &Sweeper{
		store:    store,
		interval: 30 * time.Second,
		staleAge: 60,
		log:      log,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Run starts the sweep loop. It blocks until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	s.log.Info("sweeper started", "interval", s.interval, "stale_age_secs", s.staleAge)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("sweeper stopped")
			return nil
		case <-ticker.C:
			s.sweep(ctx)
		}
	}
}

func (s *Sweeper) sweep(ctx context.Context) {
	requeued, err := s.store.ResetStalePending(ctx, s.staleAge)
	if err != nil {
		s.log.Error("sweeper: failed to re-queue stale payments", "err", err)
		return
	}
	if requeued > 0 {
		s.log.Warn("sweeper: re-queued stale pending payments", "count", requeued)
	}
}
