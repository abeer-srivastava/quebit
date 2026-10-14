package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

// Sweeper periodically re-enqueues payments stuck in "pending" state.
// This handles the gap where a DB insert succeeded but the Redis enqueue failed.
type Sweeper struct {
	store    *payment.Store
	client   *Client
	interval time.Duration
	staleAge int // seconds a payment must be pending before it's considered stale
	log      *slog.Logger
}

func NewSweeper(store *payment.Store, client *Client, log *slog.Logger) *Sweeper {
	return &Sweeper{
		store:    store,
		client:   client,
		interval: 30 * time.Second,
		staleAge: 60,
		log:      log,
	}
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
	stale, err := s.store.FindStalePending(ctx, s.staleAge)
	if err != nil {
		s.log.Error("sweeper: failed to find stale payments", "err", err)
		return
	}

	if len(stale) == 0 {
		return
	}

	s.log.Info("sweeper: found stale pending payments", "count", len(stale))

	for _, sp := range stale {
		if err := s.client.EnqueueProcess(ctx, sp.ID, sp.Amount); err != nil {
			s.log.Error("sweeper: failed to enqueue", "id", sp.ID, "err", err)
			continue
		}
		s.log.Info("sweeper: re-enqueued stale payment", "id", sp.ID)
	}
}
