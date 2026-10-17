package test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/queue"
)

// fakeOutbox is a test double for payment.OutboxRelay. It hands back a fixed set
// of payments on the first drain and nothing afterwards.
type fakeOutbox struct {
	items []payment.PendingPayment
}

func (f *fakeOutbox) PublishPending(_ context.Context, _ int, publish func(uuid.UUID, int64) error) (int, error) {
	published := 0
	for _, it := range f.items {
		if err := publish(it.ID, it.Amount); err != nil {
			return published, err
		}
		published++
	}
	f.items = nil
	return published, nil
}

// recordingEnqueuer is a test double for queue.Enqueuer that reports each
// published payment on a channel.
type recordingEnqueuer struct {
	ch chan uuid.UUID
}

func (e *recordingEnqueuer) EnqueueProcess(_ context.Context, id uuid.UUID, _ int64) error {
	e.ch <- id
	return nil
}

func TestRelayPublishesOutbox(t *testing.T) {
	id := uuid.New()
	store := &fakeOutbox{items: []payment.PendingPayment{{ID: id, Amount: 4200}}}
	enqueuer := &recordingEnqueuer{ch: make(chan uuid.UUID, 8)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	relay := queue.NewRelay(store, enqueuer, log)
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	select {
	case got := <-enqueuer.ch:
		if got != id {
			t.Fatalf("relay published %s, want %s", got, id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("relay did not publish the payment in time")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("relay returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("relay did not stop after cancellation")
	}
}
