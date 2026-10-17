package test

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

func TestResetForRetryRejectsNonFailedFinal(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-retry-state-" + id.String()
	defer cleanupPayment(t, pool, id)

	if _, _, err := store.Create(ctx, &payment.Payment{ID: id, IdempotencyKey: key, Amount: 1000, Currency: "USD"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	// A pending payment is not retryable.
	if _, err := store.ResetForRetry(ctx, id); err != payment.ErrNotRetryable {
		t.Fatalf("expected ErrNotRetryable for a pending payment, got %v", err)
	}

	// A missing payment is not found.
	if _, err := store.ResetForRetry(ctx, uuid.New()); err != payment.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// TestResetForRetryIsAtomic ensures concurrent retries cannot both win: the
// failed_final → pending transition must happen exactly once.
func TestResetForRetryIsAtomic(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-retry-atomic-" + id.String()
	defer cleanupPayment(t, pool, id)

	if _, _, err := store.Create(ctx, &payment.Payment{ID: id, IdempotencyKey: key, Amount: 1000, Currency: "USD"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := store.Claim(ctx, id); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := store.MarkFailedFinal(ctx, id, "boom"); err != nil {
		t.Fatalf("mark failed_final: %v", err)
	}

	const workers = 8
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := store.ResetForRetry(ctx, id)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	succeeded := 0
	for err := range results {
		switch err {
		case nil:
			succeeded++
		case payment.ErrNotRetryable:
			// expected for the losers
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected exactly one successful reset, got %d", succeeded)
	}
}
