package test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

// markerIsNull reports whether the payment still needs publishing.
func markerIsNull(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) bool {
	t.Helper()
	var isNull bool
	if err := pool.QueryRow(context.Background(),
		`SELECT published_at IS NULL FROM payments WHERE id = $1`, id).Scan(&isNull); err != nil {
		t.Fatalf("read published_at: %v", err)
	}
	return isNull
}

func createPending(t *testing.T, store *payment.Store, id uuid.UUID, key string, amount int64) {
	t.Helper()
	if _, _, err := store.Create(context.Background(), &payment.Payment{
		ID: id, IdempotencyKey: key, Amount: amount, Currency: "USD",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
}

// TestPublishPendingMarksPublished covers the relay's happy path: a freshly
// created payment is unpublished, and draining the outbox publishes it exactly
// once.
func TestPublishPendingMarksPublished(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	defer cleanupPayment(t, pool, id)
	createPending(t, store, id, "test-outbox-publish-"+id.String(), 4200)

	if !markerIsNull(t, pool, id) {
		t.Fatal("expected a freshly created payment to be unpublished")
	}

	seen := map[uuid.UUID]int64{}
	if _, err := store.PublishPending(ctx, 10_000, func(pid uuid.UUID, amount int64) error {
		seen[pid] = amount
		return nil
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if amount, ok := seen[id]; !ok || amount != 4200 {
		t.Fatalf("expected the payment to be published with amount 4200, saw %v", seen)
	}
	if markerIsNull(t, pool, id) {
		t.Fatal("expected the payment to be marked published")
	}

	// A second drain must not publish it again.
	again := false
	if _, err := store.PublishPending(ctx, 10_000, func(pid uuid.UUID, _ int64) error {
		if pid == id {
			again = true
		}
		return nil
	}); err != nil {
		t.Fatalf("second publish: %v", err)
	}
	if again {
		t.Fatal("expected a published payment not to be published again")
	}
}

// TestPublishPendingLeavesMarkerOnError verifies that a payment whose publish
// fails is left unpublished so it is retried on the next drain.
func TestPublishPendingLeavesMarkerOnError(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	defer cleanupPayment(t, pool, id)
	createPending(t, store, id, "test-outbox-error-"+id.String(), 4200)

	if _, err := store.PublishPending(ctx, 10_000, func(pid uuid.UUID, _ int64) error {
		if pid == id {
			return context.DeadlineExceeded // stand-in for a queue outage
		}
		return nil
	}); err == nil {
		t.Fatal("expected the publish error to be returned")
	}

	if !markerIsNull(t, pool, id) {
		t.Fatal("expected a failed publish to leave the payment unpublished")
	}
}

// TestResetStalePendingRequeues covers the sweeper: a payment that has been
// pending past the threshold is marked for re-publication, while a fresh one is
// left alone.
func TestResetStalePendingRequeues(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	defer cleanupPayment(t, pool, id)
	createPending(t, store, id, "test-outbox-sweep-"+id.String(), 4200)

	if _, err := store.PublishPending(ctx, 10_000, func(uuid.UUID, int64) error { return nil }); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Freshly published: not stale, so the marker must be left set.
	if _, err := store.ResetStalePending(ctx, 3600); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if markerIsNull(t, pool, id) {
		t.Fatal("expected a freshly published payment not to be reset")
	}

	// Backdate the row so it looks abandoned, then sweep.
	if _, err := pool.Exec(ctx,
		`UPDATE payments SET updated_at = now() - interval '1 hour' WHERE id = $1`, id); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	if _, err := store.ResetStalePending(ctx, 60); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if !markerIsNull(t, pool, id) {
		t.Fatal("expected a stale pending payment to be marked for re-publication")
	}
}

// TestRetryAndReaperClearOutboxMarker verifies that the paths which take a
// payment back to pending also request a re-publish.
func TestRetryAndReaperClearOutboxMarker(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	t.Run("manual retry", func(t *testing.T) {
		id := uuid.New()
		defer cleanupPayment(t, pool, id)
		createPending(t, store, id, "test-outbox-retry-"+id.String(), 4200)

		if _, err := store.PublishPending(ctx, 10_000, func(uuid.UUID, int64) error { return nil }); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if err := store.MarkFailedFinal(ctx, id, "boom"); err != nil {
			t.Fatalf("mark failed_final: %v", err)
		}
		if _, err := store.ResetForRetry(ctx, id); err != nil {
			t.Fatalf("reset for retry: %v", err)
		}
		if !markerIsNull(t, pool, id) {
			t.Fatal("expected a manual retry to request a re-publish")
		}
	})

	t.Run("expired lease", func(t *testing.T) {
		id := uuid.New()
		defer cleanupPayment(t, pool, id)
		createPending(t, store, id, "test-outbox-reaper-"+id.String(), 4200)

		if _, err := store.PublishPending(ctx, 10_000, func(uuid.UUID, int64) error { return nil }); err != nil {
			t.Fatalf("publish: %v", err)
		}
		if _, err := store.Claim(ctx, id); err != nil {
			t.Fatalf("claim: %v", err)
		}
		if _, err := pool.Exec(ctx,
			`UPDATE payments SET locked_until = now() - interval '1 hour' WHERE id = $1`, id); err != nil {
			t.Fatalf("expire lease: %v", err)
		}
		if _, err := store.ReclaimExpiredLeases(ctx); err != nil {
			t.Fatalf("reclaim: %v", err)
		}
		if !markerIsNull(t, pool, id) {
			t.Fatal("expected the reaper to request a re-publish")
		}
	})
}
