package test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

// TestClaimRejectsWhileProcessing verifies that a payment already in
// "processing" cannot be claimed again. This is the guard that makes exactly
// one worker responsible for a payment at a time.
func TestClaimRejectsWhileProcessing(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-claim-guard-" + id.String()
	defer cleanupPayment(t, pool, id)

	if _, _, err := store.Create(ctx, &payment.Payment{ID: id, IdempotencyKey: key, Amount: 1000, Currency: "USD"}); err != nil {
		t.Fatalf("create: %v", err)
	}

	first, err := store.Claim(ctx, id)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if first.LockedUntil == nil {
		t.Fatal("expected a processing lease to be set on claim")
	}

	if _, err := store.Claim(ctx, id); err != payment.ErrAlreadyDone {
		t.Fatalf("expected ErrAlreadyDone on second claim, got %v", err)
	}
}

// TestReclaimExpiredLeases covers recovery of a payment whose worker died
// between claiming it and persisting the result.
func TestReclaimExpiredLeases(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-reaper-" + id.String()
	defer cleanupPayment(t, pool, id)

	if _, _, err := store.Create(ctx, &payment.Payment{ID: id, IdempotencyKey: key, Amount: 3000, Currency: "USD"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := store.Claim(ctx, id); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// A live lease must not be reclaimed.
	got, err := store.ReclaimExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no reclaims while the lease is live, got %d", len(got))
	}

	// Force the lease into the past; the payment must be returned to pending.
	if _, err := pool.Exec(ctx, `UPDATE payments SET locked_until = now() - interval '1 hour' WHERE id = $1`, id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}

	got, err = store.ReclaimExpiredLeases(ctx)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(got) != 1 || got[0].ID != id {
		t.Fatalf("expected the expired payment to be reclaimed, got %+v", got)
	}

	p, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if p.Status != payment.StatusPending {
		t.Fatalf("expected pending after reclaim, got %s", p.Status)
	}
	if p.LockedUntil != nil {
		t.Fatalf("expected lease to be cleared, got %v", p.LockedUntil)
	}
}
