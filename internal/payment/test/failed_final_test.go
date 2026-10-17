package test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

// TestMarkFailedFinal covers the store transition used by the queue handler
// when a payment exhausts its retry budget.
func TestMarkFailedFinal(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-failed-final-" + id.String()
	defer cleanupPayment(t, pool, id)

	p := &payment.Payment{
		ID:             id,
		IdempotencyKey: key,
		Amount:         2500,
		Currency:       "USD",
	}
	if _, _, err := store.Create(ctx, p); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := store.Claim(ctx, id); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if err := store.MarkFailedFinal(ctx, id, "retry budget exhausted"); err != nil {
		t.Fatalf("mark failed_final: %v", err)
	}

	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != payment.StatusFailedFinal {
		t.Fatalf("expected failed_final, got %s", got.Status)
	}
	if got.LastError == nil || *got.LastError != "retry budget exhausted" {
		t.Fatalf("expected last_error to be recorded, got %v", got.LastError)
	}
}
