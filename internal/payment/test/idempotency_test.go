package test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

func TestRequestHashNormalisesCurrency(t *testing.T) {
	if payment.RequestHash(1000, "usd") != payment.RequestHash(1000, "USD") {
		t.Fatal("currency should be compared case-insensitively")
	}
	if payment.RequestHash(1000, "USD") == payment.RequestHash(2000, "USD") {
		t.Fatal("a different amount must produce a different hash")
	}
}

func TestCreateRejectsIdempotencyKeyReuseWithDifferentParams(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-hash-mismatch-" + id.String()
	defer cleanupPayment(t, pool, id)

	first := &payment.Payment{
		ID: id, IdempotencyKey: key, Amount: 1000, Currency: "USD",
		RequestHash: payment.RequestHash(1000, "USD"),
	}
	if _, isNew, err := store.Create(ctx, first); err != nil || !isNew {
		t.Fatalf("first create: new=%v err=%v", isNew, err)
	}

	// Same key, same parameters → replay, no error.
	same := &payment.Payment{
		ID: uuid.New(), IdempotencyKey: key, Amount: 1000, Currency: "USD",
		RequestHash: payment.RequestHash(1000, "USD"),
	}
	if got, isNew, err := store.Create(ctx, same); err != nil || isNew {
		t.Fatalf("replay: new=%v err=%v", isNew, err)
	} else if got.ID != id {
		t.Fatalf("replay returned a different payment: %s", got.ID)
	}

	// Same key, different amount → mismatch.
	diff := &payment.Payment{
		ID: uuid.New(), IdempotencyKey: key, Amount: 9999, Currency: "USD",
		RequestHash: payment.RequestHash(9999, "USD"),
	}
	if _, _, err := store.Create(ctx, diff); !errors.Is(err, payment.ErrIdempotencyMismatch) {
		t.Fatalf("expected ErrIdempotencyMismatch, got %v", err)
	}
}
