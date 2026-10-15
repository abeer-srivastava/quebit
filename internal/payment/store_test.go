package payment_test

import (
	"context"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set, skipping integration test")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { pool.Close() })
	return pool
}

func cleanupPayment(t *testing.T, pool *pgxpool.Pool, id uuid.UUID) {
	t.Helper()
	_, _ = pool.Exec(context.Background(), "DELETE FROM payments WHERE id = $1", id)
}

func TestCreateAndGet(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-create-" + id.String()
	defer cleanupPayment(t, pool, id)

	p := &payment.Payment{
		ID:             id,
		IdempotencyKey: key,
		Amount:         5000,
		Currency:       "USD",
	}

	created, isNew, err := store.Create(ctx, p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !isNew {
		t.Fatal("expected isNew=true")
	}
	if created.Status != payment.StatusPending {
		t.Fatalf("expected status=pending, got %s", created.Status)
	}

	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Amount != 5000 {
		t.Fatalf("expected amount=5000, got %d", got.Amount)
	}
}

func TestIdempotentCreate(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-idempotent-" + id.String()
	defer cleanupPayment(t, pool, id)

	p := &payment.Payment{
		ID:             id,
		IdempotencyKey: key,
		Amount:         1000,
		Currency:       "USD",
	}

	_, _, err := store.Create(ctx, p)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}

	// Second create with same idempotency key but different ID
	dup := &payment.Payment{
		ID:             uuid.New(),
		IdempotencyKey: key,
		Amount:         9999,
		Currency:       "EUR",
	}

	existing, isNew, err := store.Create(ctx, dup)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if isNew {
		t.Fatal("expected isNew=false for duplicate key")
	}
	if existing.ID != id {
		t.Fatalf("expected original ID %s, got %s", id, existing.ID)
	}
	// Original values preserved
	if existing.Amount != 1000 {
		t.Fatalf("expected original amount=1000, got %d", existing.Amount)
	}
}

func TestClaimAndTerminalGuard(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	ctx := context.Background()

	id := uuid.New()
	key := "test-claim-" + id.String()
	defer cleanupPayment(t, pool, id)

	p := &payment.Payment{
		ID:             id,
		IdempotencyKey: key,
		Amount:         2000,
		Currency:       "USD",
	}

	_, _, err := store.Create(ctx, p)
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	claimed, err := store.Claim(ctx, id)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if claimed.Status != payment.StatusProcessing {
		t.Fatalf("expected processing, got %s", claimed.Status)
	}
	if claimed.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", claimed.Attempts)
	}

	// Mark as succeeded
	if err := store.MarkSucceeded(ctx, id, "test-provider-ref-123"); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}

	// Claim again should fail with ErrAlreadyDone
	_, err = store.Claim(ctx, id)
	if err != payment.ErrAlreadyDone {
		t.Fatalf("expected ErrAlreadyDone, got %v", err)
	}
}

func TestGetNotFound(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)

	_, err := store.Get(context.Background(), uuid.New())
	if err != payment.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}
