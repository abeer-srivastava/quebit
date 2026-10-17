package test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/provider"
	"github.com/abeer-srivastava/quebit/internal/service"
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

// countingProvider records how many times Charge is called. The short sleep
// widens the window during which concurrent claimants would overlap.
type countingProvider struct {
	calls atomic.Int64
}

func (c *countingProvider) Charge(ctx context.Context, req provider.ChargeRequest) (*provider.ChargeResult, error) {
	c.calls.Add(1)
	time.Sleep(25 * time.Millisecond)
	return &provider.ChargeResult{ProviderRef: "prov_" + req.PaymentID}, nil
}

// TestConcurrentProcessingChargesExactlyOnce is the core correctness invariant:
// however many workers receive the same payment task at the same time, the
// provider is charged exactly once and the payment ends up succeeded.
func TestConcurrentProcessingChargesExactlyOnce(t *testing.T) {
	pool := testPool(t)
	store := payment.NewStore(pool)
	prov := &countingProvider{}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := service.New(store, prov, log)

	ctx := context.Background()
	id := uuid.New()
	key := "test-invariant-" + id.String()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM payments WHERE id = $1", id)
	})

	if _, _, err := store.Create(ctx, &payment.Payment{
		ID: id, IdempotencyKey: key, Amount: 5000, Currency: "USD",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const workers = 10
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = svc.ProcessPayment(ctx, id)
		}()
	}
	close(start)
	wg.Wait()

	if got := prov.calls.Load(); got != 1 {
		t.Fatalf("expected exactly one provider charge, got %d", got)
	}

	got, err := store.Get(ctx, id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != payment.StatusSucceeded {
		t.Fatalf("expected succeeded, got %s", got.Status)
	}
	if got.Attempts != 1 {
		t.Fatalf("expected attempts=1, got %d", got.Attempts)
	}
}
