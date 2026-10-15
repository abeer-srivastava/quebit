package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abeer-srivastava/quebit/internal/httpapi"
	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/provider"
	"github.com/abeer-srivastava/quebit/internal/queue"
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

func setupHandler(t *testing.T) (*httpapi.Handler, *payment.Store) {
	t.Helper()
	pool := testPool(t)
	store := payment.NewStore(pool)

	// These will fail to connect to Redis, but the handler tests
	// that don't touch queue operations will still pass.
	redisOpt := asynq.RedisClientOpt{Addr: "localhost:6379"}
	queueCli := queue.NewClient(redisOpt)
	inspector := queue.NewInspector(redisOpt)
	t.Cleanup(func() {
		queueCli.Close()
		inspector.Close()
	})

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	providerCli := provider.NewClient("http://localhost:9090")
	svc := service.New(store, queueCli, inspector, providerCli, log)
	h := httpapi.NewHandler(svc, log)
	return h, store
}

func TestCreatePaymentAPI(t *testing.T) {
	h, store := setupHandler(t)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"idempotency_key":"api-test-1","amount":5000,"currency":"USD"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	// Now returns 202 Accepted instead of 201 Created
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp httpapi.CreateResponse
	json.NewDecoder(rec.Body).Decode(&resp)

	if resp.Payment.Amount != 5000 || resp.Payment.Currency != "USD" || resp.Payment.Status != payment.StatusPending {
		t.Fatalf("unexpected payment: %+v", resp.Payment)
	}
	if !resp.Created {
		t.Fatal("expected created=true")
	}

	// Cleanup
	t.Cleanup(func() {
		store.Delete(context.Background(), resp.Payment.ID)
	})
}

func TestCreatePaymentIdempotent(t *testing.T) {
	h, store := setupHandler(t)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	body := `{"idempotency_key":"api-test-idem","amount":3000,"currency":"EUR"}`

	// First request
	req := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("first: expected 202, got %d", rec.Code)
	}

	var first httpapi.CreateResponse
	json.NewDecoder(rec.Body).Decode(&first)

	// Second request with same idempotency key
	req = httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(body))
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("second: expected 200, got %d", rec.Code)
	}

	var second httpapi.CreateResponse
	json.NewDecoder(rec.Body).Decode(&second)

	if first.Payment.ID != second.Payment.ID {
		t.Fatalf("idempotency broken: first=%s second=%s", first.Payment.ID, second.Payment.ID)
	}
	if second.Created {
		t.Fatal("expected created=false for idempotent replay")
	}

	t.Cleanup(func() {
		store.Delete(context.Background(), first.Payment.ID)
	})
}

func TestCreatePaymentValidation(t *testing.T) {
	h, _ := setupHandler(t)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	tests := []struct {
		name string
		body string
		code int
	}{
		{"empty body", `{}`, http.StatusUnprocessableEntity},
		{"missing amount", `{"idempotency_key":"k","currency":"USD"}`, http.StatusUnprocessableEntity},
		{"negative amount", `{"idempotency_key":"k","amount":-1,"currency":"USD"}`, http.StatusUnprocessableEntity},
		{"invalid json", `{bad`, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(tt.body))
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code != tt.code {
				t.Fatalf("expected %d, got %d: %s", tt.code, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestGetPaymentNotFound(t *testing.T) {
	h, _ := setupHandler(t)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/v1/payments/00000000-0000-0000-0000-000000000001", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestGetPaymentBadUUID(t *testing.T) {
	h, _ := setupHandler(t)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/v1/payments/not-a-uuid", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}
