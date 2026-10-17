package test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/abeer-srivastava/quebit/internal/provider"
)

func TestChargeSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"provider_ref": "ch_abc123"})
	}))
	defer srv.Close()

	cli := provider.NewClient(srv.URL)
	result, err := cli.Charge(context.Background(), provider.ChargeRequest{
		PaymentID: "test-123",
		Amount:    5000,
		Currency:  "USD",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.ProviderRef != "ch_abc123" {
		t.Fatalf("expected provider_ref=ch_abc123, got %s", result.ProviderRef)
	}
}

func TestChargeSuccessEmptyBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cli := provider.NewClient(srv.URL)
	result, err := cli.Charge(context.Background(), provider.ChargeRequest{
		PaymentID: "test-empty",
		Amount:    1000,
		Currency:  "USD",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.ProviderRef != "" {
		t.Fatalf("expected empty provider_ref, got %s", result.ProviderRef)
	}
}

func TestChargeRetryableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("temporarily down"))
	}))
	defer srv.Close()

	cli := provider.NewClient(srv.URL)
	_, err := cli.Charge(context.Background(), provider.ChargeRequest{
		PaymentID: "test-456",
		Amount:    5000,
		Currency:  "USD",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	perr, ok := err.(*provider.Error)
	if !ok {
		t.Fatalf("expected *provider.Error, got %T", err)
	}
	if !perr.Retryable() {
		t.Fatal("503 should be retryable")
	}
}

func TestChargeTerminalError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("invalid card"))
	}))
	defer srv.Close()

	cli := provider.NewClient(srv.URL)
	_, err := cli.Charge(context.Background(), provider.ChargeRequest{
		PaymentID: "test-789",
		Amount:    5000,
		Currency:  "USD",
	})
	if err == nil {
		t.Fatal("expected error")
	}

	perr, ok := err.(*provider.Error)
	if !ok {
		t.Fatalf("expected *provider.Error, got %T", err)
	}
	if perr.Retryable() {
		t.Fatal("422 should NOT be retryable")
	}
}
