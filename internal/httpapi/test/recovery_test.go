package test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/abeer-srivastava/quebit/internal/httpapi"
)

func TestRecoverMiddlewareRecoversPanic(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("boom")
	})
	h := httpapi.RecoverMiddleware(log, inner)

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req) // must not propagate the panic

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if rec.Body.Len() == 0 {
		t.Fatal("expected a JSON error body")
	}
}

func TestRecoverMiddlewarePassesThrough(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	h := httpapi.RecoverMiddleware(log, inner)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ok", nil))

	if rec.Code != http.StatusTeapot {
		t.Fatalf("expected 418, got %d", rec.Code)
	}
}
