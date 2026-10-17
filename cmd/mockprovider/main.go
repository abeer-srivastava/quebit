package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"time"
)

type chargeRequest struct {
	PaymentID string `json:"payment_id"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
}

// Sentinel amounts that let a caller force a specific provider outcome while
// developing locally. They are far outside normal amounts and are documented in
// the README ("Driving failure paths locally").
const (
	// TerminalAmount makes the provider return 422, which the worker treats as
	// non-retryable, so the payment goes straight to failed_final.
	TerminalAmount int64 = 999_999
	// RetryableAmount makes the provider return 503, which the worker retries
	// with backoff until the retry budget is exhausted.
	RetryableAmount int64 = 888_888
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "9090"
	}

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	http.HandleFunc("/charge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req chargeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		// Allow callers to drive outcomes via sentinel amounts, useful for
		// exercising the retry / terminal paths locally. Sentinels are on the
		// amount because the API validates currency against the ISO 4217
		// allowlist, so a sentinel currency would be rejected before it ever
		// reached us.
		switch req.Amount {
		case TerminalAmount:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":"invalid card"}`))
			return
		case RetryableAmount:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"temporarily down"}`))
			return
		}

		// Simulate a small amount of latency.
		time.Sleep(time.Duration(rand.IntN(50)) * time.Millisecond)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"provider_ref": fmt.Sprintf("prov_%s", req.PaymentID),
		})
	})

	addr := ":" + port
	log.Printf("mock provider listening on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
