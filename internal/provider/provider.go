package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ChargeRequest struct {
	PaymentID string `json:"payment_id"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
}

// chargeResponse is the expected JSON body from a successful provider charge.
type chargeResponse struct {
	ProviderRef string `json:"provider_ref"`
}

type Error struct {
	StatusCode int
	Body       string
}

func (e *Error) Error() string {
	return fmt.Sprintf("provider returned %d: %s", e.StatusCode, e.Body)
}

// Retryable returns true for transient failures: 429 (rate limited) and 5xx (server errors).
// Other 4xx like 422 are terminal — retrying won't help.
func (e *Error) Retryable() bool {
	return e.StatusCode == 429 || e.StatusCode >= 500
}

type Client struct {
	baseURL string
	http    *http.Client
}

func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// Charge calls the external payment provider and returns a ChargeResult with the
// provider's reference ID on success.
func (c *Client) Charge(ctx context.Context, req ChargeRequest) (*ChargeResult, error) {
	body, _ := json.Marshal(req)

	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/charge", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", req.PaymentID)

	resp, err := c.http.Do(r)
	if err != nil {
		return nil, err // network errors are treated as retryable by the handler
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))

	if resp.StatusCode/100 != 2 {
		return nil, &Error{StatusCode: resp.StatusCode, Body: string(b)}
	}

	// Parse provider response for the reference ID
	var cr chargeResponse
	if err := json.Unmarshal(b, &cr); err != nil {
		// Provider returned 2xx but didn't include a parseable body — use empty ref
		return &ChargeResult{ProviderRef: ""}, nil
	}
	return &ChargeResult{ProviderRef: cr.ProviderRef}, nil
}
