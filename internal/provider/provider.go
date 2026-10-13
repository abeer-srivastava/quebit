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

func (c *Client) Charge(ctx context.Context, req ChargeRequest) error {
	body, _ := json.Marshal(req)

	r, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/charge", bytes.NewReader(body))
	if err != nil {
		return err
	}
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", req.PaymentID)

	resp, err := c.http.Do(r)
	if err != nil {
		return err // network errors are treated as retryable by the handler
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 == 2 {
		return nil
	}

	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	return &Error{StatusCode: resp.StatusCode, Body: string(b)}
}
