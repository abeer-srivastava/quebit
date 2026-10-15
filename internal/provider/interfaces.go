package provider

import "context"

// ChargeResult holds the outcome of a successful charge.
type ChargeResult struct {
	// ProviderRef is the external system's reference ID for this charge.
	// Stored in the payment record as proof-of-charge.
	ProviderRef string
}

// Charger is the interface for charging a payment through an external provider.
// Implementations: *Client (HTTP). Test doubles can mock this interface.
type Charger interface {
	Charge(ctx context.Context, req ChargeRequest) (*ChargeResult, error)
}
