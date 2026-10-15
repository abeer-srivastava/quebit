package provider

import "context"

// MockCharger is an in-memory implementation of Charger for tests and local
// development. By default it returns a successful charge with a generated
// provider reference. Set ChargeFn to customise behaviour (e.g. inject errors).
type MockCharger struct {
	ChargeFn func(ctx context.Context, req ChargeRequest) (*ChargeResult, error)

	// Calls records every charge request, useful for assertions in tests.
	Calls []ChargeRequest
}

// Charge implements Charger.
func (m *MockCharger) Charge(ctx context.Context, req ChargeRequest) (*ChargeResult, error) {
	m.Calls = append(m.Calls, req)
	if m.ChargeFn != nil {
		return m.ChargeFn(ctx, req)
	}
	return &ChargeResult{ProviderRef: "mock_provider_ref"}, nil
}

var _ Charger = (*MockCharger)(nil)
