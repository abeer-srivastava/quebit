package test

import (
	"testing"

	"github.com/abeer-srivastava/quebit/internal/payment"
)

func TestIsSupportedCurrency(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"USD", true},
		{"usd", true},
		{" usd ", true},
		{"EUR", true},
		{"XYZ", false},
		{"", false},
		{"US", false},
	}

	for _, tt := range tests {
		if got := payment.IsSupportedCurrency(tt.in); got != tt.want {
			t.Errorf("IsSupportedCurrency(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
