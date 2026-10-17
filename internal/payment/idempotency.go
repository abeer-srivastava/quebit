package payment

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ErrIdempotencyMismatch is returned when an idempotency key is replayed with a
// different request payload (amount or currency). The client is reusing a key
// for a different payment, which is a bug on their side.
var ErrIdempotencyMismatch = errors.New("idempotency key reused with different parameters")

// RequestHash fingerprints the mutable parts of a create-payment request. It is
// stored alongside the payment so a replay of the same idempotency key can be
// verified to describe the same payment. Currency is compared case-insensitively.
func RequestHash(amount int64, currency string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s", amount, strings.ToUpper(currency))))
	return hex.EncodeToString(sum[:])
}
