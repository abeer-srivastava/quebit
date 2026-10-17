package payment

import "strings"

// SupportedCurrencies is the ISO 4217 allowlist Quebit accepts.
//
// Amounts are always expressed in the currency's minor unit — the smallest
// indivisible unit — so a 50.00 USD charge is amount=5000 (cents) and a
// 500 JPY charge is amount=500 (JPY has no minor unit). Never pass a decimal
// major-unit value.
var SupportedCurrencies = map[string]struct{}{
	"AUD": {}, "BRL": {}, "CAD": {}, "CHF": {}, "CNY": {}, "CZK": {},
	"DKK": {}, "EUR": {}, "GBP": {}, "HKD": {}, "HUF": {}, "IDR": {},
	"ILS": {}, "INR": {}, "ISK": {}, "JPY": {}, "KRW": {}, "MXN": {},
	"MYR": {}, "NOK": {}, "NZD": {}, "PHP": {}, "PLN": {}, "RON": {},
	"SEK": {}, "SGD": {}, "THB": {}, "TRY": {}, "USD": {}, "ZAR": {},
}

// NormalizeCurrency upper-cases and trims a currency code.
func NormalizeCurrency(c string) string {
	return strings.ToUpper(strings.TrimSpace(c))
}

// IsSupportedCurrency reports whether c is in the allowlist. The comparison is
// case-insensitive.
func IsSupportedCurrency(c string) bool {
	_, ok := SupportedCurrencies[NormalizeCurrency(c)]
	return ok
}
