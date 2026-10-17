package httpapi

import (
	"encoding/json"
	"net/http"
)

// APIError is a structured error response returned to API clients.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// Common API errors
var (
	ErrInvalidJSON = &APIError{Code: "invalid_json", Message: "request body is not valid JSON"}
	ErrInternal    = &APIError{Code: "internal_error", Message: "an internal error occurred"}
	ErrNotFound    = &APIError{Code: "not_found", Message: "resource not found"}
	ErrBadUUID     = &APIError{Code: "invalid_id", Message: "id must be a valid UUID", Field: "id"}
)

func validationError(field, message string) *APIError {
	return &APIError{Code: "validation_error", Message: message, Field: field}
}

func conflictError(message string) *APIError {
	return &APIError{Code: "conflict", Message: message}
}

// writeError writes a structured error response.
func writeError(w http.ResponseWriter, status int, apiErr *APIError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": apiErr})
}
