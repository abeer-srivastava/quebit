package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/service"
)

const maxRequestBodyBytes = 1 << 20 // 1 MiB

// Handler is a thin HTTP adapter that delegates business logic to service.Service.
type Handler struct {
	svc *service.Service
	log *slog.Logger
}

func NewHandler(svc *service.Service, l *slog.Logger) *Handler {
	return &Handler{svc: svc, log: l}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/payments", h.CreatePayment)
	mux.HandleFunc("GET /v1/payments/{id}", h.GetPayment)
	mux.HandleFunc("POST /v1/payments/{id}/retry", h.RetryPayment)
}

type CreateRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	// Amount is in the currency's minor unit (e.g. cents): 12.34 USD is 1234.
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
}

// CreateResponse wraps the payment with metadata about what happened.
type CreateResponse struct {
	Payment  *payment.Payment `json:"payment"`
	Created  bool             `json:"created"`
	Enqueued bool             `json:"enqueued"`
}

// GetResponse wraps the payment for consistency.
type GetResponse struct {
	Payment *payment.Payment `json:"payment"`
}

// RetryResponse wraps the retry outcome.
type RetryResponse struct {
	Status string `json:"status"`
}

func (h *Handler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)

	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, ErrInvalidJSON)
		return
	}
	req.Currency = payment.NormalizeCurrency(req.Currency)

	if apiErr := validateCreateRequest(req); apiErr != nil {
		writeError(w, http.StatusUnprocessableEntity, apiErr)
		return
	}

	result, err := h.svc.CreatePayment(r.Context(), req.IdempotencyKey, req.Amount, req.Currency)
	if errors.Is(err, payment.ErrIdempotencyMismatch) {
		writeError(w, http.StatusConflict,
			conflictError("idempotency key was reused with different request parameters"))
		return
	}
	if err != nil {
		h.log.Error("failed to create payment", "err", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	// 202 Accepted for new payments (accepted for async processing)
	// 200 OK for idempotent replays
	status := http.StatusAccepted
	if !result.Created {
		status = http.StatusOK
	}

	writeJSON(w, status, CreateResponse{
		Payment:  result.Payment,
		Created:  result.Created,
		Enqueued: result.Enqueued,
	})
}

func (h *Handler) GetPayment(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrBadUUID)
		return
	}

	p, err := h.svc.GetPayment(r.Context(), id)
	if errors.Is(err, payment.ErrNotFound) {
		writeError(w, http.StatusNotFound, ErrNotFound)
		return
	}
	if err != nil {
		h.log.Error("failed to get payment", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, GetResponse{Payment: p})
}

func (h *Handler) RetryPayment(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, ErrBadUUID)
		return
	}

	err = h.svc.RetryPayment(r.Context(), id)
	if errors.Is(err, payment.ErrNotFound) {
		writeError(w, http.StatusNotFound, ErrNotFound)
		return
	}
	if errors.Is(err, payment.ErrNotRetryable) {
		writeError(w, http.StatusConflict, conflictError("only failed_final payments can be retried"))
		return
	}
	if err != nil {
		h.log.Error("failed to retry payment", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	writeJSON(w, http.StatusAccepted, RetryResponse{Status: "retrying"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

// validateCreateRequest checks the fields of a create payment request and
// returns the first validation error encountered, or nil when valid.
func validateCreateRequest(req CreateRequest) *APIError {
	if req.IdempotencyKey == "" {
		return validationError("idempotency_key", "idempotency_key is required")
	}
	if len(req.IdempotencyKey) > 255 {
		return validationError("idempotency_key", "idempotency_key must be at most 255 characters")
	}
	if req.Amount <= 0 {
		return validationError("amount", "amount must be greater than 0")
	}
	if req.Currency == "" {
		return validationError("currency", "currency is required")
	}
	if !payment.IsSupportedCurrency(req.Currency) {
		return validationError("currency", "unsupported currency")
	}
	return nil
}
