package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"

	"github.com/abeer-srivastava/quebit/internal/payment"
	"github.com/abeer-srivastava/quebit/internal/queue"
)

type Handler struct {
	store     *payment.Store
	queueCli  *queue.Client
	inspector *queue.Inspector
	log       *slog.Logger
}

func NewHandler(s *payment.Store, q *queue.Client, insp *queue.Inspector, l *slog.Logger) *Handler {
	return &Handler{store: s, queueCli: q, inspector: insp, log: l}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/payments", h.CreatePayment)
	mux.HandleFunc("GET /v1/payments/{id}", h.GetPayment)
	mux.HandleFunc("POST /v1/payments/{id}/retry", h.RetryPayment)
}

type CreateRequest struct {
	IdempotencyKey string `json:"idempotency_key"`
	Amount         int64  `json:"amount"`
	Currency       string `json:"currency"`
}

func (h *Handler) CreatePayment(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json"})
		return
	}

	if req.IdempotencyKey == "" || req.Amount <= 0 || req.Currency == "" {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]string{
			"error": "idempotency_key, amount (>0), and currency are required",
		})
		return
	}

	p := &payment.Payment{
		ID:             uuid.New(),
		IdempotencyKey: req.IdempotencyKey,
		Amount:         req.Amount,
		Currency:       req.Currency,
	}

	created, isNew, err := h.store.Create(r.Context(), p)
	if err != nil {
		h.log.Error("failed to create payment", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	if isNew {
		if err := h.queueCli.EnqueueProcess(r.Context(), created.ID, created.Amount); err != nil {
			h.log.Error("failed to enqueue payment", "id", created.ID, "err", err)
			// Payment exists in DB as pending — a sweeper or manual retry can pick it up
		}
	}

	status := http.StatusCreated
	if !isNew {
		status = http.StatusOK
	}
	writeJSON(w, status, created)
}

func (h *Handler) GetPayment(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid uuid"})
		return
	}

	p, err := h.store.Get(r.Context(), id)
	if errors.Is(err, payment.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if err != nil {
		h.log.Error("failed to get payment", "id", id, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	writeJSON(w, http.StatusOK, p)
}

func (h *Handler) RetryPayment(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid uuid"})
		return
	}

	p, err := h.store.Get(r.Context(), id)
	if errors.Is(err, payment.ErrNotFound) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	if err != nil {
		h.log.Error("failed to get payment", "id", id, "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	if p.Status != payment.StatusFailedFinal {
		writeJSON(w, http.StatusConflict, map[string]string{
			"error": "only failed_final payments can be retried",
		})
		return
	}

	// Try to replay the archived task via Asynq Inspector
	if err := h.inspector.ReplayArchivedTask(id.String()); err != nil {
		h.log.Warn("inspector replay failed, re-enqueuing fresh task", "id", id, "err", err)
		// Fallback: reset state and enqueue a fresh task
		if err := h.store.ResetForRetry(r.Context(), id); err != nil {
			h.log.Error("failed to reset payment", "id", id, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}
		if err := h.queueCli.EnqueueProcess(r.Context(), id, p.Amount); err != nil {
			h.log.Error("failed to re-enqueue payment", "id", id, "err", err)
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
			return
		}
	} else {
		if err := h.store.ResetForRetry(r.Context(), id); err != nil {
			h.log.Error("failed to reset payment", "id", id, "err", err)
		}
	}

	writeJSON(w, http.StatusAccepted, map[string]string{"status": "retrying"})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
