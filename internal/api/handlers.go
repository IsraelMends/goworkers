package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/IsraelMends/goworkers/internal/domain"
	"github.com/IsraelMends/goworkers/internal/metrics"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type enqueueRequest struct {
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
}

func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Type == "" {
		http.Error(w, "field 'type' is required", http.StatusBadRequest)
		return
	}

	if req.MaxAttempts <= 0 {
		req.MaxAttempts = 3
	}

	job := &domain.Job{
		ID:          uuid.New().String(),
		Type:        req.Type,
		Payload:     req.Payload,
		Status:      domain.StatusPending,
		MaxAttempts: req.MaxAttempts,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.queue.Enqueue(r.Context(), job); err != nil {
		s.logger.Error("failed to enqueue job", "error", err)
		http.Error(w, "failed to enqueue job", http.StatusInternalServerError)
		return
	}

	metrics.JobsEnqueued.WithLabelValues(job.Type).Inc()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(job)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	job, err := s.queue.GetJob(r.Context(), id)
	if err != nil {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(job)
}

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.queue.ListJobs(r.Context())
	if err != nil {
		http.Error(w, "failed to list jobs", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobs)
}
