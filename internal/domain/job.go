package domain

import (
	"context"
	"encoding/json"
	"time"
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusDead       JobStatus = "dead"
)

type Job struct {
	ID          string          `json: id`
	Type        string          `json:type`
	Payload     json.RawMessage `json:payload`
	Status      JobStatus       `json:status`
	Attempts    int             `json:attenpts`
	MaxAttempts int             `json:max_attenpts`
	CreatedAt   time.Time       `json:created_at`
	UpdatedAt   time.Time       `json:updated_at`
	LastError   string          `json:"last_error,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
}

type HandlerFunc func(ctx context.Context, job *Job) error
