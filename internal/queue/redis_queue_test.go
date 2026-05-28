package queue_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/IsraelMends/goworkers/internal/domain"
	"github.com/IsraelMends/goworkers/internal/queue"
	"github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestRedisQueue_EnqueueDequeue(t *testing.T) {
	ctx := context.Background()

	// Sobe Redis em container
	container, err := redis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis: %v", err)
	}
	defer container.Terminate(ctx)

	addr, _ := container.Endpoint(ctx, "")
	q := queue.NewRedisQueue(addr, "", 0)

	payload, _ := json.Marshal(map[string]string{"key": "value"})
	job := &domain.Job{
		ID:          "test-job-1",
		Type:        "test",
		Payload:     payload,
		Status:      domain.StatusPending,
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
	}

	// Enqueue
	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Dequeue com timeout curto
	deqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	got, err := q.Dequeue(deqCtx)
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}

	if got.ID != job.ID {
		t.Errorf("expected job ID %s, got %s", job.ID, got.ID)
	}
}
