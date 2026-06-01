package worker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/IsraelMends/goworkers/internal/domain"
	"github.com/IsraelMends/goworkers/internal/handler"
	"github.com/IsraelMends/goworkers/internal/queue"
	"github.com/IsraelMends/goworkers/internal/worker"
	"github.com/google/uuid"
)

// testLogger retorna um logger silencioso para não poluir a saída dos testes.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))
}

// waitForStatus aguarda até que o job tenha o status esperado ou o timeout expire.
func waitForStatus(t *testing.T, q queue.Queue, jobID string, want domain.JobStatus, timeout time.Duration) *domain.Job {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		job, err := q.GetJob(context.Background(), jobID)
		if err == nil && job.Status == want {
			return job
		}
		time.Sleep(100 * time.Millisecond)
	}
	job, _ := q.GetJob(context.Background(), jobID)
	if job != nil {
		t.Fatalf("timeout: expected status %q, got %q for job %s", want, job.Status, jobID)
	} else {
		t.Fatalf("timeout: job %s not found after %s", jobID, timeout)
	}
	return nil
}

// TestWorkerPool_ProcessesJobSuccessfully verifica que um job válido é executado
// e marcado como 'completed'.
func TestWorkerPool_ProcessesJobSuccessfully(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	registry := handler.NewRegistry()
	_ = registry.Register("send_email", handler.SendEmail)

	pool := worker.NewPool(2, q, registry, testLogger(), 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	go pool.Start(ctx)
	defer func() {
		cancel()
		pool.Wait()
	}()

	payload, _ := json.Marshal(map[string]string{
		"to":      "user@example.com",
		"subject": "Hello",
		"body":    "World",
	})
	job := &domain.Job{
		ID:          uuid.New().String(),
		Type:        "send_email",
		Payload:     payload,
		Status:      domain.StatusPending,
		MaxAttempts: 3,
		CreatedAt:   time.Now(),
	}

	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	got := waitForStatus(t, q, job.ID, domain.StatusCompleted, 5*time.Second)
	if got.Attempts != 1 {
		t.Errorf("expected 1 attempt, got %d", got.Attempts)
	}
}

// TestWorkerPool_RetryOnFailure verifica que um job que falha sempre é retentado
// o número correto de vezes e então movido para a DLQ.
func TestWorkerPool_RetryOnFailure(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	registry := handler.NewRegistry()
	_ = registry.Register("always_fail", handler.AlwaysFail)

	// Usa timeout generoso para dar tempo ao backoff
	pool := worker.NewPool(1, q, registry, testLogger(), 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	go pool.Start(ctx)
	defer func() {
		cancel()
		pool.Wait()
	}()

	const maxAttempts = 2 // backoff: 2s após 1ª falha → total ~3s

	payload, _ := json.Marshal(map[string]string{"test": "value"})
	job := &domain.Job{
		ID:          uuid.New().String(),
		Type:        "always_fail",
		Payload:     payload,
		Status:      domain.StatusPending,
		MaxAttempts: maxAttempts,
		CreatedAt:   time.Now(),
	}

	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// MaxAttempts=2: falha na 1ª → backoff ~2s → falha na 2ª → DLQ
	got := waitForStatus(t, q, job.ID, domain.StatusDead, 10*time.Second)
	if got.Attempts != maxAttempts {
		t.Errorf("expected %d attempts, got %d", maxAttempts, got.Attempts)
	}
	if got.LastError == "" {
		t.Error("expected LastError to be set on DLQ job")
	}
}

// TestWorkerPool_TimeoutJob verifica que um job lento é cancelado pelo timeout
// e marcado como falho.
func TestWorkerPool_TimeoutJob(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	registry := handler.NewRegistry()
	_ = registry.Register("slow_job", handler.SlowJob)

	// Timeout de 200ms: SlowJob espera 5 minutos, então deve ser cancelado
	pool := worker.NewPool(1, q, registry, testLogger(), 200*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	go pool.Start(ctx)
	defer func() {
		cancel()
		pool.Wait()
	}()

	payload, _ := json.Marshal(map[string]string{})
	job := &domain.Job{
		ID:          uuid.New().String(),
		Type:        "slow_job",
		Payload:     payload,
		Status:      domain.StatusPending,
		MaxAttempts: 1, // sem retry, vai direto para DLQ
		CreatedAt:   time.Now(),
	}

	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Com MaxAttempts=1 e timeout curto, o job deve ir para DLQ rapidamente
	got := waitForStatus(t, q, job.ID, domain.StatusDead, 5*time.Second)
	if got.LastError == "" {
		t.Error("expected LastError to be set after timeout")
	}
}

// TestWorkerPool_PanicRecovery verifica que um panic no handler não derruba o worker.
func TestWorkerPool_PanicRecovery(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	registry := handler.NewRegistry()

	// Handler que faz panic
	_ = registry.Register("panic_job", func(ctx context.Context, job *domain.Job) error {
		panic("simulated panic for testing")
	})

	pool := worker.NewPool(1, q, registry, testLogger(), 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	go pool.Start(ctx)
	defer func() {
		cancel()
		pool.Wait()
	}()

	payload, _ := json.Marshal(map[string]string{})
	panicJob := &domain.Job{
		ID:          uuid.New().String(),
		Type:        "panic_job",
		Payload:     payload,
		Status:      domain.StatusPending,
		MaxAttempts: 1,
		CreatedAt:   time.Now(),
	}

	if err := q.Enqueue(ctx, panicJob); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Panic deve ser capturado e o job deve ir para DLQ
	_ = waitForStatus(t, q, panicJob.ID, domain.StatusDead, 5*time.Second)

	// O worker ainda está vivo: enfileira outro job e verifica que é processado
	_ = registry.Register("send_email", handler.SendEmail)
	emailPayload, _ := json.Marshal(map[string]string{
		"to":      "user@example.com",
		"subject": "Still alive",
		"body":    "Worker recovered from panic",
	})
	aliveJob := &domain.Job{
		ID:          uuid.New().String(),
		Type:        "send_email",
		Payload:     emailPayload,
		Status:      domain.StatusPending,
		MaxAttempts: 1,
		CreatedAt:   time.Now(),
	}

	if err := q.Enqueue(ctx, aliveJob); err != nil {
		t.Fatalf("Enqueue of recovery job failed: %v", err)
	}

	waitForStatus(t, q, aliveJob.ID, domain.StatusCompleted, 5*time.Second)
}

// TestWorkerPool_GracefulShutdown verifica que o pool espera os jobs em andamento
// terminarem antes de parar completamente.
func TestWorkerPool_GracefulShutdown(t *testing.T) {
	q := queue.NewMemoryQueue(10)
	registry := handler.NewRegistry()

	// Handler com latência de 300ms para garantir que esteja em execução durante shutdown
	_ = registry.Register("slow_email", func(ctx context.Context, job *domain.Job) error {
		select {
		case <-time.After(300 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	pool := worker.NewPool(1, q, registry, testLogger(), 5*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() {
		pool.Start(ctx)
		close(poolDone)
	}()

	payload, _ := json.Marshal(map[string]string{})
	job := &domain.Job{
		ID:          uuid.New().String(),
		Type:        "slow_email",
		Payload:     payload,
		Status:      domain.StatusPending,
		MaxAttempts: 1,
		CreatedAt:   time.Now(),
	}

	if err := q.Enqueue(ctx, job); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	// Aguarda o job começar a ser processado
	time.Sleep(50 * time.Millisecond)

	// Cancela o contexto (graceful shutdown)
	cancel()

	// Pool deve terminar em até 2s (tempo do job + overhead)
	select {
	case <-poolDone:
		// OK
	case <-time.After(2 * time.Second):
		t.Fatal("worker pool did not shut down gracefully in time")
	}
}
