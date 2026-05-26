package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/IsraelMends/goworkers/internal/domain"
	"github.com/IsraelMends/goworkers/internal/handler"
	"github.com/IsraelMends/goworkers/internal/queue"
)

type Pool struct {
	size     int
	queue    queue.Queue
	registry *handler.Registry
	logger   *slog.Logger
	wg       sync.WaitGroup
}

func NewPool(size int, q queue.Queue, r *handler.Registry, logger *slog.Logger) *Pool {
	return &Pool{
		size:     size,
		queue:    q,
		registry: r,
		logger:   logger,
	}
}

// Start inicia N workers. Bloqueia até o ctx ser cancelado e todos os workers finalizarem.
func (p *Pool) Start(ctx context.Context) {
	p.logger.Info("starting worker pool", "size", p.size)

	for i := range p.size {
		p.wg.Add(1)
		go func(id int) {
			defer p.wg.Done()
			p.runWorker(ctx, id)
		}(i)
	}

	p.wg.Wait()
	p.logger.Info("worker pool stopped")
}

func (p *Pool) runWorker(ctx context.Context, id int) {
	p.logger.Info("worker started", "worker_id", id)
	defer p.logger.Info("worker stopped",
		"worker_id", id)

	for {
		job, err := p.queue.Dequeue(ctx)
		if err != nil {
			// ctx cancelado = shutdown solicitado
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("failed to dequeue", "error", err, "worker_id", id)
			continue
		}

		p.processJob(ctx, job, id)
	}
}

func (p *Pool) processJob(ctx context.Context, job *domain.Job, workerID int) {
	start := time.Now()
	logger := p.logger.With(
		"job_id", job.ID,
		"job_type", job.Type,
		"attempt", job.Attempts+1,
		"worker_id", workerID,
	)

	logger.Info("processing job")

	// Marca como processing
	_ = p.queue.UpdateStatus(ctx, job.ID, domain.StatusProcessing, "")

	// Obtém o handler
	fn, err := p.registry.Get(job.Type)
	if err != nil {
		logger.Error("no handler found", "error", err)
		_ = p.queue.UpdateStatus(ctx, job.ID, domain.StatusFailed, err.Error())
		return
	}

	// Cria um contexto com timeout por job (30s padrão)
	jobCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Executa o handler com proteção contra panic
	jobErr := safeRun(jobCtx, fn, job)
	duration := time.Since(start)
	job.Attempts++

	if jobErr == nil {
		logger.Info("job completed", "duration_ms", duration.Milliseconds())
		_ = p.queue.UpdateStatus(ctx, job.ID, domain.StatusCompleted, "")
		return
	}

	logger.Warn("job failed", "error", jobErr, "duration_ms", duration.Milliseconds())

	// Decide entre retry e DLQ
	if job.Attempts < job.MaxAttempts {
		delay := backoff(job.Attempts)
		logger.Info("scheduling retry", "delay", delay, "attempt", job.Attempts)

		go func() {
			time.Sleep(delay)
			job.Status = domain.StatusPending
			_ = p.queue.Enqueue(ctx, job)
		}()
	} else {
		logger.Warn("moving to DLQ", "attempts", job.Attempts)
		_ = p.queue.MoveToDLQ(ctx, job)
	}
}

// safeRun excuta o handler e recupera panics, tranformando em error.
func safeRun(ctx context.Context, fn domain.HandleFunc, job *domain.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(ctx, job)
}

// backoff retorna o delay de retry com exponencial backoff + jitter.
func backoff(attempt int) time.Duration {
	base := time.Duration(attempt*attempt) * time.Second
	jitter := time.Duration(rand.Int63n(int64(time.Second)))
	return base + jitter
}
