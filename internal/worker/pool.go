package worker

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/IsraelMends/goworkers/internal/domain"
	"github.com/IsraelMends/goworkers/internal/handler"
	"github.com/IsraelMends/goworkers/internal/metrics"
	"github.com/IsraelMends/goworkers/internal/queue"
)

type Pool struct {
	size       int
	jobTimeout time.Duration
	queue      queue.Queue
	registry   *handler.Registry
	logger     *slog.Logger
	wg         sync.WaitGroup
}

func NewPool(size int, q queue.Queue, r *handler.Registry, logger *slog.Logger, jobTimeout time.Duration) *Pool {
	return &Pool{
		size:       size,
		jobTimeout: jobTimeout,
		queue:      q,
		registry:   r,
		logger:     logger,
	}
}

// Wait bloqueia até todos os workers finalizarem.
func (p *Pool) Wait() {
	p.wg.Wait()
}

// Start inicia N workers. Bloqueia até ctx ser cancelado e todos os workers finalizarem.
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
	defer p.logger.Info("worker stopped", "worker_id", id)

	for {
		job, err := p.queue.Dequeue(ctx)
		if err != nil {
			// ctx cancelado = shutdown solicitado
			if ctx.Err() != nil {
				return
			}
			p.logger.Error("failed to dequeue", "error", err, "worker_id", id)
			// Aguarda antes de tentar novamente para evitar spin loop
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return
			}
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

	// Rastreia worker ativo
	metrics.WorkersActive.Inc()
	defer metrics.WorkersActive.Dec()

	// Marca como processing (sem alterar attempts ainda)
	_ = p.queue.UpdateStatus(ctx, job.ID, domain.StatusProcessing, "", 0)

	// Obtém o handler
	fn, err := p.registry.Get(job.Type)
	if err != nil {
		logger.Error("no handler found", "error", err)
		_ = p.queue.UpdateStatus(ctx, job.ID, domain.StatusFailed, err.Error(), 0)
		metrics.JobsFailed.WithLabelValues(job.Type).Inc()
		return
	}

	// Cria um contexto com timeout por job (configurável)
	jobCtx, cancel := context.WithTimeout(ctx, p.jobTimeout)
	defer cancel()

	jobErr := safeRun(jobCtx, fn, job)

	// Executa o handler com proteção contra panic
	duration := time.Since(start)
	job.Attempts++

	if jobErr == nil {
		logger.Info("job completed", "duration_ms", duration.Milliseconds())
		// Persiste o status e o número de tentativas no store canônico.
		_ = p.queue.UpdateStatus(ctx, job.ID, domain.StatusCompleted, "", job.Attempts)
		metrics.JobsCompleted.WithLabelValues(job.Type).Inc()
		metrics.JobDuration.WithLabelValues(job.Type, "completed").Observe(duration.Seconds())
		return
	}

	// Persiste o erro no job antes de decidir retry ou DLQ.
	job.LastError = jobErr.Error()

	logger.Warn("job failed", "error", jobErr, "duration_ms", duration.Milliseconds())
	metrics.JobsFailed.WithLabelValues(job.Type).Inc()
	metrics.JobDuration.WithLabelValues(job.Type, "failed").Observe(duration.Seconds())

	// Decide entre retry e DLQ
	if job.Attempts < job.MaxAttempts {
		delay := backoff(job.Attempts)
		logger.Info("scheduling retry", "delay", delay, "attempt", job.Attempts)
		metrics.JobRetries.WithLabelValues(job.Type).Inc()

		go func() {
			time.Sleep(delay)
			job.Status = domain.StatusPending
			_ = p.queue.Enqueue(ctx, job)
		}()
	} else {
		logger.Warn("moving to DLQ", "attempts", job.Attempts)
		metrics.JobsDeadLettered.WithLabelValues(job.Type).Inc()
		_ = p.queue.MoveToDLQ(ctx, job)
	}
}

// safeRun executa o handler e recupera panics, transformando em error.
func safeRun(ctx context.Context, fn domain.HandlerFunc, job *domain.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn(ctx, job)
}

// backoff retorna o delay de retry com exponential backoff + jitter.
func backoff(attempt int) time.Duration {
	base := time.Duration(1<<attempt) * time.Second
	maxJitter := time.Second
	jitter := time.Duration(rand.Int64N(int64(maxJitter)))
	return base + jitter
}
