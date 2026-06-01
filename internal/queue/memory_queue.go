package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IsraelMends/goworkers/internal/domain"
)

type MemoryQueue struct {
	mu   sync.RWMutex
	jobs map[string]*domain.Job
	dlq  map[string]*domain.Job
	ch   chan *domain.Job
}

func NewMemoryQueue(buffSize int) *MemoryQueue {
	return &MemoryQueue{
		jobs: make(map[string]*domain.Job),
		dlq:  make(map[string]*domain.Job),
		ch:   make(chan *domain.Job, buffSize),
	}
}

func (q *MemoryQueue) Enqueue(ctx context.Context, job *domain.Job) error {
	q.mu.Lock()
	q.jobs[job.ID] = job
	q.mu.Unlock()

	select {
	case q.ch <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *MemoryQueue) Dequeue(ctx context.Context) (*domain.Job, error) {
	select {
	case job := <-q.ch:
		// Retorna uma cópia para evitar data races: quem processa o job
		// tem exclusividade sobre sua cópia, sem interferir no mapa interno.
		jobCopy := *job
		return &jobCopy, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *MemoryQueue) UpdateStatus(ctx context.Context, jobID string, status domain.JobStatus, lastError string, attempts int) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return fmt.Errorf("job %s not found", jobID)
	}

	job.Status = status
	job.LastError = lastError
	if attempts > 0 {
		job.Attempts = attempts
	}
	job.UpdatedAt = time.Now()
	return nil
}

func (q *MemoryQueue) GetJob(ctx context.Context, jobID string) (*domain.Job, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	job, ok := q.jobs[jobID]
	if !ok {
		return nil, fmt.Errorf("job %s not found", jobID)
	}
	// Retorna cópia para não expor o ponteiro interno do mapa.
	jobCopy := *job
	return &jobCopy, nil
}

func (q *MemoryQueue) ListJobs(ctx context.Context) ([]*domain.Job, error) {
	q.mu.RLock()
	defer q.mu.RUnlock()

	result := make([]*domain.Job, 0, len(q.jobs))
	for _, j := range q.jobs {
		// Cópias para evitar data races nos campos dos jobs.
		jobCopy := *j
		result = append(result, &jobCopy)
	}
	return result, nil
}

func (q *MemoryQueue) MoveToDLQ(ctx context.Context, job *domain.Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	job.Status = domain.StatusDead
	job.UpdatedAt = time.Now()
	q.dlq[job.ID] = job
	// Atualiza o mapa principal também para que GetJob reflita o status dead.
	q.jobs[job.ID] = job
	return nil
}
