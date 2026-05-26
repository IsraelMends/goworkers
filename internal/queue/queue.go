package queue

import (
	"context"

	"github.com/IsraelMends/goworkers/internal/domain"
)

type Queue interface {
	// Enqueue adiciona um job à fila.
	Enqueue(ctx context.Context, job *domain.Job) error

	// Dequeue remove e retorna o próximo job disponível.
	// Bloqueia até ter um job ou o ctx ser cancelado.
	Dequeue(ctx context.Context) (*domain.Job, error)

	// UpdatedStatus atualiza o status de um job pelo ID.
	UpdatedStatus(ctx context.Context, jobID string, status domain.JobStatus, lastError string)

	// GetJob retorna um job pelo ID
	GetJob(ctx context.Context, jobID string) (*domain.Job, error)

	// ListJobs retorna todos os jobs (para o endpoint de listagem).
	ListJobs(ctx context.Context) ([]*domain.Job, error)

	// MoveToDLQ move um job para o Dead Letter Queue.
	MoveToDLQ(ctx context.Context, job *domain.Job) error
}

func (q Queue) UpdateStatus(ctx context.Context, d string, failed domain.JobStatus, s string) any {
	panic("unimplemented")
}
