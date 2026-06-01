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

	// UpdateStatus atualiza o status, o último erro e o número de tentativas de um job pelo ID.
	// Passe attempts=0 para não alterar o valor corrente de tentativas.
	UpdateStatus(ctx context.Context, jobID string, status domain.JobStatus, lastError string, attempts int) error

	// GetJob retorna um job pelo ID
	GetJob(ctx context.Context, jobID string) (*domain.Job, error)

	// ListJobs retorna todos os jobs (para o endpoint de listagem).
	ListJobs(ctx context.Context) ([]*domain.Job, error)

	// MoveToDLQ move um job para o Dead Letter Queue.
	MoveToDLQ(ctx context.Context, job *domain.Job) error
}