package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IsraelMends/goworkers/internal/domain"
	"github.com/redis/go-redis/v9"
)

const (
	pendingKey    = "goworkers:queue:pending"
	processingKey = "goworkers:jobs:processing"
	dlqKey        = "goworkers:queue:dlq"
	jobPrefix     = "goworkers:job:"
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(addr, password string, db int) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisQueue{client: client}
}

func (q *RedisQueue) Enqueue(ctx context.Context, job *domain.Job) error {
	// Serializar o job completo em uma hash
	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}

	pipe := q.client.Pipeline()
	//Salvar o job completo
	pipe.Set(ctx, jobPrefix+job.ID, data, 24*time.Hour)
	// Adicionar ID na fila (lista)
	pipe.LPush(ctx, pendingKey, job.ID)

	_, err = pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) Dequeue(ctx context.Context) (*domain.Job, error) {
	// BRPOP bloqueia até ter um item ou timeout
	result, err := q.client.BRPop(ctx, 5*time.Second, pendingKey).Result()
	if err == redis.Nil {
		// Timeout: tenta novamente (o loop do worker vai chamar de novo)
		return nil, fmt.Errorf("queue empty, retrying")
	}
	if err != nil {
		return nil, fmt.Errorf("brpop: %w", err)
	}

	jobID := result[1]
	return q.GetJob(ctx, jobID)
}

func (q *RedisQueue) UpdateStatus(ctx context.Context, jobID string, status domain.JobStatus, lastError string) error {
	job, err := q.GetJob(ctx, jobID)
	if err != nil {
		return err
	}

	job.Status = status
	job.LastError = lastError
	job.UpdatedAt = time.Now()

	data, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return q.client.Set(ctx, jobPrefix+jobID, data, 24*time.Hour).Err()
}

func (q *RedisQueue) GetJob(ctx context.Context, jobID string) (*domain.Job, error) {
	data, err := q.client.Get(ctx, jobPrefix+jobID).Bytes()
	if err == redis.Nil {
		return nil, fmt.Errorf("job %s not found", jobID)
	}
	if err != nil {
		return nil, err
	}

	var job domain.Job
	if err := json.Unmarshal(data, &job); err != nil {
		return nil, err
	}
	return &job, nil
}

func (q *RedisQueue) ListJobs(ctx context.Context) ([]*domain.Job, error) {
	keys, err := q.client.Keys(ctx, jobPrefix+"*").Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]*domain.Job, 0, len(keys))
	for _, key := range keys {
		data, err := q.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}
		var job domain.Job
		if err := json.Unmarshal(data, &job); err != nil {
			continue
		}
		jobs = append(jobs, &job)
	}
	return jobs, nil
}

func (q *RedisQueue) MoveToDLQ(ctx context.Context, job *domain.Job) error {
	job.Status = domain.StatusDead
	job.UpdatedAt = time.Now()

	data, _ := json.Marshal(job)
	pipe := q.client.Pipeline()
	pipe.Set(ctx, jobPrefix+job.ID, data, 7*24*time.Hour) // DLQ retém por 7 dias
	pipe.LPush(ctx, dlqKey, job.ID)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *RedisQueue) Close() error {
	return q.client.Close()
}
