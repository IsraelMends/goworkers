package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// JobsEnqueued conta o total de jobs enfileirados por tipo.
	JobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworkers_jobs_enqueued_total",
		Help: "Total de jobs enfileirados.",
	}, []string{"job_type"})

	// JobsCompleted conta o total de jobs concluídos com sucesso por tipo.
	JobsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworkers_jobs_completed_total",
		Help: "Total de jobs concluídos com sucesso.",
	}, []string{"job_type"})

	// JobsFailed conta o total de jobs que falharam (incluindo retries) por tipo.
	JobsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworkers_jobs_failed_total",
		Help: "Total de jobs que falharam (incluindo retries).",
	}, []string{"job_type"})

	// JobsDeadLettered conta o total de jobs movidos para DLQ por tipo.
	JobsDeadLettered = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworkers_jobs_dead_lettered_total",
		Help: "Total de jobs movidos para Dead Letter Queue.",
	}, []string{"job_type"})

	// JobDuration observa a duração de execução dos jobs.
	JobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "goworkers_job_duration_seconds",
		Help:    "Duração de execução dos jobs em segundos.",
		Buckets: prometheus.DefBuckets,
	}, []string{"job_type", "status"})

	// WorkersActive rastreia o número de workers atualmente processando jobs.
	WorkersActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworkers_workers_active",
		Help: "Número de workers atualmente processando jobs.",
	})

	// QueueSize rastreia o número estimado de jobs aguardando na fila.
	QueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "goworkers_queue_size",
		Help: "Número estimado de jobs aguardando na fila.",
	})

	// JobRetries conta o total de retries realizados por tipo de job.
	JobRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "goworkers_job_retries_total",
		Help: "Total de retries realizados.",
	}, []string{"job_type"})
)
