package main

import (
	"context"
	"encoding/json"
	"os/signal"
	"syscall"
	"time"

	"github.com/IsraelMends/goworkers/internal/config"
	"github.com/IsraelMends/goworkers/internal/domain"
	"github.com/IsraelMends/goworkers/internal/handler"
	"github.com/IsraelMends/goworkers/internal/queue"
	"github.com/IsraelMends/goworkers/internal/worker"
	"github.com/IsraelMends/goworkers/pkg/logger"
	"github.com/google/uuid"
)

func main() {
	cfg := config.Load()
	log := logger.New(cfg.LogLevel)

	var q queue.Queue
	if cfg.RedisAddr != "" {
		q = queue.NewRedisQueue(cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
		log.Info("using redis queue", "addr", cfg.RedisAddr)
	} else {
		q = queue.NewMemoryQueue(cfg.QueueBufferSize)
		log.Info("using in-memory queue")
	}

	// Componentes

	registry := handler.NewRegistry()

	// Register handlers
	registry.Register("send_email", handler.SendEmail)
	registry.Register("generate_report", handler.GenerateReport)

	// Worker pool
	pool := worker.NewPool(cfg.WorkerCount, q, registry, log)

	// Contexto que cancela no sinal de shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Inicia o pool de workers em background
	go pool.Start(ctx)

	// Enfileirar alguns jobs de teste
	for i := range 10 {
		payload, _ := json.Marshal(map[string]string{
			"to":      "user@example.com",
			"subject": "Test",
			"body":    "Hello",
		})
		job := &domain.Job{
			ID:          uuid.New().String(),
			Type:        "send_email",
			Payload:     payload,
			Status:      domain.StatusPending,
			MaxAttempts: 3,
			CreatedAt:   time.Now(),
		}
		_ = q.Enqueue(ctx, job)
		_ = i
	}

	// Aguardar sinal de shutdown
	<-ctx.Done()
	log.Info("shutdown signal received, draining workers...")

	// Aguarda workers finalizarem (com timeout de 30s)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		pool.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("all workers drained")
	case <-shutdownCtx.Done():
		log.Warn("shutdown timeout reached, forcing stop")
	}

	log.Info("shutdown complete")
}
