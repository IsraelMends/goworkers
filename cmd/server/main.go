package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/IsraelMends/goworkers/internal/api"
	"github.com/IsraelMends/goworkers/internal/config"
	"github.com/IsraelMends/goworkers/internal/handler"
	"github.com/IsraelMends/goworkers/internal/queue"
	"github.com/IsraelMends/goworkers/internal/worker"
	"github.com/IsraelMends/goworkers/pkg/logger"
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
	pool := worker.NewPool(cfg.WorkerCount, q, registry, log, cfg.JobTimeout)

	// Contexto que cancela no sinal de shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Inicia o pool de workers em background
	go pool.Start(ctx)

	// Inicia servidor HTTP em goroutine
	srv := api.NewServer(cfg.HTTPAddr, q, log)
	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "error", err)
		}
	}()

	// Aguardar sinal de shutdown
	<-ctx.Done()
	log.Info("shutdown signal received, draining workers...")

	// Aguarda workers finalizarem (com timeout de 30s)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown do HTTP primeiro (para de aceitar novas requisições)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown error", "error", err)
	}

	log.Info("shutdown complete")
}
