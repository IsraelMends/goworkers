package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/IsraelMends/goworkers/internal/queue"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Server struct {
	queue  queue.Queue
	logger *slog.Logger
	http   *http.Server
}

func NewServer(addr string, q queue.Queue, logger *slog.Logger) *Server {
	s := &Server{queue: q, logger: logger}

	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Recoverer)
	r.Use(middlewareLogger(logger))

	r.Post("/jobs", s.handleEnqueue)
	r.Get("/jobs", s.handleListJobs)
	r.Get("/jobs/{id}", s.handleGetJob)
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	s.http = &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	return s
}

func (s *Server) Start() error {
	s.logger.Info("http server starting", "addr", s.http.Addr)
	return s.http.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.http.Shutdown(ctx)
}

func middlewareLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			next.ServeHTTP(ww, r)
			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", ww.Status(),
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", middleware.GetReqID(r.Context()),
			)
		})
	}
}
