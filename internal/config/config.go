package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	// Servidor HTTP
	HTTPAddr string

	// Worker Pool
	WorkerCount     int
	JobTimeout      time.Duration
	QueueBufferSize int

	// Redis
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Log
	LogLevel string
}

func Load() *Config {
	return &Config{
		// Padrão :8080 — alinhado com o docker-compose e convenção de serviços HTTP.
		HTTPAddr: getEnv("HTTP_ADDR", ":8080"),
		// Número de workers concorrentes. Ajuste conforme CPU disponível.
		WorkerCount:     getEnvInt("WORKER_COUNT", 4),
		JobTimeout:      getEnvDuration("JOB_TIMEOUT", 30*time.Second),
		QueueBufferSize: getEnvInt("QUEUE_BUFFER_SIZE", 100),
		// RedisAddr vazio por padrão → usa MemoryQueue localmente.
		// Defina REDIS_ADDR (ex: "localhost:6379") para usar Redis.
		RedisAddr:     getEnv("REDIS_ADDR", ""),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		RedisDB:       getEnvInt("REDIS_DB", 0),
		LogLevel:      getEnv("LOG_LEVEL", "info"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
