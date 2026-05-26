package handler

import (
	"fmt"
	"sync"

	"github.com/IsraelMends/goworkers/internal/domain"
)

// Registry mapeia JobType -> HandleFunc
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]domain.HandleFunc
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]domain.HandleFunc),
	}
}

// Register cadastra um handle para um tipo de job.
// Retorna erro se o tipo já estiver registrado.
func (r *Registry) Register(jobType string, fn domain.HandleFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; exists {
		return fmt.Errorf("handler for job type %q already registred", jobType)
	}
	r.handlers[jobType] = fn
	return nil
}

// Get retorna o handler para um tipo de job.
func (r *Registry) Get(jobType string) (domain.HandleFunc, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fn, ok := r.handlers[jobType]
	if !ok {
		return nil, fmt.Errorf("no handler registred for job type %q", jobType)
	}
	return fn, nil
}
