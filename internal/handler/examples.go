package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IsraelMends/goworkers/internal/domain"
)

// SendEmailPayload é o payload esperado pelo handler de email.
type SendEmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// SendEmail simula o envio de um e-mail.
func SendEmail(ctx context.Context, job *domain.Job) error {
	var p SendEmailPayload
	if err := json.Unmarshal(job.Payload, &p); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	//Simula latencia de envio
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	fmt.Printf("[email] sent to=%s subject=%s\n", p.To, p.Subject)
	return nil
}

// GenerateReport simula geração de relatório pesado.
func GenerateReport(ctx context.Context, job *domain.Job) error {
	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// AlwaysFail é um handler para testar a lógica de retry e DLQ.
func AlwaysFail(ctx context.Context, job *domain.Job) error {
	return fmt.Errorf("intentional failure for testing")
}

// SlowJob simula um job que estoura o timeout.
func SlowJob(ctx context.Context, job *domain.Job) error {
	select {
	case <-time.After(5 * time.Minute):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
