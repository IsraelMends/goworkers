// Package worker implementa o worker pool responsável por consumir e processar
// jobs da fila de forma concorrente e resiliente.
//
// Componentes principais:
//   - Pool: gerencia N workers (goroutines) e orquestra o ciclo de vida do processamento.
//     Cada worker consome jobs via Dequeue, executa o handler registrado para o tipo
//     do job, e aplica a lógica de retry/DLQ em caso de falha.
//
// Características:
//   - Concorrência controlada: exatamente N jobs em paralelo.
//   - Graceful shutdown: ao cancelar o contexto, workers terminam o job em andamento
//     antes de parar.
//   - Timeout por job: cada job tem um deadline configurável (context.WithTimeout).
//   - Retry com exponential backoff + jitter: 2^attempt segundos + até 1s aleatório.
//   - Dead Letter Queue: jobs que esgotam MaxAttempts são movidos para DLQ.
//   - Proteção contra panic: panics nos handlers são recuperados e tratados como erro.
//   - Métricas Prometheus: workers ativos, jobs completados/falhos/DLQ/retries, duração.
package worker
