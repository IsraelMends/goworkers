# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Cache de dependências em camada separada (rebuild só se go.mod/go.sum mudar)
COPY go.mod go.sum ./
RUN go mod download

# Copia o restante do código e compila
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-w -s" \
    -o /goworkers \
    ./cmd/server

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
# Usa scratch para imagem mínima (~5 MB) sem shell ou utilitários desnecessários
FROM scratch

# Certificados TLS para que handlers possam fazer chamadas HTTPS
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

COPY --from=builder /goworkers /goworkers

EXPOSE 8080

ENTRYPOINT ["/goworkers"]
