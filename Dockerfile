# ---- Build stage ----
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Cache dependency downloads
COPY go.mod go.sum ./
RUN go mod download

# Build the binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /quebit ./cmd/server

# ---- Runtime stage ----
FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata

COPY --from=builder /quebit /usr/local/bin/quebit

# Non-root user for security
RUN addgroup -S quebit && adduser -S quebit -G quebit
USER quebit

EXPOSE 8080

ENTRYPOINT ["quebit"]
