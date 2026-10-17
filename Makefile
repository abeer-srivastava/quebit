.PHONY: dev up down test lint build clean load

# Start infrastructure (Postgres + Redis)
up:
	docker compose up -d

# Stop infrastructure and remove volumes
down:
	docker compose down -v

# Run the server locally (requires up)
dev:
	go run ./cmd/server

# Run all tests with race detector
test:
	go test -v -race ./...

# Static analysis
lint:
	go vet ./...
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed, skipping"

# Build binary
build:
	go build -o bin/quebit ./cmd/server

# Remove build artifacts
clean:
	rm -rf bin/

# Run the load generator against a running server (requires dev + mockprovider)
load:
	go run ./cmd/loadgen -n 1000 -c 50

# Run tests that don't need external services
test-unit:
	go test -v -race -short ./...

# Format code
fmt:
	gofmt -s -w .
