.PHONY: test test-short lint tidy docker-up docker-down

# Run all tests
test:
	go test -v ./...

# Run unit tests only (skips integration tests)
test-short:
	go test -v -short ./...

# Run golangci-lint
lint:
	golangci-lint run ./...

# Tidy go modules
tidy:
	go mod tidy

# Start Kafka for integration tests
docker-up:
	docker-compose -f docker-compose.yml up -d

# Stop Kafka
docker-down:
	docker-compose -f docker-compose.yml down
