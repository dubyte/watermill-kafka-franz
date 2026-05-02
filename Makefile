.PHONY: test test-integration lint tidy docker-up docker-down wait-for-kafka

GOLANGCI_LINT := $(shell which golangci-lint 2>/dev/null || echo "$(GOPATH)/bin/golangci-lint")

# Run all tests (including integration)
test: docker-up wait-for-kafka
	go test -v -race ./...
	$(MAKE) docker-down

# Run unit tests only (skips integration tests)
test-short:
	go test -v -short ./...

# Run golangci-lint
lint:
	@if [ -x "$(GOLANGCI_LINT)" ]; then \
		"$(GOLANGCI_LINT)" run ./...; \
	else \
		echo "golangci-lint not found. Please install it or ensure GOPATH/bin is in your PATH."; \
		exit 1; \
	fi

# Tidy go modules
tidy:
	go mod tidy

# Start Kafka for integration tests
docker-up:
	docker compose up -d

# Wait for Kafka to be ready
wait-for-kafka:
	@echo "Waiting for Kafka to be ready..."
	@timeout 60s bash -c 'until docker compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do sleep 2; done'
	@echo "Kafka is ready!"

# Stop Kafka
docker-down:
	docker compose down
