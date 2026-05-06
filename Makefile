.PHONY: help test test-integration test-short lint tidy docker-up docker-up-redpanda docker-down wait-for-kafka wait-for-redpanda act check-deps

# Variables
GOLANGCI_LINT := $(shell which golangci-lint 2>/dev/null || echo "$(GOPATH)/bin/golangci-lint")
ACT := $(shell which act 2>/dev/null)
DOCKER := $(shell which docker 2>/dev/null)
GO := $(shell which go 2>/dev/null)

# Colors for output
BLUE := \033[36m
GREEN := \033[32m
RED := \033[31m
YELLOW := \033[33m
NC := \033[0m # No Color

# Default target
help:
	@echo "$(BLUE)Watermill Kafka Franz - Available targets:$(NC)"
	@echo ""
	@echo "$(GREEN)Development:$(NC)"
	@echo "  make test              - Run all tests (starts Redpanda, runs, stops)"
	@echo "  make test-integration  - Run integration tests only (requires Redpanda running)"
	@echo "  make test-short        - Run unit tests only (no broker required)"
	@echo "  make lint              - Run golangci-lint"
	@echo "  make tidy              - Tidy go modules"
	@echo ""
	@echo "$(GREEN)Docker:$(NC)"
	@echo "  make docker-up-redpanda - Start Redpanda + Toxiproxy for integration tests"
	@echo "  make docker-up          - Alias for docker-up-redpanda"
	@echo "  make docker-down        - Stop all containers"
	@echo "  make wait-for-redpanda  - Wait until Redpanda is healthy"
	@echo ""
	@echo "$(GREEN)CI:$(NC)"
	@echo "  make act            - Run GitHub Actions locally (requires act)"
	@echo "  make check-deps     - Check all dependencies"
	@echo ""

# Check dependencies
check-deps:
	@echo "$(BLUE)Checking dependencies...$(NC)"
	@echo ""
	@echo -n "Go: "
	@if [ -x "$(GO)" ]; then echo "$(GREEN)✓$(NC) ($(GO))"; else echo "$(RED)✗ missing$(NC)"; fi
	@echo -n "Docker: "
	@if [ -x "$(DOCKER)" ]; then echo "$(GREEN)✓$(NC) ($(DOCKER))"; else echo "$(RED)✗ missing$(NC)"; fi
	@echo -n "golangci-lint: "
	@if [ -x "$(GOLANGCI_LINT)" ]; then echo "$(GREEN)✓$(NC) ($(GOLANGCI_LINT))"; else echo "$(YELLOW)⚠ not found$(NC) - Run: $(BLUE)go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest$(NC)"; fi
	@echo -n "act: "
	@if [ -x "$(ACT)" ]; then echo "$(GREEN)✓$(NC) ($(ACT))"; else echo "$(YELLOW)⚠ not found$(NC) - Run: $(BLUE)go install github.com/nektos/act@latest$(NC) or $(BLUE)brew install act$(NC)"; fi
	@echo ""

# Run GitHub Actions locally with act
act: check-act docker-up-redpanda wait-for-redpanda
	@echo "$(BLUE)Running GitHub Actions locally using catthehacker/ubuntu:act-latest image...$(NC)"
	@if [ -x "$(ACT)" ]; then \
		$(ACT) push --job lint -P ubuntu-latest=catthehacker/ubuntu:act-latest; \
		$(ACT) push --job test --network host -P ubuntu-latest=catthehacker/ubuntu:act-latest; \
	else \
		exit 1; \
	fi
	$(MAKE) docker-down

# Check if act is installed
check-act:
	@if [ ! -x "$(ACT)" ]; then \
		echo "$(RED)Error: act is not installed$(NC)"; \
		echo ""; \
		echo "$(YELLOW)To install act:$(NC)"; \
		echo "  $(BLUE)go install github.com/nektos/act@latest$(NC)"; \
		echo "  or"; \
		echo "  $(BLUE)brew install act$(NC) (macOS)"; \
		echo "  or"; \
		echo "  $(BLUE)curl https://raw.githubusercontent.com/nektos/act/master/install.sh | sudo bash$(NC)"; \
		echo ""; \
		exit 1; \
	fi

# Check if golangci-lint is installed
check-lint:
	@if [ ! -x "$(GOLANGCI_LINT)" ]; then \
		echo "$(RED)Error: golangci-lint is not installed$(NC)"; \
		echo ""; \
		echo "$(YELLOW)To install golangci-lint:$(NC)"; \
		echo "  $(BLUE)go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest$(NC)"; \
		echo "  or"; \
		echo "  $(BLUE)brew install golangci-lint$(NC) (macOS)"; \
		echo ""; \
		exit 1; \
	fi

# Run all tests (including integration) — starts Redpanda, runs, tears down
test: docker-up-redpanda wait-for-redpanda
	go test -v -race -tags integration ./...
	$(MAKE) docker-down

# Run only integration-tagged tests against an already-running Redpanda
test-integration:
	@echo "$(BLUE)Running integration tests (requires Redpanda at 127.0.0.1:9092)...$(NC)"
	go test -v -race -tags integration ./tests/integration/...
	@echo "$(GREEN)Integration tests complete.$(NC)"

# Run unit tests only (skips integration tests)
test-short:
	go test -v -short ./...

# Run golangci-lint
lint: check-lint
	"$(GOLANGCI_LINT)" run ./...

# Tidy go modules
tidy:
	go mod tidy

# Start Redpanda + Toxiproxy for integration tests
docker-up-redpanda:
	@echo "$(BLUE)Starting Redpanda + Toxiproxy...$(NC)"
	docker compose up -d

# Alias kept for backwards compatibility
docker-up: docker-up-redpanda

# Wait for Redpanda to be healthy (checks port 9092 and container health)
wait-for-redpanda:
	@echo "$(BLUE)Waiting for Redpanda to be ready...$(NC)"
	@timeout 120s bash -c 'until [ "$$(docker inspect -f "{{.State.Health.Status}}" watermill-redpanda 2>/dev/null)" = "healthy" ]; do sleep 2; done'
	@echo "$(GREEN)Redpanda is ready!$(NC)"

# Backwards-compatible alias
wait-for-kafka: wait-for-redpanda

# Stop all containers
docker-down:
	docker compose down
