SHELL := /bin/bash
BINARY := bin/engine
PKG := ./...

.PHONY: build run tidy fmt vet lint test test-race vuln cover migrate-up clean help

build: ## Build engine binary
	go build -trimpath -o $(BINARY) ./cmd/engine

run: ## Run engine locally (loads .env if present)
	@if [ -f .env ]; then set -a; . ./.env; set +a; fi; \
	go run ./cmd/engine

tidy: ## Sync go.mod / go.sum
	go mod tidy

fmt: ## gofmt
	gofmt -w .

vet: ## go vet
	go vet $(PKG)

test: ## Unit tests
	go test -count=1 $(PKG)

test-race: ## Tests with race detector
	go test -count=1 -race $(PKG)

cover: ## Coverage profile
	go test -count=1 -coverprofile=coverage.out $(PKG)
	go tool cover -func=coverage.out | tail -n 1

vuln: ## govulncheck
	@command -v govulncheck >/dev/null || go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck $(PKG)

migrate-up: ## Apply engine-operational migrations (requires psql + ENGINE_POSTGRES_URL)
	@if [ -z "$$ENGINE_POSTGRES_URL" ]; then echo "ENGINE_POSTGRES_URL not set"; exit 1; fi
	@for f in migrations/engine/*.up.sql; do \
		echo "applying $$f"; \
		psql "$$ENGINE_POSTGRES_URL" -v ON_ERROR_STOP=1 -f "$$f" || exit 1; \
	done

clean:
	rm -rf bin coverage.out

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  %-14s %s\n",$$1,$$2}'
