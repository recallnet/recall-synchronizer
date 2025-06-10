.PHONY: all build test test-fast test-coverage clean docker-up docker-down fmt lint recall-start recall-start-if-needed recall-stop fund-wallets fund-wallet

# Default target
all: build

# Build the project
build:
	cargo build

# Run unit tests (fake implementations only)
test-fast:
	cargo test

# Run integration tests with real implementations
test: docker-up recall-start-if-needed
	@ENABLE_DB_TESTS=true ENABLE_S3_TESTS=true ENABLE_RECALL_TESTS=true ENABLE_SQLITE_TESTS=true RUST_BACKTRACE=1 cargo test -- --nocapture

# Run all tests with coverage
test-coverage: docker-up recall-start-if-needed
	@which cargo-tarpaulin > /dev/null || cargo install cargo-tarpaulin
	@ENABLE_DB_TESTS=true ENABLE_S3_TESTS=true ENABLE_RECALL_TESTS=true ENABLE_SQLITE_TESTS=true RUST_BACKTRACE=1 cargo tarpaulin --out html

# Format code
fmt:
	cargo fmt --all

# Run linter
lint:
	cargo clippy --all-targets --all-features -- -D warnings

# Start Docker containers for integration tests
docker-up:
	# Stop and remove any existing containers to avoid network issues
	@docker compose down --remove-orphans 2>/dev/null || true
	# Start fresh containers
	@docker compose up -d
	# Wait for services to initialize
	@sleep 2
	# Wait for PostgreSQL to be ready
	@for i in 1 2 3 4 5; do \
		docker compose exec -T postgres pg_isready -U recall -q && break || sleep 2; \
	done
	@docker compose exec -T postgres pg_isready -U recall -q || (echo "Error: PostgreSQL failed to start" && exit 1)
	# Wait for MinIO to be ready
	@echo "Waiting for MinIO to be ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		docker compose exec -T minio mc --version > /dev/null 2>&1 && break || (echo "  Waiting for MinIO..." && sleep 2); \
	done
	@echo "Docker services are ready"

# Stop Docker containers
docker-down: recall-stop
	@docker compose down

# Start Recall container (always restarts)
recall-start:
	@./scripts/recall-start.sh

# Start Recall container only if not running
recall-start-if-needed:
	@./scripts/recall-start-if-needed.sh

# Stop Recall container
recall-stop:
	@./scripts/recall-stop.sh

# Fund all test wallets
fund-wallets:
	@./scripts/fund-all-test-wallets.sh

# Fund a single wallet (usage: make fund-wallet ADDRESS=0x... AMOUNT=100)
fund-wallet:
	@if [ -z "$(ADDRESS)" ]; then \
		echo "Error: ADDRESS is required. Usage: make fund-wallet ADDRESS=0x... AMOUNT=100"; \
		exit 1; \
	fi
	@./scripts/fund-wallet.sh $(ADDRESS) $(or $(AMOUNT),100)

# Clean the project
clean: docker-down
	@cargo clean
