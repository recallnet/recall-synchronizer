.PHONY: all build test test-integration clean docker-up docker-down fmt lint

# Default target
all: build

# Build the project
build:
	cargo build

# Run unit tests (fake implementations only)
test:
	cargo test

# Run integration tests with real implementations
test-integration: docker-up
	@echo "Running integration tests with real implementations..."
	@# Give PostgreSQL some time to start up
	@sleep 2
	RUST_BACKTRACE=1 cargo test

# Run all tests with coverage
test-coverage: docker-up
	@echo "Running tests with coverage..."
	@# Install cargo-tarpaulin if not installed
	@which cargo-tarpaulin > /dev/null || cargo install cargo-tarpaulin
	@sleep 2
	cargo tarpaulin --out html

# Format code
fmt:
	cargo fmt --all

# Run linter
lint:
	cargo clippy --all-targets --all-features -- -D warnings

# Start Docker containers for integration tests
docker-up:
	@echo "Starting Docker containers for integration tests..."
	docker-compose up -d
	@echo "Waiting for containers to be ready..."

# Stop Docker containers
docker-down:
	@echo "Stopping Docker containers..."
	docker-compose down

# Clean the project
clean: docker-down
	cargo clean

# Init schema
init-schema: docker-up
	@echo "Initializing database schema..."
	@sleep 2
	docker-compose exec postgres psql -U recall -d recall_competitions -f /docker-entrypoint-initdb.d/init.sql