.PHONY: all build test test-fast test-coverage clean docker-up docker-down init-db fmt lint start-recall stop-recall

# Default target
all: build

# Build the project
build:
	cargo build

# Run unit tests (fake implementations only)
test-fast:
	cargo test

# Run integration tests with real implementations
test: docker-up init-db
	@ENABLE_DB_TESTS=true ENABLE_S3_TESTS=true ENABLE_RECALL_TESTS=true ENABLE_SQLITE_TESTS=true RUST_BACKTRACE=1 cargo test -- --nocapture

# Run all tests with coverage
test-coverage: docker-up init-db
	@which cargo-tarpaulin > /dev/null || cargo install cargo-tarpaulin
	@ENABLE_DB_TESTS=true ENABLE_SQLITE_TESTS=true RUST_BACKTRACE=1 cargo tarpaulin --out html

# Format code
fmt:
	cargo fmt --all

# Run linter
lint:
	cargo clippy --all-targets --all-features -- -D warnings

# Start Docker containers for integration tests
docker-up: stop-recall
	@docker compose up -d
	# Wait for PostgreSQL to be ready
	@docker compose exec -T postgres pg_isready -U recall -q || sleep 5
	@docker compose exec -T postgres pg_isready -U recall -q || sleep 5
	@docker compose exec -T postgres pg_isready -U recall -q || (echo "Error: PostgreSQL failed to start" && exit 1)
	# Wait for MinIO to be ready
	@docker compose exec -T minio mc --version > /dev/null 2>&1 || sleep 5
	# Start Recall container
	@$(MAKE) start-recall
	@echo "All services are ready"

# Initialize test database
init-db:
	@docker compose exec -T postgres psql -U recall -d recall_competitions -c "DROP TABLE IF EXISTS object_index CASCADE;" >/dev/null 2>&1 || true
	@docker compose exec -T postgres psql -U recall -d recall_competitions -f /docker-entrypoint-initdb.d/init.sql >/dev/null 2>&1

# Stop Docker containers
docker-down: stop-recall
	@docker compose down

# Start Recall container
start-recall:
	@./scripts/start-recall.sh

# Stop Recall container
stop-recall:
	@./scripts/stop-recall.sh

# Clean the project
clean: docker-down
	@cargo clean
