.PHONY: all build test test-fast test-coverage clean docker-up docker-down init-db fmt lint

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
	@ENABLE_DB_TESTS=true ENABLE_S3_TESTS=true ENABLE_RECALL_TESTS=false ENABLE_SQLITE_TESTS=true RUST_BACKTRACE=1 cargo test -- --nocapture

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
docker-up:
	@docker-compose up -d
	@docker-compose exec -T postgres pg_isready -U recall -q || sleep 5
	@docker-compose exec -T postgres pg_isready -U recall -q || sleep 5
	@docker-compose exec -T postgres pg_isready -U recall -q || (echo "Error: PostgreSQL failed to start" && exit 1)

# Initialize test database
init-db:
	@docker-compose exec -T postgres psql -U recall -d recall_competitions -c "DROP TABLE IF EXISTS object_index CASCADE;" >/dev/null 2>&1 || true
	@docker-compose exec -T postgres psql -U recall -d recall_competitions -f /docker-entrypoint-initdb.d/init.sql >/dev/null 2>&1

# Stop Docker containers
docker-down:
	@docker-compose down

# Clean the project
clean: docker-down
	@cargo clean
