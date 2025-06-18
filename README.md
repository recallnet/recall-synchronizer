# Recall Data Synchronizer

A Rust application that synchronizes data from PostgreSQL (with optional S3 storage) to the Recall Network.

## Overview

The Recall Data Synchronizer bridges centralized data storage with the Recall Network by:
- Reading object metadata from PostgreSQL's `object_index` table
- Fetching object data from S3 (when configured) or directly from PostgreSQL
- Writing data to the Recall Network with preserved structure
- Maintaining synchronization state to resume from interruptions

## Architecture

### Data Flow
1. Query PostgreSQL `object_index` table for new/updated objects
2. Fetch object data from S3 (if configured) or PostgreSQL
3. Write to Recall Network with key format: `[competition_id/][agent_id/]<data_type>/<uuid>`
4. Track progress in local SQLite database

### Storage Modes
- **S3 Mode**: Object metadata in PostgreSQL, data in S3
- **Direct Mode**: Both metadata and data in PostgreSQL

## Quick Start

### Using Docker

```bash
# With configuration file
docker run -it --rm \
  -v ./config.toml:/app/config.toml:ro \
  -v ./sync-data:/data \
  textile/recall-synchronizer:latest \
  run --config /app/config.toml

# Start continuous sync (every 60 seconds)
docker run -it --rm \
  -v ./config.toml:/app/config.toml:ro \
  -v ./sync-data:/data \
  textile/recall-synchronizer:latest \
  start --interval 60
```

### Local Development

```bash
# Clone and setup
git clone <repository-url>
cd recall-synchronizer
cp config.example.toml config.toml

# Start development environment
docker-compose up -d

# Run once
cargo run -- run

# Run continuously (every 60 seconds)
cargo run -- start --interval 60

# Reset sync state
cargo run -- reset
```

## Configuration

Create a `config.toml` file:

```toml
[database]
url = "postgresql://user:pass@localhost:5432/db"

[sync_storage]
db_path = "./sync-state.db"

[sync]
batch_size = 100

[recall]
network = "localnet"
private_key = "your-private-key"
config_path = "./networks.toml"

# Optional S3 configuration
[s3]
endpoint = "https://s3.amazonaws.com"
region = "us-east-1"
bucket = "your-bucket"
access_key_id = "your-key"
secret_access_key = "your-secret"
```

## Commands

### run
Run synchronization once:
```bash
cargo run -- run [--since <TIMESTAMP>]
```
- `--since`: Sync objects created after this timestamp (RFC3339 format)

### start
Run continuously at specified interval:
```bash
cargo run -- start --interval <SECONDS> [--since <TIMESTAMP>]
```
- `--interval`: Seconds between sync runs (required)
- `--since`: Initial timestamp filter

### reset
Clear synchronization state:
```bash
cargo run -- reset
```

## Data Model

### PostgreSQL Schema

The `object_index` table stores object metadata:
- `id`: UUID primary key
- `object_key`: S3 key (required in S3 mode)
- `competition_id`: Optional UUID
- `agent_id`: Optional UUID
- `data_type`: String identifier (e.g., "LOGS", "METRICS")
- `data`: Binary data (required in direct mode)
- `created_at`: Timestamp for sync ordering

### Recall Network Keys

Objects are stored with structured keys:
```
[competition_id/][agent_id/]<data_type>/<uuid>
```

Optional segments are omitted if not present.

## Development

### Running Tests

```bash
# Fast tests (in-memory only)
make test-fast

# Integration tests (PostgreSQL + SQLite)
make test

# With coverage
make test-coverage
```

### Code Quality

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

## Docker Support

Pre-built images are available on Docker Hub:
- `textile/recall-synchronizer:latest`
- `textile/recall-synchronizer:<version>`

See [DOCKER.md](DOCKER.md) for detailed Docker usage.

## Architecture Decisions

- **Single Database Type**: Uses PostgreSQL with different schemas for S3/Direct modes
- **No Per-Competition Tracking**: Simplified to global synchronization progress
- **Batch Processing**: Configurable batch size for efficient processing
- **Resilient State Management**: SQLite for tracking sync progress with atomic operations