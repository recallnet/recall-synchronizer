# Docker Support for Recall Synchronizer

This document describes how to build and run the Recall Synchronizer using Docker.

## Docker Image

The Recall Synchronizer is automatically built and published to Docker Hub on every push to the main branch and for every tagged release.

### Image Tags

- `latest` - Latest build from main branch
- `v1.0.0` - Specific version tags
- `main-<sha>` - Specific commit builds
- `pr-<number>` - Pull request builds (not pushed)

## Building Locally

To build the Docker image locally:

```bash
docker build -t recall-synchronizer:local .
```

For multi-platform build (AMD64 and ARM64):

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t recall-synchronizer:local .
```

## Running with Docker

### Quick Start

Run the synchronizer with minimal configuration:

```bash
docker run -it --rm \
  -v ./config.toml:/app/config.toml:ro \
  -v ./networks.toml:/app/networks.toml:ro \
  -v ./sync-data:/data \
  textile/recall-synchronizer:latest \
  run --config /app/config.toml
```


## Configuration

The Docker image can be configured in two ways:

### Method 1: Using Configuration Files (Recommended)

Mount your `config.toml` and `networks.toml` files:

```bash
docker run -it --rm \
  -v ./config.toml:/app/config.toml:ro \
  -v ./networks.toml:/app/networks.toml:ro \
  -v ./sync-data:/data \
  textile/recall-synchronizer:latest \
  run --config /app/config.toml
```

### Method 2: Using Environment Variables

The application primarily uses a configuration file, but some Recall settings can be overridden via environment variables:

### Environment Variables (for overriding config.toml)

- `RECALL_NETWORK` - Override the network name (e.g., testnet, localnet)
- `RECALL_NETWORK_FILE` - Override the path to networks.toml file
- `RECALL_PRIVATE_KEY` - Override the private key for Recall network
- `RECALL_BUCKET_ADDRESS` - Override the bucket address for Recall storage

### Logging

- `RUST_LOG` - Log level (default: info, options: trace, debug, info, warn, error)

## Volumes

The container uses two main volume mounts:

1. **`/data`** - Working directory for storing the SQLite sync state database. Mount this to persist sync state across container restarts:
   ```bash
   -v /path/to/data:/data
   ```

2. **`/app/networks.toml`** - Recall network configuration file. Mount your networks.toml file:
   ```bash
   -v ./networks.toml:/app/networks.toml:ro
   ```

Example with both volumes:
```bash
docker run -v ./networks.toml:/app/networks.toml:ro -v ./sync-data:/data ...
```

## Command Line Options

The Docker image supports all CLI commands:

### Run Once

```bash
docker run --rm textile/recall-synchronizer:latest run
```

### Start Continuous Sync

```bash
docker run --rm textile/recall-synchronizer:latest start --interval 60
```

### Reset Sync State

```bash
docker run --rm textile/recall-synchronizer:latest reset
```

### With Competition Filter

```bash
docker run --rm textile/recall-synchronizer:latest run --competition-id <uuid>
```

## Troubleshooting

### Container Exits Immediately

Check logs for configuration errors:

```bash
docker logs <container-id>
```

### Config File Not Found

If you see an error like "Failed to load configuration: Failed to read config file", ensure you're using the correct paths:

```bash
# Use absolute paths or $(pwd) for current directory
docker run -it --rm \
  -v $(pwd)/config.toml:/app/config.toml:ro \
  -v $(pwd)/networks.toml:/app/networks.toml:ro \
  -v /home/user/sync_state:/data \
  textile/recall-synchronizer:latest \
  run --config /app/config.toml
```

### Permission Denied Errors

Ensure the mounted volume has proper permissions for uid 1000:

```bash
chown 1000:1000 /path/to/data
```
