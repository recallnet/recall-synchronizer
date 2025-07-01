# Recall Synchronizer Deployment

This directory contains the Docker Compose configuration and scripts for deploying the Recall Synchronizer service.

## Files

- `docker-compose.yaml` - Docker Compose configuration for deployment
- `deploy.sh` - Deployment script with health checks
- `cleanup.sh` - Graceful shutdown and cleanup script

## Prerequisites

1. Docker and Docker Compose installed
2. IPv6 enabled on the host
3. Configuration files:
   - `config.toml` - Service configuration
   - `networks.toml` - Network configuration

## Building and Publishing

### Build Docker Image

```bash
# From the project root directory
docker build -t textile/recall-synchronizer:latest .
```

### Push to Docker Hub

```bash
# Login to Docker Hub (required once)
docker login

# Push the image
docker push textile/recall-synchronizer:latest
```

### Tag with Version

```bash
# Tag with specific version
docker tag textile/recall-synchronizer:latest textile/recall-synchronizer:v0.1.0

# Push versioned tag
docker push textile/recall-synchronizer:v0.1.0
```

## Deployment

### Using Deploy Script (Recommended)

```bash
./deploy.sh
```

The deploy script automatically:
- Creates required directories with proper permissions
- Verifies configuration files exist
- Pulls the latest Docker image
- Starts the service with health checks

#### Custom Sync State Directory

To use a different sync state directory:

```bash
SYNC_STATE_DIR=/path/to/your/data ./deploy.sh
```

### Manual Deployment

If deploying manually:

```bash
# Set sync state directory (optional, defaults to ./sync_state)
export SYNC_STATE_DIR=/path/to/your/data

# Create directories with write permissions
mkdir -p ./logs "${SYNC_STATE_DIR:-./sync_state}"
chmod 777 ./logs "${SYNC_STATE_DIR:-./sync_state}"

# Start services
docker-compose up -d
```

## Configuration

### Network Configuration

The service requires IPv6 support for connecting to PostgreSQL:
- `recall-synchronizer` - Network with IPv6 enabled

### Volume Mounts

- `./config.toml:/app/config.toml:ro` - Service configuration (read-only)
- `./networks.toml:/app/networks.toml:ro` - Network configuration (read-only)
- `${SYNC_STATE_DIR}:/data` - Persistent state storage (defaults to `./sync_state`)
- `./logs:/app/logs` - Application log files


## Monitoring

### Logging

The synchronizer always logs to console. File logging is optional and configured in `config.toml`:

```toml
# Optional - if omitted, only logs to console
[logging]
path = "/app/logs/synchronizer.log"
level = "info"  # Options: trace, debug, info, warn, error
size = 50  # Maximum log file size in MB before rotation
max_files = 5  # Number of rotated files to keep (default: 5)
```

When file logging is enabled in Docker, ensure the path is under `/app/logs/` (mounted to `./logs` on the host).

Docker logs (console output):
```bash
docker-compose logs -f synchronizer
```

File logs (on host):
```bash
tail -f ./logs/synchronizer.log
```

The log level is configured in the `config.toml` file (default is INFO).


## Maintenance

### Stop Services

```bash
./cleanup.sh
```

### Update Service

```bash
docker-compose pull
./deploy.sh
```

### View Status

```bash
docker-compose ps
```

## Troubleshooting

### Common Issues

#### Permission Denied Errors

If you see "Permission denied" errors:

```bash
# Ensure directories have write permissions
chmod 777 ./logs ${SYNC_STATE_DIR:-./sync_state}
```

#### Container Can't Start

Check logs for errors:
```bash
docker-compose logs -f --tail=100 synchronizer
```

### Debugging Commands

#### Inspect Container
```bash
docker-compose exec synchronizer /bin/sh
```

#### Verify State Directory
```bash
ls -la ${SYNC_STATE_DIR:-./sync_state}/
```

#### Check Permissions
```bash
ls -ld ./logs ${SYNC_STATE_DIR:-./sync_state}
```
