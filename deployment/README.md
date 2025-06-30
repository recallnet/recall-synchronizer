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
docker build -t textilemachine/recall-synchronizer:latest .
```

### Push to Docker Hub

```bash
# Login to Docker Hub (required once)
docker login

# Push the image
docker push textilemachine/recall-synchronizer:latest
```

### Tag with Version

```bash
# Tag with specific version
docker tag textilemachine/recall-synchronizer:latest textilemachine/recall-synchronizer:v0.1.0

# Push versioned tag
docker push textilemachine/recall-synchronizer:v0.1.0
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

### Manual Deployment

If deploying manually, ensure directories exist and have proper permissions:

```bash
# Create directories
mkdir -p ./logs /home/islam/sync_state

# Set permissions for container access
chmod 777 ./logs /home/islam/sync_state

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
- `/home/islam/sync_state:/data` - Persistent state storage
- `./logs:/app/logs` - Application log files


## Monitoring

### Logging

The synchronizer logs to both console and file. Use `--log-file` to specify the log file path (default: `./logs.log`).

In Docker, logs are written to `/app/logs/synchronizer.log` which is mounted to `./logs` on the host.

Docker logs (console output):
```bash
docker-compose logs -f synchronizer
```

File logs (on host):
```bash
tail -f ./logs/synchronizer.log
```

Use `--verbose` or `-v` flag to enable DEBUG level logging (default is INFO).


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

If you see "Permission denied" errors for logs or database:

```bash
# Fix permissions
chmod 777 ./logs /home/islam/sync_state
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
ls -la /home/islam/sync_state/
```

#### Check Permissions
```bash
ls -ld ./logs /home/islam/sync_state
```
