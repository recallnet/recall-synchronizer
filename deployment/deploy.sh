#!/usr/bin/env bash
# Deployment script for recall-synchronizer
# This script handles the deployment with proper checks and logging

set -e

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yaml}"
LOG_DIR="./logs"
SYNC_STATE_DIR="${SYNC_STATE_DIR:-./sync_state}"

echo "Starting deployment of recall-synchronizer..."
echo "Using compose file: $COMPOSE_FILE"
echo "Using sync state directory: $SYNC_STATE_DIR"

echo "Creating required directories..."
mkdir -p "$LOG_DIR"
mkdir -p "$SYNC_STATE_DIR"

chmod 777 "$LOG_DIR"
chmod 777 "$SYNC_STATE_DIR"

echo "Checking configuration files..."
if [ ! -f "./config.toml" ]; then
    echo "ERROR: config.toml not found!"
    exit 1
fi

if [ ! -f "./networks.toml" ]; then
    echo "ERROR: networks.toml not found!"
    exit 1
fi

echo "Pulling latest image..."
docker compose -f "$COMPOSE_FILE" pull

echo "Stopping existing services..."
docker compose -f "$COMPOSE_FILE" down --timeout 30

echo "Starting services..."
docker compose -f "$COMPOSE_FILE" up -d

echo "Service status:"
docker compose -f "$COMPOSE_FILE" ps

echo "Recent logs:"
docker compose -f "$COMPOSE_FILE" logs --tail=20

echo "Deployment completed successfully!"
echo "To view logs: docker compose -f $COMPOSE_FILE logs -f"
echo "To stop: ./cleanup.sh"