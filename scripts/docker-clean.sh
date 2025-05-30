#!/bin/bash

# Clean up Docker resources for the recall-synchronizer project

echo "Cleaning up Docker resources..."

# Stop and remove containers
echo "Stopping and removing containers..."
docker compose down --remove-orphans 2>/dev/null || true

# Remove any dangling recall-localnet container
CONTAINER_NAME="recall-localnet"
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Removing $CONTAINER_NAME container..."
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
fi

# Prune unused networks (be careful, this removes all unused networks)
echo "Pruning unused Docker networks..."
docker network prune -f

# Optional: Remove volumes (commented out by default to preserve data)
# echo "Removing volumes..."
# docker compose down -v

echo "Docker cleanup complete!"
echo ""
echo "You can now run 'make test' or 'make docker-up' to start fresh."