#!/bin/bash

# Configuration
CONTAINER_NAME="recall-localnet"
RECALL_LOCALNET_IMAGE="${RECALL_LOCALNET_IMAGE:-textile/recall-localnet:latest}"
RECALL_NETWORK_CONFIG_FILE="${RECALL_NETWORK_CONFIG_FILE:-networks.toml}"

# Stop existing container if running
echo "Stopping existing recall-localnet container if running..."
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

# Start the recall container
echo "Starting recall-localnet container..."
docker run --privileged --rm -d --name $CONTAINER_NAME \
    -p 8545:8545 \
    -p 8645:8645 \
    -p 26657:26657 \
    -p 8001:8001 \
    $RECALL_LOCALNET_IMAGE

# Check if container started successfully
if [ $? -ne 0 ]; then
    echo "Failed to start recall-localnet container"
    exit 1
fi

# Wait for container to be ready
echo "Waiting for recall-localnet to be ready..."
./scripts/check-localnet-container.sh

# Check if wait was successful
if [ $? -ne 0 ]; then
    echo "Recall-localnet failed to become ready"
    docker stop $CONTAINER_NAME 2>/dev/null || true
    docker rm $CONTAINER_NAME 2>/dev/null || true
    exit 1
fi

# Copy networks.toml from container
echo "Copying networks.toml from container..."
docker cp $CONTAINER_NAME:/workdir/localnet-data/networks.toml $RECALL_NETWORK_CONFIG_FILE

if [ $? -ne 0 ]; then
    echo "Failed to copy networks.toml from container"
    exit 1
fi

echo "Recall-localnet is ready and networks.toml has been copied!"