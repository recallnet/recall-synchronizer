#!/bin/bash

# Configuration
CONTAINER_NAME="recall-localnet"

# Stop the recall container
echo "Stopping recall-localnet container..."
docker stop $CONTAINER_NAME 2>/dev/null || true
docker rm $CONTAINER_NAME 2>/dev/null || true

echo "Recall-localnet container stopped."