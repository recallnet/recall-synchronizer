#!/usr/bin/env bash
# Cleanup script for recall-synchronizer
# This script ensures graceful shutdown and cleanup of resources

set -e

echo "Starting cleanup for recall-synchronizer..."

echo "Stopping docker compose services..."
docker compose down --timeout 30

echo "Removing volumes..."
docker compose down -v

echo "Cleaning up orphaned containers..."
docker compose down --remove-orphans

echo "Cleanup completed successfully"