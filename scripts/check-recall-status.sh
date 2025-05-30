#!/bin/bash

# Check the status of the recall-localnet container

CONTAINER_NAME="${CONTAINER_NAME:-recall-localnet}"

# Check if container exists
if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Container $CONTAINER_NAME does not exist"
    exit 1
fi

# Check if container is running
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Container $CONTAINER_NAME is running"
    
    # Get container status details
    STATUS=$(docker inspect -f '{{.State.Status}}' $CONTAINER_NAME)
    STARTED_AT=$(docker inspect -f '{{.State.StartedAt}}' $CONTAINER_NAME)
    
    echo "  Status: $STATUS"
    echo "  Started at: $STARTED_AT"
    
    # Check container health if available
    HEALTH_STATUS=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}no health check{{end}}' $CONTAINER_NAME)
    echo "  Health: $HEALTH_STATUS"
    
    exit 0
else
    echo "Container $CONTAINER_NAME exists but is not running"
    
    # Get last status
    STATUS=$(docker inspect -f '{{.State.Status}}' $CONTAINER_NAME)
    FINISHED_AT=$(docker inspect -f '{{.State.FinishedAt}}' $CONTAINER_NAME)
    EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' $CONTAINER_NAME)
    
    echo "  Status: $STATUS"
    echo "  Finished at: $FINISHED_AT"
    echo "  Exit code: $EXIT_CODE"
    
    exit 2
fi