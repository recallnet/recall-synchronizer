#!/bin/bash

# Configuration
CONTAINER_NAME="recall-localnet"
RECALL_LOCALNET_IMAGE="${RECALL_LOCALNET_IMAGE:-textile/recall-localnet:sha-a72edb8-e7f57d2}"
RECALL_NETWORK_CONFIG_FILE="${RECALL_NETWORK_CONFIG_FILE:-networks.toml}"
TEST_WALLETS_FILE="${TEST_WALLETS_FILE:-test-wallets.json}"
ETH_PER_WALLET="${ETH_PER_WALLET:-100000}"

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

# Fund test wallets if jq is available
if command -v jq &> /dev/null; then
    echo "Funding test wallets..."
    
    # Check if test-wallets.json exists
    if [ ! -f "$TEST_WALLETS_FILE" ]; then
        echo "Error: $TEST_WALLETS_FILE not found."
        echo "Please ensure test-wallets.json exists in the project root."
        exit 1
    fi
    
    # Check if fund-wallet.sh exists
    FUND_WALLET_SCRIPT="./scripts/fund-wallet.sh"
    if [ ! -x "$FUND_WALLET_SCRIPT" ]; then
        echo "Error: $FUND_WALLET_SCRIPT not found or not executable."
        exit 1
    fi
    
    # Read addresses from test-wallets.json and fund them
    ADDRESSES=$(jq -r '.[].address' $TEST_WALLETS_FILE)
    WALLET_COUNT=$(echo "$ADDRESSES" | wc -l | tr -d ' ')
    
    echo "Funding $WALLET_COUNT test wallets with $ETH_PER_WALLET ETH each..."
    
    i=1
    SUCCESS_COUNT=0
    FAIL_COUNT=0
    
    while IFS= read -r ADDRESS; do
        echo ""
        echo "[$i/$WALLET_COUNT] Processing wallet: $ADDRESS"
        
        # Use the fund-wallet.sh script
        if $FUND_WALLET_SCRIPT "$ADDRESS" "$ETH_PER_WALLET"; then
            SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
        else
            FAIL_COUNT=$((FAIL_COUNT + 1))
            echo "  ✗ Failed to fund $ADDRESS"
        fi
        
        i=$((i + 1))
    done <<< "$ADDRESSES"
    
    echo ""
    echo "Funding complete: $SUCCESS_COUNT succeeded, $FAIL_COUNT failed"
    
    if [ $FAIL_COUNT -gt 0 ]; then
        echo "Warning: Some wallets failed to fund. Tests may fail for those wallets."
    fi
else
    echo "Warning: 'jq' command not found. Install jq to parse JSON files."
    echo "Tests may fail if wallets are not funded."
    echo "You can manually fund wallets using: ./scripts/fund-wallet.sh <address> <amount>"
fi