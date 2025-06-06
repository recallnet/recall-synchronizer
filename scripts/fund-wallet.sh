#!/bin/bash

# Fund a single wallet with ETH from the faucet
# Usage: ./fund-wallet.sh <address> <amount_in_eth>

# Configuration
CONTAINER_NAME="${CONTAINER_NAME:-recall-localnet}"
RECALL_NETWORK_CONFIG_FILE="${RECALL_NETWORK_CONFIG_FILE:-networks.toml}"

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <address> <amount_in_eth>"
    echo "Example: $0 0x1234567890abcdef 100"
    exit 1
fi

ADDRESS="$1"
AMOUNT="$2"

# Validate address format (basic check)
if [[ ! "$ADDRESS" =~ ^0x[a-fA-F0-9]{40}$ ]]; then
    echo "Error: Invalid Ethereum address format: $ADDRESS"
    exit 1
fi

# Check if cast is available
if ! command -v cast &> /dev/null; then
    echo "Error: 'cast' command not found. Install Foundry to fund wallets."
    exit 1
fi

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "Error: Container $CONTAINER_NAME is not running."
    exit 1
fi

# Check if networks.toml exists
if [ ! -f "$RECALL_NETWORK_CONFIG_FILE" ]; then
    echo "Error: $RECALL_NETWORK_CONFIG_FILE not found."
    exit 1
fi

# Extract faucet owner private key from container
FAUCET_KEY=$(docker exec $CONTAINER_NAME sed -n '/^faucet_owner:/,/^[^ ]/{/private_key:/p}' /workdir/localnet-data/state.yml | awk '{print $2}' 2>/dev/null || echo "")

if [ -z "$FAUCET_KEY" ]; then
    echo "Error: Could not extract faucet owner private key from container."
    echo "Please ensure the recall-localnet container is properly initialized."
    exit 1
fi

# Extract RPC URL from networks.toml
RPC_URL=$(awk '/^\[localnet\.subnet_config\]/{f=1} f && /^evm_rpc_url/{gsub(/.*=[ ]*"|"[ ]*$/, ""); print; exit}' $RECALL_NETWORK_CONFIG_FILE)

if [ -z "$RPC_URL" ]; then
    echo "Error: Could not extract EVM RPC URL from $RECALL_NETWORK_CONFIG_FILE"
    echo "Please ensure networks.toml contains localnet.subnet_config.evm_rpc_url"
    exit 1
fi

# Fund the wallet
echo "Funding wallet $ADDRESS with $AMOUNT ETH..."
cast send --rpc-url "$RPC_URL" \
    --private-key "$FAUCET_KEY" \
    "$ADDRESS" \
    --value "${AMOUNT}ether" > /dev/null 2>&1

if [ $? -eq 0 ]; then
    echo "✓ Successfully funded $ADDRESS with $AMOUNT ETH"
    
    # Check balance
    BALANCE=$(cast balance --rpc-url "$RPC_URL" "$ADDRESS" | sed 's/[^0-9]*//g')
    if [ -n "$BALANCE" ]; then
        # Convert from wei to ETH (divide by 10^18)
        BALANCE_ETH=$(echo "scale=4; $BALANCE / 1000000000000000000" | bc 2>/dev/null || echo "unknown")
        echo "  New balance: $BALANCE_ETH ETH"
    fi
else
    echo "✗ Failed to fund $ADDRESS"
    exit 1
fi