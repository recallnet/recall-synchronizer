#!/bin/bash

# Check the balance of a wallet
# Usage: ./check-wallet-balance.sh <address>

# Configuration
RECALL_NETWORK_CONFIG_FILE="${RECALL_NETWORK_CONFIG_FILE:-networks.toml}"

# Check arguments
if [ $# -lt 1 ]; then
    echo "Usage: $0 <address>"
    echo "Example: $0 0x1234567890abcdef"
    exit 1
fi

ADDRESS="$1"

# Validate address format (basic check)
if [[ ! "$ADDRESS" =~ ^0x[a-fA-F0-9]{40}$ ]]; then
    echo "Error: Invalid Ethereum address format: $ADDRESS"
    exit 1
fi

# Check if cast is available
if ! command -v cast &> /dev/null; then
    echo "Error: 'cast' command not found. Install Foundry to check balances."
    exit 1
fi

# Check if networks.toml exists
if [ ! -f "$RECALL_NETWORK_CONFIG_FILE" ]; then
    echo "Error: $RECALL_NETWORK_CONFIG_FILE not found."
    exit 1
fi

# Extract RPC URL from networks.toml
RPC_URL=$(awk '/^\[localnet\.subnet_config\]/{f=1} f && /^evm_rpc_url/{gsub(/.*=[ ]*"|"[ ]*$/, ""); print; exit}' $RECALL_NETWORK_CONFIG_FILE)

if [ -z "$RPC_URL" ]; then
    echo "Error: Could not extract EVM RPC URL from $RECALL_NETWORK_CONFIG_FILE"
    exit 1
fi

# Check balance
echo "Checking balance for $ADDRESS..."
BALANCE_WEI=$(cast balance --rpc-url "$RPC_URL" "$ADDRESS" 2>/dev/null)

if [ $? -eq 0 ] && [ -n "$BALANCE_WEI" ]; then
    # Remove any non-numeric characters
    BALANCE_WEI_CLEAN=$(echo "$BALANCE_WEI" | sed 's/[^0-9]//g')
    
    # Convert from wei to ETH using awk (more portable than bc)
    BALANCE_ETH=$(echo "$BALANCE_WEI_CLEAN" | awk '{printf "%.6f", $1/1000000000000000000}')
    
    echo "Balance: $BALANCE_ETH ETH ($BALANCE_WEI_CLEAN wei)"
else
    echo "Error: Failed to check balance"
    exit 1
fi