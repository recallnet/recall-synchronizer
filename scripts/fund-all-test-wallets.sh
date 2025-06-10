#!/bin/bash

# Fund all test wallets from test-wallets.json

# Configuration
TEST_WALLETS_FILE="${TEST_WALLETS_FILE:-test-wallets.json}"
ETH_PER_WALLET="${ETH_PER_WALLET:-100}"

# Check if jq is available
if ! command -v jq &> /dev/null; then
    echo "Error: 'jq' command not found. Install jq to parse JSON files."
    exit 1
fi

# Check if test-wallets.json exists
if [ ! -f "$TEST_WALLETS_FILE" ]; then
    echo "Error: $TEST_WALLETS_FILE not found."
    echo "Please ensure test-wallets.json exists in the project root."
    exit 1
fi

# Get the script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FUND_WALLET_SCRIPT="$SCRIPT_DIR/fund-wallet.sh"

# Read addresses from test-wallets.json
ADDRESSES=$(jq -r '.[].address' $TEST_WALLETS_FILE)
WALLET_COUNT=$(echo "$ADDRESSES" | wc -l | tr -d ' ')

echo "Funding $WALLET_COUNT test wallets with $ETH_PER_WALLET ETH each..."
echo ""

i=1
SUCCESS_COUNT=0
FAIL_COUNT=0
FAILED_ADDRESSES=()

while IFS= read -r ADDRESS; do
    echo "[$i/$WALLET_COUNT] Processing wallet: $ADDRESS"
    
    # Use the fund-wallet.sh script
    if $FUND_WALLET_SCRIPT "$ADDRESS" "$ETH_PER_WALLET"; then
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_ADDRESSES+=("$ADDRESS")
    fi
    
    echo ""
    i=$((i + 1))
done <<< "$ADDRESSES"

echo "========================================="
echo "Funding Summary:"
echo "  Total wallets: $WALLET_COUNT"
echo "  Successful: $SUCCESS_COUNT"
echo "  Failed: $FAIL_COUNT"

if [ $FAIL_COUNT -gt 0 ]; then
    echo ""
    echo "Failed wallets:"
    for addr in "${FAILED_ADDRESSES[@]}"; do
        echo "  - $addr"
    done
    echo ""
    echo "You can retry funding individual wallets with:"
    echo "  $FUND_WALLET_SCRIPT <address> $ETH_PER_WALLET"
    exit 1
else
    echo ""
    echo "✓ All wallets funded successfully!"
fi