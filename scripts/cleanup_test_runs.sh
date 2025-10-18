#!/bin/bash
# Cleanup old test runs, keeping only the N most recent
# Usage: ./scripts/cleanup_test_runs.sh [keep_count]

KEEP_COUNT=${1:-3}
TEST_DIR="data/test"

if [ ! -d "$TEST_DIR" ]; then
    echo "No test directory found at $TEST_DIR"
    exit 0
fi

# Count existing test runs
TOTAL=$(find "$TEST_DIR" -maxdepth 1 -type d ! -path "$TEST_DIR" | wc -l | tr -d ' ')

if [ "$TOTAL" -le "$KEEP_COUNT" ]; then
    echo "Only $TOTAL test run(s) found. Nothing to clean (keeping $KEEP_COUNT)."
    exit 0
fi

echo "Found $TOTAL test run(s). Keeping most recent $KEEP_COUNT..."

# Remove old test runs (keep most recent N)
find "$TEST_DIR" -maxdepth 1 -type d ! -path "$TEST_DIR" | \
    sort -r | \
    tail -n +$((KEEP_COUNT + 1)) | \
    while read -r dir; do
        echo "  Removing: $(basename "$dir")"
        rm -rf "$dir"
    done

REMAINING=$(find "$TEST_DIR" -maxdepth 1 -type d ! -path "$TEST_DIR" | wc -l | tr -d ' ')
echo "Cleanup complete. $REMAINING test run(s) remaining."
