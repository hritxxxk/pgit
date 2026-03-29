#!/bin/bash
set -e

# Parse inputs
DATA_FILE="$1"
DATASET_NAME="$2"
THRESHOLD="$3"
FAIL_ON_DRIFT="$4"
REMOTE_PROVIDER="$5"
REMOTE_BUCKET="$6"
REMOTE_PREFIX="$7"
AWS_REGION="$8"
ACTION_TYPE="$9"

echo "🔍 PGit GitHub Action"
echo "===================="
echo "Data file: $DATA_FILE"
echo "Dataset: $DATASET_NAME"
echo "Threshold: $THRESHOLD"
echo "Action: $ACTION_TYPE"
echo ""

# Set outputs file
OUTPUTS_FILE="${GITHUB_OUTPUT:-/tmp/outputs}"

# Initialize outputs
echo "drift_detected=false" >> "$OUTPUTS_FILE"
echo "min_p_value=1.0" >> "$OUTPUTS_FILE"
echo "features_checked=0" >> "$OUTPUTS_FILE"

# Check if file exists
if [ ! -f "$DATA_FILE" ]; then
    echo "❌ Error: Data file not found: $DATA_FILE"
    exit 1
fi

# Initialize pgit if needed
if [ ! -d ".pgit" ]; then
    echo "📦 Initializing pgit repository..."
    pgit init
fi

# Configure remote if provided
if [ -n "$REMOTE_PROVIDER" ] && [ -n "$REMOTE_BUCKET" ]; then
    echo "🌐 Configuring remote storage..."
    REMOTE_ARGS="remote add $REMOTE_PROVIDER $REMOTE_BUCKET"
    
    if [ -n "$REMOTE_PREFIX" ]; then
        REMOTE_ARGS="$REMOTE_ARGS --prefix $REMOTE_PREFIX"
    fi
    
    if [ -n "$AWS_REGION" ] && [ "$REMOTE_PROVIDER" = "s3" ]; then
        export AWS_REGION="$AWS_REGION"
    fi
    
    pgit $REMOTE_ARGS
fi

# Pull from remote first if action_type is 'pull' or 'all'
if [ "$ACTION_TYPE" = "pull" ] || [ "$ACTION_TYPE" = "all" ]; then
    if [ -f ".pgit-remote" ]; then
        echo "⬇️  Pulling manifests from remote..."
        pgit pull
    fi
fi

# Run drift check if action_type is 'check' or 'all'
if [ "$ACTION_TYPE" = "check" ] || [ "$ACTION_TYPE" = "all" ]; then
    echo "📊 Running statistical drift check..."
    echo ""
    
    # Run pgit check and capture output
    set +e
    CHECK_OUTPUT=$(pgit check "$DATA_FILE" "$DATASET_NAME" --threshold "$THRESHOLD" 2>&1)
    CHECK_EXIT_CODE=$?
    set -e
    
    echo "$CHECK_OUTPUT"
    echo ""
    
    # Parse output for metrics
    DRIFT_COUNT=$(echo "$CHECK_OUTPUT" | grep -c "⚠️  DRIFT" || true)
    OK_COUNT=$(echo "$CHECK_OUTPUT" | grep -c "✓ OK" || true)
    FEATURES_CHECKED=$((DRIFT_COUNT + OK_COUNT))
    
    # Extract minimum p-value
    MIN_P_VALUE=$(echo "$CHECK_OUTPUT" | grep -oP 'p=\K[0-9.]+' | sort -n | head -1)
    if [ -z "$MIN_P_VALUE" ]; then
        MIN_P_VALUE="1.0"
    fi
    
    # Set outputs
    if [ "$CHECK_EXIT_CODE" -eq 0 ]; then
        echo "drift_detected=false" >> "$OUTPUTS_FILE"
    else
        echo "drift_detected=true" >> "$OUTPUTS_FILE"
    fi
    echo "min_p_value=$MIN_P_VALUE" >> "$OUTPUTS_FILE"
    echo "features_checked=$FEATURES_CHECKED" >> "$OUTPUTS_FILE"
    
    # Handle drift detection
    if [ "$CHECK_EXIT_CODE" -ne 0 ]; then
        if [ "$FAIL_ON_DRIFT" = "true" ]; then
            echo "❌ Drift detected and fail_on_drift is true. Failing workflow."
            exit 1
        else
            echo "⚠️  Drift detected but fail_on_drift is false. Continuing workflow."
        fi
    fi
fi

# Push to remote if action_type is 'push' or 'all'
if [ "$ACTION_TYPE" = "push" ] || [ "$ACTION_TYPE" = "all" ]; then
    if [ -f ".pgit-remote" ]; then
        echo "⬆️  Pushing manifests to remote..."
        pgit push
    else
        echo "⚠️  No remote configured. Skipping push."
    fi
fi

echo ""
echo "✅ PGit action completed successfully"
