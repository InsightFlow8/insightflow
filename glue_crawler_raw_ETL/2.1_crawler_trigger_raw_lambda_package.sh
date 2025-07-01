#!/bin/bash
# =============================================
# Script: 2.1_crawler_trigger_raw_lambda_package.sh
# Purpose: Package Lambda function for triggering Glue Crawlers (raw layer)
# Target ZIP: /terraform/assets/2.1_glue_crawler_raw_scheduler.zip
# Handler: lambda_trigger_raw_crawlers.lambda_handler
# =============================================

set -e  # exit on first error

# Define project root directory (assuming this script lives in glue_crawler_raw_ETL/)
PROJECT_ROOT=$(cd "$(dirname "$0")/.." && pwd)

# Define paths
SOURCE_PY="$PROJECT_ROOT/glue_crawler_raw_ETL/2.1_glue_crawler_raw_scheduler.py"
DEST_ZIP="$PROJECT_ROOT/terraform/assets/2.1_glue_crawler_raw_scheduler.zip"
HANDLER_NAME="lambda_trigger_raw_crawlers"
TEMP_DIR="/tmp/lambda_build_raw"

# Step 1: Prepare build folder and copy source
mkdir -p "$TEMP_DIR"
cp "$SOURCE_PY" "$TEMP_DIR/${HANDLER_NAME}.py"

# Step 2: Create zip package
mkdir -p "$PROJECT_ROOT/terraform/assets"
cd "$TEMP_DIR"
zip -r9 "$DEST_ZIP" .

# Step 3: Cleanup temporary directory
cd "$PROJECT_ROOT"
rm -rf "$TEMP_DIR"

# Step 4: Confirm result
echo "✅ Lambda package created: $DEST_ZIP"
echo "➡  Handler must be set as: ${HANDLER_NAME}.lambda_handler"