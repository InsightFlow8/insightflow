#!/bin/bash
# create_s3_structure.sh
# Create necessary S3 directory structure for Glue crawlers
#
# PURPOSE:
# - Glue Crawlers validate S3 target paths during creation
# - Empty or non-existent paths cause deployment failures
# - This script pre-creates directory structure with placeholder files
# - Placeholder files will be overwritten when real data is uploaded
#
# USAGE:
# ./create_s3_structure.sh [bucket_name] [prefix]
# ./create_s3_structure.sh insightflow-raw-bucket data/batch

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
WHITE='\033[1;37m'
NC='\033[0m' # No Color

# Default parameters
BUCKET_NAME=${1:-"insightflow-raw-bucket"}
PREFIX=${2:-"data/batch"}

echo -e "${GREEN}=================================${NC}"
echo -e "${GREEN}Creating S3 directory structure for Glue crawlers${NC}"
echo -e "${GREEN}=================================${NC}"
echo -e "${CYAN}Bucket: ${BUCKET_NAME}${NC}"
echo -e "${CYAN}Prefix: ${PREFIX}${NC}"
echo ""

# Check if AWS CLI is available
if ! command -v aws &> /dev/null; then
    echo -e "${RED}✗ AWS CLI is not installed or not in PATH${NC}"
    echo -e "${YELLOW}Please install AWS CLI and configure credentials${NC}"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}✗ AWS credentials not configured or invalid${NC}"
    echo -e "${YELLOW}Please run 'aws configure' to set up credentials${NC}"
    exit 1
fi

# Array of tables
tables=("orders" "products" "departments" "aisles" "order_products_prior" "order_products_train")

# Create temporary file
temp_file="placeholder.txt"

for table in "${tables[@]}"; do
    s3_path="s3://${BUCKET_NAME}/${PREFIX}/${table}/"
    echo -e "${YELLOW}Creating directory: ${s3_path}${NC}"
    
    # Create placeholder content
    cat > "$temp_file" << EOF
# Placeholder file for Glue Crawler directory structure
# Created: $(date '+%Y-%m-%d %H:%M:%S')
# Purpose: Ensure S3 directory exists for Glue Crawler deployment
# Note: This file will be overwritten when real data is uploaded

Table: ${table}
S3 Path: ${s3_path}
Status: Directory structure ready for Glue Crawler
EOF
    
    # Upload to S3
    if aws s3 cp "$temp_file" "${s3_path}${temp_file}" &> /dev/null; then
        echo -e "${GREEN}✓ Created: ${s3_path}${NC}"
    else
        echo -e "${RED}✗ Failed to create: ${s3_path}${NC}"
        # Check if bucket exists
        if ! aws s3 ls "s3://${BUCKET_NAME}" &> /dev/null; then
            echo -e "${YELLOW}  Bucket '${BUCKET_NAME}' may not exist or access denied${NC}"
        fi
    fi
done

# Clean up temporary file
rm -f "$temp_file"

echo ""
echo -e "${GREEN}=================================${NC}"
echo -e "${GREEN}Directory structure creation completed!${NC}"
echo -e "${GREEN}=================================${NC}"
echo ""
echo -e "${YELLOW}What was created:${NC}"
echo -e "${WHITE}- Placeholder files in each table directory${NC}"
echo -e "${WHITE}- These files ensure S3 paths exist for Glue Crawlers${NC}"
echo -e "${WHITE}- Files will be overwritten by real data uploads${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo -e "${WHITE}1. Run 'terraform plan' to review changes${NC}"
echo -e "${WHITE}2. Run 'terraform apply' to deploy Glue crawlers${NC}"
echo -e "${WHITE}3. Upload real data to replace placeholder files${NC}"
