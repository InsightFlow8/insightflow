#!/bin/bash
# validate_terraform.sh
# Quick validation script for Terraform configuration

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${GREEN}===================================${NC}"
echo -e "${GREEN}Terraform Configuration Validation${NC}"
echo -e "${GREEN}===================================${NC}"

cd "$(dirname "$0")"

echo -e "${YELLOW}1. Checking Terraform installation...${NC}"
if ! command -v terraform &> /dev/null; then
    echo -e "${RED}✗ Terraform is not installed or not in PATH${NC}"
    exit 1
else
    echo -e "${GREEN}✓ Terraform found: $(terraform version | head -n1)${NC}"
fi

echo -e "${YELLOW}2. Initializing Terraform...${NC}"
if terraform init -backend=false; then
    echo -e "${GREEN}✓ Terraform initialized successfully${NC}"
else
    echo -e "${RED}✗ Terraform initialization failed${NC}"
    exit 1
fi

echo -e "${YELLOW}3. Validating configuration...${NC}"
if terraform validate; then
    echo -e "${GREEN}✓ Configuration is valid${NC}"
else
    echo -e "${RED}✗ Configuration validation failed${NC}"
    exit 1
fi

echo -e "${YELLOW}4. Checking formatting...${NC}"
if terraform fmt -check=true -recursive; then
    echo -e "${GREEN}✓ Code formatting is correct${NC}"
else
    echo -e "${YELLOW}⚠ Code formatting issues found. Run 'terraform fmt -recursive' to fix${NC}"
fi

echo -e "${YELLOW}5. Planning (dry run)...${NC}"
echo -e "${CYAN}Note: This will show what would be created without actually creating resources${NC}"
if terraform plan -var-file="terraform.tfvars"; then
    echo -e "${GREEN}✓ Plan completed successfully${NC}"
else
    echo -e "${RED}✗ Plan failed${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}===================================${NC}"
echo -e "${GREEN}Validation complete!${NC}"
echo -e "${GREEN}===================================${NC}"
