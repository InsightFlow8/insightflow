#!/bin/bash
set -e

# Step 1: Clean and create build directory
rm -rf lambda_s3_to_rds_raw_build
mkdir lambda_s3_to_rds_raw_build

# Step 2: Install dependencies into build directory
pip install boto3 psycopg2-binary python-dotenv -t lambda_s3_to_rds_raw_build

# Step 3: Copy Lambda function code into build directory
cp lambda_s3_to_rds_raw.py lambda_s3_to_rds_raw_build/lambda_s3_to_rds_raw.py

# Step 4: Zip the build directory into deployment packages
cd lambda_s3_to_rds_raw_build
zip -r ../../../../../terraform/assets/lambda_s3_to_rds_raw.zip .
cd ..
cd ..

echo "✅ Lambda deployment package created: terraform/assets/lambda_s3_to_rds_raw.zip"
