#!/bin/bash
set -e

# Step 1: Clean and create build directory
rm -rf 1.1_batch_ingestion_lambda_build
mkdir 1.1_batch_ingestion_lambda_build

# Step 2: Install dependencies into build directory
pip install snowflake-connector-python boto3 python-dotenv -t 1.1_batch_ingestion_lambda_build

# Step 3: Copy Lambda function code into build directory
cp 1.1_batch_ingestion_lambda_function.py 1.1_batch_ingestion_lambda_build/lambda_function.py   # Need to rename to lambda_function.py when doing Lambda deployment on AWS

# Step 4: Zip the build directory into deployment package
cd 1.1_batch_ingestion_lambda_build
zip -r ../1.1_batch_ingestion_lambda.zip .
cd ..

echo "✅ Lambda deployment package created: lambda_snowflake_ingestion.zip"
