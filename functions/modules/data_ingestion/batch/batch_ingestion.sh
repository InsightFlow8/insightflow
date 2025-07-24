#!/bin/bash
set -e

# Step 1: Clean and create build directory
rm -rf batch_ingestion_lambda
mkdir batch_ingestion_lambda

# Step 2: Install dependencies into build directory d
pip install snowflake-connector-python boto3 python-dotenv -t batch_ingestion_lambda

# Step 3: Copy Lambda function code into build directory
cp batch_ingestion.py batch_ingestion_lambda/lambda_function.py   # Need to rename to lambda_function.py when doing Lambda deployment on AWS

# Step 4: Zip the build directory into deployment package
cd batch_ingestion_lambda
zip -r ../../../../../terraform/assets/batch_ingestion_lambda.zip .
cd .. 
cd ..

echo "✅ Lambda deployment package created: terraform/assets/batch_ingestion_lambda.zip"
