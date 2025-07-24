### The bash script for streaming ingestion lambda functions deployment

#!/bin/bash
set -e

### publisher.py
# Step 1: Clean and create build directory 
rm -rf streaming_lambda_build
mkdir streaming_lambda_build

# Step 2: Copy Lambda function代码到build目录d
cp streaming_data_publisher.py streaming_lambda_build/streaming_data_publisher.py   # Need to keep the name consistent with the handler in Terraform

# Step 3: 打包为zip，存入terraform/assets/
cd streaming_lambda_build
zip -r ../../../../../terraform/assets/streaming_data_publisher.zip .
cd ..
rm -rf streaming_lambda_build

echo "✅ Lambda deployment package created: terraform/assets/streaming_data_publisher.zip"


### transformer.py
# Step 5: Clean and create build directory
rm -rf streaming_lambda_build
mkdir streaming_lambda_build

# Step 2: Copy Lambda function代码到build目录
cp streaming_data_transformer.py streaming_lambda_build/streaming_data_transformer.py   # Need to keep the name consistent with the handler in Terraform

# Step 3: 打包为zip，存入terraform/assets/
cd streaming_lambda_build
zip -r ../../../../../terraform/assets/streaming_data_transformer.zip .
cd ..
rm -rf streaming_lambda_build

echo "✅ Lambda deployment package created: terraform/assets/streaming_data_transformer.zip"