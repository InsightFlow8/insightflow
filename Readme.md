# AWS Glue Ingestion Pipeline

## Components

- job_script.py: Glue Job PySpark script to ingest CSV and write logs
- lambda_upload/upload_csv_batch.py: Lambda function to simulate daily CSV upload
- ingestion_log is registered to Glue Catalog

## Usage

1. Upload job_script.py to AWS Glue
2. Create a Glue Job, set S3 input/output path
3. Create a Lambda function using upload_csv_batch.py
4. Use EventBridge to trigger the Lambda daily

Replace `your-bucket` with your actual bucket name before deployment.
