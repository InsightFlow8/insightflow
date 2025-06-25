import os
import csv
import io
import boto3
import snowflake.connector
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load credentials if running locally (Lambda will use IAM roles)
load_dotenv()

def lambda_handler(event=None, context=None):
    # Configuration
    default_tables = ["AISLES", "DEPARTMENTS", "PRODUCTS"]
    database = "IMBA_AARON_TEST"
    schema = "PUBLIC"
    bucket = "imba-test-aaron-landing"
    prefix = "data"
    log_prefix = "logs"

    # Allow override of tables from event
    tables = event.get("tables") if event and "tables" in event else default_tables

    # Timestamp for current execution (timezone-aware UTC)
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%H%M")

    log_lines = []

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        database=database,
        schema=schema
    )
    cs = conn.cursor()

    try:
        for table_name in tables:
            try:
                print(f"\n--- Processing table: {table_name} ---")
                cs.execute(f"SELECT * FROM {database}.{schema}.{table_name}")
                columns = [col[0] for col in cs.description]
                rows = cs.fetchall()

                # Write CSV to memory - local_file tmp cannot satisfy the project as it has a limit of 512MB file size
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                writer.writerow(columns)
                writer.writerows(rows)
                csv_data = csv_buffer.getvalue()

                # Upload to S3
                s3_key = f"{prefix}/{date_path}/{table_name.lower()}/{table_name.lower()}_{timestamp}.csv"
                s3 = boto3.client('s3')
                s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_data)
                print(f"✅ Uploaded {table_name} to s3://{bucket}/{s3_key}")

                log_lines.append(f"{now.isoformat()} - SUCCESS - {table_name} - {len(rows)} rows uploaded to {s3_key}")

            except Exception as table_error:
                print(f"❌ Failed to process {table_name}: {table_error}")
                log_lines.append(f"{now.isoformat()} - ERROR - {table_name} - {str(table_error)}")

    finally:
        cs.close()
        conn.close()
        print("\n✅ All tables attempted.")

    # Write log to S3
    try:
        log_data = "\n".join(log_lines)
        log_key = f"{log_prefix}/{date_path}/lambda_log_{timestamp}.txt"
        s3.put_object(Bucket=bucket, Key=log_key, Body=log_data)
        print(f"📝 Log written to s3://{bucket}/{log_key}")
    except Exception as log_error:
        print(f"⚠️ Failed to write log: {log_error}")

# For local testing
if __name__ == "__main__":
    lambda_handler(event={"tables": ["AISLES", "DEPARTMENTS"]})
