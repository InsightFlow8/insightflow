import os
import csv
import io
import boto3
import snowflake.connector
from datetime import datetime, timezone
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

# load_dotenv()  # Local dev only, keep commented in Lambda

def lambda_handler(event=None, context=None):
    # Config
    default_tables = ["AISLES", "DEPARTMENTS", "PRODUCTS", "ORDERS", "ORDER_PRODUCTS_PRIOR", "ORDER_PRODUCTS_TRAIN"]
    database = "INSIGHTFLOW_IMBA"   # Snowflake database name, adjust as needed
    schema = "PUBLIC"   # Snowflake schema name, adjust as needed
    bucket = "insightflow-raw-bucket" # S3 bucket name, adjust as needed
    base_prefix = "data/batch"
    log_prefix = "logs/batch"
    PAGE_SIZE = 1_000_000

    # Read override tables
    tables = event.get("tables") if event and "tables" in event else default_tables

    # Timestamp
    now = datetime.now(ZoneInfo("Australia/Sydney"))    # Use ZoneInfo for timezone handling
    date_path = now.strftime("year=%Y/month=%m/day=%d")
    hhmm_path = now.strftime("hhmm=%H%M")
    log_lines = []

    s3 = boto3.client("s3")

    # Snowflake connect
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
                offset = 0
                part = 0
                total_rows = 0

                while True:
                    query = f"SELECT * FROM {database}.{schema}.{table_name} LIMIT {PAGE_SIZE} OFFSET {offset}"
                    cs.execute(query)
                    columns = [col[0] for col in cs.description]
                    rows = cs.fetchall()

                    if not rows:
                        break

                    csv_buffer = io.StringIO()
                    writer = csv.writer(csv_buffer)
                    writer.writerow(columns)
                    writer.writerows(rows)
                    csv_data = csv_buffer.getvalue()

                    # 🆕 Update s3_key with new structure
                    s3_key = f"{base_prefix}/{table_name.lower()}/{date_path}/{hhmm_path}/{table_name.lower()}_part{part}.csv"
                    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_data)

                    print(f"✅ Uploaded {table_name} part {part} to s3://{bucket}/{s3_key}")
                    log_lines.append(f"{now.isoformat()} - SUCCESS - {table_name} - part {part} - {len(rows)} rows uploaded to {s3_key}")

                    total_rows += len(rows)
                    offset += PAGE_SIZE
                    part += 1

                print(f"✅ {table_name} total: {total_rows} rows, {part} files")

            except Exception as table_error:
                print(f"❌ Failed to process {table_name}: {table_error}")
                log_lines.append(f"{now.isoformat()} - ERROR - {table_name} - {str(table_error)}")

    finally:
        cs.close()
        conn.close()
        print("\n✅ All tables attempted.")

    # 🆕 Write log to aligned path with date/hour d
    try:
        log_data = "\n".join(log_lines)
        log_key = f"{log_prefix}/{date_path}/{hhmm_path}/lambda_log.txt"
        s3.put_object(Bucket=bucket, Key=log_key, Body=log_data)
        print(f"📝 Log written to s3://{bucket}/{log_key}")
    except Exception as log_error:
        print(f"⚠️ Failed to write log: {log_error}")

# For local test
if __name__ == "__main__":
    lambda_handler()
