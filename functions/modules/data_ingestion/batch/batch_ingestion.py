import os
import csv
import io
import boto3
import json
import snowflake.connector
from datetime import datetime, timezone
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

def get_snowflake_credentials(secret_name):
    """Fetch Snowflake credentials from AWS Secrets Manager"""
    client = boto3.client("secretsmanager")
    secret_value = client.get_secret_value(SecretId=secret_name)
    return json.loads(secret_value["SecretString"])

def add_glue_partition(glue_client, database_name, table_name, partition_values, s3_location):
    """Add partition to Glue table with complete StorageDescriptor"""
    try:
        partition_input = {
            "Values": partition_values,
            "StorageDescriptor": {
                "Location": s3_location,
                "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed": False,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                    "Parameters": {
                        "separatorChar": ",",
                        "skip.header.line.count": "1"
                    }
                }
            }
        }
        
        glue_client.create_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionInput=partition_input
        )
        print(f"✅ Added partition {partition_values} to {table_name}")
        return True
    except Exception as e:
        if "Partition already exists" in str(e):
            print(f"ℹ️ Partition {partition_values} already exists in {table_name}")
            return True
        else:
            print(f"❌ Failed to add partition {partition_values} to {table_name}: {e}")
            return False

# load_dotenv()  # Local dev only, keep commented in Lambda

def lambda_handler(event=None, context=None):
    # Config
    default_tables = ["AISLES", "DEPARTMENTS", "PRODUCTS", "ORDERS", "ORDER_PRODUCTS_PRIOR", "ORDER_PRODUCTS_TRAIN"]
    database = "INSIGHTFLOW_IMBA"   # Snowflake database name, adjust as needed
    schema = "PUBLIC"   # Snowflake schema name, adjust as needed
    bucket = os.environ.get("RAW_BUCKET", "insightflow-imba-group-raw") # S3 bucket name
    base_prefix = "data/batch"
    log_prefix = "logs/batch"
    PAGE_SIZE = 1_000_000
    
    # Glue configuration
    glue_database = "insightflow_imba_raw_data_catalog"
    glue_tables = {
        "AISLES": "raw_aisles",
        "DEPARTMENTS": "raw_departments", 
        "PRODUCTS": "raw_products",
        "ORDERS": "raw_orders",
        "ORDER_PRODUCTS_PRIOR": "raw_order_products_prior",
        "ORDER_PRODUCTS_TRAIN": "raw_order_products_train"
    }

    # Read override tables
    tables = event.get("tables") if event and "tables" in event else default_tables

    # Timestamp
    now = datetime.now(ZoneInfo("Australia/Sydney"))    # Use ZoneInfo for timezone handling
    date_path = now.strftime("year=%Y/month=%m/day=%d")
    hhmm_path = now.strftime("hhmm=%H%M")
    log_lines = []

    s3 = boto3.client("s3")
    glue = boto3.client("glue")

    # Get Snowflake credentials from Secrets Manager
    secret_name = os.environ.get("SNOWFLAKE_SECRET_NAME", "snowflake-insightflow")
    snowflake_creds = get_snowflake_credentials(secret_name)

    # Snowflake connect
    conn = snowflake.connector.connect(
        user=snowflake_creds["SNOWFLAKE_USER"],
        password=snowflake_creds["SNOWFLAKE_PASSWORD"],
        account=snowflake_creds["SNOWFLAKE_ACCOUNT"],
        warehouse=snowflake_creds["SNOWFLAKE_WAREHOUSE"],
        role=snowflake_creds["SNOWFLAKE_ROLE"],
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
                data_uploaded = False

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

                    # Update s3_key with new structure
                    s3_key = f"{base_prefix}/{table_name.lower()}/{date_path}/{hhmm_path}/{table_name.lower()}_part{part}.csv"
                    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_data)

                    print(f"✅ Uploaded {table_name} part {part} to s3://{bucket}/{s3_key}")
                    log_lines.append(f"{now.isoformat()} - SUCCESS - {table_name} - part {part} - {len(rows)} rows uploaded to {s3_key}")

                    total_rows += len(rows)
                    offset += PAGE_SIZE
                    part += 1
                    data_uploaded = True

                print(f"✅ {table_name} total: {total_rows} rows, {part} files")

                # Add partition to Glue table if data was uploaded
                if data_uploaded and table_name in glue_tables:
                    glue_table_name = glue_tables[table_name]
                    partition_values = [
                        now.strftime("%Y"),  # year
                        now.strftime("%m"),  # month
                        now.strftime("%d"),  # day
                        now.strftime("%H%M")  # hhmm
                    ]
                    s3_location = f"s3://{bucket}/{base_prefix}/{table_name.lower()}/{date_path}/{hhmm_path}/"
                    
                    add_glue_partition(glue, glue_database, glue_table_name, partition_values, s3_location)

            except Exception as table_error:
                print(f"❌ Failed to process {table_name}: {table_error}")
                log_lines.append(f"{now.isoformat()} - ERROR - {table_name} - {str(table_error)}")

    finally:
        cs.close()
        conn.close()
        print("\n✅ All tables attempted.")

    # Write log to aligned path with date/hour
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
