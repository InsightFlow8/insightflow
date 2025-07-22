import os
import boto3
import snowflake.connector
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS & Snowflake configuration
aws_config = {
    'aws_access_key_id': os.getenv("AWS_ACCESS_KEY_ID"),    # need to update for the group AWS account
    'aws_secret_access_key': os.getenv("AWS_SECRET_ACCESS_KEY"),    # need to update for the group AWS account
    'bucket': 'imba-test-bucket-aaron', # need to update for the group raw data bucket
    's3_prefix': 'snowflake-data/'  # need to update for the group raw data bucket prefix
}

# need to update for the group Snowflake account
sf_config = {
    'user': os.getenv("SNOWFLAKE_USER"),    
    'password': os.getenv("SNOWFLAKE_PASSWORD"),
    'account': os.getenv("SNOWFLAKE_ACCOUNT"),
    'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
    'role': os.getenv("SNOWFLAKE_ROLE"),
    'database': 'INSIGHTFLOW_IMBA',
    'schema': 'PUBLIC',
    'stage_name': 'INSIGHTFLOW_IMBA_STAGE',
    'storage_integration_name': 'IMBA_INSIGHTFLOW1'
}

# Define multiple tables
tables_config = [
    {
        'local_file': './imba_data/aisles.csv',
        's3_file': 'aisles.csv',
        'table_name': 'AISLES',
        'create_sql': '''
            CREATE OR REPLACE TABLE AISLES (
                AISLE_ID INT,
                AISLE STRING
            )
        '''
    },
    {
        'local_file': './imba_data/departments.csv',
        's3_file': 'departments.csv',
        'table_name': 'DEPARTMENTS',
        'create_sql': '''
            CREATE OR REPLACE TABLE DEPARTMENTS (
                DEPARTMENT_ID INT,
                DEPARTMENT STRING
            )
        '''
    },
       {
        'local_file': './imba_data/products.csv',
        's3_file': 'products.csv',
        'table_name': 'PRODUCTS',
        'create_sql': '''
            CREATE OR REPLACE TABLE PRODUCTS (
                PRODUCT_ID INT,
                PRODUCT_NAME STRING,
                AISLE_ID INT,
                DEPARTMENT_ID INT
            )
        '''
    },
       {
        'local_file': './imba_data/orders.csv',
        's3_file': 'orders.csv',
        'table_name': 'ORDERS',
        'create_sql': '''
            CREATE OR REPLACE TABLE ORDERS (
                ORDER_ID INT,
                USER_ID INT,
                EVAL_SET STRING,
                ORDER_NUMBER INT,
                ORDER_DOW INT,
                ORDER_HOUR_OF_DAY INT,
                DAY_SINCE_PRIOR_ORDER INT
            )
        '''
    },
       {
        'local_file': './imba_data/order_products__prior.csv.gz',
        's3_file': 'order_products__prior.csv.gz',
        'table_name': 'ORDER_PRODUCTS_PRIOR',
        'create_sql': '''
            CREATE OR REPLACE TABLE ORDER_PRODUCTS_PRIOR (
                ORDER_ID INT,
                PRODUCT_ID INT,
                ADD_TO_CART_ORDER INT,
                REORDERED INT
            )
        '''
    },
       {
        'local_file': './imba_data/order_products__train.csv.gz',
        's3_file': 'order_products__train.csv.gz',
        'table_name': 'ORDER_PRODUCTS_TRAIN',
        'create_sql': '''
            CREATE OR REPLACE TABLE ORDER_PRODUCTS_TRAIN (
                ORDER_ID INT,
                PRODUCT_ID INT,
                ADD_TO_CART_ORDER INT,
                REORDERED INT
            )
        '''
    }   
    # Add more tables here as needed...
]

# Step 1: Upload files to S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_config['aws_access_key_id'],
    aws_secret_access_key=aws_config['aws_secret_access_key']
)

# Step 2: Connect to Snowflake
conn = snowflake.connector.connect(
    user=sf_config['user'],
    password=sf_config['password'],
    account=sf_config['account'],
    warehouse=sf_config['warehouse'],
    role=sf_config['role'],
    database=sf_config['database'],
    schema=sf_config['schema']
)
cs = conn.cursor()

# Create database and schema if they don't exist, then set current
print(f"Attempting to create database if not exists: {sf_config['database']}")
cs.execute(f"CREATE DATABASE IF NOT EXISTS {sf_config['database']}")
print(f"Attempting to use database: {sf_config['database']}")
cs.execute(f"USE DATABASE {sf_config['database']}")

print(f"Attempting to create schema if not exists: {sf_config['schema']}")
cs.execute(f"CREATE SCHEMA IF NOT EXISTS {sf_config['schema']}")
print(f"Attempting to use schema: {sf_config['schema']}")
cs.execute(f"USE SCHEMA {sf_config['schema']}")

# Step 2.5: Resume warehouse if needed
resume_sql = f"ALTER WAREHOUSE {sf_config['warehouse']} RESUME"
try:
    cs.execute(resume_sql)
    print(f"Warehouse {sf_config['warehouse']} resumed.")
except Exception as e:
    print(f"Warning: Could not resume warehouse. It may already be running.\n{e}")

# Step 3: Create stage (only once)
create_stage_sql = f'''
CREATE OR REPLACE STAGE {sf_config['database']}.{sf_config['schema']}.{sf_config['stage_name']}
URL='s3://{aws_config['bucket']}/{aws_config['s3_prefix']}'
STORAGE_INTEGRATION = {sf_config['storage_integration_name']}
FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
'''
cs.execute(create_stage_sql)

# Step 4: Loop through all table configs
for table in tables_config:
    # 4.1 Upload to S3
    s3_path = aws_config['s3_prefix'] + table['s3_file']
    s3.upload_file(table['local_file'], aws_config['bucket'], s3_path)
    print(f"Uploaded {table['local_file']} to s3://{aws_config['bucket']}/{s3_path}")

    # 4.2 Create table
    cs.execute(table['create_sql'])
    print(f"Created table {table['table_name']}.")

    # 4.3 COPY INTO
    copy_sql = f'''
    COPY INTO {table['table_name']}
    FROM @{sf_config['stage_name']}/{table['s3_file']}
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    '''
    cs.execute(copy_sql)
    print(f"Data copied into {table['table_name']}.")

# Cleanup
cs.close()
conn.close()
print("✅ All tables processed successfully.")