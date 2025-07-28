import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import broadcast

# 参数化输入输出路径
args = getResolvedOptions(sys.argv, [
    'aisles_path',
    'departments_path',
    'products_path',
    'orders_path',
    'order_products_prior_path',
    'order_products_train_path',
    'output_path'
])

aisles_path = args['aisles_path']
departments_path = args['departments_path']
products_path = args['products_path']
orders_path = args['orders_path']
order_products_prior_path = args['order_products_prior_path']
order_products_train_path = args['order_products_train_path']
output_path = args['output_path']

# GlueContext/SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

### Load data from S3 parquet
df_aisles_raw = spark.read.parquet(aisles_path)
print("Total rows in Aisles:", df_aisles_raw.count())

df_departments_raw = spark.read.parquet(departments_path)
print("Total rows in Departments:", df_departments_raw.count())

df_products_raw = spark.read.parquet(products_path)
print("Total rows in Products:", df_products_raw.count())

df_orders_raw = spark.read.parquet(orders_path)
print("Total rows in Orders:", df_orders_raw.count())

df_order_products_prior_raw = spark.read.parquet(order_products_prior_path)
print("Total rows in Order_Products_Prior:", df_order_products_prior_raw.count())

df_order_products_train_raw = spark.read.parquet(order_products_train_path)
print("Total rows in Order_Products_Train:", df_order_products_train_raw.count())

# Union order_products_prior and order_products_train
df_order_products = df_order_products_prior_raw.union(df_order_products_train_raw)
print("Total rows in Order_Products:", df_order_products.count())

### Join tables to a large one DF
# 1. orders INNER JOIN order_products on order_id
df_joined = df_orders_raw.join(df_order_products, on="order_id", how="inner")
print("Step 1: orders + order_products join rows:", df_joined.count())

# 2. use broadcast join products
df_joined = df_joined.join(broadcast(df_products_raw), on="product_id", how="inner")
print("Step 2: + products join rows:", df_joined.count())

# 3. use broadcast join aisles
df_joined = df_joined.join(broadcast(df_aisles_raw), on="aisle_id", how="inner")
print("Step 3: + aisles join rows:", df_joined.count())

# 4. use broadcast join departments
df_joined = df_joined.join(broadcast(df_departments_raw), on="department_id", how="inner")
print("Step 4: + departments join rows:", df_joined.count())

print("All tables have been joined successfully.")

# save the large table to the target S3 path.
df_joined.write.mode("overwrite").partitionBy("eval_set").parquet(output_path)  # partition by the appropriate field per need
print(f"Combined table saved to {output_path} with partitioning by eval_set")

print("Table combination completed successfully.")