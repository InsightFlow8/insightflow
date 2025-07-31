from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import IntegerType, StringType, FloatType
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import col, expr, max, mean, count as f_count, countDistinct, sum as spark_sum, isnan
import sys

# GlueContext/SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 参数化输入输出路径
args = getResolvedOptions(sys.argv, [
    'aisles_path',
    'departments_path',
    'products_path',
    'orders_path',
    'order_products_prior_path',
    'order_products_train_path',
    'user_features_output',
    'product_features_output',
    'upi_features_output',
    'product_features_union_output',
    'upi_features_union_output'
])

aisles_path = args['aisles_path']
departments_path = args['departments_path']
products_path = args['products_path']
orders_path = args['orders_path']
order_products_prior_path = args['order_products_prior_path']
order_products_train_path = args['order_products_train_path']
user_features_output = args['user_features_output']
product_features_output = args['product_features_output']
upi_features_output = args['upi_features_output']
product_features_union_output = args['product_features_union_output']
upi_features_union_output = args['upi_features_union_output']

### Load data
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
df_order_products_union = df_order_products_prior_raw.union(df_order_products_train_raw)

count_order_products_union = df_order_products_union.count()
print("Total rows in Order_Products:", count_order_products_union)

### Join tables to a large one DF - prior set only
# 1. orders INNER JOIN order_products on order_id，drop order_products.order_id
df_joined = df_orders_raw.join(df_order_products_prior_raw, on="order_id", how="inner") \
    .drop(df_order_products_prior_raw.order_id)
print("Step 1: orders + order_products_prior rows:", df_joined.count())

# 2. use broadcast join products
df_joined = df_joined.join(broadcast(df_products_raw), on="product_id", how="inner") \
    .drop(df_products_raw.product_id)
print("Step 2: + products join rows:", df_joined.count())

# 3. use broadcast join aisles
df_joined = df_joined.join(broadcast(df_aisles_raw), on="aisle_id", how="inner") \
    .drop(df_aisles_raw.aisle_id)
print("Step 3: + aisles join rows:", df_joined.count())

# 4. use broadcast join departments
df_joined = df_joined.join(broadcast(df_departments_raw), on="department_id", how="inner") \
    .drop(df_departments_raw.department_id)
print("Step 4: + departments join rows:", df_joined.count())



### Join tables to a large one DF - prior + train set
# 1. orders INNER JOIN order_products_union on order_id，drop order_products.order_id
df_joined_union = df_orders_raw.join(df_order_products_union, on="order_id", how="inner") \
    .drop(df_order_products_union.order_id)
print("Step 1: orders + order_products_union join rows:", df_joined_union.count())

# 2. use broadcast join products
df_joined_union = df_joined_union.join(broadcast(df_products_raw), on="product_id", how="inner") \
    .drop(df_products_raw.product_id)
print("Step 2: + products join rows:", df_joined_union.count())

# 3. use broadcast join aisles
df_joined_union = df_joined_union.join(broadcast(df_aisles_raw), on="aisle_id", how="inner") \
    .drop(df_aisles_raw.aisle_id)
print("Step 3: + aisles join rows:", df_joined_union.count())

# 4. use broadcast join departments
df_joined_union = df_joined_union.join(broadcast(df_departments_raw), on="department_id", how="inner") \
    .drop(df_departments_raw.department_id)
print("Step 4: + departments join rows:", df_joined_union.count())

# 对 df_orders_raw 的 days_since_prior_order 空值替换为 0
df_orders_null_fill = df_orders_raw.na.fill({"days_since_prior_order": 0})

# 对 df_joined & df_joined_union 的 days_since_prior_order 空值替换为 0
df_joined_null_fill = df_joined.na.fill({"days_since_prior_order": 0})
df_joined_union_null_fill = df_joined_union.na.fill({"days_since_prior_order": 0})

### Data Transformation
### Only prior set
### user_features
# after checking the raw data, there are some order_id in the orders table that are not in the order_products table (around 75000 order_id are not included in order_products(after union prior and train)). Thus, we use orders table to calculate user features so that there are more information collected.
user_features = df_orders_null_fill.groupBy("user_id") \
    .agg(
        max("order_number").alias("total_order_number"),
        mean("days_since_prior_order").alias("avg_days_since_prior_order"),
    ).orderBy("user_id")

print("number of user_features:", user_features.count())


### product_features
product_features = df_joined_null_fill.groupBy("product_id", "product_name") \
    .agg(
        (spark_sum("reordered") / countDistinct("order_id")).alias("reorder_rate_global"),
        mean("add_to_cart_order").alias("avg_add_to_cart_order")
    ).orderBy("product_id")
print("number of product_features:", product_features.count())



### User-Product interaction Features
upi_features = df_joined_null_fill.groupBy("user_id", "product_id") \
    .agg(
        count("order_id").alias("total_orders"),
        mean("add_to_cart_order").alias("avg_add_to_cart_order"),
    ).orderBy("user_id", "product_id")
print("number of user-product interaction features:", upi_features.count())


### prior + train set
### product_features
product_features_union = df_joined_union_null_fill.groupBy("product_id", "product_name") \
    .agg(
        (spark_sum("reordered") / countDistinct("order_id")).alias("reorder_rate_global"),
        mean("add_to_cart_order").alias("avg_add_to_cart_order")
    ).orderBy("product_id")
print("number of product_features:", product_features_union.count())


### User-Product interaction Features
upi_features_union = df_joined_union_null_fill.groupBy("user_id", "product_id") \
    .agg(
        count("order_id").alias("total_orders"),
        mean("add_to_cart_order").alias("avg_add_to_cart_order"),
    ).orderBy("user_id", "product_id")
print("number of user-product interaction features:", upi_features_union.count())

### Save output files to S3
print("Saving user_features to:", user_features_output)
user_features.coalesce(1).write.mode("overwrite").parquet(user_features_output)

print("Saving product_features to:", product_features_output)
product_features.coalesce(1).write.mode("overwrite").parquet(product_features_output)

print("Saving upi_features to:", upi_features_output)
upi_features.coalesce(1).write.mode("overwrite").parquet(upi_features_output)

print("Saving product_features_union to:", product_features_union_output)
product_features_union.coalesce(1).write.mode("overwrite").parquet(product_features_union_output)

print("Saving upi_features_union to:", upi_features_union_output)
upi_features_union.coalesce(1).write.mode("overwrite").parquet(upi_features_union_output)

print("✅ All feature files saved successfully!")
