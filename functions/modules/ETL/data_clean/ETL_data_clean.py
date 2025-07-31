
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import count, col, countDistinct, isnan
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql import Window
import pandas as pd

# GlueContext/SparkSession
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
    

### Load data



# 通过Glue Job参数获取输入/输出路径
args = getResolvedOptions(sys.argv, [
    'aisles_path',
    'departments_path',
    'products_path',
    'orders_path',
    'order_products_prior_path',
    'order_products_train_path',
    'aisles_out',
    'departments_out',
    'products_out',
    'orders_out',
    'order_products_prior_out',
    'order_products_train_out'
])

aisles_path = args['aisles_path']
departments_path = args['departments_path']
products_path = args['products_path']
orders_path = args['orders_path']
order_products_prior_path = args['order_products_prior_path']
order_products_train_path = args['order_products_train_path']
aisles_out = args['aisles_out']
departments_out = args['departments_out']
products_out = args['products_out']
orders_out = args['orders_out']
order_products_prior_out = args['order_products_prior_out']
order_products_train_out = args['order_products_train_out']

aisles_schema = StructType([
    StructField("aisle_id", IntegerType(), True),
    StructField("aisle", StringType(), True)
])
departments_schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True)
])
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("aisle_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True)
])
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("eval_set", StringType(), True),
    StructField("order_number", IntegerType(), True),
    StructField("order_dow", IntegerType(), True),
    StructField("order_hour_of_day", IntegerType(), True),
    StructField("days_since_prior_order", IntegerType(), True)
])
order_products_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True)
])

df_aisles_raw = spark.read.option("recursiveFileLookup", "true").csv(aisles_path, header=True, schema=aisles_schema)
print("Total rows in Aisles:", df_aisles_raw.count())
df_departments_raw = spark.read.option("recursiveFileLookup", "true").csv(departments_path, header=True, schema=departments_schema)
print("Total rows in Departments:", df_departments_raw.count())
df_products_raw = spark.read.option("recursiveFileLookup", "true").csv(products_path, header=True, schema=products_schema)
print("Total rows in Products:", df_products_raw.count())
df_orders_raw = spark.read.option("recursiveFileLookup", "true").csv(orders_path, header=True, schema=orders_schema)
print("Total rows in Orders (before deduplication):", df_orders_raw.count())
# Deduplicate orders at source level to handle batch ingestion artifacts
df_orders_raw = df_orders_raw.dropDuplicates(["order_id"])
print("Total rows in Orders (after deduplication):", df_orders_raw.count())
df_order_products_prior_raw = spark.read.option("recursiveFileLookup", "true").csv(order_products_prior_path, header=True, schema=order_products_schema)
print("Total rows in Order_Products_Prior:", df_order_products_prior_raw.count())
df_order_products_train_raw = spark.read.option("recursiveFileLookup", "true").csv(order_products_train_path, header=True, schema=order_products_schema)
print("Total rows in Order_Products_Train:", df_order_products_train_raw.count())


### Clean data
# aisles DataFrame


# aisle_id column check
# 1. drop duplicates
df_aisles_clean = df_aisles_raw.dropDuplicates(["aisle_id"])
print("number of unique aisle_id:", df_aisles_clean.select("aisle_id").distinct().count())
# 2. aisle_id check non-integer values
aisle_id_not_int = df_aisles_clean.filter(~col("aisle_id").cast(IntegerType()).isNotNull())
not_int_count = aisle_id_not_int.count()
total_aisle_id = df_aisles_clean.select("aisle_id").distinct().count()
if not_int_count / total_aisle_id < 0.05:
    df_aisles_clean = df_aisles_clean.filter(col("aisle_id").cast(IntegerType()).isNotNull())
    print("delete non-integer aisle_id:", not_int_count)
else:
    print("non-integer aisle_id count:", not_int_count)
print("number of unique aisle_id after removing non-integer:", df_aisles_clean.select("aisle_id").distinct().count())
# 3. check null values
aisle_id_null = df_aisles_clean.filter(col("aisle_id").isNull())
null_count = aisle_id_null.count()
total_aisle_id2 = df_aisles_clean.select("aisle_id").distinct().count()
if null_count / total_aisle_id2 < 0.05:
    df_aisles_clean = df_aisles_clean.filter(~(col("aisle_id").isNull()))
    print("delete null aisle_id:", null_count)
else:
    print("null aisle_id count:", null_count)
print("number of unique aisle_id after removing null:", df_aisles_clean.select("aisle_id").distinct().count())

# aisle column check
# 1. check non-string values
aisle_not_str = df_aisles_clean.filter(~col("aisle").cast(StringType()).isNotNull())
not_str_count = aisle_not_str.count()
total_aisle = df_aisles_clean.count()
if not_str_count / total_aisle < 0.05:
    df_aisles_clean = df_aisles_clean.filter(col("aisle").cast(StringType()).isNotNull())
    print("delete non-string aisle:", not_str_count)
else:
    print("non-string aisle count:", not_str_count)
print("number of unique aisle after removing non-string:", df_aisles_clean.count())

# 2. check null values
aisle_null = df_aisles_clean.filter(col("aisle").isNull() | (col("aisle") == ""))
aisle_null_count = aisle_null.count()
total_aisle2 = df_aisles_clean.count()
if aisle_null_count / total_aisle2 < 0.05:
    df_aisles_clean = df_aisles_clean.filter(~(col("aisle").isNull() | (col("aisle") == "")))
    print("delete null aisle:", aisle_null_count)
else:
    print("null aisle count:", aisle_null_count)
print("number of unique aisle after removing null:", df_aisles_clean.count())

# departments DataFrame
# department_id column check
# 1. drop duplicates
df_departments_clean = df_departments_raw.dropDuplicates(["department_id"])
print("number of unique department_id:", df_departments_clean.select("department_id").distinct().count())
# 2. check non-integer values
department_id_not_int = df_departments_clean.filter(~col("department_id").cast(IntegerType()).isNotNull())
not_int_count = department_id_not_int.count()
total_department_id = df_departments_clean.select("department_id").distinct().count()
if not_int_count / total_department_id < 0.05:
    df_departments_clean = df_departments_clean.filter(col("department_id").cast(IntegerType()).isNotNull())
    print("delete non-integer department_id:", not_int_count)
else:
    print("non-integer department_id count:", not_int_count)
print("number of unique department_id after removing non-integer:", df_departments_clean.select("department_id").distinct().count())
# 3. check null values
department_id_null = df_departments_clean.filter(col("department_id").isNull())
null_count = department_id_null.count()
total_department_id2 = df_departments_clean.select("department_id").distinct().count()
if null_count / total_department_id2 < 0.05:
    df_departments_clean = df_departments_clean.filter(~(col("department_id").isNull()))
    print("delete null department_id:", null_count)
else:
    print("null department_id count:", null_count)
print("number of unique department_id after removing null:", df_departments_clean.select("department_id").distinct().count())

# department column check
# 1. check non-string values
department_id_not_int = df_departments_clean.filter(~col("department_id").cast(StringType()).isNotNull())
not_str_count = department_id_not_int.count()
total_department = df_departments_clean.count()
if not_str_count / total_department < 0.05:
    df_departments_clean = df_departments_clean.filter(col("department_id").cast(StringType()).isNotNull())
    print("delete non-string department_id:", not_str_count)
else:
    print("non-string department_id count:", not_str_count)
print("number of unique department_id after removing non-string:", df_departments_clean.count())

# 2. check null values
department_null = df_departments_clean.filter(col("department").isNull() | (col("department") == ""))
department_null_count = department_null.count()
total_department2 = df_departments_clean.count()
if department_null_count / total_department2 < 0.05:
    df_departments_clean = df_departments_clean.filter(~(col("department").isNull() | (col("department") == "")))
    print("delete null department:", department_null_count)
else:
    print("null department count:", department_null_count)
print("number of unique department after removing null:", df_departments_clean.count())

# products DataFrame
# product_id column check
# 1. product_id duplicates check
df_products_clean = df_products_raw.dropDuplicates(["product_id"])
print("number of unique product_id:", df_products_clean.select("product_id").distinct().count())

# 2. non-integer product_id check
product_id_not_int = df_products_clean.filter(~col("product_id").cast(IntegerType()).isNotNull())
not_int_count = product_id_not_int.count()
total_product_id = df_products_clean.select("product_id").distinct().count()
if not_int_count / total_product_id < 0.05:
    df_products_clean = df_products_clean.filter(col("product_id").cast(IntegerType()).isNotNull())
    print("delete non-integer product_id:", not_int_count)
else:
    print("non-integer product_id count:", not_int_count)
print("number of unique product_id after removing non-integer:", df_products_clean.select("product_id").distinct().count())

# 3. null product_id check
product_id_null = df_products_clean.filter(col("product_id").isNull())
null_count = product_id_null.count()
total_product_id2 = df_products_clean.select("product_id").distinct().count()
if null_count / total_product_id2 < 0.05:
    df_products_clean = df_products_clean.filter(~col("product_id").isNull())
    print("delete null product_id:", null_count)
else:
    print("null product_id count:", null_count)
print("number of unique product_id after removing null:", df_products_clean.select("product_id").distinct().count())

# product_name column check
# 1. non-string product_name check
product_name_not_str = df_products_clean.filter(~col("product_name").cast(StringType()).isNotNull())
not_str_count = product_name_not_str.count()
total_product = df_products_clean.count()
if not_str_count / total_product < 0.05:
    df_products_clean = df_products_clean.filter(col("product_name").cast(StringType()).isNotNull())
    print("delete non-string product_name:", not_str_count)
else:
    print("non-string product_name count:", not_str_count)
print("number of products after removing non-string product_name:", df_products_clean.count())

# 2. null product_name check
product_name_null = df_products_clean.filter(col("product_name").isNull() | (col("product_name") == ""))
product_name_null_count = product_name_null.count()
total_product2 = df_products_clean.count()
if product_name_null_count / total_product2 < 0.05:
    df_products_clean = df_products_clean.filter(~(col("product_name").isNull() | (col("product_name") == "")))
    print("delete null product_name:", product_name_null_count)
else:
    print("null product_name count:", product_name_null_count)
print("number of products after removing null product_name:", df_products_clean.count())

# foreign key checks
# get the foreign key sets
aisle_id_set = set([row['aisle_id'] for row in df_aisles_clean.select("aisle_id").distinct().collect()])
department_id_set = set([row['department_id'] for row in df_departments_clean.select("department_id").distinct().collect()])

# check whether product.aisle_id is in aisles table
products_aisle_not_in_aisles = df_products_clean.filter(~col("aisle_id").cast(IntegerType()).isin(aisle_id_set))
not_in_aisles_count = products_aisle_not_in_aisles.count()
if not_in_aisles_count / df_products_clean.count() < 0.05:
    df_products_clean = df_products_clean.filter(col("aisle_id").cast(IntegerType()).isin(aisle_id_set))
    print("delete products with invalid aisle_id:", not_in_aisles_count)
else:
    print("products with invalid aisle_id count:", not_in_aisles_count)

# check whether product.department_id is in departments table
products_department_not_in_departments = df_products_clean.filter(~col("department_id").cast(IntegerType()).isin(department_id_set))
not_in_departments_count = products_department_not_in_departments.count()
if not_in_departments_count / df_products_clean.count() < 0.05:
    df_products_clean = df_products_clean.filter(col("department_id").cast(IntegerType()).isin(department_id_set))
    print("delete products with invalid department_id:", not_in_departments_count)
else:
    print("products with invalid department_id count:", not_in_departments_count)

print("number of products after foreign key check:", df_products_clean.count())

# orders DataFrame

# column order_id
# 1. non-integer order_id check
order_id_not_int = df_orders_raw.filter(~col("order_id").cast(IntegerType()).isNotNull())
print("non-integer order_id count:", order_id_not_int.count())
df_orders_clean = df_orders_raw.filter(col("order_id").cast(IntegerType()).isNotNull())
print("number of rows after removing non-integer order_id:", df_orders_clean.count())

# 2. duplicate order_id check
dup_order_id = df_orders_clean.groupBy("order_id").count().filter(col("count") > 1)
dup_count = dup_order_id.agg({"count": "sum"}).collect()[0][0] or 0
print("duplicate order_id count:", dup_count)
df_orders_clean = df_orders_clean.dropDuplicates(["order_id"])
print("number of rows after removing duplicate order_id:", df_orders_clean.count())

# 3. null values check
order_id_null = df_orders_clean.filter(col("order_id").isNull())
print("null order_id count:", order_id_null.count())
df_orders_clean = df_orders_clean.filter(col("order_id").isNotNull())
print("number of rows after removing null order_id:", df_orders_clean.count())

# column user_id
# 1. non-integer user_id check
user_id_not_int = df_orders_clean.filter(~col("user_id").cast(IntegerType()).isNotNull())
print("non-integer user_id count:", user_id_not_int.count())
df_orders_clean = df_orders_clean.filter(col("user_id").cast(IntegerType()).isNotNull())
print("number of rows after removing non-integer user_id:", df_orders_clean.count())
# 2. null values check
user_id_null = df_orders_clean.filter(col("user_id").isNull())
print("null user_id count:", user_id_null.count())
df_orders_clean = df_orders_clean.filter(col("user_id").isNotNull())
print("number of rows after removing null user_id:", df_orders_clean.count())

# column eval_set
# 1. valid eval_set values check
valid_eval_set = ['prior', 'train', 'test']
eval_set_invalid = df_orders_clean.filter(~col("eval_set").isin(valid_eval_set))
print("invalid eval_set count:", eval_set_invalid.count())
df_orders_clean = df_orders_clean.filter(col("eval_set").isin(valid_eval_set))
print("number of rows after removing invalid eval_set:", df_orders_clean.count())

# column order_number
# 1. valid order_number values check
order_number_invalid = df_orders_clean.filter(
    (~col("order_number").cast(IntegerType()).isNotNull()) | (col("order_number").cast(IntegerType()) < 1)
)
print("invalid order_number count:", order_number_invalid.count())
df_orders_clean = df_orders_clean.filter(
    col("order_number").cast(IntegerType()).isNotNull() & (col("order_number").cast(IntegerType()) >= 1)
)
print("number of rows after removing invalid order_number:", df_orders_clean.count())

# column order_dow
# 1. valid order_dow values check
order_dow_invalid = df_orders_clean.filter(
    (~col("order_dow").cast(IntegerType()).isNotNull()) | (col("order_dow").cast(IntegerType()) < 0) | (col("order_dow").cast(IntegerType()) > 6)
)
print("invalid order_dow count:", order_dow_invalid.count())
df_orders_clean = df_orders_clean.filter(
    col("order_dow").cast(IntegerType()).isNotNull() & (col("order_dow").cast(IntegerType()) >= 0) & (col("order_dow").cast(IntegerType()) <= 6)
)
print("number of rows after removing invalid order_dow:", df_orders_clean.count())

# column order_hour_of_day
# 1. valid order_hour_of_day values check
order_hour_invalid = df_orders_clean.filter(
    (~col("order_hour_of_day").cast(IntegerType()).isNotNull()) | (col("order_hour_of_day").cast(IntegerType()) < 0) | (col("order_hour_of_day").cast(IntegerType()) > 23)
)
print("invalid order_hour_of_day count:", order_hour_invalid.count())
df_orders_clean = df_orders_clean.filter(
    col("order_hour_of_day").cast(IntegerType()).isNotNull() & (col("order_hour_of_day").cast(IntegerType()) >= 0) & (col("order_hour_of_day").cast(IntegerType()) <= 23)
)
print("number of rows after removing invalid order_hour_of_day:", df_orders_clean.count())

# column days_since_prior_order
# 1. valid days_since_prior_order values check  
days_since_prior_invalid = df_orders_clean.filter(
    (col("days_since_prior_order").isNotNull()) &
    (
        (~col("days_since_prior_order").cast(IntegerType()).isNotNull()) |
        (col("days_since_prior_order").cast(IntegerType()) < 0) |   # there are different orders that are set in the same day, thus we allow 0
        (col("days_since_prior_order").cast(IntegerType()) > 30)
    )
)
print("number of rows after removing invalid days_since_prior_order:", days_since_prior_invalid.count())
df_orders_clean = df_orders_clean.filter(
    col("days_since_prior_order").isNull() |
    (
        col("days_since_prior_order").cast(IntegerType()).isNotNull() &
        (col("days_since_prior_order").cast(IntegerType()) >= 0) &
        (col("days_since_prior_order").cast(IntegerType()) <= 30)
    )
)
print("number of rows after removing invalid days_since_prior_order:", df_orders_clean.count())

# order_products_prior DataFrame
# get the order_id and product_id sets from orders and products DataFrames
order_id_set = set([row['order_id'] for row in df_orders_clean.select("order_id").distinct().collect()])
product_id_set = set([row['product_id'] for row in df_products_clean.select("product_id").distinct().collect()])

print(len(order_id_set), len(product_id_set))

order_id_list = list(order_id_set)
order_id_pd = pd.DataFrame({'order_id': order_id_list})
order_id_spark_df = spark.createDataFrame(order_id_pd)

product_id_list = list(product_id_set)
product_id_pd = pd.DataFrame({'product_id': product_id_list})
product_id_spark_df = spark.createDataFrame(product_id_pd)

# order_id column
# 1. not nil and must be in orders table
invalid_order_id_df = df_order_products_prior_raw.select("order_id").distinct() \
    .join(order_id_spark_df, on='order_id', how='left_anti')
print("Number of invalid order_id:", invalid_order_id_df.count())

df_order_products_clean = df_order_products_prior_raw.join(
    invalid_order_id_df,
    on='order_id',
    how='left_anti'
)
print("number of rows after removing invalid order_id:", df_order_products_clean.count())

# product_id column: 
# 1) not nil and must be in products table
invalid_product_id_df = df_order_products_prior_raw.select("product_id").distinct() \
    .join(product_id_spark_df, on='product_id', how='left_anti')
print("number of invalid product_id:", invalid_product_id_df.count())

df_order_products_clean = df_order_products_clean.join(
    invalid_product_id_df,
    on='product_id',
    how='left_anti'
)
print("number of rows after removing invalid product_id:", df_order_products_clean.count())

# add_to_cart_order column: must be >=1 integer
add_to_cart_order_invalid = df_order_products_clean.filter(
    (~col("add_to_cart_order").cast(IntegerType()).isNotNull()) | (col("add_to_cart_order").cast(IntegerType()) < 1)
)
print("number of invalid add_to_cart_order:", add_to_cart_order_invalid.count())
df_order_products_clean = df_order_products_clean.filter(
    col("add_to_cart_order").cast(IntegerType()).isNotNull() & (col("add_to_cart_order").cast(IntegerType()) >= 1)
)
print("number of rows after removing invalid add_to_cart_order:", df_order_products_clean.count())

# reordered column: must be 0 or 1
reordered_invalid = df_order_products_clean.filter(
    col("reordered").isNull() | (~col("reordered").cast(IntegerType()).isNotNull()) | (~col("reordered").cast(IntegerType()).isin([0, 1]))
)
print("number of invalid reordered:", reordered_invalid.count())
df_order_products_clean = df_order_products_clean.filter(
    col("reordered").cast(IntegerType()).isNotNull() & col("reordered").cast(IntegerType()).isin([0, 1])
)
print("number of rows after removing invalid reordered:", df_order_products_clean.count())

# order_products_train DataFrame

# order_id column
# 1. not nil and must be in orders table
invalid_order_id_df = df_order_products_train_raw.select("order_id").distinct() \
    .join(order_id_spark_df, on='order_id', how='left_anti')
print("Number of invalid order_id:", invalid_order_id_df.count())

df_order_products_train_clean = df_order_products_train_raw.join(
    invalid_order_id_df,
    on='order_id',
    how='left_anti'
)
print("number of rows after removing invalid order_id:", df_order_products_train_clean.count())

# product_id column: 
# 1) not nil and must be in products table
invalid_product_id_df = df_order_products_train_raw.select("product_id").distinct() \
    .join(product_id_spark_df, on='product_id', how='left_anti')
print("number of invalid product_id:", invalid_product_id_df.count())

df_order_products_train_clean = df_order_products_train_clean.join(
    invalid_product_id_df,
    on='product_id',
    how='left_anti'
)
print("number of rows after removing invalid product_id:", df_order_products_train_clean.count())

# add_to_cart_order column: must be >=1 integer
add_to_cart_order_invalid = df_order_products_train_clean.filter(
    (~col("add_to_cart_order").cast(IntegerType()).isNotNull()) | (col("add_to_cart_order").cast(IntegerType()) < 1)
)
print("number of invalid add_to_cart_order:", add_to_cart_order_invalid.count())
df_order_products_train_clean = df_order_products_train_clean.filter(
    col("add_to_cart_order").cast(IntegerType()).isNotNull() & (col("add_to_cart_order").cast(IntegerType()) >= 1)
)
print("number of rows after removing invalid add_to_cart_order:", df_order_products_train_clean.count())

# reordered column: must be 0 or 1
reordered_invalid = df_order_products_train_clean.filter(
    col("reordered").isNull() | (~col("reordered").cast(IntegerType()).isNotNull()) | (~col("reordered").cast(IntegerType()).isin([0, 1]))
)
print("number of invalid reordered:", reordered_invalid.count())
df_order_products_train_clean = df_order_products_train_clean.filter(
    col("reordered").cast(IntegerType()).isNotNull() & col("reordered").cast(IntegerType()).isin([0, 1])
)
print("number of rows after removing invalid reordered:", df_order_products_train_clean.count())



# save the data after cleaning to the temp path, small tables coalesce to 1 partition, big ones keep default
df_aisles_clean.coalesce(1).write.mode("overwrite").parquet(aisles_out)
df_departments_clean.coalesce(1).write.mode("overwrite").parquet(departments_out)
df_products_clean.coalesce(1).write.mode("overwrite").parquet(products_out)
df_orders_clean.write.mode("overwrite").parquet(orders_out)
df_order_products_clean.write.mode("overwrite").parquet(order_products_prior_out)
df_order_products_train_clean.coalesce(1).write.mode("overwrite").parquet(order_products_train_out)

print("Data cleaning completed and saved to the specified output paths.")