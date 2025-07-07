import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import lit, col, format_string

# 初始化 Glue 环境
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 参数获取
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_table',
    'output_path'
])

input_table = args['input_table']
output_path = args['output_path']

# 当前运行时间
today = datetime.today()
year_str, month_str, day_str, hhmm_str = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"), today.strftime("%H%M")

# 读取原始 products 表
df_products = glueContext.create_dynamic_frame.from_catalog(
    database="imba_landing_db",
    table_name=input_table
).toDF()

# 读取 aisles 和 departments clean 表（只保留未过期记录）
aisles_path = "s3://imba-test-glue-etl-aaron/data-clean/batch/aisles/"
departments_path = "s3://imba-test-glue-etl-aaron/data-clean/batch/departments/"

df_aisles = spark.read.parquet(aisles_path).filter(col("expire_date") == "9999-12-31").select("aisle_id", "aisle")
df_departments = spark.read.parquet(departments_path).filter(col("expire_date") == "9999-12-31").select("department_id", "department")

# Join 操作
df_joined = df_products \
    .join(df_aisles, on="aisle_id", how="left") \
    .join(df_departments, on="department_id", how="left")

# 添加分区字段
df_joined = df_joined \
    .withColumn("year", format_string("%04d", col("year").cast("int")).alias("year")).withColumn("year", lit(year_str)) \
    .withColumn("month", lit(month_str)) \
    .withColumn("day", lit(day_str)) \
    .withColumn("hhmm", lit(hhmm_str))

# 写入 clean 层
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df_joined.coalesce(1).write.mode("overwrite") \
    .partitionBy("year", "month", "day", "hhmm") \
    .parquet(output_path)

print(f"Products ETL 成功写入至：{output_path}")
