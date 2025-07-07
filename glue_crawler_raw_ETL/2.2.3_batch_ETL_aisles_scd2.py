import sys
from datetime import datetime, timedelta
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import lit, col, when, concat_ws, to_date, format_string, row_number
from pyspark.sql.window import Window
import boto3

# 初始化 Glue 环境
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# 获取参数
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_table',
    'historical_path',
    'output_path'
])

input_table = args['input_table']
historical_path = args['historical_path']
output_path = args['output_path']

# 当前运行时间
today = datetime.today().date()
today_str = today.strftime("%Y-%m-%d")
year_str, month_str, day_str = today_str.split("-")

# 读取原始 snapshot
df_snapshot = glueContext.create_dynamic_frame.from_catalog(
    database="imba_landing_db",
    table_name=input_table
).toDF()

# 分区字段格式标准化
df_snapshot = df_snapshot \
    .withColumn("year", format_string("%04d", col("year").cast("int"))) \
    .withColumn("month", format_string("%02d", col("month").cast("int"))) \
    .withColumn("day", format_string("%02d", col("day").cast("int"))) \
    .withColumn("hhmm", format_string("%04d", col("hhmm").cast("int")))

# SCD2：只保留每个 aisle_id 的最新版本
window_spec = Window.partitionBy("aisle_id").orderBy(
    col("year").desc(), col("month").desc(), col("day").desc(), col("hhmm").desc()
)

df_snapshot = df_snapshot.withColumn("rn", row_number().over(window_spec)) \
                         .filter(col("rn") == 1) \
                         .drop("rn")

# 添加 effective_date / expire_date 字段
df_snapshot = df_snapshot \
    .withColumn("effective_date", to_date(concat_ws("-", col("year"), col("month"), col("day")))) \
    .withColumn("expire_date", lit("9999-12-31"))

# 检查历史是否存在
s3 = boto3.client("s3")
bucket = "imba-test-glue-etl-aaron"
prefix = "data-clean/batch/aisles/"
has_existing = "Contents" in s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

if not has_existing:
    print("Initial run: writing snapshot only")
    if df_snapshot.count() == 0:
        print("WARNING: snapshot empty, skipping write")
        sys.exit(0)

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df_snapshot.coalesce(1).write.mode("overwrite") \
        .partitionBy("year", "month", "day", "hhmm") \
        .parquet(output_path)
    sys.exit(0)

# 正常运行：读取历史 clean 数据
df_existing = spark.read.parquet(historical_path).distinct().cache()
df_existing.count()

# 标准化历史分区字段
df_existing = df_existing \
    .withColumn("year", format_string("%04d", col("year").cast("int"))) \
    .withColumn("month", format_string("%02d", col("month").cast("int"))) \
    .withColumn("day", format_string("%02d", col("day").cast("int"))) \
    .withColumn("hhmm", format_string("%04d", col("hhmm").cast("int")))

# 拆分为有效/过期历史记录
df_existing_valid = df_existing.filter(col("expire_date") == "9999-12-31")
df_existing_expired = df_existing.filter(col("expire_date") != "9999-12-31")

# 找出需要 expire 的旧记录
df_to_expire = df_existing_valid.join(
    df_snapshot.select("aisle_id"), on="aisle_id", how="inner"
).withColumn(
    "expire_date",
    when(col("effective_date") == to_date(lit(today_str)),
         lit(today_str)).otherwise(lit((today - timedelta(days=1)).strftime("%Y-%m-%d")))
)

# 找出未变化的记录
df_existing_unchanged = df_existing_valid.join(
    df_snapshot.select("aisle_id"), on="aisle_id", how="left_anti"
)

# 合并最终结果
df_final = df_existing_expired.unionByName(df_to_expire) \
    .unionByName(df_existing_unchanged) \
    .unionByName(df_snapshot)

# 删除历史路径（全覆盖模式）
def delete_s3_prefix(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket_obj = s3.Bucket(bucket)
    objs = [{'Key': obj.key} for obj in bucket_obj.objects.filter(Prefix=prefix)]
    if objs:
        print(f"Deleting {len(objs)} objects in s3://{bucket}/{prefix}")
        bucket_obj.delete_objects(Delete={'Objects': objs})
    else:
        print(f"No objects to delete under s3://{bucket}/{prefix}")

delete_s3_prefix(bucket, prefix)

# 写入 clean 层（全量覆盖）
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df_final.coalesce(1).write.mode("overwrite") \
    .partitionBy("year", "month", "day", "hhmm") \
    .parquet(output_path)
