import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lit

# 初始化上下文
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()

# 获取 Job 参数
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'input_table',
    'output_path',
    'eval_set_filter',
    # 'job_date',  # 用于后续精细化批次处理
    # 'hhmm'
])

input_table = args['input_table']
output_path = args['output_path']
eval_set_filter = args['eval_set_filter']

# 构造带 eval_set_filter 的输出路径（避免冲突）
final_output_path = f"{output_path.rstrip('/')}/eval_set={eval_set_filter}/"

logger.info(f"读取数据源表：{input_table}")
logger.info(f"写入目标路径：{final_output_path}")

# 读取 Glue Catalog 表
orders_raw = glueContext.create_dynamic_frame.from_catalog(
    database="imba_landing_db",
    table_name=input_table
)

# 转换为 Spark DataFrame
df_raw = orders_raw.toDF()

# 过滤清洗逻辑
df_filtered = df_raw.filter(
    (df_raw.order_id.isNotNull()) &
    (df_raw.user_id.isNotNull()) &
    (df_raw.eval_set == eval_set_filter) &
    (df_raw.order_number.isNotNull()) & (df_raw.order_number > 0) &
    (df_raw.order_dow.between(0, 6)) &
    (df_raw.order_hour_of_day.between(0, 23))
)

# 删除重复的eval_set列，避免Query时报错
df_cleaned = df_filtered.drop("eval_set")

# 写出为 Parquet，按 Glue 推荐格式输出
df_cleaned.coalesce(2).write \
    .mode("overwrite") \
    .parquet(final_output_path)

logger.info("ETL 处理完成 ✅")
