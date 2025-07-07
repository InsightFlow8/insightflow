import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, expr

def load_config():
    return yaml.safe_load(open('config/paths.yaml'))

def main():
    cfg = load_config()
    bucket = cfg['bucket']
    input_prefix = cfg['input_prefix']
    output_prefix = cfg['output_prefix']

    spark = SparkSession.builder.appName("IngestionJob").getOrCreate()
    df = spark.read.csv(f"s3://{bucket}/{input_prefix}/*", header=True)
    df = df.withColumn("ingest_date",
                       expr("regexp_extract(input_file_name(), r'(\d{4}-\d{2}-\d{2})', 1)"))
    df = df.withColumn("processed_ts", current_timestamp())
    df.write.mode("append")         .partitionBy("ingest_date")         .parquet(f"s3://{bucket}/{output_prefix}/")
    spark.stop()

if __name__ == '__main__':
    main()
