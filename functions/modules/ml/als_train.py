# functions/modules/ml/als_train.py
"""
ALS 训练脚本（在 SageMaker PySparkProcessor 里运行）
- 输入：/opt/ml/processing/input/*.parquet 需要包含 user_id, product_id, rating(或次数)
- 输出：/opt/ml/processing/model 目录（Spark ALSModel 保存）
"""

import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.recommendation import ALS


def build_spark(app_name="recsys-als-train") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def read_df(spark: SparkSession, data_dir: Path):
    df = spark.read.parquet(str(data_dir))
    return df


def ensure_int(df, col):
    # Spark ALS 需要 int 类型
    return df.withColumn(col, F.col(col).cast(T.IntegerType()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", default="/opt/ml/processing/input")
    parser.add_argument("--model_dir", default="/opt/ml/processing/model")
    parser.add_argument("--user_col", default="user_id")
    parser.add_argument("--item_col", default="product_id")
    parser.add_argument("--rating_col", default="rating")  # 可用 购买次数/是否复购 等
    parser.add_argument("--factors", type=int, default=64)
    parser.add_argument("--reg", type=float, default=0.01)
    parser.add_argument("--alpha", type=float, default=40.0)
    parser.add_argument("--iters", type=int, default=10)
    args = parser.parse_args()

    spark = build_spark()
    data_dir = Path(args.input_dir)

    print(f"[ALS] Reading parquet from {data_dir}")
    df = read_df(spark, data_dir)

    # 列名对齐+类型转换
    df = df.select(
        F.col(args.user_col).alias("userId"),
        F.col(args.item_col).alias("itemId"),
        F.col(args.rating_col).alias("rating"),
    )
    df = ensure_int(df, "userId")
    df = ensure_int(df, "itemId")
    df = df.withColumn("rating", F.col("rating").cast(T.FloatType()))

    print(f"[ALS] sample:")
    df.show(5)

    als = ALS(
        userCol="userId",
        itemCol="itemId",
        ratingCol="rating",
        rank=args.factors,
        regParam=args.reg,
        alpha=args.alpha,
        maxIter=args.iters,
        implicitPrefs=True,
        coldStartStrategy="drop",  # 避免 NaN 预测
    )
    model = als.fit(df)

    model_dir = Path(args.model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    print(f"[ALS] Saving model to {model_dir}")
    model.save(str(model_dir))

    spark.stop()
    print("[ALS] Train done.")


if __name__ == "__main__":
    main()