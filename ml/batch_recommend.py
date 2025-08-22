# functions/modules/ml/batch_recommend.py
"""
批量离线推荐（在 SageMaker PySparkProcessor 里运行）
- 输入1：/opt/ml/processing/model  （ALSModel）
- 输入2：/opt/ml/processing/input  （用于取 user_id 列，或全量交互数据）
- 输出： /opt/ml/processing/output/recommendations.parquet
"""

import argparse
from pathlib import Path
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.recommendation import ALSModel


def build_spark(app_name="recsys-batch-infer") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", default="/opt/ml/processing/input")
    parser.add_argument("--model_dir", default="/opt/ml/processing/model")
    parser.add_argument("--output_dir", default="/opt/ml/processing/output")
    parser.add_argument("--user_col", default="user_id")
    parser.add_argument("--top_k", type=int, default=20)
    args = parser.parse_args()

    spark = build_spark()

    print(f"[PRED] Loading model from {args.model_dir}")
    model = ALSModel.load(args.model_dir)

    # 读取数据以获得用户列表
    df = spark.read.parquet(args.input_dir)
    users = df.select(F.col(args.user_col).cast(T.IntegerType()).alias("userId")).distinct()

    print(f"[PRED] users: {users.count()} unique")

    recs = model.recommendForUserSubset(users, args.top_k)  # DataFrame[userId, recommendations]
    # 展开 recommendations 为 (userId, itemId, rating)
    exploded = recs.select(
        "userId", F.explode("recommendations").alias("rec")
    ).select(
        "userId",
        F.col("rec.itemId").alias("itemId"),
        F.col("rec.rating").alias("score"),
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = str(out_dir / "recommendations.parquet")
    exploded.write.mode("overwrite").parquet(out_path)

    print(f"[PRED] Saved: {out_path}")
    spark.stop()


if __name__ == "__main__":
    main()