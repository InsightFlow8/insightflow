# -*- coding: utf-8 -*-
"""
在 SageMaker PySparkProcessor 中训练 Spark MLlib ALS（隐式反馈）
- 读取 ratings (user_id, product_id, rating)
- 训练 ALSModel 并保存到 /opt/ml/processing/model/als_model
- 打印/落盘 meta.json；异常时落盘 _error.txt
"""

import json
import os
import sys
import traceback
import argparse
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml.recommendation import ALS, ALSModel


def _spark_uri(p: str) -> str:
    """本地路径加 file://，目录默认读 *.parquet。"""
    if p.startswith("/opt/ml/"):
        if not p.endswith(".parquet"):
            p = p.rstrip("/") + "/*.parquet"
        return "file://" + p
    return p

def _as_file_uri(p: str) -> str:
    return p if p.startswith("file://") else "file://" + p


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", required=True)
    parser.add_argument("--model_dir", required=True)
    parser.add_argument("--user_col", default="user_id")
    parser.add_argument("--item_col", default="product_id")
    parser.add_argument("--rating_col", default="rating")
    parser.add_argument("--factors", type=int, default=64)
    parser.add_argument("--reg", type=float, default=1e-2)
    parser.add_argument("--alpha", type=float, default=40.0)
    parser.add_argument("--iters", type=int, default=10)
    parser.add_argument("--shuffle_partitions", type=int, default=200)
    args = parser.parse_args()

    os.makedirs(args.model_dir, exist_ok=True)
    meta_path = os.path.join(args.model_dir, "meta.json")
    err_path = os.path.join(args.model_dir, "_error.txt")

    spark = (SparkSession.builder
             .appName("train-als-implicit")
             .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
             .getOrCreate())

    start_ts = datetime.utcnow().isoformat()

    try:
        read_path = _spark_uri(args.input_dir)
        print(f"[ALS] reading ratings from: {read_path}")

        df = spark.read.parquet(read_path)

        # 字段与类型
        needed = [args.user_col, args.item_col, args.rating_col]
        for c in needed:
            if c not in df.columns:
                raise ValueError(f"column '{c}' not found in input! columns={df.columns}")
        df = (df
              .withColumn(args.user_col, F.col(args.user_col).cast(T.IntegerType()))
              .withColumn(args.item_col, F.col(args.item_col).cast(T.IntegerType()))
              .withColumn(args.rating_col, F.col(args.rating_col).cast(T.DoubleType())))

        before_cnt = df.count()
        df = (df.groupBy(args.user_col, args.item_col)
                .agg(F.max(F.col(args.rating_col)).alias(args.rating_col)))
        after_cnt = df.count()

        stats = df.select(
            F.count("*").alias("cnt"),
            F.min(args.rating_col).alias("min"),
            F.max(args.rating_col).alias("max"),
            F.avg(args.rating_col).alias("avg"),
        ).collect()[0]
        print(f"[ALS] rows(before={before_cnt}, after_dedup={after_cnt}) stats={stats.asDict()}")

        if stats["cnt"] == 0:
            raise ValueError("ratings dataset is empty after dedup.")
        if stats["min"] is None or stats["min"] <= 0:
            raise ValueError(f"rating must be > 0 for implicit ALS; got min={stats['min']}")

        als = ALS(
            userCol=args.user_col,
            itemCol=args.item_col,
            ratingCol=args.rating_col,
            rank=args.factors,
            regParam=args.reg,
            maxIter=args.iters,
            implicitPrefs=True,
            alpha=args.alpha,
            coldStartStrategy="drop",
            nonnegative=True,
        )

        model: ALSModel = als.fit(df)

        # 关键修复：保存到本地文件系统（可被 ProcessingOutput 同步到 S3）
        model_dir_local = os.path.join(args.model_dir, "als_model")
        save_uri = _as_file_uri(model_dir_local)
        print(f"[ALS] saving model -> {save_uri}")
        model.save(save_uri)

        # 写 meta
        meta = {
            "started_at_utc": start_ts,
            "finished_at_utc": datetime.utcnow().isoformat(),
            "input": args.input_dir,
            "rows_before": int(before_cnt),
            "rows_after_dedup": int(after_cnt),
            "rating_stats": {
                "min": float(stats["min"]),
                "max": float(stats["max"]),
                "avg": float(stats["avg"]),
            },
            "params": {
                "rank": args.factors,
                "regParam": args.reg,
                "alpha": args.alpha,
                "maxIter": args.iters,
                "implicitPrefs": True,
                "coldStartStrategy": "drop",
                "nonnegative": True,
                "shuffle_partitions": args.shuffle_partitions,
            },
            "spark": {"version": spark.version},
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        # 打印一下本地输出目录结构，便于排查
        print("[ALS] model_dir tree:")
        for r, ds, fs in os.walk(args.model_dir):
            for name in ds:
                print("  [D]", os.path.join(r, name))
            for name in fs:
                print("  [F]", os.path.join(r, name))

        # 标记成功
        open(os.path.join(args.model_dir, "_SUCCESS"), "w").close()
        print("[ALS] training done.")

    except Exception as e:
        tb = traceback.format_exc()
        print("[ALS][ERROR]", str(e))
        print(tb)
        with open(err_path, "w", encoding="utf-8") as f:
            f.write(tb)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
