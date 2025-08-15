# functions/modules/ml/build_ratings.py
# -*- coding: utf-8 -*-
"""
从 after-clean/after-MICE/latest/orders.parquet 与 order_products_prior/
构建 (user_id, product_id, rating) 与 UPI 聚合特征。

注意：在 SageMaker PySpark 容器中，未加协议的本地路径会被解析成 HDFS。
因此这里对所有本地路径显式加 "file://"。
"""

import json
import traceback
from argparse import ArgumentParser
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def _with_file_uri(p: str) -> str:
    """本地路径补上 file:// 协议；S3 或已带协议的保持不变。"""
    if "://" in p:
        return p
    return "file://" + p


def main():
    parser = ArgumentParser()
    parser.add_argument("--orders_dir", required=True)      # /opt/ml/processing/input/orders
    parser.add_argument("--op_dir", required=True)          # /opt/ml/processing/input/op
    parser.add_argument("--output_dir", required=True)      # /opt/ml/processing/output
    parser.add_argument("--shuffle_partitions", type=int, default=200)
    parser.add_argument("--coalesce", type=int, default=1)
    args = parser.parse_args()

    # 本地物理目录（用于写 _debug.json/_error.txt）
    out_local = Path(args.output_dir)
    out_local.mkdir(parents=True, exist_ok=True)

    try:
        spark = (
            SparkSession.builder.appName("build-ratings")
            .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
            .getOrCreate()
        )

        # ============ 读数据（显式 file://） ============
        orders_path = _with_file_uri(args.orders_dir.rstrip("/"))
        op_path = _with_file_uri(args.op_dir.rstrip("/"))
        out_path = _with_file_uri(args.output_dir.rstrip("/"))

        orders = spark.read.parquet(orders_path)
        op = spark.read.parquet(op_path)

        # 只取连接所需列，避免 schema 冲突
        orders_sel = orders.select("order_id", "user_id", "days_since_prior_order")
        op_sel = op.select("order_id", "product_id")

        # 贴回 user_id 与间隔
        up = (
            op_sel.join(orders_sel, on="order_id", how="inner")
                .select("user_id", "product_id", "days_since_prior_order")
        )

        # ============ UPI 聚合 ============
        agg = (
            up.groupBy("user_id", "product_id")
              .agg(
                  F.count(F.lit(1)).alias("upi_count"),
                  F.avg(F.col("days_since_prior_order").cast("double")).alias("upi_avg_interval"),
              )
        )

        # ============ 评分 ============
        ratings = (
            agg.withColumn(
                "rating",
                F.lit(1.0) + F.log1p(F.col("upi_count").cast("double"))
            )
            .select("user_id", "product_id", "rating")
        )

        # 可选降分区，便于下游消费
        if args.coalesce and args.coalesce > 0:
            ratings_out = ratings.coalesce(args.coalesce)
            agg_out = agg.coalesce(args.coalesce)
        else:
            ratings_out = ratings
            agg_out = agg

        # ============ 写结果（显式 file://） ============
        ratings_out.write.mode("overwrite").parquet(out_path + "/ratings")
        agg_out.write.mode("overwrite").parquet(out_path + "/upi_features")

        # ============ 写调试信息到本地目录（会跟随 ProcessingOutput 上传到 S3） ============
        debug = {
            "orders_path": orders_path,
            "op_path": op_path,
            "out_path": out_path,
            "orders_schema": [f"{f.name}: {f.dataType}" for f in orders.schema.fields],
            "op_schema": [f"{f.name}: {f.dataType}" for f in op.schema.fields],
            "orders_count": orders.count(),
            "op_count": op.count(),
            "ratings_count": ratings.count(),
            "upi_features_count": agg.count(),
        }
        (out_local / "_debug.json").write_text(json.dumps(debug, ensure_ascii=False, indent=2))

        spark.stop()

    except Exception as e:
        # 将完整异常写入 _error.txt，便于你直接 aws s3 cp 查看
        (out_local / "_error.txt").write_text(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
