#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, shutil, uuid, argparse, json, traceback
from typing import Optional, List
from pyspark.sql import SparkSession, Window, functions as F, types as T
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans

# ---------- helpers ----------
def fspath(p: Optional[str]) -> Optional[str]:
    if not p:
        return p
    if p.startswith("/") and not p.startswith("file://"):
        return "file://" + p
    return p

def write_text(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def copy_tree_contents(src_dir: str, dst_dir: str):
    for root, dirs, files in os.walk(src_dir):
        rel = os.path.relpath(root, src_dir)
        rel = "" if rel == "." else rel
        tgt_root = os.path.join(dst_dir, rel)
        os.makedirs(tgt_root, exist_ok=True)
        for d in dirs:
            os.makedirs(os.path.join(tgt_root, d), exist_ok=True)
        for fn in files:
            s = os.path.join(root, fn)
            t = os.path.join(tgt_root, fn)
            try:
                if os.path.exists(t):
                    os.remove(t)
                os.replace(s, t)
            except Exception:
                shutil.copy2(s, t)

def write_parquet_via_work(df, final_dir: str, coalesce: int = 1):
    # /opt/ml/processing/output/artifacts/<leaf>
    work_root = os.path.join(os.path.dirname(os.path.dirname(final_dir)), "_work")
    os.makedirs(work_root, exist_ok=True)
    work_dir = os.path.join(work_root, os.path.basename(final_dir) + "." + uuid.uuid4().hex)
    df.coalesce(coalesce).write.mode("overwrite").parquet(fspath(work_dir))
    os.makedirs(final_dir, exist_ok=True)
    copy_tree_contents(work_dir, final_dir)
    try:
        shutil.rmtree(work_dir, ignore_errors=True)
    except Exception:
        pass

def build_spark(app="user-seg-rfm-kmeans", shuffle_parts: int = 200):
    return (SparkSession.builder
            .appName(app)
            .config("spark.sql.shuffle.partitions", str(shuffle_parts))
            .getOrCreate())

# ---------- args ----------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--orders_dir", required=True)
    p.add_argument("--op_prior_dir", required=False, default=None)
    p.add_argument("--op_train_dir", required=False, default=None)
    p.add_argument("--output_dir", required=True)
    p.add_argument("--k", type=int, default=10)
    p.add_argument("--r_cap", type=int, default=60)
    p.add_argument("--top_n", type=int, default=30)
    p.add_argument("--beta", type=float, default=500.0)
    p.add_argument("--shuffle_partitions", type=int, default=200)
    return p.parse_args()

# ---------- seg_pop ----------
def compute_segment_popularity(spark, seg_df, orders, op_dirs: List[str], top_n: int, beta: float):
    # 汇总 prior/train 交互
    inter = None
    for d in op_dirs:
        if not d:
            continue
        df = spark.read.parquet(fspath(d)).select("order_id", "product_id")
        inter = df if inter is None else inter.unionByName(df)
    if inter is None:
        schema = T.StructType([
            T.StructField("segment", T.IntegerType()),
            T.StructField("product_id", T.IntegerType()),
            T.StructField("score", T.DoubleType()),
            T.StructField("rank", T.IntegerType()),
        ])
        return spark.createDataFrame([], schema)

    # order_id -> user_id
    o2u = orders.select("order_id","user_id").dropDuplicates(["order_id"])
    inter_u = inter.join(o2u, "order_id", "inner").select("user_id","product_id")
    sp = inter_u.join(seg_df.select("user_id","segment"), "user_id", "inner")

    cnt_sp = sp.groupBy("segment","product_id").agg(F.count("*").alias("cnt"))
    seg_total = sp.groupBy("segment").agg(F.count("*").alias("seg_total"))

    g_cnt = inter.groupBy("product_id").agg(F.count("*").alias("gcnt"))
    g_total = inter.count()
    g_total_lit = F.lit(float(g_total))

    joined = (cnt_sp.join(seg_total, "segment", "inner")
                    .join(g_cnt, "product_id", "left"))
    joined = joined.withColumn("gcnt", F.coalesce(F.col("gcnt"), F.lit(0.0)))
    joined = joined.withColumn("g_rate", F.when(g_total_lit > 0, F.col("gcnt")/g_total_lit).otherwise(F.lit(0.0)))
    joined = joined.withColumn("score", (F.col("cnt") + F.lit(beta)*F.col("g_rate")) / (F.col("seg_total") + F.lit(beta)))

    w = Window.partitionBy("segment").orderBy(F.col("score").desc(), F.col("cnt").desc(), F.col("product_id").asc())
    res = (joined.withColumn("rank", F.row_number().over(w))
                 .filter(F.col("rank") <= int(top_n))
                 .select("segment","product_id","score","rank"))
    return res

# ---------- main ----------
def main():
    args = parse_args()
    spark = build_spark(shuffle_parts=args.shuffle_partitions)

    artifacts_root = os.path.join(args.output_dir, "artifacts")
    os.makedirs(artifacts_root, exist_ok=True)
    user_seg_out = os.path.join(artifacts_root, "user_seg.parquet")
    seg_pop_out  = os.path.join(artifacts_root, "segment_popularity.parquet")
    diag_dir     = os.path.join(args.output_dir, "_diag"); os.makedirs(diag_dir, exist_ok=True)

    try:
        orders = spark.read.parquet(fspath(args.orders_dir))
        # 列类型统一
        keep = [c for c in ["order_id","user_id","order_number","days_since_prior_order"] if c in orders.columns]
        if "days_since_prior" in orders.columns and "days_since_prior_order" not in orders.columns:
            orders = orders.withColumnRenamed("days_since_prior", "days_since_prior_order")
            keep = [c if c!="days_since_prior" else "days_since_prior_order" for c in keep]
        orders = (orders
                  .withColumn("user_id", F.col("user_id").cast("long"))
                  .withColumn("order_id", F.col("order_id").cast("long"))
                  .select(*keep)
                  .cache())
        write_text(os.path.join(diag_dir, "orders_schema.json"), json.dumps(orders.dtypes, ensure_ascii=False, indent=2))

        # ---------- R ----------
        if "order_number" in orders.columns:
            w_last = Window.partitionBy("user_id").orderBy(F.col("order_number").desc())
            last_rows = orders.withColumn("_rn", F.row_number().over(w_last)).filter(F.col("_rn")==1)
        elif "order_id" in orders.columns:
            w_last = Window.partitionBy("user_id").orderBy(F.col("order_id").desc())
            last_rows = orders.withColumn("_rn", F.row_number().over(w_last)).filter(F.col("_rn")==1)
        else:
            raise RuntimeError("orders 缺少 order_number 与 order_id，无法确定最近一次订单。")

        if "days_since_prior_order" in last_rows.columns:
            r_col = F.least(F.coalesce(F.col("days_since_prior_order"), F.lit(0.0)), F.lit(float(args.r_cap)))
        else:
            r_col = F.lit(0.0)
        r_df = last_rows.select("user_id", r_col.alias("R"))

        # ---------- F ----------
        if "order_id" in orders.columns:
            f_df = orders.groupBy("user_id").agg(F.countDistinct("order_id").alias("F"))
        else:
            f_df = orders.groupBy("user_id").agg(F.count("*").alias("F"))

        # ---------- M ----------
        if args.op_train_dir:
            try:
                op_train = spark.read.parquet(fspath(args.op_train_dir)).select("order_id","product_id")
                o2u = orders.select("order_id","user_id").dropDuplicates(["order_id"])
                op_u = op_train.join(o2u, "order_id", "inner")
                items_per_order = op_u.groupBy("order_id","user_id").agg(F.count("*").alias("cnt"))
                m_df = items_per_order.groupBy("user_id").agg(F.avg("cnt").alias("M"))
            except Exception:
                write_text(os.path.join(diag_dir, "op_train_read_error.txt"), traceback.format_exc())
                m_df = r_df.select("user_id").withColumn("M", F.lit(1.0))
        else:
            m_df = r_df.select("user_id").withColumn("M", F.lit(1.0))

        rfm = (r_df.join(f_df, "user_id", "left")
                   .join(m_df, "user_id", "left")
                   .fillna({"F":1, "M":1.0}))

        # ---------- KMeans ----------
        rfm_cnt = rfm.count()
        write_text(os.path.join(diag_dir, "metrics.json"), json.dumps({"rfm_count": rfm_cnt, "k_request": int(args.k)}, ensure_ascii=False, indent=2))

        if rfm_cnt < 2:
            seg_schema = T.StructType([
                T.StructField("user_id", T.LongType()),
                T.StructField("R", T.DoubleType()),
                T.StructField("F", T.DoubleType()),
                T.StructField("M", T.DoubleType()),
                T.StructField("segment", T.IntegerType()),
            ])
            seg_df = spark.createDataFrame([], seg_schema)
        else:
            k_eff = min(int(args.k), max(2, rfm_cnt))
            vec = VectorAssembler(inputCols=["R","F","M"], outputCol="features_raw")
            rfm1 = vec.transform(rfm)
            scaler = StandardScaler(withStd=True, withMean=True, inputCol="features_raw", outputCol="features")
            scaler_model = scaler.fit(rfm1)
            rfm2 = scaler_model.transform(rfm1)
            km = KMeans(k=k_eff, seed=42, featuresCol="features", predictionCol="segment")
            seg_df = km.fit(rfm2).transform(rfm2).select("user_id","R","F","M","segment")

        # 写 user_seg
        write_parquet_via_work(seg_df, os.path.join(artifacts_root, "user_seg.parquet"), coalesce=1)

        # ---------- segment_popularity ----------
        if args.op_train_dir:
            seg_pop = compute_segment_popularity(
                spark, seg_df, orders,
                [args.op_prior_dir, args.op_train_dir],
                top_n=int(args.top_n), beta=float(args.beta)
            )
        else:
            schema = T.StructType([
                T.StructField("segment", T.IntegerType()),
                T.StructField("product_id", T.IntegerType()),
                T.StructField("score", T.DoubleType()),
                T.StructField("rank", T.IntegerType()),
            ])
            seg_pop = spark.createDataFrame([], schema)

        write_parquet_via_work(seg_pop, os.path.join(artifacts_root, "segment_popularity.parquet"), coalesce=1)

        write_text(os.path.join(diag_dir, "driver_log.txt"), "OK")
        spark.stop()
        print("[SEG] DONE")
    except Exception:
        write_text(os.path.join(diag_dir, "driver_log.txt"), traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
