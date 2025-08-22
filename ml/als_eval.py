# functions/modules/ml/als_eval.py
# -*- coding: utf-8 -*-
import os, json, math, traceback, argparse, boto3, sys
from typing import Optional, Dict
from urllib.parse import urlparse
from datetime import datetime

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALSModel


def _norm_path(p):
    """本地路径统一加 file://；S3 路径统一转 s3a://；其他保持原样"""
    if p.startswith("s3://"):
        return "s3a://" + p[len("s3://"):]
    if p.startswith("s3a://") or p.startswith("file://"):
        return p
    if p.startswith("/"):
        return "file://" + p
    return p


def _ensure_dir(local_dir):
    os.makedirs(local_dir, exist_ok=True)


def _write_text_local(local_dir, name, text):
    _ensure_dir(local_dir)
    path = os.path.join(local_dir, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path


def _write_json_local(local_dir, name, obj):
    return _write_text_local(local_dir, name, json.dumps(obj, ensure_ascii=False, indent=2))


def _put_text_s3(s3_uri, text, region=None):
    """兜底：直接 put 到指定 S3 URI，用于异常时写 _error.txt。"""
    try:
        if not s3_uri or not s3_uri.startswith("s3://"):
            return
        u = urlparse(s3_uri)
        bkt, key = u.netloc, u.path.lstrip("/")
        s3 = boto3.client("s3", region_name=region)
        s3.put_object(Bucket=bkt, Key=key, Body=text.encode("utf-8"))
    except Exception:
        # 兜底失败就忽略，避免遮蔽主异常
        pass


def _idcg_table(k):  # type: (int) -> Dict[int, float]
    """预先计算 IDCG@1..k：sum_{i=1..n} 1/log2(i+1)"""
    vals = [0.0] * (k + 1)
    acc = 0.0
    for i in range(1, k + 1):
        acc += 1.0 / (math.log(i + 1, 2))
        vals[i] = acc
    return {i: vals[i] for i in range(0, k + 1)}


def _calc_strict_metrics(df_rec_gt, top_k, spark):
    """
    入参 df_rec_gt: 必须包含：
      - user_id (int)
      - rec_items (array<int>)  长度<=K 的推荐列表，按分数降序
      - gt_items  (array<int>)  该用户的真实购买集合（无序、去重）

    返回：一个 dict，含 precision/recall/MAP/NDCG 的全局平均
    """
    # 先裁剪到 K，避免超长
    base = df_rec_gt.select(
        "user_id",
        F.expr("slice(rec_items, 1, {k})".format(k=top_k)).alias("rec_items"),
        "gt_items",
    )

    # 展开推荐序列，计算相关性 rel、位置 pos（0-based）
    recx = (
        base.select(
            "user_id", "gt_items",
            F.posexplode("rec_items").alias("pos", "item")
        )
        .withColumn("rel", F.when(F.array_contains("gt_items", F.col("item")), F.lit(1.0)).otherwise(F.lit(0.0)))
    )

    # 每个用户内按位置做累积命中，得到精确率曲线
    w = Window.partitionBy("user_id").orderBy("pos").rowsBetween(Window.unboundedPreceding, 0)
    recx = recx.withColumn("cum_hits", F.sum("rel").over(w))
    recx = recx.withColumn("prec_at_pos", F.col("cum_hits") / (F.col("pos") + F.lit(1.0)))

    # 用户级 AP：只在命中位置取平均精确率
    agg_pos = recx.groupBy("user_id").agg(
        F.sum(F.col("prec_at_pos") * F.col("rel")).alias("sum_prec_at_hits"),
        F.sum("rel").alias("hits"),
    )
    # 用户级基数
    base2 = base.withColumn("gt_size", F.size("gt_items"))

    # 计算 DCG
    recx = recx.withColumn("dcg_term", F.col("rel") / (F.log(F.col("pos") + F.lit(2.0)) / F.log(F.lit(2.0))))
    dcg = recx.groupBy("user_id").agg(F.sum("dcg_term").alias("dcg"))

    # IDCG：取 min(gt_size, K)，查表
    idcg_map = _idcg_table(top_k)
    b_idcg = spark.createDataFrame([(i, idcg_map[i]) for i in range(top_k + 1)], ["n", "idcg_val"])
    users = base2.select("user_id", "gt_size")
    users = users.withColumn("ideal_n", F.least(F.col("gt_size"), F.lit(top_k)))
    users = users.join(b_idcg, users.ideal_n == b_idcg.n, "left").drop("n")

    # 合并得到用户级指标
    per_user = (
        users.join(agg_pos, on="user_id", how="left")
             .join(dcg, on="user_id", how="left")
             .fillna({"sum_prec_at_hits": 0.0, "hits": 0.0, "dcg": 0.0, "idcg_val": 0.0})
    )
    per_user = per_user.withColumn("precision", F.col("hits") / F.lit(top_k))
    per_user = per_user.withColumn(
        "recall",
        F.when(F.col("gt_size") > 0, F.col("hits") / F.col("gt_size")).otherwise(F.lit(0.0)),
    )
    per_user = per_user.withColumn(
        "ap",
        F.when(F.col("hits") > 0, F.col("sum_prec_at_hits") / F.col("hits")).otherwise(F.lit(0.0)),
    )
    per_user = per_user.withColumn(
        "ndcg",
        F.when(F.col("idcg_val") > 0, F.col("dcg") / F.col("idcg_val")).otherwise(F.lit(0.0)),
    )

    # 全局平均
    m = per_user.agg(
        F.avg("precision").alias("precision"),
        F.avg("recall").alias("recall"),
        F.avg("ap").alias("map"),
        F.avg("ndcg").alias("ndcg"),
    ).first().asDict()
    return m


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders_dir", required=True, help="orders.parquet（含 order_id, user_id ...）所在目录")
    ap.add_argument("--op_train_dir", required=True, help="order_products_train/ 目录")
    ap.add_argument("--model_dir", required=True, help="已下载好的 ALS 模型目录（包含 als_model/）")
    ap.add_argument("--output_dir", required=True, help="本地输出目录（SageMaker 会同步到 S3 输出）")
    ap.add_argument("--top_k", type=int, required=True)
    ap.add_argument("--shuffle_partitions", type=int, default=200)
    ap.add_argument("--out_s3", required=False, default=None, help="（可选）S3 输出目录，用于异常兜底写 _error.txt")
    ap.add_argument("--region", default=None)
    args = ap.parse_args()

    # Spark 初始化
    spark = (
        SparkSession.builder.appName("als-eval")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 规范化读路径（内部用 s3a:// 与 file://）
    orders_path = _norm_path(args.orders_dir)
    op_train_path = _norm_path(args.op_train_dir)
    model_path = _norm_path(os.path.join(args.model_dir, "als_model"))  # 本地 -> file://...
    out_local = args.output_dir  # 写本地，交给 ProcessingOutput 同步到 S3
    _ensure_dir(out_local)

    # ========== 读数据 ==========
    orders = spark.read.parquet(orders_path).select("order_id", "user_id")
    op_train = spark.read.parquet(op_train_path).select("order_id", "product_id")

    # ground-truth：user -> set(items)
    gt = (
        op_train.join(orders, on="order_id", how="inner")
                .groupBy("user_id")
                .agg(F.collect_set("product_id").alias("gt_items"))
                .cache()
    )
    users_with_gt = gt.select("user_id").distinct().count()

    # 目录规模（商品数）
    catalog_size = op_train.select("product_id").distinct().count()

    # ========== 加载模型 & 推荐 ==========
    model = ALSModel.load(model_path)
    # 注意：ALSModel 的 itemCol 在训练时是 "product_id"
    als_rec_raw = model.recommendForAllUsers(args.top_k)
    als_rec = als_rec_raw.select(
        "user_id",
        F.expr("transform(recommendations, x -> x.product_id)").alias("rec_items"),
    )
    users_with_recs = als_rec.select("user_id").distinct().count()

    # 与评估用户做 inner join（只评估“既有 gt 又有推荐”的用户），并去重统计
    als_eval = als_rec.join(gt, on="user_id", how="inner").cache()
    users_evaluated = als_eval.select("user_id").distinct().count()

    # 推荐到的去重商品数（覆盖率）
    items_recommended = (
        als_rec.select(F.explode("rec_items").alias("pid")).select("pid").distinct().count()
    )

    # ========== Baseline（全局热度 TOP-K） ==========
    topk_pop = (
        op_train.groupBy("product_id").count()
                .orderBy(F.col("count").desc(), F.col("product_id").asc())
                .limit(args.top_k)
                .select("product_id")
                .rdd.map(lambda r: r[0]).collect()
    )
    if topk_pop:
        pop_array = F.array(*[F.lit(int(x)) for x in topk_pop])
    else:
        # 极端兜底：给一个不会出现在 gt 的虚拟 id，避免 F.array() 空参数报错
        pop_array = F.array(F.lit(-1).cast("int"))

    pop_eval = gt.select("user_id", "gt_items").withColumn("rec_items", pop_array)

    # ========== 严格 MAP@K / NDCG@K / P@K / R@K ==========
    m_als = _calc_strict_metrics(als_eval, args.top_k, spark)
    m_pop = _calc_strict_metrics(pop_eval, args.top_k, spark)

    def _safe_lift(a, b):
        return (a - b) / b if (b is not None and b != 0.0) else None

    metrics = {
        "top_k": args.top_k,
        "finished_at_utc": datetime.utcnow().isoformat(),
        "users_evaluated": users_evaluated,
        "als": {
            "precision": m_als.get("precision", 0.0),
            "recall":    m_als.get("recall", 0.0),
            "map":       m_als.get("map", 0.0),
            "ndcg":      m_als.get("ndcg", 0.0),
        },
        "baseline": {
            "precision": m_pop.get("precision", 0.0),
            "recall":    m_pop.get("recall", 0.0),
            "map":       m_pop.get("map", 0.0),
            "ndcg":      m_pop.get("ndcg", 0.0),
        },
        "lift": {
            "precision": _safe_lift(m_als.get("precision", 0.0), m_pop.get("precision", 0.0)),
            "recall":    _safe_lift(m_als.get("recall", 0.0),    m_pop.get("recall", 0.0)),
            "map":       _safe_lift(m_als.get("map", 0.0),       m_pop.get("map", 0.0)),
            "ndcg":      _safe_lift(m_als.get("ndcg", 0.0),      m_pop.get("ndcg", 0.0)),
        },
        "notes": "严格 MAP@K / NDCG@K；Precision/Recall@K 基于集合交集；Baseline=全局热度 Top-K。",
    }

    coverage = {
        "top_k": args.top_k,
        "users_with_gt": int(users_with_gt),
        "users_with_recs": int(users_with_recs),
        "users_evaluated": int(users_evaluated),
        "coverage_user_rec": float(users_with_recs / users_with_gt) if users_with_gt else 0.0,
        "coverage_user_eval": float(users_evaluated / users_with_gt) if users_with_gt else 0.0,
        "catalog_size": int(catalog_size),
        "items_recommended": int(items_recommended),
        "coverage_item": float(items_recommended / catalog_size) if catalog_size else 0.0,
    }

    # ========== 写出 ==========
    _write_json_local(out_local, "metrics.json", metrics)
    _write_json_local(out_local, "coverage.json", coverage)

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        # 最后的兜底：尽量把错误写到 _error.txt（本地 + S3，如果提供了 --out_s3）
        err = "{name}: {msg}\n\n{stk}".format(
            name=type(e).__name__, msg=str(e), stk=traceback.format_exc()
        )
        try:
            out_dir = os.environ.get("SM_OUTPUT_DATA_DIR", "/opt/ml/processing/output")
            _write_text_local(out_dir, "_error.txt", err)
        finally:
            # 从命令行里兜底解析 --out_s3
            out_s3 = None
            try:
                if "--out_s3" in sys.argv:
                    idx = sys.argv.index("--out_s3")
                    if idx + 1 < len(sys.argv):
                        out_s3 = sys.argv[idx + 1].rstrip("/") + "/_error.txt"
            except Exception:
                pass
            if out_s3:
                _put_text_s3(out_s3, err, region=None)
        raise
