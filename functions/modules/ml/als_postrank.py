#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
functions/modules/ml/als_postrank.py

独立后处理 + 评估：
- 基于 ALS 模型召回 cand_k
- 与全局热度混排/回退（blend_gamma）
- 可选 MMR 多样性（mmr_lambda > 0 时启用；基于 ALS itemFactors）
- 产出与 als_eval.py 相同的 metrics.json / coverage.json
- 另外落盘 recs.parquet（user_id, rec_items）

依赖：PySpark（与 als_eval 相同环境）
"""

import os, json, math, argparse, traceback
from urllib.parse import urlparse
from datetime import datetime

import boto3
import numpy as np

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F, types as T
from pyspark.ml.recommendation import ALSModel

# ---------- 工具 & I/O ----------

def _norm_path(p: str) -> str:
    """本地路径统一加 file://；S3 路径统一转 s3a://；其他保持原样"""
    if p.startswith("s3://"):
        return "s3a://" + p[len("s3://"):]
    if p.startswith("s3a://") or p.startswith("file://"):
        return p
    if p.startswith("/"):
        return "file://" + p
    return p

def _ensure_dir(local_dir: str):
    os.makedirs(local_dir, exist_ok=True)

def _write_text_local(local_dir: str, name: str, text: str):
    _ensure_dir(local_dir)
    path = os.path.join(local_dir, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    return path

def _write_json_local(local_dir: str, name: str, obj):
    return _write_text_local(local_dir, name, json.dumps(obj, ensure_ascii=False, indent=2))

def _put_text_s3(s3_uri: str, text: str, region=None):
    """兜底：直接 put 到指定 S3 URI，用于异常时写 _error.txt。"""
    try:
        if not s3_uri or not s3_uri.startswith("s3://"):
            return
        u = urlparse(s3_uri)
        bkt, key = u.netloc, u.path.lstrip("/")
        s3 = boto3.client("s3", region_name=region)
        s3.put_object(Bucket=bkt, Key=key, Body=text.encode("utf-8"))
    except Exception:
        pass

# ---------- 评估指标（与 als_eval.py 保持一致口径） ----------

def _idcg_table(k: int):
    vals = [0.0] * (k + 1)
    acc = 0.0
    for i in range(1, k + 1):
        acc += 1.0 / (math.log(i + 1, 2))
        vals[i] = acc
    return {i: vals[i] for i in range(0, k + 1)}

def _calc_strict_metrics(df_rec_gt, top_k, spark):
    """
    入参 df_rec_gt: 包含 user_id, rec_items(array<int>), gt_items(array<int>)
    返回：precision/recall/MAP/NDCG 的全局平均
    """
    base = df_rec_gt.select(
        "user_id",
        F.expr(f"slice(rec_items, 1, {top_k})").alias("rec_items"),
        "gt_items",
    )

    recx = (
        base.select("user_id", "gt_items",
                    F.posexplode("rec_items").alias("pos", "item"))
            .withColumn("rel", F.when(F.array_contains("gt_items", F.col("item")),
                                      F.lit(1.0)).otherwise(F.lit(0.0)))
    )

    w = Window.partitionBy("user_id").orderBy("pos") \
              .rowsBetween(Window.unboundedPreceding, 0)
    recx = recx.withColumn("cum_hits", F.sum("rel").over(w))
    recx = recx.withColumn("prec_at_pos", F.col("cum_hits") / (F.col("pos") + F.lit(1.0)))

    agg_pos = recx.groupBy("user_id").agg(
        F.sum(F.col("prec_at_pos") * F.col("rel")).alias("sum_prec_at_hits"),
        F.sum("rel").alias("hits"),
    )
    base2 = base.withColumn("gt_size", F.size("gt_items"))

    recx = recx.withColumn("dcg_term", F.col("rel") /
                           (F.log(F.col("pos") + F.lit(2.0)) / F.log(F.lit(2.0))))
    dcg = recx.groupBy("user_id").agg(F.sum("dcg_term").alias("dcg"))

    idcg_map = _idcg_table(top_k)
    b_idcg = spark.createDataFrame([(i, idcg_map[i]) for i in range(top_k + 1)],
                                   ["n", "idcg_val"])
    users = base2.select("user_id", "gt_size")
    users = users.withColumn("ideal_n", F.least(F.col("gt_size"), F.lit(top_k)))
    users = users.join(b_idcg, users.ideal_n == b_idcg.n, "left").drop("n")

    per_user = (users.join(agg_pos, on="user_id", how="left")
                     .join(dcg, on="user_id", how="left")
                     .fillna({"sum_prec_at_hits": 0.0, "hits": 0.0,
                              "dcg": 0.0, "idcg_val": 0.0}))
    per_user = per_user.withColumn("precision", F.col("hits") / F.lit(top_k))
    per_user = per_user.withColumn("recall",
                                   F.when(F.col("gt_size") > 0,
                                          F.col("hits") / F.col("gt_size"))
                                    .otherwise(F.lit(0.0)))
    per_user = per_user.withColumn("ap",
                                   F.when(F.col("hits") > 0,
                                          F.col("sum_prec_at_hits") / F.col("hits"))
                                    .otherwise(F.lit(0.0)))
    per_user = per_user.withColumn("ndcg",
                                   F.when(F.col("idcg_val") > 0,
                                          F.col("dcg") / F.col("idcg_val"))
                                    .otherwise(F.lit(0.0)))

    m = per_user.agg(F.avg("precision").alias("precision"),
                     F.avg("recall").alias("recall"),
                     F.avg("ap").alias("map"),
                     F.avg("ndcg").alias("ndcg")).first().asDict()
    return m

# ---------- MMR 相关 ----------

def _cosine(u, v):
    if u is None or v is None:
        return 0.0
    du = math.sqrt(sum(x*x for x in u))
    dv = math.sqrt(sum(x*x for x in v))
    if du == 0.0 or dv == 0.0:
        return 0.0
    return sum(a*b for a, b in zip(u, v)) / (du * dv)

def _mmr_rerank(item_ids, base_scores, top_k, lambda_mmr, feat_map):
    """经典 MMR：argmax λ*score(i) - (1-λ)*max_{j∈S} sim(i,j)"""
    selected = []
    cand = list(zip(item_ids, base_scores))
    while cand and len(selected) < top_k:
        best = None
        best_val = -1e18
        for iid, s in cand:
            if not selected:
                val = s
            else:
                vi = feat_map.get(int(iid))
                if vi is None:
                    max_sim = 0.0
                else:
                    max_sim = 0.0
                    for jid, _ in selected:
                        vj = feat_map.get(int(jid))
                        if vj is not None:
                            max_sim = max(max_sim, _cosine(vi, vj))
                val = lambda_mmr * s - (1.0 - lambda_mmr) * max_sim
            if val > best_val:
                best_val = val
                best = (iid, s)
        selected.append(best)
        cand = [(iid, s) for (iid, s) in cand if iid != best[0]]
    return [int(i) for i, _ in selected]

# ---------- 主逻辑 ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--orders_dir", required=True, help="orders.parquet（含 order_id, user_id）所在目录")
    ap.add_argument("--op_train_dir", required=True, help="order_products_train/ 目录")
    ap.add_argument("--model_dir", required=True, help="已下载好的 ALS 模型目录（包含 als_model/）")
    ap.add_argument("--output_dir", required=True, help="本地输出目录（SageMaker 会同步到 S3 输出）")
    ap.add_argument("--out_s3", default=None, help="（可选）S3 输出目录，用于异常兜底写 _error.txt")
    ap.add_argument("--top_k", type=int, required=True, help="最终展示的 K")
    ap.add_argument("--cand_k", type=int, default=50, help="ALS 候选数（应 >= top_k）")
    ap.add_argument("--blend_gamma", type=float, default=0.3, help="混排权重 γ：score=(1-γ)*z(ALS)+γ*z(POP)")
    ap.add_argument("--mmr_lambda", type=float, default=0.0, help="MMR λ（0 关闭 MMR；建议 0.2~0.5）")
    ap.add_argument("--filter_seen", action="store_true", help="是否剔除训练集中已购（与现有评估口径不一致，默认关闭）")
    ap.add_argument("--shuffle_partitions", type=int, default=200)
    ap.add_argument("--region", default=None)
    args = ap.parse_args()

    # Spark
    spark = (
        SparkSession.builder.appName("als-postrank")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    try:
        orders_path = _norm_path(args.orders_dir)
        op_train_path = _norm_path(args.op_train_dir)
        model_path = _norm_path(os.path.join(args.model_dir, "als_model"))
        out_local = args.output_dir
        _ensure_dir(out_local)

        print(f"[POST] orders_dir : {orders_path}")
        print(f"[POST] op_train   : {op_train_path}")
        print(f"[POST] model_dir  : {model_path}")
        print(f"[POST] out_local  : {out_local}")
        print(f"[POST] top_k={args.top_k}, cand_k={args.cand_k}, gamma={args.blend_gamma}, mmr={args.mmr_lambda}, filter_seen={args.filter_seen}")

        # ===== 读数据 =====
        orders = spark.read.parquet(orders_path).select("order_id", "user_id")
        op_train = spark.read.parquet(op_train_path).select("order_id", "product_id")

        gt = (
            op_train.join(orders, on="order_id", how="inner")
                    .groupBy("user_id")
                    .agg(F.collect_set("product_id").alias("gt_items"))
                    .cache()
        )
        users_with_gt = gt.select("user_id").distinct().count()
        catalog_size = op_train.select("product_id").distinct().count()
        print(f"[POST] users_with_gt={users_with_gt}, catalog_size={catalog_size}")

        # ===== 热度分布 & pop z-score =====
        pop_df = (op_train.groupBy("product_id").count()
                           .withColumn("logc", F.log1p(F.col("count"))))
        mu_sig = pop_df.agg(F.avg("logc").alias("mu"), F.stddev("logc").alias("sig")).first()
        mu = float(mu_sig["mu"] or 0.0)
        sig = float(mu_sig["sig"] or 0.0)
        pop_score_df = pop_df.select(
            F.col("product_id").cast("int").alias("pid"),
            ((F.col("logc") - F.lit(mu)) / F.lit(sig)).alias("pop_z") if sig > 0 else F.lit(0.0).alias("pop_z")
        )

        # 全局热度 Top-K（作为 baseline 也用于 backoff）
        topk_pop = (pop_df.orderBy(F.col("count").desc(), F.col("product_id").asc())
                          .limit(args.top_k)
                          .select("product_id")
                          .rdd.map(lambda r: int(r[0]))
                          .collect())
        if not topk_pop:
            topk_pop = [-1]

        # 广播 pop 分数 & global topK
        pop_map = {int(r["pid"]): float(r["pop_z"]) for r in pop_score_df.collect()}
        bc_pop_map = sc.broadcast(pop_map)
        bc_pop_topk = sc.broadcast([int(x) for x in topk_pop])

        # ===== ALS 候选 =====
        model = ALSModel.load(model_path)
        als_raw = model.recommendForAllUsers(args.cand_k)
        # recommendations: array<struct<product_id:int, rating:float>>
        rec_df = als_raw.select(
            "user_id",
            F.expr("transform(recommendations, x -> int(x.product_id))").alias("items"),
            F.expr("transform(recommendations, x -> double(x.rating))").alias("als_scores")
        )

        # 可选：剔除 seen（注意：这会改变与原评估的口径）
        if args.filter_seen:
            rec_df = (rec_df.join(gt, on="user_id", how="left")
                            .withColumn("items_scores",
                                        F.arrays_zip("items", "als_scores"))
                            .withColumn(
                                "items_scores",
                                F.expr("filter(items_scores, x -> NOT array_contains(gt_items, x.items))")
                            )
                            .withColumn("items", F.expr("transform(items_scores, x -> x.items)"))
                            .withColumn("als_scores", F.expr("transform(items_scores, x -> x.als_scores)"))
                            .drop("items_scores", "gt_items"))

        # ===== UDF：混排 + 回退 + （可选）MMR =====
        gamma = float(args.blend_gamma)
        mmr_lambda = float(args.mmr_lambda)
        top_k = int(args.top_k)

        # 需要 itemFactors 时再广播（MMR）
        feat_map = {}
        if mmr_lambda > 0.0:
            # ALSModel.itemFactors: id(int), features(array<float>)
            feat_map = {int(r["id"]): [float(x) for x in r["features"]] for r in model.itemFactors.collect()}
        bc_feat_map = sc.broadcast(feat_map)

        def _rerank(items, als_scores):
            if items is None or als_scores is None:
                return []
            n = len(items)
            if n == 0:
                return []

            # per-user 标准化 ALS 分数
            arr = np.array(als_scores, dtype=float)
            mu = float(arr.mean()) if n > 0 else 0.0
            sd = float(arr.std()) if n > 0 else 0.0
            z_als = (arr - mu) / sd if sd > 0 else np.zeros_like(arr)

            # 查 pop z-score
            pm = bc_pop_map.value
            z_pop = np.array([pm.get(int(i), 0.0) for i in items], dtype=float)

            base = (1.0 - gamma) * z_als + gamma * z_pop
            order = list(range(n))

            if mmr_lambda > 0.0 and len(order) > 1:
                # MMR 重排
                fm = bc_feat_map.value
                selected = []
                # 候选池（item_id, base_score）
                pool = [(int(items[i]), float(base[i])) for i in order]
                while pool and len(selected) < top_k:
                    best = None
                    best_val = -1e18
                    for iid, s in pool:
                        if not selected:
                            val = s
                        else:
                            vi = fm.get(int(iid))
                            if vi is None:
                                max_sim = 0.0
                            else:
                                max_sim = 0.0
                                for (jid, _) in selected:
                                    vj = fm.get(int(jid))
                                    if vj is not None:
                                        # 余弦
                                        du = math.sqrt(sum(x*x for x in vi))
                                        dv = math.sqrt(sum(x*x for x in vj))
                                        if du > 0.0 and dv > 0.0:
                                            sim = sum(a*b for a, b in zip(vi, vj)) / (du * dv)
                                            if sim > max_sim:
                                                max_sim = sim
                            val = mmr_lambda * s - (1.0 - mmr_lambda) * max_sim
                        if val > best_val:
                            best_val = val
                            best = (iid, s)
                    selected.append(best)
                    pool = [(iid, s) for (iid, s) in pool if iid != best[0]]
                ranked = [iid for iid, _ in selected] + [iid for iid, _ in pool]
            else:
                # 纯混排（按 base 从大到小）
                ranked = [int(items[i]) for i in np.argsort(-base)]

            # 回退：不足 K 用全局热度补齐
            ranked2 = []
            seen = set()
            for i in ranked:
                if i not in seen:
                    ranked2.append(int(i))
                    seen.add(int(i))
                if len(ranked2) >= top_k:
                    break
            if len(ranked2) < top_k:
                for i in bc_pop_topk.value:
                    if i not in seen:
                        ranked2.append(int(i))
                        seen.add(int(i))
                    if len(ranked2) >= top_k:
                        break
            return ranked2

        rerank_udf = F.udf(_rerank, T.ArrayType(T.IntegerType()))

        final_rec = rec_df.select(
            "user_id",
            rerank_udf(F.col("items"), F.col("als_scores")).alias("rec_items")
        )

        # ===== 评估（与 als_eval 相同口径/写法） =====
        als_eval = final_rec.join(gt, on="user_id", how="inner").cache()
        users_evaluated = als_eval.select("user_id").distinct().count()

        items_recommended = (
            final_rec.select(F.explode("rec_items").alias("pid"))
                     .select("pid").distinct().count()
        )

        # Baseline（全局热度 Top-K）
        pop_array = F.array(*[F.lit(int(x)) for x in bc_pop_topk.value]) if bc_pop_topk.value else F.array(F.lit(-1).cast("int"))
        pop_eval = gt.select("user_id", "gt_items").withColumn("rec_items", pop_array)

        m_post = _calc_strict_metrics(als_eval, args.top_k, spark)
        m_pop  = _calc_strict_metrics(pop_eval, args.top_k, spark)

        def _safe_lift(a, b):
            return (a - b) / b if (b is not None and b != 0.0) else None

        metrics = {
            "top_k": args.top_k,
            "finished_at_utc": datetime.utcnow().isoformat(),
            "users_evaluated": users_evaluated,
            # 为了兼容你现有可视化，仍放在 "als" 键下（其实是 ALS+Hybrid/MMR 的结果）
            "als": {
                "precision": m_post.get("precision", 0.0),
                "recall":    m_post.get("recall", 0.0),
                "map":       m_post.get("map", 0.0),
                "ndcg":      m_post.get("ndcg", 0.0),
            },
            "baseline": {
                "precision": m_pop.get("precision", 0.0),
                "recall":    m_pop.get("recall", 0.0),
                "map":       m_pop.get("map", 0.0),
                "ndcg":      m_pop.get("ndcg", 0.0),
            },
            "lift": {
                "precision": _safe_lift(m_post.get("precision", 0.0), m_pop.get("precision", 0.0)),
                "recall":    _safe_lift(m_post.get("recall", 0.0),    m_pop.get("recall", 0.0)),
                "map":       _safe_lift(m_post.get("map", 0.0),       m_pop.get("map", 0.0)),
                "ndcg":      _safe_lift(m_post.get("ndcg", 0.0),      m_pop.get("ndcg", 0.0)),
            },
            "notes": f"ALS cand_k={args.cand_k}; hybrid gamma={args.blend_gamma}; MMR lambda={args.mmr_lambda}; filter_seen={args.filter_seen}",
        }

        coverage = {
            "top_k": args.top_k,
            "users_with_gt": int(users_with_gt),
            "users_with_recs": int(final_rec.select("user_id").distinct().count()),
            "users_evaluated": int(users_evaluated),
            "coverage_user_rec": float((final_rec.select("user_id").distinct().count()) / users_with_gt) if users_with_gt else 0.0,
            "coverage_user_eval": float(users_evaluated / users_with_gt) if users_with_gt else 0.0,
            "catalog_size": int(catalog_size),
            "items_recommended": int(items_recommended),
            "coverage_item": float(items_recommended / catalog_size) if catalog_size else 0.0,
        }

        # ===== 写出 =====
        # 1) 推荐明细（供审计/复现）
        (final_rec.coalesce(1)
                  .write.mode("overwrite")
                  .parquet(os.path.join(out_local, "recs.parquet")))
        # 2) 指标
        _write_json_local(out_local, "metrics.json", metrics)
        _write_json_local(out_local, "coverage.json", coverage)

        print("[POST] DONE.")
    except SystemExit:
        raise
    except Exception as e:
        err = "{name}: {msg}\n\n{stk}".format(
            name=type(e).__name__, msg=str(e), stk=traceback.format_exc()
        )
        try:
            _write_text_local(os.path.join(os.environ.get("SM_OUTPUT_DATA_DIR", "/opt/ml/processing/output")), "_error.txt", err)
        finally:
            if args.out_s3:
                _put_text_s3(args.out_s3.rstrip("/") + "/_error.txt", err, region=args.region)
        raise
    finally:
        try:
            spark.stop()
        except Exception:
            pass

if __name__ == "__main__":
    main()
