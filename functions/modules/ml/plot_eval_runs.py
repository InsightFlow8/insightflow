#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, os, io, json, re
import boto3, pandas as pd
import matplotlib.pyplot as plt
from urllib.parse import urlparse
from datetime import datetime

def parse_s3(uri):
    u = urlparse(uri)
    if u.scheme != "s3":
        raise ValueError(f"not an s3 uri: {uri}")
    return u.netloc, u.path.lstrip("/")

def list_run_keys(bkt, prefix, region=None, suffix="metrics.json"):
    s3 = boto3.client("s3", region_name=region)
    runs = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bkt, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(suffix):
                runs.append(k)
    return runs

def load_json(s3, bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))

def infer_run_id(prefix, key):
    # 从 s3 key 去掉公共前缀后的第一级目录名作为 run_id
    rel = key[len(prefix):]
    return rel.split("/")[0]

def parse_timestamp(m, run_id):
    # 优先 metrics 里的 finished_at_utc
    ts = m.get("finished_at_utc")
    if ts:
        # 兼容 ISO8601（含小数秒）
        ts_parsed = pd.to_datetime(ts, utc=True, errors="coerce")
        if pd.notnull(ts_parsed):
            return ts_parsed
    # 回退：从 run_id 中抽形如 20250817-234605
    r = re.search(r"(\d{8}-\d{6})", run_id)
    if r:
        return pd.to_datetime(datetime.strptime(r.group(1), "%Y%m%d-%H%M%S"), utc=True)
    return pd.NaT

def list_metrics(eval_root, region=None):
    bkt, prefix = parse_s3(eval_root.rstrip("/") + "/")
    s3 = boto3.client("s3", region_name=region)

    metric_keys = list_run_keys(bkt, prefix, region, "metrics.json")
    cov_keys    = list_run_keys(bkt, prefix, region, "coverage.json")
    cov_index   = {infer_run_id(prefix, k): k for k in cov_keys}

    rows = []
    for mk in metric_keys:
        m = load_json(s3, bkt, mk)
        run_id = infer_run_id(prefix, mk)

        # baseline / popular 兼容
        base_key = "baseline" if "baseline" in m else ("popular" if "popular" in m else None)
        als = m.get("als", {}) or {}
        base = m.get(base_key, {}) if base_key else {}

        # coverage（可选）
        cov = {}
        ck = cov_index.get(run_id)
        if ck:
            cov = load_json(s3, bkt, ck)

        rows.append({
            "run_id": run_id,
            "timestamp": parse_timestamp(m, run_id),
            "top_k": m.get("top_k"),
            "users_evaluated": m.get("users_evaluated") or cov.get("users_evaluated"),
            # ALS
            "precision_als": als.get("precision"),
            "recall_als":    als.get("recall"),
            "map_als":       als.get("map"),
            "ndcg_als":      als.get("ndcg"),
            # Baseline
            "precision_base": base.get("precision"),
            "recall_base":    base.get("recall"),
            "map_base":       base.get("map"),
            "ndcg_base":      base.get("ndcg"),
            # Lift
            "lift_precision": (m.get("lift", {}) or {}).get("precision"),
            "lift_recall":    (m.get("lift", {}) or {}).get("recall"),
            "lift_map":       (m.get("lift", {}) or {}).get("map"),
            "lift_ndcg":      (m.get("lift", {}) or {}).get("ndcg"),
            # Coverage（若有）
            "users_with_gt":    cov.get("users_with_gt"),
            "users_with_recs":  cov.get("users_with_recs"),
            "coverage_user_eval": cov.get("coverage_user_eval"),
            "catalog_size":     cov.get("catalog_size"),
            "items_recommended": cov.get("items_recommended"),
            "coverage_item":    cov.get("coverage_item"),
            "s3_key": f"s3://{bkt}/{mk}",
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        # 排序前把时间全转为 datetime，NaT 放最后
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.sort_values(["timestamp", "run_id"]).reset_index(drop=True)
    return df

def save_plot(df, ycols, title, outfile):
    plt.figure()
    xs = pd.to_datetime(df["timestamp"], utc=True)
    for c in ycols:
        if c in df and df[c].notnull().any():
            plt.plot(xs, df[c], marker="o", label=c)
    plt.title(title)
    plt.xlabel("time")
    plt.ylabel("score")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eval-root", required=True, help="s3://.../recsys/eval/ 根目录")
    ap.add_argument("--out", default="./eval_charts")
    ap.add_argument("--region", default=None)
    ap.add_argument("--upload-s3", default=None, help="可选：把生成的图和CSV再上传回S3")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    df = list_metrics(args.eval_root, region=args.region)
    if df.empty:
        print("No metrics.json found under:", args.eval_root)
        return

    # 导出 CSV
    csv_path = os.path.join(args.out, "eval_summary.csv")
    df.to_csv(csv_path, index=False)
    print("Wrote:", csv_path)

    # 三张效果对比图（ALS vs Baseline）+ 一张 LIFT + 两张覆盖率
    save_plot(df, ["precision_als", "precision_base"],
              "Precision@K (ALS vs Baseline)", os.path.join(args.out, "precision.png"))
    save_plot(df, ["map_als", "map_base"],
              "MAP@K (ALS vs Baseline)", os.path.join(args.out, "map.png"))
    save_plot(df, ["lift_precision", "lift_map"],
              "LIFT (ALS / Baseline)", os.path.join(args.out, "lift.png"))
    save_plot(df, ["coverage_user_eval"],
              "User Coverage (evaluated / users_with_gt)", os.path.join(args.out, "coverage_user.png"))
    save_plot(df, ["coverage_item"],
              "Item Coverage (distinct_recommended / catalog_size)", os.path.join(args.out, "coverage_item.png"))

    # 可选上传
    if args.upload_s3:
        bkt, prefix = parse_s3(args.upload_s3.rstrip("/") + "/")
        s3 = boto3.client("s3", region_name=args.region)
        for fn in os.listdir(args.out):
            p = os.path.join(args.out, fn)
            if os.path.isfile(p):
                s3.upload_file(p, bkt, prefix + fn)
        print("Uploaded charts & CSV to:", args.upload_s3)

if __name__ == "__main__":
    main()
