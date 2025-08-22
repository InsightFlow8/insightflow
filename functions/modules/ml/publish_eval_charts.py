#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, io, re, json, tempfile, csv
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import awswrangler as wr
import pandas as pd
import matplotlib.pyplot as plt
import yaml

# ---------- helpers ----------
def _now_ts():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

def _get(cfg, *keys, default=None):
    d = cfg
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d

def _s3_join(*parts):
    p = "/".join(x.strip("/ ") for x in parts if x is not None and x != "")
    return "s3://" + p if not p.startswith("s3://") else p

def _strip_bucket_key(s3_uri):
    u = urlparse(s3_uri)
    return u.netloc, u.path.lstrip("/")

def _upload_local(local_path, s3_path):
    wr.s3.upload(local_file=local_path, path=s3_path)

def _upload_bytes(data: bytes, s3_path: str):
    bkt, key = _strip_bucket_key(s3_path)
    boto3.client("s3").put_object(Bucket=bkt, Key=key, Body=data)

def _savefig_s3(fig, s3_path, dpi=140):
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
        fig.savefig(tmp.name, bbox_inches="tight", dpi=dpi)
        plt.close(fig)
        _upload_local(tmp.name, s3_path)
    try:
        os.unlink(tmp.name)
    except Exception:
        pass

def _write_json_s3(obj, s3_path):
    _upload_bytes(json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"), s3_path)

def _write_csv_s3(rows, header, s3_path):
    with io.StringIO() as buf:
        w = csv.writer(buf)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
        _upload_bytes(buf.getvalue().encode("utf-8"), s3_path)

# ---------- data loading ----------
def _list_metric_keys(prefix: str):
    bkt, pre = _strip_bucket_key(prefix if prefix.endswith("/") else prefix + "/")
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bkt, Prefix=pre):
        for it in page.get("Contents", []):
            key = it["Key"]
            if key.endswith("/metrics.json"):
                keys.append((f"s3://{bkt}/{key}", it["LastModified"]))  # tz-aware
    return keys

def _read_json_s3(s3_path):
    bkt, key = _strip_bucket_key(s3_path)
    obj = boto3.client("s3").get_object(Bucket=bkt, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))

def _parse_run_ts_from_key(key: str):
    m = re.search(r"(\d{8}-\d{6})", key)
    return m.group(1) if m else None

def _collect_runs(prefix: str) -> pd.DataFrame:
    rows = []
    for s3_path, last_mod in _list_metric_keys(prefix):
        run_prefix = s3_path.rsplit("/", 1)[0]
        run_id = run_prefix.rstrip("/").split("/")[-1]
        variant = "als-postrank" if "als-postrank-" in run_id else ("als" if "als-" in run_id else "unknown")

        m = _read_json_s3(s3_path)
        cov_path = f"{run_prefix}/coverage.json"
        try:
            cov = _read_json_s3(cov_path)
        except Exception:
            cov = {}

        # ---- finished_at 回退逻辑，统一 tz-aware ----
        dt = None
        fa = m.get("finished_at_utc")
        if isinstance(fa, str):
            try:
                # 允许 '...Z' 或无时区字符串
                fa = fa.replace("Z", "+00:00") if "Z" in fa and "+" not in fa else fa
                dt = datetime.fromisoformat(fa)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
            except Exception:
                dt = None
        if dt is None:
            ts = _parse_run_ts_from_key(run_id)
            if ts:
                dt = datetime.strptime(ts, "%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
        if dt is None:
            dt = last_mod  # boto3 LastModified 已带 tzinfo

        def g(path, default=None):
            d = m
            for k in path:
                if not isinstance(d, dict) or k not in d:
                    return default
                d = d[k]
            return d

        rows.append({
            "run_id": run_id,
            "variant": variant,
            "run_prefix": run_prefix,
            "finished_at_dt": dt,  # tz-aware
            "als.precision":   g(["als", "precision"], 0.0),
            "als.recall":      g(["als", "recall"], 0.0),
            "als.map":         g(["als", "map"], 0.0),
            "als.ndcg":        g(["als", "ndcg"], 0.0),
            "baseline.precision": g(["baseline", "precision"], 0.0),
            "baseline.recall":    g(["baseline", "recall"], 0.0),
            "baseline.map":       g(["baseline", "map"], 0.0),
            "baseline.ndcg":      g(["baseline", "ndcg"], 0.0),
            "lift.precision":   g(["lift", "precision"]),
            "lift.recall":      g(["lift", "recall"]),
            "lift.map":         g(["lift", "map"]),
            "lift.ndcg":        g(["lift", "ndcg"]),
            "coverage_item":      cov.get("coverage_item"),
            "coverage_user_eval": cov.get("coverage_user_eval"),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # 关键修复：在这里统一转为 tz-aware UTC，再排序
    df["finished_at_dt"] = pd.to_datetime(df["finished_at_dt"], utc=True)
    df = df.sort_values("finished_at_dt").reset_index(drop=True)
    return df

# ---------- plotting ----------
def _plot_trend(df, ycols, title, ylabel, s3_path):
    fig = plt.figure(figsize=(11, 7))
    x = df["finished_at_dt"].dt.tz_convert("UTC").dt.strftime("%m-%d %H")
    for col in ycols:
        if col in df.columns and df[col].notna().any():
            plt.plot(x, df[col], marker="o", label=col)
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xlabel("run time (UTC)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=35, ha="right")
    _savefig_s3(fig, s3_path)

def _plot_bar_latest(df, s3_path):
    if df.empty:
        return
    last = df.iloc[-1]
    labels = ["Precision@K", "Recall@K", "MAP@K", "NDCG@K"]
    als_vals  = [last["als.precision"], last["als.recall"], last["als.map"], last["als.ndcg"]]
    base_vals = [last["baseline.precision"], last["baseline.recall"], last["baseline.map"], last["baseline.ndcg"]]

    fig = plt.figure(figsize=(6.4, 4.8))
    import numpy as np
    x = np.arange(len(labels)); w = 0.35
    plt.bar(x - w/2, base_vals, width=w, label="Baseline")
    plt.bar(x + w/2, als_vals,   width=w, label="ALS")
    plt.title("ALS family - ALS vs Baseline @K")
    plt.ylabel("score")
    plt.xticks(x, labels)
    plt.legend()
    _savefig_s3(fig, s3_path)

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics_prefix", help="评估结果前缀（包含 metrics.json/coverage.json）")
    ap.add_argument("--out_s3", help="发布前缀（图表和索引）")
    ap.add_argument("--config", help="可选：recsys.yaml，用于获取 curated_bucket / region 等")
    args = ap.parse_args()

    cfg = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

    curated_bucket = _get(cfg, "s3", "curated_bucket", default="insightflow-dev-curated-bucket")
    default_metrics_prefix = _s3_join(curated_bucket, "recsys", "eval")
    default_out_prefix     = _s3_join(curated_bucket, "recsys", "eval_charts")

    metrics_prefix = (args.metrics_prefix or default_metrics_prefix).rstrip("/") + "/"
    out_root = (args.out_s3 or default_out_prefix).rstrip("/") + "/"

    ts = _now_ts()
    out_run = out_root + f"publish-{ts}/"
    out_latest = out_root + "latest/"

    df = _collect_runs(metrics_prefix)
    if df.empty:
        raise SystemExit(f"No metrics.json found under: {metrics_prefix}")

    # 导出 CSV
    csv_cols = [
        "run_id","variant","run_prefix","finished_at_dt",
        "als.precision","als.recall","als.map","als.ndcg",
        "baseline.precision","baseline.recall","baseline.map","baseline.ndcg",
        "lift.precision","lift.recall","lift.map","lift.ndcg",
        "coverage_user_eval","coverage_item"
    ]
    rows = [[df.at[i,c] if c in df.columns else None for c in csv_cols] for i in range(len(df))]
    _write_csv_s3(rows, csv_cols, out_run + "metrics_trend.csv")

    # 图表
    _plot_trend(df, ["als.map","baseline.map","lift.map"], "MAP Trend", "score", out_run + "map_trend.png")
    _plot_trend(df, ["als.ndcg","baseline.ndcg","lift.ndcg"], "NDCG Trend", "score", out_run + "ndcg_trend.png")
    _plot_trend(df, ["als.precision","baseline.precision","lift.precision"], "Precision Trend", "score", out_run + "precision_trend.png")
    _plot_trend(df, ["als.recall","baseline.recall","lift.recall"], "Recall Trend", "score", out_run + "recall_trend.png")
    _plot_trend(df, ["coverage_item"], "Item Coverage (distinct_recommended/catalog_size)", "coverage", out_run + "coverage_item_trend.png")
    _plot_trend(df, ["coverage_user_eval"], "User Coverage (evaluated/users_with_gt)", "coverage", out_run + "coverage_user_trend.png")
    _plot_bar_latest(df, out_run + "metrics_bar.png")

    index = {
        "metrics_prefix": metrics_prefix,
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "runs": len(df),
        "latest_run": df.iloc[-1]["run_id"],
        "artifacts": {
            "metrics_csv": out_run + "metrics_trend.csv",
            "map_trend_png": out_run + "map_trend.png",
            "ndcg_trend_png": out_run + "ndcg_trend.png",
            "precision_trend_png": out_run + "precision_trend.png",
            "recall_trend_png": out_run + "recall_trend.png",
            "coverage_item_trend_png": out_run + "coverage_item_trend.png",
            "coverage_user_trend_png": out_run + "coverage_user_trend.png",
            "metrics_bar_png": out_run + "metrics_bar.png",
        },
        "note": "sorted by finished_at_utc; fallback to run id timestamp; fallback to S3 LastModified (all UTC tz-aware)."
    }
    _write_json_s3(index, out_run + "index.json")
    _write_json_s3(index, out_latest + "index.json")

    print(f"[OK] charts published to: {out_run}")

if __name__ == "__main__":
    main()
