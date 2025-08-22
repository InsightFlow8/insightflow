#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, argparse
from datetime import datetime
from urllib.parse import urlparse

import numpy as np
import boto3
import pandas as pd
import pyarrow.dataset as ds
from pyarrow.fs import S3FileSystem, LocalFileSystem
import matplotlib.pyplot as plt


# ------------------ IO helpers ------------------

def parse_s3(uri: str):
    u = urlparse(uri)
    if u.scheme != "s3":
        raise ValueError(f"Not an s3 uri: {uri}")
    return u.netloc, u.path.lstrip("/")


def to_pa_source(path: str, region: str):
    """统一把路径转换成 (filesystem, path_for_pa)"""
    if path.startswith("s3://"):
        bkt, key = parse_s3(path)
        return S3FileSystem(region=region), f"{bkt}/{key}"
    if path.startswith("file://"):
        return LocalFileSystem(), path[len("file://"):]
    return LocalFileSystem(), path


def pick_column(dataset: ds.Dataset, candidates, force_col: str | None, side_name: str, path: str):
    cols = [f.name for f in dataset.schema]
    if force_col:
        if force_col not in cols:
            raise KeyError(f"[{side_name}] 指定列 --col-{side_name}='{force_col}' 在 {path} 不存在；实际列：{cols}")
        print(f"[{side_name}] 使用指定列: {force_col}")
        return force_col
    for c in candidates:
        if c in cols:
            print(f"[{side_name}] 自动选到列: {c}")
            return c
    raise KeyError(f"[{side_name}] 在 {path} 未找到可用列；候选：{candidates}；实际列：{cols}")


def load_parquet_to_series(path: str, region: str, col_candidates, side_name: str, force_col: str | None):
    """
    读取 parquet（文件或目录），选列 -> pandas.Series（float 天数），做基本清洗。
    用于直方图/CDF对比；注意：这里会过滤到 (0, 730) 区间。
    """
    fs, pa_path = to_pa_source(path, region)
    dataset = ds.dataset(pa_path, format="parquet", filesystem=fs)
    use_col = pick_column(dataset, col_candidates, force_col, side_name, path)

    tbl = dataset.to_table(columns=[use_col])
    s = pd.to_numeric(tbl.to_pandas()[use_col], errors="coerce").dropna()
    # 基本清洗：去非正数、去极端大值(> 2 年)
    s = s[(s > 0) & (s < 365 * 2)]
    return s.astype(float), use_col


def try_load_json(s3_uri: str, region: str):
    try:
        bkt, key = parse_s3(s3_uri)
        s3 = boto3.client("s3", region_name=region)
        obj = s3.get_object(Bucket=bkt, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None


# ------------------ stats & plots ------------------

def describe_series(s: pd.Series, day30: float = 30.0):
    if s.empty:
        return {"count": 0}
    return {
        "count": int(s.shape[0]),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "p10": float(s.quantile(0.10)),
        "p25": float(s.quantile(0.25)),
        "p75": float(s.quantile(0.75)),
        "p90": float(s.quantile(0.90)),
        "share_le_30d": float((s <= day30).mean()),
    }


def save_hist(s_before, s_after, outfile, bins=60, x_max=60.0):
    plt.figure()
    plt.hist(s_before.clip(upper=x_max), bins=bins, alpha=0.5, label=f"before (≤{int(x_max)}d clipped)", density=True)
    plt.hist(s_after.clip(upper=x_max),  bins=bins, alpha=0.5, label=f"after (≤{int(x_max)}d clipped)",  density=True)
    plt.title(f"Distribution of order intervals (days) — clipped at {int(x_max)} days")
    plt.xlabel("days")
    plt.ylabel("density")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()


def save_cdf(s_before, s_after, outfile, x_max=60.0):
    def ecdf(x):
        x_sorted = np.sort(x)
        y = np.arange(1, x_sorted.size + 1) / x_sorted.size
        return x_sorted, y

    plt.figure()
    b = s_before[s_before <= x_max].values
    a = s_after[s_after <= x_max].values
    if b.size > 0:
        xb, yb = ecdf(b)
        plt.plot(xb, yb, label="before", drawstyle="steps-post")
    if a.size > 0:
        xa, ya = ecdf(a)
        plt.plot(xa, ya, label="after", drawstyle="steps-post")
    plt.title(f"CDF of order intervals (days) — up to {int(x_max)} days")
    plt.xlabel("days")
    plt.ylabel("cumulative share")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()


def save_bar_30d(before_share, after_share, outfile):
    plt.figure()
    xs = ["≤30d before", "≤30d after"]
    ys = [before_share, after_share]
    plt.bar(xs, ys)
    plt.title("Share of intervals within 30 days")
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()


def last_order_stats(path: str, region: str, col_days: str):
    """
    计算“最后一单占比”和“最后一单=30天占比”。
    注意：这里不做 (0,730) 的过滤，基于完整明细来算。
    """
    fs, pa_path = to_pa_source(path, region)
    dataset = ds.dataset(pa_path, format="parquet", filesystem=fs)
    tbl = dataset.to_table(columns=["user_id", "order_number", col_days])
    df = tbl.to_pandas()

    last_idx = df.groupby("user_id")["order_number"].idxmax()
    is_last = pd.Series(False, index=df.index)
    is_last.loc[last_idx] = True

    total_rows = len(df)
    num_users = int(is_last.sum())
    share_last_rows = num_users / total_rows if total_rows else 0.0
    last_eq30 = int((df.loc[is_last, col_days] == 30).sum())
    share_last_eq30 = last_eq30 / num_users if num_users else 0.0
    return {
        "total_rows": int(total_rows),
        "num_users": num_users,
        "share_last_rows": float(share_last_rows),
        "last_eq30_count": last_eq30,
        "share_last_eq30": float(share_last_eq30),
    }


def save_bar_last30(share_b, share_a, outfile):
    plt.figure()
    xs = ["last=30d before", "last=30d after"]
    ys = [share_b, share_a]
    plt.bar(xs, ys)
    plt.title("Share of last orders that equal 30 days")
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()


# ------------------ main ------------------

def main():
    ap = argparse.ArgumentParser(description="Compare pre/post-MICE order-interval distributions (30-day focus).")
    ap.add_argument("--before", required=True, help="S3/本地：pre-MICE orders parquet 目录")
    ap.add_argument("--after",  required=True, help="S3/本地：post-MICE orders_imputed.parquet（文件或目录）")
    ap.add_argument("--metrics", default=None, help="可选：s3://.../mice_metrics.json")
    ap.add_argument("--region",  default="ap-southeast-2")
    ap.add_argument("--outdir",  default="./mice_check")
    ap.add_argument("--day30",   type=float, default=30.0)
    ap.add_argument("--x-max",   type=float, default=60.0, help="截断绘图的最大天数，上限用于直方图与CDF")
    # 可选：手动指定列名
    ap.add_argument("--col-before", default=None, help="可选：pre-MICE 使用的列名")
    ap.add_argument("--col-after",  default=None, help="可选：post-MICE 使用的列名")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # 读取数据（自动挑列, 并允许强制指定）
    pre_col_candidates  = ["days_since_prior_order", "days_imp", "days_imp_imputed"]
    # after 端更宽松：你的数据把 MICE 结果直接写回了 days_since_prior_order
    post_col_candidates = ["days_imp_imputed", "days_imp", "days_since_prior_order_imputed", "days_since_prior_order"]

    s_before, col_before = load_parquet_to_series(
        args.before, args.region, pre_col_candidates, side_name="before", force_col=args.col_before
    )
    s_after,  col_after  = load_parquet_to_series(
        args.after,  args.region, post_col_candidates, side_name="after",  force_col=args.col_after
    )

    # 统计（基于截断后的样本）
    stat_before = describe_series(s_before, args.day30)
    stat_after  = describe_series(s_after,  args.day30)

    # 基于完整明细的“最后一单”指标
    stats_last_before = last_order_stats(args.before, args.region, col_before)
    stats_last_after  = last_order_stats(args.after,  args.region, col_after)

    # 导出抽样（避免超大）
    df_export = pd.DataFrame({
        "before_days": s_before.reset_index(drop=True),
        "after_days":  s_after.reset_index(drop=True)
    })
    df_export.to_csv(os.path.join(args.outdir, "distributions_sample.csv"), index=False)

    # 绘图（标题与裁剪范围随 x_max）
    save_hist(s_before, s_after, os.path.join(args.outdir, "hist_0_60.png"), x_max=args.x_max)
    save_cdf(s_before,  s_after, os.path.join(args.outdir, "cdf_0_60.png"),  x_max=args.x_max)
    save_bar_30d(stat_before.get("share_le_30d", 0.0), stat_after.get("share_le_30d", 0.0),
                 os.path.join(args.outdir, "share_30d_bar.png"))
    save_bar_last30(
        stats_last_before["share_last_eq30"],
        stats_last_after["share_last_eq30"],
        os.path.join(args.outdir, "last_eq30_bar.png")
    )

    # 汇总 + 可选合并 mice_metrics.json
    summary = {
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds"),
        "region": args.region,
        "inputs": {
            "before": {"path": args.before, "used_column": col_before, "count": stat_before.get("count", 0)},
            "after":  {"path": args.after,  "used_column": col_after,  "count": stat_after.get("count", 0)},
            "mice_metrics_json": args.metrics,
        },
        "day30_threshold": args.day30,
        "x_max_for_plots": args.x_max,
        "stats": {
            "before": stat_before,
            "after":  stat_after,
            "delta_share_le_30d": stat_after.get("share_le_30d", 0) - stat_before.get("share_le_30d", 0),
            "delta_mean_days":    stat_after.get("mean", 0) - stat_before.get("mean", 0),
            "delta_median_days":  stat_after.get("median", 0) - stat_before.get("median", 0),
        },
        "last_order": {
            "before": stats_last_before,
            "after":  stats_last_after,
            "delta_share_last_eq30": stats_last_after["share_last_eq30"] - stats_last_before["share_last_eq30"],
        }
    }

    if args.metrics:
        mm = try_load_json(args.metrics, args.region)
        if mm:
            summary["mice_metrics"] = mm

    with open(os.path.join(args.outdir, "summary_day30.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("Done. Outputs in:", os.path.abspath(args.outdir))
    print("  - hist_0_60.png / cdf_0_60.png / share_30d_bar.png / last_eq30_bar.png")
    print("  - distributions_sample.csv")
    print("  - summary_day30.json")


if __name__ == "__main__":
    main()
