#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, argparse
import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from pyarrow.fs import S3FileSystem, LocalFileSystem
from urllib.parse import urlparse

def to_fs_and_path(path: str, region: str):
    if path.startswith("s3://"):
        u = urlparse(path)
        return S3FileSystem(region=region), f"{u.netloc}/{u.path.lstrip('/')}"
    if path.startswith("file://"):
        return LocalFileSystem(), path[len("file://"):]
    return LocalFileSystem(), path

def load_df(path: str, region: str, cols):
    fs, p = to_fs_and_path(path, region)
    d = ds.dataset(p, format="parquet", filesystem=fs)
    # 只读需要的列，加快速度
    tbl = d.to_table(columns=cols)
    return tbl.to_pandas()

def compute_last_shares(df: pd.DataFrame, col_days="days_since_prior_order"):
    # 标记每个用户的最后一单
    last_idx = df.groupby("user_id")["order_number"].idxmax()
    is_last = pd.Series(False, index=df.index)
    is_last.loc[last_idx] = True

    total_rows = len(df)
    users = is_last.sum()  # = 用户数
    share_last = users / total_rows if total_rows else 0.0

    # 最后一单里 =30 的占比
    last_30 = (df.loc[is_last, col_days] == 30).sum()
    share_last_eq30 = last_30 / users if users else 0.0

    return {
        "total_rows": int(total_rows),
        "num_users": int(users),
        "share_last_rows": float(share_last),
        "last_eq30_count": int(last_30),
        "share_last_eq30": float(share_last_eq30),
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--before", required=True, help="pre-MICE：orders/ 目录（S3 或本地）")
    ap.add_argument("--after",  required=True, help="post-MICE：orders_imputed.parquet（文件或目录）")
    ap.add_argument("--region", default="ap-southeast-2")
    args = ap.parse_args()

    cols = ["user_id", "order_number", "days_since_prior_order"]

    print("[INFO] Loading BEFORE ...")
    df_b = load_df(args.before, args.region, cols)
    res_b = compute_last_shares(df_b)

    print("[INFO] Loading AFTER ...")
    df_a = load_df(args.after, args.region, cols)
    res_a = compute_last_shares(df_a)

    print("\n=== BEFORE ===")
    for k, v in res_b.items(): print(f"{k}: {v}")

    print("\n=== AFTER ===")
    for k, v in res_a.items(): print(f"{k}: {v}")

    # 小差异提示（插补只影响 '最后一单==30' 的值，但“是不是最后一单”不会变）
    print("\nDiff (after - before):")
    print(f"share_last_rows: {res_a['share_last_rows'] - res_b['share_last_rows']:.6f}")
    print(f"share_last_eq30: {res_a['share_last_eq30'] - res_b['share_last_eq30']:.6f}")

if __name__ == "__main__":
    main()
