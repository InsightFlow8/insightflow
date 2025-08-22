#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
rfm_analysis_and_export.py (final)
- 读取 latest 的 user_seg + segment_popularity
- 生成图表（簇画像、PCA、分群热度），默认发布到 s3://<curated_bucket>/recsys/eval_charts/rfm_analysis-<TS>/
- 小样本导出（高价值+随机，拼接簇TopN）=> CSV/Parquet/JSONL 至 --export_base/run-<TS>/
- 新增：
  * 严格只读 .parquet（先 list 再读，避开 _SUCCESS/.crc/_ERROR.txt）
  * --pca_sample 抽样（默认 50k）
  * boto3.put_object 上传二进制，避免 awswrangler.upload(body=...) 兼容性问题
  * 支持 --config 读取 curated_bucket（可选）
"""

import os, io, json, random, argparse, tempfile
from datetime import datetime, timezone
from typing import List, Tuple, Dict, Any
import numpy as np
import pandas as pd
import awswrangler as wr
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
import boto3
import yaml

# ---------- 默认路径（如未指定 --config，将使用下列默认） ----------
DEFAULT_CURATED_BUCKET = "insightflow-dev-curated-bucket"
DEFAULT_USER_SEG = f"s3://{DEFAULT_CURATED_BUCKET}/recsys/user_seg/latest/user_seg.parquet/"
DEFAULT_SEG_POP  = f"s3://{DEFAULT_CURATED_BUCKET}/recsys/segment_popularity/latest/segment_popularity.parquet/"
DEFAULT_EXPORT_BASE = f"s3://{DEFAULT_CURATED_BUCKET}/recsys/exports/"

# ---------- 小样本规模 ----------
N_HIGH_VALUE_USERS = 150
N_RANDOM_USERS     = 150
TOPN_PER_SEGMENT   = 5

# ---------- utils ----------
_S3 = boto3.client("s3")

def ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

def s3_join(*parts) -> str:
    p = "/".join([x.strip("/") for x in parts if x])
    return p if p.startswith("s3://") else "s3://" + p

def s3_split(uri: str) -> Tuple[str, str]:
    assert uri.startswith("s3://")
    b, k = uri[5:].split("/", 1)
    return b, k

def put_bytes(uri: str, data: bytes):
    b, k = s3_split(uri)
    _S3.put_object(Bucket=b, Key=k, Body=data)

def upload_png(fig, s3_path: str, dpi: int = 160):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    put_bytes(s3_path, buf.getvalue())

def list_parquet_objects(prefix: str) -> List[str]:
    """只列出 .parquet 对象，避免 _SUCCESS/.crc/_ERROR.txt 干扰"""
    objs = wr.s3.list_objects(path=prefix if prefix.endswith("/") else prefix + "/")
    return [u for u in objs if u.lower().endswith(".parquet")]

def read_parquet_strict(prefix: str, use_strict: bool = True, columns=None) -> pd.DataFrame:
    if not use_strict:
        return wr.s3.read_parquet(path=prefix, columns=columns)
    paths = list_parquet_objects(prefix)
    if not paths:
        return wr.s3.read_parquet(path=prefix, columns=columns)
    return wr.s3.read_parquet(path=paths, columns=columns)

# ---------- 数据读取 ----------
def load_user_seg(path: str, strict: bool) -> pd.DataFrame:
    df = read_parquet_strict(path, use_strict=strict)
    # 列名兼容
    if "segment" not in df.columns and "cluster" in df.columns:
        df = df.rename(columns={"cluster":"segment"})
    for c in ["R","F","M","r","f","m"]:
        if c in df.columns:
            df = df.rename(columns={c: c.lower()})
    for c in ["r_z","f_z","m_z"]:
        if c not in df.columns:
            df[c] = np.nan
    keep = [c for c in ["user_id","segment","r","f","m","r_z","f_z","m_z"] if c in df.columns]
    return df[keep].copy()

def load_seg_pop(path: str, strict: bool) -> pd.DataFrame:
    sp = read_parquet_strict(path, use_strict=strict)
    # 列名兼容
    if "segment" not in sp.columns and "cluster" in sp.columns:
        sp = sp.rename(columns={"cluster":"segment"})
    if "score" not in sp.columns and "pop_score" in sp.columns:
        sp = sp.rename(columns={"pop_score":"score"})
    if "rank" not in sp.columns and "score" in sp.columns:
        sp = sp.sort_values(["segment","score"], ascending=[True,False])
        sp["rank"] = sp.groupby("segment").cumcount() + 1
    for c in ["support_seg","support_global"]:
        if c not in sp.columns:
            sp[c] = np.nan
    cols = [c for c in ["segment","product_id","score","rank","support_seg","support_global"] if c in sp.columns]
    return sp[cols].copy()

# ---------- 主逻辑 ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", help="可选：recsys.yaml，用于读取 s3.curated_bucket")
    ap.add_argument("--user_seg_s3", default=None)
    ap.add_argument("--seg_pop_s3",  default=None)
    ap.add_argument("--export_base", default=None)
    ap.add_argument("--charts_out_s3", default=None,
                    help="图表发布前缀；若不提供，则默认 s3://<curated_bucket>/recsys/eval_charts/rfm_analysis-<TS>/")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--topn_per_segment", type=int, default=TOPN_PER_SEGMENT)
    ap.add_argument("--hv_n", type=int, default=N_HIGH_VALUE_USERS)
    ap.add_argument("--rand_n", type=int, default=N_RANDOM_USERS)
    ap.add_argument("--strict_parquet", action="store_true",
                    help="启用严格只读 .parquet（先 list 再读），避免诊断文件干扰")
    ap.add_argument("--pca_sample", type=int, default=50000,
                    help="PCA 抽样上限（行），<=0 则全量")
    args = ap.parse_args()

    # 读取配置
    curated_bucket = DEFAULT_CURATED_BUCKET
    if args.config and os.path.exists(args.config):
        with open(args.config, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
            curated_bucket = (cfg.get("s3") or {}).get("curated_bucket", curated_bucket)

    # 时间戳（一次生成，保证 run 目录与图表目录同一批次）
    ts_str = ts()

    # 解析默认路径
    user_seg_s3 = args.user_seg_s3 or f"s3://{curated_bucket}/recsys/user_seg/latest/user_seg.parquet/"
    seg_pop_s3  = args.seg_pop_s3  or f"s3://{curated_bucket}/recsys/segment_popularity/latest/segment_popularity.parquet/"
    export_base = args.export_base or f"s3://{curated_bucket}/recsys/exports/"
    charts_dir  = args.charts_out_s3 or f"s3://{curated_bucket}/recsys/eval_charts/rfm_analysis-{ts_str}/"

    # 数据导出 run 目录
    run_dir = s3_join(export_base, "run-" + ts_str)
    data_dir = s3_join(run_dir, "data")

    random.seed(args.seed); np.random.seed(args.seed)

    # 1) 读取
    df = load_user_seg(user_seg_s3, strict=args.strict_parquet)
    sp = load_seg_pop(seg_pop_s3, strict=args.strict_parquet)

    n_clusters = int(df["segment"].nunique())
    centroids = (df.groupby("segment")[["r","f","m","r_z","f_z","m_z"]]
                   .mean().round(3).reset_index())

    # 2) 可视化
    # 2.1 簇画像（z-score）
    if {"r_z","f_z","m_z"}.issubset(df.columns) and df[["r_z","f_z","m_z"]].notna().any().any():
        mat = centroids.set_index("segment")[["r_z","f_z","m_z"]]
        fig = plt.figure(figsize=(6, 0.5*max(3, len(mat))+2))
        plt.imshow(mat.values, aspect="auto")
        plt.colorbar(label="z-score")
        plt.yticks(range(len(mat)), mat.index)
        plt.xticks(range(3), ["r_z","f_z","m_z"])
        plt.title("Cluster profiles (z-scores)")
        upload_png(fig, s3_join(charts_dir, "cluster_profiles.png"))

    # 2.2 PCA（抽样）
    if {"r","f","m"}.issubset(df.columns):
        X = df[["r","f","m"]].values.astype(float)
        n = len(X)
        if args.pca_sample > 0 and n > args.pca_sample:
            idx = np.random.choice(n, size=args.pca_sample, replace=False)
            X_plot = X[idx]
            seg_plot = df["segment"].values[idx]
        else:
            X_plot = X
            seg_plot = df["segment"].values
        if len(X_plot) >= 2:
            p2 = PCA(n_components=2).fit_transform(X_plot)
            fig = plt.figure(figsize=(6,5))
            sc = plt.scatter(p2[:,0], p2[:,1], s=2, c=seg_plot, alpha=0.4)
            plt.title(f"PCA of users by RFM (colored by segment)  (n={len(X_plot)})")
            plt.xlabel("PC1"); plt.ylabel("PC2")
            upload_png(fig, s3_join(charts_dir, "pca_segments.png"))

    # 2.3 分群热度（lift）
    if {"segment","product_id"}.issubset(sp.columns):
        sp = sp.copy()
        sp["lift"] = sp["support_seg"] / np.maximum(sp["support_global"], 1e-9)
        grp = sp.groupby("product_id")["lift"].agg(["mean","var"]).fillna(0.0)
        cands = grp.sort_values(["var","mean"], ascending=[False,False]).head(30).index
        mat2 = (sp[sp["product_id"].isin(cands)]
                .pivot_table(index="segment", columns="product_id",
                             values="lift", aggfunc="max")
                .fillna(1.0))
        if len(mat2) > 0:
            fig = plt.figure(figsize=(min(12, 0.35*mat2.shape[1]+4), 6))
            plt.imshow(mat2.values, aspect="auto")
            plt.colorbar(label="lift(seg/global)")
            plt.yticks(range(mat2.shape[0]), mat2.index)
            plt.xticks(range(mat2.shape[1]), mat2.columns, rotation=90)
            plt.title("Segment-level relative popularity (lift)")
            upload_png(fig, s3_join(charts_dir, "seg_pop_lift.png"))

    # 3) 小样本导出
    for c in ["r_z","f_z","m_z"]:
        if c not in df.columns:
            df[c] = 0.0
    df["hv"] = 0.4*df["r_z"].fillna(0) + 0.3*df["f_z"].fillna(0) + 0.3*df["m_z"].fillna(0)

    high_value = df.sort_values("hv", ascending=False).head(args.hv_n)
    rnd = df.sample(n=min(args.rand_n, len(df)), random_state=args.seed)

    sp2 = sp.copy()
    if "rank" not in sp2.columns and "score" in sp2.columns:
        sp2 = sp2.sort_values(["segment","score"], ascending=[True,False])
        sp2["rank"] = sp2.groupby("segment").cumcount() + 1

    topn = (sp2.sort_values(["segment","rank"])
              .groupby("segment").head(args.topn_per_segment)
              .reset_index(drop=True))

    def build_user_recos(sample_users: pd.DataFrame, label: str) -> pd.DataFrame:
        return sample_users[["user_id","segment","hv"]].merge(
            topn[["segment","product_id","rank","score"]],
            on="segment", how="left"
        ).assign(cohort=label)

    recos_all = pd.concat([
        build_user_recos(high_value, "high_value"),
        build_user_recos(rnd, "random")
    ], ignore_index=True).head(400)

    # 4) 导出
    overview = pd.DataFrame({
        "n_users":[len(df)],
        "n_clusters":[int(df["segment"].nunique())],
        "ts":[datetime.now(timezone.utc).isoformat()],
    })
    out = {
        "overview_csv": s3_join(data_dir, "overview.csv"),
        "centroids_parquet": s3_join(data_dir, "centroids.parquet"),
        "topn_parquet": s3_join(data_dir, "segment_topn.parquet"),
        "users_hv_parquet": s3_join(data_dir, "users_high_value.parquet"),
        "users_random_parquet": s3_join(data_dir, "users_random.parquet"),
        "recos_csv": s3_join(data_dir, "user_recos.csv"),
        "recos_parquet": s3_join(data_dir, "user_recos.parquet"),
        "recos_jsonl": s3_join(data_dir, "bedrock_inputs.jsonl"),
        "charts_dir": charts_dir,
        "run_dir": run_dir,
    }

    wr.s3.to_csv(df=overview, path=out["overview_csv"], index=False)
    wr.s3.to_parquet(df=centroids, path=out["centroids_parquet"], dataset=True, mode="overwrite")
    wr.s3.to_parquet(df=topn,      path=out["topn_parquet"],      dataset=True, mode="overwrite")
    wr.s3.to_parquet(df=high_value,path=out["users_hv_parquet"],  dataset=True, mode="overwrite")
    wr.s3.to_parquet(df=rnd,       path=out["users_random_parquet"], dataset=True, mode="overwrite")
    wr.s3.to_csv(df=recos_all,     path=out["recos_csv"], index=False)
    wr.s3.to_parquet(df=recos_all, path=out["recos_parquet"], dataset=True, mode="overwrite")

    # JSONL（每用户一行，附带该簇TopN）
    tmap = (topn.sort_values(["segment","rank"])
                 .groupby("segment")[["product_id","rank","score"]]
                 .apply(lambda x: x.assign(rank=x["rank"].astype(int)).to_dict(orient="records"))
                 ).to_dict()

    lines = []
    for _, r in recos_all.drop_duplicates("user_id").head(200).iterrows():
        lines.append(json.dumps({
            "user_id": int(r["user_id"]),
            "segment": int(r["segment"]),
            "rfm_z": {
                "r_z": float(r.get("r_z", np.nan)) if pd.notna(r.get("r_z", np.nan)) else None,
                "f_z": float(r.get("f_z", np.nan)) if pd.notna(r.get("f_z", np.nan)) else None,
                "m_z": float(r.get("m_z", np.nan)) if pd.notna(r.get("m_z", np.nan)) else None
            },
            "top_products": tmap.get(int(r["segment"]), [])
        }, ensure_ascii=False))

    put_bytes(out["recos_jsonl"], ("\n".join(lines)).encode("utf-8"))

    # 简易索引
    index = {
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
        "charts_dir": charts_dir,
        "run_dir": run_dir,
        "artifacts": {
            "cluster_profiles_png": s3_join(charts_dir, "cluster_profiles.png"),
            "pca_segments_png":     s3_join(charts_dir, "pca_segments.png"),
            "seg_pop_lift_png":     s3_join(charts_dir, "seg_pop_lift.png"),
            "centroids_parquet": out["centroids_parquet"],
            "segment_topn_parquet": out["topn_parquet"],
            "users_high_value_parquet": out["users_hv_parquet"],
            "users_random_parquet": out["users_random_parquet"],
            "user_recos_csv": out["recos_csv"],
            "user_recos_parquet": out["recos_parquet"],
            "bedrock_inputs_jsonl": out["recos_jsonl"],
        }
    }
    put_bytes(s3_join(run_dir, "index.json"), json.dumps(index, ensure_ascii=False, indent=2).encode("utf-8"))
    put_bytes(s3_join(charts_dir, "index.json"), json.dumps(index, ensure_ascii=False, indent=2).encode("utf-8"))

    print("[OK] charts:", charts_dir)
    print("[OK] exports:", json.dumps({k:v for k,v in out.items() if k.endswith("_parquet") or k.endswith("_csv") or k.endswith("_jsonl")}, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    main()
