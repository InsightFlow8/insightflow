"""
SageMaker Processing entrypoint for MICE imputation (statsmodels, PMM).

目标：
- 明细级：生成 orders_imputed.parquet（除了“最后一单==30”的行被填补，其它行原样保留）
- 汇总级：保留 user_imp_imputed_mean.parquet（审计/下游可选）

业务规则：
1) 首单 NaN -> 0（保持）
2) 仅把“每个用户的最后一单且 days_since_prior_order==30”视作缺失
3) 先用 MICE(PMM) 按用户估计；若个别用户仍无值，用“该用户其它订单的中位数”兜底，再无则用全局中位数
"""

import argparse
import os
import sys
import subprocess
from pathlib import Path
import json

import numpy as np
import pandas as pd


# ---------- IO: 递归读写 parquet ----------

def read_all_parquet(folder: Path) -> pd.DataFrame:
    files = list(folder.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"[MICE] No parquet files under: {folder}")
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            print(f"[MICE][WARN] read {f} failed: {e}", flush=True)
    if not dfs:
        raise RuntimeError("[MICE] Could not read any parquet successfully.")
    return pd.concat(dfs, ignore_index=True)


# ---------- 依赖：statsmodels（Processing 容器内自装） ----------

def ensure_statsmodels():
    try:
        from statsmodels.imputation.mice import MICEData  # noqa: F401
        return
    except Exception:
        print("[MICE] statsmodels not found. Installing ...", flush=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "statsmodels"], check=True)
    from statsmodels.imputation.mice import MICEData  # noqa: F401
    print("[MICE] statsmodels installed.", flush=True)


# ---------- 构造用户级表（仅最后一单的 30 视作缺失） ----------

def build_user_table(orders: pd.DataFrame) -> pd.DataFrame:
    req = {"user_id", "order_number", "days_since_prior_order"}
    missing = req - set(orders.columns)
    if missing:
        raise ValueError(f"[MICE] Missing required columns: {missing}")

    # 1) 首单 NaN -> 0
    before = orders["days_since_prior_order"].isna().sum()
    orders["days_since_prior_order"] = orders["days_since_prior_order"].fillna(0)
    after = orders["days_since_prior_order"].isna().sum()
    print(f"[MICE] fillna(0) on days_since_prior_order: {before} -> {after}", flush=True)

    # 2) 找每个用户的最后一单
    last_idx = orders.groupby("user_id")["order_number"].idxmax()
    last_orders = orders.loc[last_idx, ["user_id", "days_since_prior_order"]].copy()

    # 3) 仅把“最后一单==30”视作缺失
    last_orders = last_orders.rename(columns={"days_since_prior_order": "days_imp"})
    mask_30 = last_orders["days_imp"] == 30
    n30 = int(mask_30.sum())
    last_orders.loc[mask_30, "days_imp"] = np.nan
    print(f"[MICE] users with last-order==30 (set to NaN): {n30}", flush=True)

    # 4) 频次特征
    freq = (
        orders.groupby("user_id", as_index=False)["order_number"]
        .max().rename(columns={"order_number": "frequency"})
    )

    user_imp = last_orders.merge(freq, on="user_id", how="left")
    print("[MICE] user_imp shape:", tuple(user_imp.shape), flush=True)
    print("[MICE] Missing days_imp:", int(user_imp["days_imp"].isna().sum()), flush=True)
    return user_imp


# ---------- MICE (PMM) ----------

def run_mice_pmm(user_imp: pd.DataFrame, M: int = 7, max_iter: int = 10, seed: int = 42):
    ensure_statsmodels()
    from statsmodels.imputation.mice import MICEData

    np.random.seed(seed)
    cols = ["user_id", "days_imp", "frequency"]
    base = user_imp[cols].copy()

    imputed = []
    for m in range(M):
        md = MICEData(base)
        md.update_all(n_iter=max_iter)  # PMM
        df = md.data.copy()
        df["impute_id"] = m
        imputed.append(df)
        print(f"[MICE] chain {m+1}/{M} done", flush=True)
    imputed_all = pd.concat(imputed, ignore_index=True)

    final_mean = (
        imputed_all.groupby("user_id")["days_imp"]
        .mean().round().astype(int)
        .rename("days_imp_imputed").reset_index()
    )
    return final_mean, imputed_all


# ---------- 把 MICE 结果回填到“明细级 orders” ----------

def write_detail_orders(orders: pd.DataFrame, final_mean: pd.DataFrame, out_dir: Path):
    # 定位“最后一单”
    last_idx = orders.groupby("user_id")["order_number"].idxmax()
    orders2 = orders.copy()
    orders2["_is_last"] = False
    orders2.loc[last_idx, "_is_last"] = True

    # 需要回填的行：最后一单且原值==30
    need_mask = orders2["_is_last"] & (orders2["days_since_prior_order"] == 30)

    # MICE 估计值：user_id -> 值
    mice_map = final_mean.set_index("user_id")["days_imp_imputed"]

    # 用户中位数兜底：把需要回填的行先置 NaN 再求每个用户的中位数
    tmp = orders2.copy()
    tmp.loc[need_mask, "days_since_prior_order"] = np.nan
    user_median = tmp.groupby("user_id")["days_since_prior_order"].median()
    global_median = float(tmp["days_since_prior_order"].median())

    # 三段回填：MICE -> 用户中位数 -> 全局中位数
    fill = orders2.loc[need_mask, "user_id"].map(mice_map)
    fill = fill.fillna(orders2.loc[need_mask, "user_id"].map(user_median))
    fill = fill.fillna(global_median).round().astype(int)

    orders2.loc[need_mask, "days_since_prior_order"] = fill
    orders2 = orders2.drop(columns=["_is_last"])
    # 最终类型收紧为 int
    orders2["days_since_prior_order"] = orders2["days_since_prior_order"].astype("int32")

    out_path = out_dir / "orders_imputed.parquet"
    orders2.to_parquet(out_path, index=False)
    print(f"[MICE] Saved detail parquet: {out_path}", flush=True)


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", default=os.environ.get("SM_INPUT_DIR", "/opt/ml/processing/input"))
    ap.add_argument("--output_dir", default=os.environ.get("SM_OUTPUT_DIR", "/opt/ml/processing/output"))
    ap.add_argument("--chains", type=int, default=7)
    ap.add_argument("--iters", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--write_all_chains", action="store_true")
    args = ap.parse_args()

    in_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[MICE] reading parquet from: {in_dir}", flush=True)
    orders = read_all_parquet(in_dir)
    print(f"[MICE] input shape: {orders.shape}", flush=True)

    # 若整数列被读成 float（如 1.0），安全转回 int
    for col in ["user_id", "order_number"]:
        if col in orders.columns and pd.api.types.is_float_dtype(orders[col]):
            if (orders[col].dropna() % 1 == 0).all():
                orders[col] = orders[col].astype("Int64").astype("int64")

    user_imp = build_user_table(orders)
    print("[MICE] running MICE ...", flush=True)
    final_mean, imputed_all = run_mice_pmm(user_imp, M=args.chains, max_iter=args.iters, seed=args.seed)

    # 汇总级（审计/回看）
    merged = user_imp.merge(final_mean, on="user_id", how="left")
    merged.to_parquet(out_dir / "user_imp_imputed_mean.parquet", index=False)
    print(f"[MICE] saved: {out_dir / 'user_imp_imputed_mean.parquet'}", flush=True)

    # 明细级写回（训练主用）
    write_detail_orders(orders, final_mean, out_dir)

    # 可选：每条链输出
    if args.write_all_chains and imputed_all is not None:
        for m in sorted(imputed_all["impute_id"].unique()):
            df = imputed_all[imputed_all["impute_id"] == m][["user_id", "days_imp", "frequency"]]
            df.to_parquet(out_dir / f"user_imp_imputed_m{m}.parquet", index=False)

    # 简单指标
    metrics = {
        "input_rows": int(len(orders)),
        "user_rows": int(len(user_imp)),
        "missing_after_mask": int(user_imp["days_imp"].isna().sum()),
        "chains": int(args.chains),
        "iters": int(args.iters),
    }
    with open(out_dir / "mice_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[MICE] metrics: {metrics}", flush=True)
    print("[MICE] DONE", flush=True)


if __name__ == "__main__":
    main()
