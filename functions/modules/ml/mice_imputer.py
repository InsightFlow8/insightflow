"""
SageMaker Processing entrypoint for MICE imputation (statsmodels, PMM).
- Fill first-order NaN -> 0
- Only treat "30" on each user's LAST order as missing
- Build user-level features: [user_id, days_imp, frequency]
- Run MICE (PMM default): M=7 chains, 10 iters per chain
- Output mean-imputed parquet + optional per-chain parquet

Input dir (default):  /opt/ml/processing/input
Output dir (default): /opt/ml/processing/output
"""

import argparse
import os
import sys
import subprocess
import json
from pathlib import Path

import numpy as np
import pandas as pd

# ---------- Utils: read parquet recursively ----------

def read_all_parquet(folder: Path) -> pd.DataFrame:
    files = list(folder.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"[MICE] No parquet files found under: {folder}")
    dfs = []
    for f in files:
        try:
            df = pd.read_parquet(f)
            dfs.append(df)
        except Exception as e:
            print(f"[MICE][WARN] Failed to read {f}: {e}")
    if not dfs:
        raise RuntimeError("[MICE] Could not read any parquet files successfully.")
    df_all = pd.concat(dfs, ignore_index=True)
    return df_all

# ---------- Ensure statsmodels is available ----------

def ensure_statsmodels():
    try:
        from statsmodels.imputation.mice import MICEData  # noqa: F401
        return
    except Exception:
        print("[MICE] statsmodels not found. Installing ...", flush=True)
        cmd = [sys.executable, "-m", "pip", "install", "statsmodels"]
        subprocess.run(cmd, check=True)
    # Verify
    from statsmodels.imputation.mice import MICEData  # noqa: F401
    print("[MICE] statsmodels installed.", flush=True)

# ---------- Core: build user-level table ----------

def build_user_table(orders: pd.DataFrame) -> pd.DataFrame:
    """
    orders columns required:
      - user_id (int)
      - order_number (int)
      - days_since_prior_order (float, may contain NaN)
    Steps:
      1) Fill first-order NaN -> 0
      2) Get each user's LAST order row
      3) For last orders: rename days -> days_imp; mask value==30 -> NaN
      4) frequency = max(order_number) per user
    """
    req = {"user_id", "order_number", "days_since_prior_order"}
    missing_cols = req - set(orders.columns)
    if missing_cols:
        raise ValueError(f"[MICE] Missing required columns: {missing_cols}")

    # 1) 首单 NaN -> 0
    before_na = orders["days_since_prior_order"].isna().sum()
    orders["days_since_prior_order"] = orders["days_since_prior_order"].fillna(0)
    after_na = orders["days_since_prior_order"].isna().sum()
    print(f"[MICE] fillna(0) on days_since_prior_order: {before_na} -> {after_na}", flush=True)

    # 基本范围检查
    print("[MICE] days_since_prior_order: min/max after fillna:",
          float(orders["days_since_prior_order"].min()),
          float(orders["days_since_prior_order"].max()), flush=True)

    # 2) 每个用户最后一单索引
    last_idx = orders.groupby("user_id")["order_number"].idxmax()
    last_orders = orders.loc[last_idx, ["user_id", "days_since_prior_order"]].copy()

    # 3) 只把最后一单里值==30 的视作缺失
    last_orders = last_orders.rename(columns={"days_since_prior_order": "days_imp"})
    mask_30 = (last_orders["days_imp"] == 30)
    need_imp = int(mask_30.sum())
    last_orders.loc[mask_30, "days_imp"] = np.nan
    print(f"[MICE] Users with days_imp==30 on last order (set to NaN): {need_imp}", flush=True)

    # 4) 频次（max order_number）
    freq = (
        orders.groupby("user_id", as_index=False)["order_number"]
        .max()
        .rename(columns={"order_number": "frequency"})
    )

    user_imp = last_orders.merge(freq, on="user_id", how="left")
    print("[MICE] user_imp shape:", tuple(user_imp.shape), flush=True)
    print("[MICE] Missing days_imp count:", int(user_imp["days_imp"].isna().sum()), flush=True)
    return user_imp

# ---------- MICE runner (PMM by default in statsmodels.MICEData) ----------

def run_mice_pmm(user_imp: pd.DataFrame, M: int = 7, max_iter: int = 10, seed: int = 42,
                 keep_all_chains: bool = True) -> (pd.DataFrame, pd.DataFrame):
    """
    Input: user_imp with columns [user_id, days_imp, frequency]
    Returns:
      - final_mean: DataFrame[user_id, days_imp_imputed]  (rounded int)
      - imputed_all: concatenated chains with 'impute_id' (optional auditing)
    """
    ensure_statsmodels()
    from statsmodels.imputation.mice import MICEData

    np.random.seed(seed)

    # 只保留需要的列，拷贝以免污染原表
    mice_features = user_imp[["user_id", "days_imp", "frequency"]].copy()

    imputed_list = []
    for chain in range(M):
        md = MICEData(mice_features)
        md.update_all(n_iter=max_iter)  # 默认 PMM
        df_imp = md.data.copy()
        df_imp["impute_id"] = chain
        imputed_list.append(df_imp)
        print(f"[MICE] Completed chain {chain+1}/{M}", flush=True)

    imputed_all = pd.concat(imputed_list, ignore_index=True)

    # 对每个 user_id 的 days_imp 取平均 -> 四舍五入 -> int
    final_mean = (
        imputed_all.groupby("user_id")["days_imp"]
        .mean()
        .round()
        .astype(int)
        .rename("days_imp_imputed")
        .reset_index()
    )

    # 审计输出一些范围检查
    print("[MICE] Imputed days_imp max:", float(imputed_all["days_imp"].max()), flush=True)
    print("[MICE] Final (rounded) days_imp_imputed max:", int(final_mean["days_imp_imputed"].max()), flush=True)

    if keep_all_chains:
        return final_mean, imputed_all
    else:
        return final_mean, None

# ---------- Main ----------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_dir", type=str,
                        default=os.environ.get("SM_INPUT_DIR", "/opt/ml/processing/input"))
    parser.add_argument("--output_dir", type=str,
                        default=os.environ.get("SM_OUTPUT_DIR", "/opt/ml/processing/output"))
    parser.add_argument("--chains", type=int, default=7)
    parser.add_argument("--iters", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--write_all_chains", action="store_true", help="write per-chain parquet files")
    args = parser.parse_args()

    in_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[MICE] Reading parquet from: {in_dir}", flush=True)
    orders = read_all_parquet(in_dir)
    print(f"[MICE] Raw input shape: {orders.shape}", flush=True)

    # 类型提示（尽量不强转，除非必要）
    for col in ["user_id", "order_number"]:
        if col in orders.columns:
            # 如果是浮点且全为整数值，安全转 int
            if pd.api.types.is_float_dtype(orders[col]):
                if (orders[col].dropna() % 1 == 0).all():
                    orders[col] = orders[col].astype("Int64").astype("int64")

    user_imp = build_user_table(orders)

    print("[MICE] Running MICE (PMM) ...", flush=True)
    final_mean, imputed_all = run_mice_pmm(
        user_imp,
        M=args.chains,
        max_iter=args.iters,
        seed=args.seed,
        keep_all_chains=args.write_all_chains
    )

    # 合并回 user_imp（便于对比/审计）
    merged = user_imp.merge(final_mean, on="user_id", how="left")
    print("[MICE] Merged user_imp + days_imp_imputed, shape:", merged.shape, flush=True)

    # 写主结果（推荐消费）
    out_mean = out_dir / "user_imp_imputed_mean.parquet"
    merged.to_parquet(out_mean, index=False)
    print(f"[MICE] Saved mean-imputed result: {out_mean}", flush=True)

    # 可选：写每链结果
    if imputed_all is not None and args.write_all_chains:
        for m in sorted(imputed_all["impute_id"].unique()):
            df_m = imputed_all[imputed_all["impute_id"] == m][["user_id", "days_imp", "frequency"]].copy()
            df_m.to_parquet(out_dir / f"user_imp_imputed_m{m}.parquet", index=False)
        print(f"[MICE] Saved per-chain files under: {out_dir}", flush=True)

    # 记录指标（可选）：写一个 JSON 汇总，方便下游/监控
    metrics = {
        "input_rows": int(len(orders)),
        "user_rows": int(len(user_imp)),
        "missing_days_imp_after_mask": int(user_imp["days_imp"].isna().sum()),
        "chains": int(args.chains),
        "iters_per_chain": int(args.iters),
    }
    with open(out_dir / "mice_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)
    print(f"[MICE] Metrics: {metrics}", flush=True)

    print("[MICE] DONE.", flush=True)


if __name__ == "__main__":
    main()
