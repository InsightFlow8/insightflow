# functions/modules/ml/als_train.py
"""
在 SageMaker PySparkProcessor 容器内训练隐式反馈 ALS（使用 python `implicit` 库）。
- 自动安装缺失依赖（implicit / scipy / pandas / pyarrow）
- 自动定位包含 (user_id, product_id, rating) 的 Parquet 数据集（从 --input_dir 根开始若干常见子目录）
- 充足日志，失败时打印友好错误并 exit(2)

用法（由 train_als.py 调用）：
  spark-submit ... als_train.py --input_dir /opt/ml/processing/input --model_dir /opt/ml/processing/model \
    --user_col user_id --item_col product_id --rating_col rating --factors 64 --reg 0.01 --alpha 40 --iters 10
"""

import os
import sys
import time
import json
import argparse
import subprocess
from pathlib import Path

def _pip_install(pkgs):
    """在容器内安装所需依赖（如果已装则跳过）。"""
    for p in pkgs:
        try:
            __import__(p.split("==")[0].replace("-", "_"))
            print(f"[PKG] already installed: {p}")
        except Exception:
            print(f"[PKG] installing: {p}")
            subprocess.run([sys.executable, "-m", "pip", "install", p, "--quiet"], check=True)

def _now():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input_dir", required=True)
    ap.add_argument("--model_dir", required=True)
    ap.add_argument("--user_col", default="user_id")
    ap.add_argument("--item_col", default="product_id")
    ap.add_argument("--rating_col", default="rating")
    ap.add_argument("--factors", type=int, default=64)
    ap.add_argument("--reg", type=float, default=0.01)
    ap.add_argument("--alpha", type=float, default=40.0)
    ap.add_argument("--iters", type=int, default=10)
    return ap.parse_args()

def _find_dataset(spark, root: str, want_cols):
    """在 root 下按候选路径依次尝试读取 parquet，直到发现包含所需列的 DataFrame。"""
    root = root.rstrip("/")
    cands = [
        root,  # 直接就是数据集
        f"{root}/upi_features_union",
        f"{root}/features/upi_features_union",
        f"{root}/ratings",
        f"{root}/user_product_interactions",
    ]
    errors = {}
    for p in cands:
        try:
            print(f"[{_now()}] [DATA] try: {p}")
            df = spark.read.parquet(p)
            cols = set(x.lower() for x in df.columns)
            missing = [c for c in want_cols if c.lower() not in cols]
            if missing:
                print(f"[DATA] path {p} exists but missing columns: {missing}; columns={df.columns}")
                continue
            print(f"[DATA] use dataset: {p} with columns={df.columns}")
            return df, p
        except Exception as e:
            errors[p] = str(e)
    raise RuntimeError(f"No valid dataset found under {root}. Tried: {list(errors.keys())}. Errors: {json.dumps(errors)[:500]}")

def main():
    print(f"[{_now()}] [BOOT] als_train.py starting...")

    # -------- 1) 安装依赖（单机 processing 容器可行） --------
    # 选定版本有预编译 manylinux wheel，避免编译失败
    _pip_install([
        "implicit==0.7.2",
        "scipy>=1.10,<2.0",
        "pandas>=2.1,<3.0",
        "pyarrow>=14,<22",
    ])
    # 限制 BLAS 线程，避免小实例过度并发
    os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
    os.environ.setdefault("MKL_NUM_THREADS", "1")

    # -------- 2) 解析参数 / 构建 Spark --------
    args = parse_args()
    print(f"[ARGS] {vars(args)}")

    # Pyspark 在 SageMaker Spark 容器已就绪，这里直接 import / 构建会话
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("recsys-als-train")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    want_cols = [args.user_col, args.item_col, args.rating_col]

    # -------- 3) 自动发现数据集并做基本校验 --------
    sdf, used_path = _find_dataset(spark, args.input_dir, want_cols)
    sdf = sdf.select(args.user_col, args.item_col, args.rating_col).dropna()
    print(f"[DATA] sample:")
    print(sdf.limit(5).toPandas())

    # -------- 4) 构造稀疏交互矩阵，训练 implicit ALS --------
    import numpy as np
    import pandas as pd
    from scipy.sparse import coo_matrix
    from implicit.als import AlternatingLeastSquares

    # 统一为 int 类型并从 0 开始索引
    pdf = sdf.toPandas()
    user_ids = pdf[args.user_col].astype("int64")
    item_ids = pdf[args.item_col].astype("int64")
    ratings  = pdf[args.rating_col].astype("float32")

    # 映射到 [0..n)（implicit 需要从 0 开始）
    u_unique, u_idx = np.unique(user_ids.values, return_inverse=True)
    i_unique, i_idx = np.unique(item_ids.values, return_inverse=True)

    rows = u_idx
    cols = i_idx
    data = ratings.values
    mat = coo_matrix((data, (rows, cols)), shape=(u_unique.size, i_unique.size)).tocsr()

    print(f"[SHAPE] users={u_unique.size}, items={i_unique.size}, nnz={mat.nnz}")

    # 训练（implicit 的 ALS 接受“信心”权重；alpha 已在 features 端计算或此处作为放大系数）
    model = AlternatingLeastSquares(
        factors=int(args.factors),
        regularization=float(args.reg),
        iterations=int(args.iters),
        use_gpu=False,
        num_threads=0,
    )
    # implicit 默认期望 item-user 矩阵；此处做转置
    model.fit(mat.T)

    # -------- 5) 保存模型与映射 --------
    model_dir = Path(args.model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / "als_model.npz"
    model.save(str(model_path))

    # 保存 id 映射，便于离线/在线推理
    pd.Series(u_unique).to_csv(model_dir / "users.csv", index=False, header=False)
    pd.Series(i_unique).to_csv(model_dir / "items.csv", index=False, header=False)

    meta = {
        "used_path": used_path,
        "user_col": args.user_col,
        "item_col": args.item_col,
        "rating_col": args.rating_col,
        "factors": int(args.factors),
        "reg": float(args.reg),
        "alpha": float(args.alpha),
        "iters": int(args.iters),
        "timestamp": _now(),
    }
    (model_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2))

    print(f"[SAVE] model: {model_path}")
    print(f"[SAVE] meta : {model_dir / 'meta.json'}")
    print(f"[{_now()}] [DONE] ALS training finished successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 明确打印错误，保证 spark-submit 返回非 0
        print(f"[FATAL] {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(2)
