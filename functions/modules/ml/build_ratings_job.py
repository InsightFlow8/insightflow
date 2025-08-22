# functions/modules/ml/build_ratings_job.py
import time
from pathlib import Path
import argparse
from urllib.parse import urlparse

import yaml
import boto3
import sagemaker
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.spark.processing import PySparkProcessor


def load_cfg(p: str) -> dict:
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_s3_uri(uri: str):
    """
    将 s3://bucket/prefix 解析为 (bucket, prefix_without_leading_slash)
    """
    p = urlparse(uri)
    assert p.scheme == "s3", f"Not S3: {uri}"
    return p.netloc, p.path.lstrip("/")


def _first_parquet_key(s3_client, bucket: str, prefix: str):
    """
    返回 prefix 下遇到的第一个 .parquet 对象 key（用于把分区目录合并为单文件时的“代表文件”复制）
    """
    token = None
    while True:
        kwargs = dict(Bucket=bucket, Prefix=prefix)
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3_client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            if obj["Key"].lower().endswith(".parquet"):
                return obj["Key"]
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(Path("configs/recsys.yaml")))
    args = ap.parse_args()

    cfg = load_cfg(args.config)
    region = cfg["aws"]["region"]
    role_arn = cfg["aws"]["role_arn"]
    default_bucket = cfg["aws"]["default_bucket"]

    # 数据源
    orders_latest = cfg["s3"]["mice_orders_latest"].rstrip("/")  # 明细级 after-MICE 单文件
    clean_base = cfg["s3"]["raw_after_clean"].rstrip("/")        # e.g. s3://.../after-clean/orders/
    if clean_base.endswith("/orders"):
        clean_base = clean_base[: -len("/orders")]
    op_prior = f"{clean_base}/order_products_prior/"
    trans_base = cfg["s3"]["raw_after_transformation"].rstrip("/")  # e.g. s3://.../after-transformation/

    # 作业输出
    ts = time.strftime("%Y%m%d-%H%M%S")
    out_run = f"{trans_base}/upi_features/run-{ts}/"

    print(f"[RATINGS] orders_latest: {orders_latest}")
    print(f"[RATINGS] op_prior     : {op_prior}")
    print(f"[RATINGS] out_run      : {out_run}")

    # 会话 & 计算规格（直接复用 training 的规格，Shuffle 稳定）
    boto_ses = boto3.Session(region_name=region)
    sm_ses = sagemaker.session.Session(
        boto_session=boto_ses, default_bucket=default_bucket
    )

    tr = cfg["training"]
    processor = PySparkProcessor(
        base_job_name="build-ratings",
        framework_version="3.3",
        role=role_arn,
        instance_count=tr["instance_count"],
        instance_type=tr["instance_type"],
        max_runtime_in_seconds=tr["max_runtime_sec"],
        volume_size_in_gb=tr["volume_gb"],
        sagemaker_session=sm_ses,
    )

    # 入口脚本：和本文件同目录的 build_ratings.py
    submit_app = str((Path(__file__).resolve().parent / "build_ratings.py").as_posix())

    # 输入/输出映射（容器路径）
    inputs = [
        ProcessingInput(
            source=orders_latest,
            destination="/opt/ml/processing/input/orders",
            input_name="orders",
        ),
        ProcessingInput(
            source=op_prior,
            destination="/opt/ml/processing/input/op",
            input_name="op",
        ),
    ]

    job_name = f"build-ratings-{ts}"
    processor.run(
        submit_app=submit_app,  # 本地文件路径，SDK 会自动打包上传
        inputs=inputs,
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",
                destination=out_run,
                output_name="out",
            )
        ],
        arguments=[
            "--orders_dir",
            "/opt/ml/processing/input/orders",
            "--op_dir",
            "/opt/ml/processing/input/op",
            "--output_dir",
            "/opt/ml/processing/output",
            "--shuffle_partitions",
            "200",
            "--coalesce",
            "1",
        ],
        wait=True,
        logs=True,
        job_name=job_name,
        # 可选：打开 Spark 事件日志便于诊断
        # spark_event_logs_s3_uri=f"{out_run}spark-events/",
    )

    # ---------- 同步到 latest（修复：不再把 's3://...' 整串当作 Key） ----------
    s3c = boto_ses.client("s3")
    s3r = boto_ses.resource("s3")

    # run-* 输出所在 bucket/prefix
    out_bucket, out_prefix = _parse_s3_uri(out_run)

    # latest 目标所在 bucket/prefix 需要从 trans_base 再解析一次！
    latest_bucket, trans_prefix = _parse_s3_uri(trans_base)
    latest_prefix = f"{trans_prefix.rstrip('/')}/upi_features/latest"

    for subdir, fname in [("ratings/", "ratings.parquet"),
                          ("upi_features/", "upi_features.parquet")]:
        src_key = _first_parquet_key(s3c, out_bucket, f"{out_prefix.rstrip('/')}/{subdir}")
        if src_key:
            dst_key = f"{latest_prefix.rstrip('/')}/{fname}"
            s3r.meta.client.copy(
                {"Bucket": out_bucket, "Key": src_key},
                latest_bucket,
                dst_key,
            )
            print(f"[RATINGS] synced {subdir[:-1]} -> s3://{latest_bucket}/{dst_key}")
        else:
            print(f"[RATINGS][WARN] no {subdir} parquet to sync.")

    print(
        f"[RATINGS] Done.\n"
        f"  Run output: {out_run}\n"
        f"  Latest dir: s3://{latest_bucket}/{latest_prefix.rstrip('/')}/"
    )


if __name__ == "__main__":
    main()
