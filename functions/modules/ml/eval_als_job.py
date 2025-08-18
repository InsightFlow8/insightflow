#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
from datetime import datetime, timezone
import boto3
import yaml

import sagemaker
from sagemaker.spark.processing import PySparkProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput


def _now_ts():
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _get(cfg, *keys, default=None):
    d = cfg
    for k in keys:
        if not isinstance(d, dict) or k not in d:
            return default
        d = d[k]
    return d


def _strip_s3(s3_uri: str):
    """s3://bucket/prefix -> (bucket, prefix)"""
    assert s3_uri.startswith("s3://")
    x = s3_uri[5:]
    i = x.find("/")
    if i < 0:
        return x, ""
    return x[:i], x[i + 1 :]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--model_s3", required=True, help="s3://.../als_model/ (上一步训练输出)")
    args = ap.parse_args()

    # 读取配置
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    region = _get(cfg, "aws", "region")
    role_arn = _get(cfg, "aws", "role_arn")
    # 这里非常关键：用你们允许的 bucket（而不是 SageMaker 默认桶）
    default_bucket = _get(cfg, "aws", "default_bucket", default="insightflow-dev-clean-bucket")

    # 数据源（和你之前打印一致）
    orders_latest = (
        _get(cfg, "s3", "mice_orders_latest")
        or _get(cfg, "s3", "mice_orders", "latest")
        or "s3://insightflow-dev-clean-bucket/after-clean/after-MICE/latest/orders.parquet"
    )

    op_train = (
        _get(cfg, "s3", "order_products_train")
        or _get(cfg, "s3", "op_train")
        or _get(cfg, "s3", "order_products_prior", default="").replace("prior/", "train/")
    )

    # 评估结果输出（curated 层）
    curated_bucket = _get(cfg, "s3", "curated_bucket", default="insightflow-dev-curated-bucket")
    ts = _now_ts()
    out_s3 = f"s3://{curated_bucket}/recsys/eval/als-{ts}"

    top_k = int(_get(cfg, "eval", "top_k", default=20))

    print(f"[EVAL] orders_latest: {orders_latest}")
    print(f"[EVAL] op_train     : {op_train}")
    print(f"[EVAL] model_s3     : {args.model_s3}")
    print(f"[EVAL] out_dir      : {out_s3}")
    print(f"[EVAL] top_k        : {top_k}")

    # === 关键改动：先把本地脚本上传到你们的 clean-bucket，再用 S3 URI 作为 submit_app ===
    local_script = "functions/modules/ml/als_eval.py"
    s3_code_key = f"als-eval-{ts}/code/als_eval.py"
    s3_code_uri = f"s3://{default_bucket}/{s3_code_key}"

    s3 = boto3.client("s3", region_name=region)
    s3.upload_file(local_script, default_bucket, s3_code_key)

    # 处理资源规格（沿用你前面的处理规格区块）
    instance_type = _get(cfg, "processing", "instance_type", default="ml.t3.xlarge")
    instance_count = int(_get(cfg, "processing", "instance_count", default=1))
    volume_gb = int(_get(cfg, "processing", "volume_gb", default=100))
    max_runtime_sec = int(_get(cfg, "processing", "max_runtime_sec", default=7200))

    # Spark 事件日志也写到允许的桶（可选，便于调试）
    spark_event_logs = f"s3://{default_bucket}/sm-test/spark-logs/als-eval/{ts}/"

    # 构造 Processor
    proc = PySparkProcessor(
        base_job_name="als-eval",
        framework_version="3.3",
        role=role_arn,
        instance_type=instance_type,
        instance_count=instance_count,
        max_runtime_in_seconds=max_runtime_sec,
        volume_size_in_gb=volume_gb,
        sagemaker_session=sagemaker.Session(),  # 不触发默认桶打包，因为我们用的是 submit_app=S3
    )

    # Inputs / Outputs
    inputs = [
        ProcessingInput(source=orders_latest, destination="/opt/ml/processing/input/orders"),
        ProcessingInput(source=op_train, destination="/opt/ml/processing/input/op_train"),
        ProcessingInput(source=args.model_s3, destination="/opt/ml/processing/model"),
    ]
    outputs = [
        ProcessingOutput(
            source="/opt/ml/processing/output",
            destination=out_s3,
        )
    ]

    # 传给脚本的参数
    arguments = [
        "--orders_dir",
        "/opt/ml/processing/input/orders",
        "--op_train_dir",
        "/opt/ml/processing/input/op_train",
        "--model_dir",
        "/opt/ml/processing/model",
        "--output_dir",
        "/opt/ml/processing/output",
        "--top_k",
        str(top_k),
        # 让脚本也知道最终 S3 输出位置（可用于写 metrics 指针等）
        "--out_s3",
        out_s3,
        # 给大数据安全一点余量
        "--shuffle_partitions",
        "200",
    ]

    job_name = f"als-eval-{ts}"
    # 提交：注意 submit_app 使用的是我们刚上传到 clean-bucket 的 S3 URI
    proc.run(
        submit_app=s3_code_uri,
        inputs=inputs,
        outputs=outputs,
        arguments=arguments,
        logs=True,
        spark_event_logs_s3_uri=spark_event_logs,
        job_name=job_name,
    )

    print(f"[EVAL] Done.\n  Run output:   {out_s3}\n  Metrics file: {out_s3}/metrics.json")


if __name__ == "__main__":
    main()
