#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--model_s3", required=True, help="S3 前缀的父目录（包含 als_model/）")
    args = ap.parse_args()

    # 读取配置
    with open(args.config, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    region = _get(cfg, "aws", "region")
    role_arn = _get(cfg, "aws", "role_arn")
    default_bucket = _get(cfg, "aws", "default_bucket", default="insightflow-dev-clean-bucket")

    # 数据源（与 eval_als_job.py 同口径）
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

    # 输出前缀（curated 层）
    curated_bucket = _get(cfg, "s3", "curated_bucket", default="insightflow-dev-curated-bucket")
    ts = _now_ts()
    out_s3 = f"s3://{curated_bucket}/recsys/eval/als-postrank-{ts}"

    # 评估/重排参数（YAML -> 脚本）
    top_k = int(_get(cfg, "eval", "top_k", default=_get(cfg, "inference", "top_k", default=10)))
    cand_k = int(_get(cfg, "eval", "candidate_k", default=50))
    # 混排（关闭时 gamma=0）
    blend_enabled = bool(_get(cfg, "eval", "blend", "enabled", default=False))
    blend_gamma = float(_get(cfg, "eval", "blend", "gamma", default=0.3)) if blend_enabled else 0.0
    # MMR（关闭时 lambda=0）
    mmr_enabled = bool(_get(cfg, "eval", "mmr", "enabled", default=False))
    mmr_lambda = float(_get(cfg, "eval", "mmr", "lambda", default=0.0)) if mmr_enabled else 0.0
    filter_seen = bool(_get(cfg, "eval", "filter_seen", default=False))

    # 处理资源规格（与 eval_als_job.py 同风格）
    instance_type = _get(cfg, "processing", "instance_type", default="ml.t3.xlarge")
    instance_count = int(_get(cfg, "processing", "instance_count", default=1))
    volume_gb = int(_get(cfg, "processing", "volume_gb", default=100))
    max_runtime_sec = int(_get(cfg, "processing", "max_runtime_sec", default=7200))
    spark_event_logs = f"s3://{default_bucket}/sm-test/spark-logs/als-postrank/{ts}/"

    print(f"[POST] orders_latest : {orders_latest}")
    print(f"[POST] op_train      : {op_train}")
    print(f"[POST] model_s3      : {args.model_s3} (父目录，包含 als_model/)")
    print(f"[POST] out_dir       : {out_s3}")
    print(f"[POST] top_k={top_k} cand_k={cand_k} gamma={blend_gamma} mmr={mmr_lambda} filter_seen={filter_seen}")

    # 把本地脚本上传到 scripts-bucket（代码仓），用 S3 作为 submit_app
    scripts_bucket = _get(cfg, "s3", "scripts_bucket", default=default_bucket)
    scripts_prefix = _get(cfg, "s3", "scripts_prefix", default="ML").strip("/")  # 例如 "ML" 或 "machine-learning"

    local_script = "functions/modules/ml/als_postrank.py"
    if scripts_prefix:
        s3_code_key = f"{scripts_prefix}/als-postrank/{ts}/als_postrank.py"
    else:
        s3_code_key = f"als-postrank/{ts}/als_postrank.py"
    s3_code_uri = f"s3://{scripts_bucket}/{s3_code_key}"
    boto3.client("s3", region_name=region).upload_file(local_script, scripts_bucket, s3_code_key)
    print(f"[POST] submit_app     : {s3_code_uri}")

    # 构造 Spark Processor
    proc = PySparkProcessor(
        base_job_name="als-postrank",
        framework_version="3.3",
        role=role_arn,
        instance_type=instance_type,
        instance_count=instance_count,
        max_runtime_in_seconds=max_runtime_sec,
        volume_size_in_gb=volume_gb,
        sagemaker_session=sagemaker.Session(),  # 直接用 submit_app=S3，避免默认桶打包
    )

    # I/O 挂载（与 eval 作业一致）
    inputs = [
        ProcessingInput(source=orders_latest, destination="/opt/ml/processing/input/orders"),
        ProcessingInput(source=op_train,      destination="/opt/ml/processing/input/op_train"),
        ProcessingInput(source=args.model_s3, destination="/opt/ml/processing/model"),
    ]
    outputs = [
        ProcessingOutput(source="/opt/ml/processing/output", destination=out_s3)
    ]

    # 传给 als_postrank.py 的参数
    arguments = [
        "--orders_dir",   "/opt/ml/processing/input/orders",
        "--op_train_dir", "/opt/ml/processing/input/op_train",
        "--model_dir",    "/opt/ml/processing/model",
        "--output_dir",   "/opt/ml/processing/output",
        "--out_s3",       out_s3,
        "--top_k",        str(top_k),
        "--cand_k",       str(cand_k),
        "--blend_gamma",  str(blend_gamma),
        "--mmr_lambda",   str(mmr_lambda),
        "--region",       region,
        "--shuffle_partitions", "200",
    ]
    if filter_seen:
        arguments.append("--filter_seen")

    job_name = f"als-postrank-{ts}"
    proc.run(
        submit_app=s3_code_uri,
        inputs=inputs,
        outputs=outputs,
        arguments=arguments,
        logs=True,
        spark_event_logs_s3_uri=spark_event_logs,
        job_name=job_name,
    )

    print(f"[POST] Done.\n  Run output:   {out_s3}\n  Metrics file: {out_s3}/metrics.json\n  Coverage file:{out_s3}/coverage.json")

if __name__ == "__main__":
    main()
