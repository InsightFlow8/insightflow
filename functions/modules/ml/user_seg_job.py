#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, io, json, traceback as tb, datetime as dt, importlib
import boto3, yaml, sagemaker
from urllib.parse import urlparse
from sagemaker.processing import ProcessingInput, ProcessingOutput

def get_spark_processor_cls():
    try:
        mod = importlib.import_module("sagemaker.spark.processing")
        if hasattr(mod, "SparkProcessor"):
            return getattr(mod, "SparkProcessor")
        if hasattr(mod, "PySparkProcessor"):
            return getattr(mod, "PySparkProcessor")
        raise ImportError("Neither SparkProcessor nor PySparkProcessor is available.")
    except Exception as e:
        raise ImportError("Unable to import SparkProcessor/PySparkProcessor from sagemaker.spark.processing.") from e

SparkProcessor = get_spark_processor_cls()

def now_stamp():
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def cfg_get(cfg, *ks, default=None):
    cur = cfg
    for k in ks:
        if cur is None or not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return default if cur is None else cur

def s3_join(bucket: str, *parts: str) -> str:
    key = "/".join(p.strip("/") for p in parts if p)
    return f"s3://{bucket}/{key}"

def s3_split(s3_uri: str):
    p = urlparse(s3_uri)
    return p.netloc, p.path.lstrip("/")

def put_debug_error(s3_uri: str, err_text: str):
    s3 = boto3.client("s3")
    b, k = s3_split(s3_uri)
    s3.put_object(Bucket=b, Key=k, Body=err_text.encode("utf-8"))

def s3_rm_prefix(s3_uri: str):
    s3 = boto3.client("s3")
    b, k = s3_split(s3_uri)
    paginator = s3.get_paginator("list_objects_v2")
    batch = []
    for page in paginator.paginate(Bucket=b, Prefix=k):
        for obj in page.get("Contents", []):
            batch.append({"Key": obj["Key"]})
            if len(batch) >= 1000:
                s3.delete_objects(Bucket=b, Delete={"Objects": batch})
                batch = []
    if batch:
        s3.delete_objects(Bucket=b, Delete={"Objects": batch})

def s3_copy_prefix(src_uri: str, dst_uri: str):
    s3 = boto3.client("s3")
    sb, sk = s3_split(src_uri)
    db, dk = s3_split(dst_uri)
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=sb, Prefix=sk):
        for obj in page.get("Contents", []):
            rel = obj["Key"][len(sk):].lstrip("/")
            s3.copy_object(
                Bucket=db,
                Key=f"{dk.rstrip('/')}/{rel}",
                CopySource={"Bucket": sb, "Key": obj["Key"]},
            )

def main():
    import argparse
    ap = argparse.ArgumentParser(description="User RFM KMeans (Spark) wrapper")
    ap.add_argument("--config", required=True)
    ap.add_argument("--orders_s3", required=False, default=None)
    ap.add_argument("--skip_seg_pop", action="store_true")
    args = ap.parse_args()

    cfg = load_yaml(args.config)
    s3cfg = cfg.get("s3", {}) or {}

    curated_bucket = cfg_get(s3cfg, "curated_bucket")
    scripts_bucket = cfg_get(s3cfg, "scripts_bucket") or cfg_get(cfg, "aws", "default_bucket")
    scripts_prefix = (cfg_get(s3cfg, "scripts_prefix", default="ML") or "").strip("/")

    if not curated_bucket or not scripts_bucket:
        raise RuntimeError("配置缺少 s3.curated_bucket 或 s3.scripts_bucket。")

    orders_s3 = args.orders_s3 or cfg_get(s3cfg, "mice_orders_latest")
    op_prior  = cfg_get(s3cfg, "order_products_prior")
    op_train  = cfg_get(s3cfg, "order_products_train")
    if not orders_s3:
        raise RuntimeError("未找到 orders 源，请通过 --orders_s3 或 s3.mice_orders_latest 指定")

    k     = int(cfg_get(cfg, "seg", "k",     default=10))
    r_cap = int(cfg_get(cfg, "seg", "r_cap", default=60))
    top_n = int(cfg_get(cfg, "seg", "top_n", default=30))
    beta  = float(cfg_get(cfg, "seg", "beta", default=500.0))

    inst_type = cfg_get(cfg, "processing", "instance_type", default="ml.t3.xlarge")
    inst_cnt  = int(cfg_get(cfg, "processing", "instance_count", default=1))
    vol_gb    = int(cfg_get(cfg, "processing", "volume_gb", default=100))
    max_sec   = int(cfg_get(cfg, "processing", "max_runtime_sec", default=1800))
    shuffle_p = int(cfg_get(cfg, "processing", "shuffle_partitions", default=200))

    role = cfg_get(cfg, "aws", "role_arn")
    if not role:
        raise RuntimeError("请在 config.yaml 的 aws.role_arn 中配置 SageMaker 执行角色。")

    stamp = now_stamp()

    # 上载 Spark 代码
    code_key = f"{scripts_prefix}/user-seg/{stamp}/user_seg_rfm_kmeans.py" if scripts_prefix else f"user-seg/{stamp}/user_seg_rfm_kmeans.py"
    code_uri = s3_join(scripts_bucket, code_key)
    local_code = os.path.join(os.path.dirname(__file__), "user_seg_rfm_kmeans.py")
    s3 = boto3.client("s3")
    with open(local_code, "rb") as f:
        s3.put_object(Bucket=scripts_bucket, Key=code_key, Body=f.read())
    print(f"[SEG] submit_app: {code_uri}")

    # run / latest 目标
    run_user    = s3_join(curated_bucket, "recsys", "user_seg", f"run-{stamp}", "user_seg.parquet")
    run_pop     = s3_join(curated_bucket, "recsys", "segment_popularity", f"run-{stamp}", "segment_popularity.parquet")
    latest_user = s3_join(curated_bucket, "recsys", "user_seg", "latest", "user_seg.parquet")
    latest_pop  = s3_join(curated_bucket, "recsys", "segment_popularity", "latest", "segment_popularity.parquet")
    diag_s3     = s3_join(curated_bucket, "recsys", "debug", f"user-seg-{stamp}", "diag")

    print(f"[SEG] orders:     {orders_s3}")
    print(f"[SEG] op_prior:   {op_prior}")
    print(f"[SEG] op_train:   {op_train}")
    print(f"[SEG] out_user:   {run_user}")
    print(f"[SEG] out_pop:    {run_pop}")

    proc = SparkProcessor(
        base_job_name=f"user-seg-{stamp}",
        framework_version="3.3",
        role=role,
        instance_type=inst_type,
        instance_count=inst_cnt,
        max_runtime_in_seconds=max_sec,
        volume_size_in_gb=vol_gb,
        sagemaker_session=sagemaker.Session(),
    )

    inputs = [
        ProcessingInput(input_name="orders", source=orders_s3, destination="/opt/ml/processing/input/orders")
    ]
    app_args = [
        "--orders_dir", "/opt/ml/processing/input/orders",
        "--output_dir", "/opt/ml/processing/output",
        "--k", str(k),
        "--r_cap", str(r_cap),
        "--top_n", str(top_n),
        "--beta", str(beta),
        "--shuffle_partitions", str(shuffle_p),
    ]
    if op_prior:
        inputs.append(ProcessingInput(input_name="op_prior", source=op_prior, destination="/opt/ml/processing/input/op_prior"))
        app_args += ["--op_prior_dir", "/opt/ml/processing/input/op_prior"]
    if op_train and not args.skip_seg_pop:
        inputs.append(ProcessingInput(input_name="op_train", source=op_train, destination="/opt/ml/processing/input/op_train"))
        app_args += ["--op_train_dir", "/opt/ml/processing/input/op_train"]

    # 关键改动：把 ProcessingOutput 的 source 指向 /output/artifacts/*，避免叶子目录被预占用
    outputs = [
        ProcessingOutput(output_name="user_seg", source="/opt/ml/processing/output/artifacts/user_seg.parquet", destination=run_user),
        ProcessingOutput(output_name="seg_pop",  source="/opt/ml/processing/output/artifacts/segment_popularity.parquet", destination=run_pop),
        ProcessingOutput(output_name="diag",     source="/opt/ml/processing/output/_diag", destination=diag_s3),
    ]

    event_logs = s3_join(curated_bucket, "recsys", "debug", "spark-events", f"user-seg-{stamp}")

    proc.run(
        submit_app=code_uri,
        arguments=app_args,
        inputs=inputs,
        outputs=outputs,
        spark_event_logs_s3_uri=event_logs,
        wait=True,
        logs=True,
    )

    # 刷新 latest
    s3_rm_prefix(latest_user); s3_copy_prefix(run_user, latest_user)
    if not args.skip_seg_pop:
        s3_rm_prefix(latest_pop);  s3_copy_prefix(run_pop,  latest_pop)

    print("[SEG] Done.\n"
          f"  run user_seg:   {s3_join(curated_bucket,'recsys','user_seg',f'run-{stamp}')}/\n"
          f"  run seg_pop:    {s3_join(curated_bucket,'recsys','segment_popularity',f'run-{stamp}')}/\n"
          f"  user_seg latest:{latest_user}/\n"
          f"  seg_pop latest: {latest_pop}/\n"
          f"  event logs:     {event_logs}/\n"
          f"  diag logs:      {diag_s3}/")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        try:
            cfg_path = None
            for i, a in enumerate(sys.argv):
                if a == "--config" and i + 1 < len(sys.argv):
                    cfg_path = sys.argv[i+1]
            curated_bucket = None
            if cfg_path and os.path.exists(cfg_path):
                cfg = load_yaml(cfg_path)
                curated_bucket = cfg_get(cfg, "s3", "curated_bucket")
            stamp = now_stamp()
            if curated_bucket:
                err_uri = f"s3://{curated_bucket}/recsys/debug/user-seg-{stamp}/_ERROR.txt"
                put_debug_error(err_uri, tb.format_exc())
                print(f"[SEG][ERROR] dumped traceback to {err_uri}")
            else:
                print(tb.format_exc())
        finally:
            raise
