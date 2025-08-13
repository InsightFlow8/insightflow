"""
SageMaker Processing（Sklearn）task，运行 mice_imputer.py。
- 输入：after-clean/orders/（分区 parquet）
- 输出：cfg.s3.mice_orders_out（建议设为 s3://.../after-clean/after-MICE）
- 同步 latest/ 时优先级：orders_imputed.parquet > orders.parquet > user_imp_imputed_mean.parquet
"""

import time
from pathlib import Path
import argparse
from urllib.parse import urlparse

import yaml
import boto3
import sagemaker
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput


def load_cfg(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def _parse_s3_uri(uri: str):
    p = urlparse(uri)
    if p.scheme != "s3":
        raise ValueError(f"Not an S3 URI: {uri}")
    return p.netloc, p.path.lstrip("/")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default=str(Path("configs/recsys.yaml")))
    args = ap.parse_args()

    cfg = load_cfg(args.config)
    region = cfg["aws"]["region"]
    role_arn = cfg["aws"]["role_arn"]
    input_s3 = cfg["s3"]["raw_after_clean"]          # e.g. s3://.../after-clean/orders/
    out_root = cfg["s3"]["mice_orders_out"].rstrip("/") + "/"   # e.g. s3://.../after-clean/after-MICE/
    proc = cfg["processing"]

    ts = time.strftime("%Y%m%d-%H%M%S")
    out_s3 = f"{out_root}run-{ts}/"

    boto_ses = boto3.Session(region_name=region)
    s3_cli = boto_ses.client("s3")
    s3_res = boto_ses.resource("s3")
    sm_ses = sagemaker.session.Session(boto_session=boto_ses, default_bucket=cfg["aws"]["default_bucket"])

    processor = SKLearnProcessor(
        framework_version="1.2-1",
        role=role_arn,
        instance_type=proc["instance_type"],
        instance_count=proc["instance_count"],
        volume_size_in_gb=proc["volume_gb"],
        max_runtime_in_seconds=proc["max_runtime_sec"],
        sagemaker_session=sm_ses,
        base_job_name="recsys-mice",
    )

    entry = Path(__file__).resolve().parent / "mice_imputer.py"

    print(f"[PROC] input : {input_s3}")
    print(f"[PROC] output: {out_s3}")

    processor.run(
        code=str(entry),
        inputs=[ProcessingInput(source=input_s3, destination="/opt/ml/processing/input", input_name="raw")],
        outputs=[ProcessingOutput(source="/opt/ml/processing/output", destination=out_s3, output_name="out")],
        arguments=[
            "--input_dir", "/opt/ml/processing/input",
            "--output_dir", "/opt/ml/processing/output",
            "--chains", "7",
            "--iters", "10",
            # "--write_all_chains",
        ],
        wait=True,
        logs=True,
        job_name=f"mice-{ts}",
    )

    # —— 同步 latest/ （优先级：明细 > 汇总）——
    out_bucket, out_prefix = _parse_s3_uri(out_s3)

    resp = s3_cli.list_objects_v2(Bucket=out_bucket, Prefix=out_prefix)
    keys = [obj["Key"] for obj in resp.get("Contents", [])]

    priority = ["orders_imputed.parquet", "orders.parquet", "user_imp_imputed_mean.parquet"]
    chosen = None
    for name in priority:
        for k in keys:
            if k.endswith("/" + name) or k.endswith(name):
                chosen = k
                break
        if chosen:
            break
    if not chosen:
        raise RuntimeError(f"[PROC] No parquet found under s3://{out_bucket}/{out_prefix}")

    latest_bucket, latest_prefix = _parse_s3_uri(out_root)
    latest_key = f"{latest_prefix.rstrip('/')}/latest/orders.parquet"

    print("[PROC] sync latest:\n"
          f"  src: s3://{out_bucket}/{chosen}\n"
          f"  dst: s3://{latest_bucket}/{latest_key}")
    s3_res.meta.client.copy({"Bucket": out_bucket, "Key": chosen}, latest_bucket, latest_key)

    print(f"[PROC] Done.\n  Run output : s3://{out_bucket}/{out_prefix}\n  Latest file: s3://{latest_bucket}/{latest_key}")


if __name__ == "__main__":
    main()
