"""
SageMaker Processing（Sklearn）task，运行 mice_imputer.py，只对 orders 做 MICE。

Usage:
python -m functions.modules.ml.mice_processing --config configs/recsys.yaml
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
    """
    s3://bucket/prefix  -> ("bucket", "prefix")
    """
    p = urlparse(uri)
    if p.scheme != "s3":
        raise ValueError(f"Not an S3 URI: {uri}")
    return p.netloc, p.path.lstrip("/")


def _s3_key_exists(s3_cli, bucket: str, key: str) -> bool:
    resp = s3_cli.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return "Contents" in resp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(Path("configs/recsys.yaml")))
    args = parser.parse_args()

    cfg = load_cfg(args.config)
    region = cfg["aws"]["region"]
    role_arn = cfg["aws"]["role_arn"]
    input_s3 = cfg["s3"]["raw_after_clean"]
    base_output = cfg["s3"]["base_output"]
    mice_out_root = cfg["s3"]["mice_orders_out"].rstrip("/") + "/"
    proc = cfg["processing"]

    ts = time.strftime("%Y%m%d-%H%M%S")
    # 本次 run 的输出目录（只放 orders 的 MICE 产物）
    out_s3 = f"{mice_out_root}run-{ts}/"

    boto_ses = boto3.Session(region_name=region)
    s3_cli = boto_ses.client("s3")
    s3_res = boto_ses.resource("s3")

    sm_ses = sagemaker.session.Session(
        boto_session=boto_ses, default_bucket=cfg["aws"]["default_bucket"]
    )

    # SDK 2.250.0 支持的 sklearn 版本里选 1.2-1（我们已验证可用）
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

    # repo_root = Path(__file__).resolve().parents[4]  # 仓库根
    entry = Path(__file__).resolve().parent / "mice_imputer.py"

    print(f"[PROC] input:  {input_s3}")
    print(f"[PROC] output: {out_s3}")

    # 运行处理作业
    processor.run(
        code=str(entry),  # 不用 source_dir，直接脚本路径；SDK 会自动把代码打包上传到 S3
        inputs=[
            ProcessingInput(
                source=input_s3,
                destination="/opt/ml/processing/input",
                input_name="raw",
            )
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",  # mice_imputer.py 会把 orders.parquet 写到这里
                destination=out_s3,
                output_name="out",
            )
        ],
        arguments=[
            "--input_dir",
            "/opt/ml/processing/input",
            "--output_dir",
            "/opt/ml/processing/output",
            "--chains",
            "7",
            "--iters",
            "10",
            # "--write_all_chains"
        ],
        wait=True,
        logs=True,
        job_name=f"mice-{ts}",
    )

    # —— 将本次 run 的 orders.parquet 同步一份到 latest/ 方便训练稳定引用 ——
    out_bucket, out_prefix = _parse_s3_uri(out_s3)
    # 一般我们写的是 output_dir/orders.parquet -> S3 中就是 <out_prefix>/orders.parquet
    candidate_key = f"{out_prefix.rstrip('/')}/orders.parquet"
    if not _s3_key_exists(s3_cli, out_bucket, candidate_key):
        # 如果用户改了文件名，这里做个兜底：找出第一个 .parquet 复制
        resp = s3_cli.list_objects_v2(Bucket=out_bucket, Prefix=out_prefix)
        found = None
        for obj in resp.get("Contents", []):
            if obj["Key"].lower().endswith(".parquet"):
                found = obj["Key"]
                break
        if not found:
            raise RuntimeError(
                f"[PROC] No parquet found under s3://{out_bucket}/{out_prefix}"
            )
        candidate_key = found

    # 目标 latest 路径
    latest_bucket, latest_prefix = _parse_s3_uri(mice_out_root)
    latest_key = f"{latest_prefix.rstrip('/')}/latest/orders.parquet"

    print(
        f"[PROC] sync latest:\n"
        f"  src: s3://{out_bucket}/{candidate_key}\n"
        f"  dst: s3://{latest_bucket}/{latest_key}"
    )
    s3_res.meta.client.copy(
        {"Bucket": out_bucket, "Key": candidate_key}, latest_bucket, latest_key
    )

    print(f"[PROC] Done.\n  Run output:   s3://{out_bucket}/{out_prefix}\n  Latest file:  s3://{latest_bucket}/{latest_key}")


if __name__ == "__main__":
    main()
