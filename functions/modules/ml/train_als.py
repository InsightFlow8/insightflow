# -*- coding: utf-8 -*-
"""
提交 SageMaker PySparkProcessor 训练 ALS（隐式反馈）
"""

import time
from pathlib import Path
import argparse
import yaml
import boto3
import sagemaker
from sagemaker.processing import ProcessingInput, ProcessingOutput
from sagemaker.spark.processing import PySparkProcessor


def load_cfg(p: str) -> dict:
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(Path("configs/recsys.yaml")))
    args = parser.parse_args()

    cfg = load_cfg(args.config)
    region = cfg["aws"]["region"]
    role_arn = cfg["aws"]["role_arn"]
    base_output = cfg["s3"]["base_output"]
    input_s3 = cfg["s3"]["ratings_latest"]  # 直接用你在 YAML 里配置的 latest ratings
    tr = cfg["training"]
    hp = cfg["training"]["als"]

    ts = time.strftime("%Y%m%d-%H%M%S")
    model_s3 = f"{base_output}/models/als-{ts}"

    boto_ses = boto3.Session(region_name=region)
    sm_ses = sagemaker.session.Session(
        boto_session=boto_ses, default_bucket=cfg["aws"]["default_bucket"]
    )

    processor = PySparkProcessor(
        base_job_name="als-train",
        framework_version="3.3",
        role=role_arn,
        instance_count=tr["instance_count"],
        instance_type=tr["instance_type"],
        max_runtime_in_seconds=tr["max_runtime_sec"],
        volume_size_in_gb=tr["volume_gb"],
        sagemaker_session=sm_ses,
    )

    repo_root = Path(__file__).resolve().parents[3]
    submit_app = str(repo_root / "functions/modules/ml/als_train.py")

    print(f"[TRAIN] input:  {input_s3}")
    print(f"[TRAIN] model:  {model_s3}")

    # 把 S3 的 ratings 拷到容器本地 /opt/ml/processing/input
    inputs = [
        ProcessingInput(
            source=input_s3,
            destination="/opt/ml/processing/input",
            input_name="ratings",
        )
    ]

    outputs = [
        ProcessingOutput(
            source="/opt/ml/processing/model",
            destination=model_s3,
            output_name="model",
        )
    ]

    arguments = [
        "--input_dir", "/opt/ml/processing/input",
        "--model_dir", "/opt/ml/processing/model",
        "--user_col", "user_id",
        "--item_col", "product_id",
        "--rating_col", "rating",
        "--factors", str(hp["factors"]),
        "--reg", str(hp["regularization"]),
        "--alpha", str(hp["alpha"]),
        "--iters", str(hp["iterations"]),
        "--shuffle_partitions", "200",
    ]

    job_name = f"als-train-{ts}"
    processor.run(
        submit_app=submit_app,     # 本地路径，SDK 会自动打包上传
        inputs=inputs,
        outputs=outputs,
        arguments=arguments,
        wait=True,
        logs=True,
        job_name=job_name,
    )

    print(f"[TRAIN] Done. Model S3: {model_s3}")


if __name__ == "__main__":
    main()
