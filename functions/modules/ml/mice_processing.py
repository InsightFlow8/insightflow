"""
SageMaker Processing（Sklearn）task，run mice_imputer.py

Usage:
python -m functions.modules.ml.mice_processing --config configs/recsys.yaml
"""

import time
from pathlib import Path
import argparse
import yaml
import boto3
import sagemaker
from sagemaker.sklearn.processing import SKLearnProcessor
from sagemaker.processing import ProcessingInput, ProcessingOutput


def load_cfg(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=str(Path("configs/recsys.yaml")))
    args = parser.parse_args()

    cfg = load_cfg(args.config)
    region = cfg["aws"]["region"]
    role_arn = cfg["aws"]["role_arn"]
    input_s3 = cfg["s3"]["raw_after_clean"]
    base_output = cfg["s3"]["base_output"]
    proc = cfg["processing"]

    ts = time.strftime("%Y%m%d-%H%M%S")
    out_s3 = f"{base_output}/processing/mice-{ts}"

    boto_ses = boto3.Session(region_name=region)
    sm_ses = sagemaker.session.Session(boto_session=boto_ses, default_bucket=cfg["aws"]["default_bucket"])

    # 选择较新的 sklearn 版本（你的账户里该镜像可用）
    processor = SKLearnProcessor(
        framework_version="1.2-2",
        role=role_arn,
        instance_type=proc["instance_type"],
        instance_count=proc["instance_count"],
        volume_size_in_gb=proc["volume_gb"],
        max_runtime_in_seconds=proc["max_runtime_sec"],
        sagemaker_session=sm_ses,
        base_job_name="recsys-mice",
    )

    repo_root = Path(__file__).resolve().parents[3]  # 项目根目录
    entry = "functions/modules/ml/mice_imputer.py"

    print(f"[PROC] input:  {input_s3}")
    print(f"[PROC] output: {out_s3}")

    processor.run(
        code=entry,
        source_dir=str(repo_root),
        inputs=[
            ProcessingInput(
                source=input_s3,
                destination="/opt/ml/processing/input",
                input_name="input",
            )
        ],
        outputs=[
            ProcessingOutput(
                destination=out_s3,
                source="/opt/ml/processing/output",
                output_name="output",
            )
        ],
        arguments=[
            "--input_dir", "/opt/ml/processing/input",
            "--output_dir", "/opt/ml/processing/output",
        ],
        wait=True,
    )

    print(f"[PROC] Done. Output S3: {out_s3}")


if __name__ == "__main__":
    main()