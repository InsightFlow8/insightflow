# functions/modules/ml/batch_inference.py
"""
提交 SageMaker PySparkProcessor 任务，运行 batch_recommend.py
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
    parser.add_argument("--model_s3", required=False, help="若不提供，将从 configs 的 base_output/models 下手动填")
    args = parser.parse_args()

    cfg = load_cfg(args.config)
    region = cfg["aws"]["region"]
    role_arn = cfg["aws"]["role_arn"]

    data_s3 = cfg["s3"]["raw_after_transformation"]
    base_output = cfg["s3"]["base_output"]
    inf = cfg["inference"]

    # 你也可以把上一步训练产出的 model_s3 显式传进来
    model_s3 = args.model_s3 or f"{base_output}/models/als-LAST"  # 如果未提供，请替换为真实路径

    ts = time.strftime("%Y%m%d-%H%M%S")
    pred_s3 = f"{base_output}/predictions/als-{ts}"

    boto_ses = boto3.Session(region_name=region)
    sm_ses = sagemaker.session.Session(boto_session=boto_ses, default_bucket=cfg["aws"]["default_bucket"])

    processor = PySparkProcessor(
        base_job_name="recsys-als-infer",
        framework_version="3.3",
        role=role_arn,
        instance_count=1,
        instance_type=inf["instance_type"],
        max_runtime_in_seconds=3600,
        volume_size_in_gb=30,
        sagemaker_session=sm_ses,
    )

    repo_root = Path(__file__).resolve().parents[3]
    submit_app = "functions/modules/ml/batch_recommend.py"

    print(f"[INFER] model: {model_s3}")
    print(f"[INFER] data:  {data_s3}")
    print(f"[INFER] out:   {pred_s3}")

    processor.run(
        submit_app=submit_app,
        source_dir=str(repo_root),
        arguments=[
            "--input_dir", "/opt/ml/processing/input",
            "--model_dir", "/opt/ml/processing/model",
            "--output_dir", "/opt/ml/processing/output",
            "--user_col", "user_id",
            "--top_k", str(inf["top_k"]),
        ],
        inputs=[
            ProcessingInput(
                source=model_s3,
                destination="/opt/ml/processing/model",
                input_name="model",
            ),
            ProcessingInput(
                source=data_s3,
                destination="/opt/ml/processing/input",
                input_name="data",
            ),
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/output",
                destination=pred_s3,
                output_name="pred",
            )
        ],
        wait=True,
    )

    print(f"[INFER] Done. Predictions S3: {pred_s3}")


if __name__ == "__main__":
    main()